#include "assistant.hpp"
#include <fstream>
#include <sys/stat.h>
#include <unistd.h>
#include <iostream>
#include <thread>
using nlohmann::json;
void check(bool b){if(!b)throw std::runtime_error("test failed");}
int main(){
 char dir[]="/tmp/spark-assistant-XXXXXX";
 check(mkdtemp(dir));
 std::filesystem::path root(dir);
 auto credential=root/"key.json";
 {std::ofstream f(credential);f<<R"({"api_key":"test-key"})";}
 chmod(credential.c_str(),0600);
 spark::AssistantConfig config{credential,root};
 spark::Identity who{"credential","phone","device",{"assistant.use"}};
 spark::Identity other{"other","watch","device",{"assistant.use"}};
 int calls=0;
 spark::Assistant::Transport transport=[&](const json& request){
   ++calls;
   check(request["store"]==false);
   check(request["model"]=="gpt-6-luna");
   check(!request.contains("tools"));
   return json::parse(R"({"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"{\"status\":\"completed\",\"message\":\"Hello\"}"}]}]})");
 };
 spark::Assistant a(config,transport);
 json p={{"request_id","one"},{"text","Hi"}};
 check(a.query(who,p)["message"]=="Hello");
 check(a.query(who,p)["status"]=="completed"&&calls==1);
 check(a.get(who,"one")["undo_available"]==false);
 bool rejected=false;
 try{a.get(other,"one");}catch(...){rejected=true;}
 check(rejected);
 p["text"]="changed";rejected=false;
 try{a.query(who,p);}catch(...){rejected=true;}
 check(rejected);
 for(const auto& entry:std::filesystem::directory_iterator(root)){
   if(entry.path().extension()!=".json"||entry.path()==credential)continue;
   auto record=spark::read_json_object(entry.path(),"test",true);
   if(record["request_id"]=="one"){
     record["status"]="processing";
     std::ofstream f(entry.path());f<<record.dump();
   }
 }
 p["text"]="Hi";
 check(a.query(who,p)["error"]=="interrupted"&&calls==1);
 spark::Assistant restarted(config,transport);
 check(restarted.get(who,"one")["status"]=="failed");
 spark::Assistant bad(config,[](const json&){
   return json::parse(R"({"status":"completed","output":[{"type":"function_call"}]})");
 });
 p["request_id"]="two";
 check(bad.query(who,p)["error"]=="unexpected_provider_output");
 who.permissions.clear();rejected=false;
 try{a.query(who,p);}catch(...){rejected=true;}
 check(rejected);
 spark::Identity open{"open","local","open",{}};
 rejected=false;try{a.query(open,p);}catch(...){rejected=true;}check(rejected);
 config.allow_open_access=true;
 spark::Assistant local(config,transport);
 p["request_id"]="local";
 check(local.query(open,p)["status"]=="completed");
 check(local.get(open,"local")["status"]=="completed");
 rejected=false;try{local.query(who,p);}catch(...){rejected=true;}check(rejected);
 auto page_assistant=std::make_shared<spark::Assistant>(config,transport);httplib::Server server;spark::configure_assistant(server,page_assistant,nullptr);
 int port=server.bind_to_any_port("127.0.0.1");check(port>0);std::thread worker([&]{server.listen_after_bind();});server.wait_until_ready();httplib::Client client("127.0.0.1",port);
 auto page=client.Get("/assistant/explorer");auto invalid=client.Get("/assistant/explorer?unexpected=yes");server.stop();worker.join();
 check(page&&page->status==200&&page->get_header_value("Content-Type").find("text/html")==0&&page->get_header_value("Cache-Control")=="no-store");
 check(page->body.find("/auth/client.js")!=std::string::npos&&page->body.find("confirmation_id")!=std::string::npos&&page->body.find("innerHTML")==std::string::npos);
 check(invalid&&invalid->status==400);
 chmod(credential.c_str(),0644);rejected=false;
 try{spark::validate_assistant_config(config);}catch(...){rejected=true;}
 check(rejected);
 std::filesystem::remove_all(root);
 std::cout<<"Assistant validation, isolation, retry, restart and provider failure tests passed\n";
}
