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

 who.permissions.insert("assistant.use");
 auto rejected_with=[&](auto run,const std::string& error){try{run();}catch(const std::exception& e){return e.what()==error;}return false;};
 check(rejected_with([&]{a.query(who,{{"request_id","no-local"},{"text","secret"},{"secure",true}});},"local_assistant_not_configured"));
 check(rejected_with([&]{a.query(who,{{"request_id","bad-flag"},{"text","secret"},{"secure","true"}});},"invalid_secure_flag"));
 config.local=spark::LocalAssistantConfig{"http://127.0.0.1:12345/v1/chat/completions","local/model:small"};
 int local_calls=0,external_calls=0;bool fail_local=false;
 auto private_transport=[&](const json& request){++local_calls;check(request["model"]=="local/model:small");if(fail_local)throw std::runtime_error("offline");return json::parse(R"({"status":"completed","output":[{"type":"message","role":"assistant","content":[{"type":"output_text","text":"{\"status\":\"completed\",\"message\":\"Private reply\"}"}]}]})");};
 auto external_transport=[&](const json& request){++external_calls;return transport(request);};
 spark::Assistant private_assistant(config,external_transport,nullptr,nullptr,private_transport);
 auto private_packet=json{{"request_id","private"},{"text","Secret"},{"secure",true}};
 auto private_reply=private_assistant.query(who,private_packet);check(private_reply["secure"]==true&&private_reply["processing_backend"]=="local"&&local_calls==1&&external_calls==0);
 private_assistant.query(who,private_packet);check(local_calls==1);
 check(private_assistant.query(who,{{"request_id","private-next"},{"conversation_id","private"},{"text","More"}})["secure"]==true);
 check(rejected_with([&]{private_assistant.query(who,{{"request_id","downgrade"},{"conversation_id","private"},{"text","More"},{"secure",false}});},"conversation_privacy_conflict"));
 check(rejected_with([&]{private_assistant.query(who,{{"request_id","private"},{"text","Secret"},{"secure",false}});},"idempotency_conflict"));
 spark::Assistant private_restarted(config,external_transport,nullptr,nullptr,private_transport);
 check(private_restarted.query(who,{{"request_id","private-restarted"},{"conversation_id","private"},{"text","More"}})["secure"]==true);
 fail_local=true;check(private_assistant.query(who,{{"request_id","private-offline"},{"text","Secret"},{"secure",true}})["status"]=="failed"&&external_calls==0);
 private_assistant.query(who,{{"request_id","ordinary"},{"text","Hello"}});check(external_calls==1);
 auto private_config=config;private_config.credentials_file.clear();private_config.require_secure=true;
 spark::Assistant private_only(private_config,external_transport,nullptr,nullptr,private_transport);
 check(rejected_with([&]{private_only.query(who,{{"request_id","require-private"},{"text","Hello"},{"secure",false}});},"secure_assistant_required"));
 fail_local=false;check(private_only.query(who,{{"request_id","default-private"},{"text","Hello"}})["secure"]==true);
 for(const auto& endpoint:{"https://api.openai.com/v1/chat/completions","http://8.8.8.8/v1/chat/completions","http://127.0.0.1.evil/v1/chat/completions","http://user@127.0.0.1/v1/chat/completions","http://127.0.0.1:0/v1/chat/completions","http://127.0.0.1/v1/chat/completions?url=evil"}){
  auto invalid_config=config;invalid_config.local->endpoint=endpoint;bool denied=false;try{spark::validate_assistant_config(invalid_config);}catch(...){denied=true;}check(denied);
 }
 // Exercise the real curl adapter against a local mock, including structured JSON and redirects.
 httplib::Server model_server;int model_calls=0,prime_calls=0;bool redirect=false,unexpected_tool=false,truncated=false;
 model_server.Post("/v1/chat/completions",[&](const httplib::Request& req,httplib::Response& res){
  ++model_calls;if(redirect){res.set_redirect("http://127.0.0.1:1/v1/chat/completions");return;}
  auto body=json::parse(req.body);if(body.value("max_tokens",0)==1){++prime_calls;check(body["messages"]==json::array({json{{"role","user"},{"content","Reply OK."}}})&&!body.contains("response_format"));res.set_content(R"({"choices":[{"finish_reason":"length","message":{"role":"assistant","content":""}}]})","application/json");return;}check(body["model"]=="local/model:small"&&body["messages"][0]["role"]=="system"&&body["response_format"]["json_schema"]["strict"]==true&&!body.contains("reasoning"));
  check(!req.has_header("Authorization"));
  check(body["tool_choice"]=="none"&&body["temperature"]==0&&body["messages"][0]["content"].get<std::string>().find("OUTPUT CONTRACT:")!=std::string::npos&&body["messages"][0]["content"].get<std::string>().find(body["response_format"]["json_schema"]["schema"].dump())!=std::string::npos);
  if(unexpected_tool){res.set_content(R"({"choices":[{"finish_reason":"tool_calls","message":{"role":"assistant","content":"","tool_calls":[{"type":"function","function":{"name":"timer.create_timer","arguments":"{\"duration_seconds\":600}"}}]}}]})","application/json");return;}
  if(truncated){res.set_content(R"({"choices":[{"finish_reason":"length","message":{"role":"assistant","content":"{"}}]})","application/json");return;}
  res.set_content(R"({"choices":[{"finish_reason":"stop","message":{"role":"assistant","content":"{\"status\":\"completed\",\"message\":\"Mock local reply\"}"}}]})","application/json");
 });
 model_server.Post("/v1/responses",[&](const httplib::Request& req,httplib::Response& res){
  ++model_calls;auto body=json::parse(req.body);check(body["model"]=="local/model:small"&&!body.contains("reasoning")&&(body.value("max_output_tokens",0)==16||body["text"]["format"]["strict"]==true)&&body["store"]==false);
  check(req.get_header_value("Authorization")=="Bearer local-only-key");
  if(body.value("max_output_tokens",0)==16){++prime_calls;check(body["input"]=="Reply OK."&&!body.contains("text"));res.set_content(R"({"status":"incomplete"})","application/json");return;}res.set_content(private_transport(body).dump(),"application/json");
 });
 int model_port=model_server.bind_to_any_port("127.0.0.1");check(model_port>0);std::thread model_worker([&]{model_server.listen_after_bind();});model_server.wait_until_ready();
 config.local->endpoint="http://127.0.0.1:"+std::to_string(model_port)+"/v1/chat/completions";
 spark::Assistant real_local(config,external_transport);
 auto real_reply=real_local.query(who,{{"request_id","real-local"},{"text","Hello"},{"secure",true}});
 unexpected_tool=true;check(real_local.query(who,{{"request_id","unexpected-tool"},{"text","Timer"},{"secure",true}})["error"]=="unexpected_provider_tool_call");unexpected_tool=false;
 truncated=true;check(real_local.query(who,{{"request_id","truncated-local"},{"text","Timer"},{"secure",true}})["error"]=="incomplete_provider_response");truncated=false;
 redirect=true;auto redirect_reply=real_local.query(who,{{"request_id","redirect-local"},{"text","Hello"},{"secure",true}});
 auto local_key=root/"local-key.json";{std::ofstream f(local_key);f<<R"({"api_key":"local-only-key"})";}chmod(local_key.c_str(),0600);
 auto responses_config=config;responses_config.local->protocol="responses";responses_config.local->endpoint="http://127.0.0.1:"+std::to_string(model_port)+"/v1/responses";responses_config.local->credentials_file=local_key;
 spark::Assistant responses_local(responses_config,external_transport);
 auto responses_reply=responses_local.query(who,{{"request_id","responses-local"},{"text","Hello"},{"secure",true}});
 check(rejected_with([&]{real_local.prime(who);},"invalid_provider_response"));
 redirect=false;
 auto count_records=[&]{size_t n=0;for(const auto& e:std::filesystem::directory_iterator(root))if(e.path().extension()==".json")++n;return n;};auto before_prime=count_records();
 check(real_local.prime(who)["status"]=="ready"&&responses_local.prime(who)["status"]=="ready");check(count_records()==before_prime);
 auto denied_identity=who;denied_identity.permissions.clear();check(rejected_with([&]{real_local.prime(denied_identity);},"forbidden"));check(rejected_with([&]{a.prime(who);},"local_assistant_not_configured"));
 auto prime_assistant=std::make_shared<spark::Assistant>(config,external_transport);httplib::Server prime_server;spark::configure_assistant(prime_server,prime_assistant,nullptr);
 int prime_port=prime_server.bind_to_any_port("127.0.0.1");check(prime_port>0);std::thread prime_worker([&]{prime_server.listen_after_bind();});prime_server.wait_until_ready();httplib::Client prime_client("127.0.0.1",prime_port);
 auto prime_ok=prime_client.Post("/assistant/prime","{}","application/json");auto prime_bad=prime_client.Post("/assistant/prime",R"({"text":"secret"})","application/json");auto prime_params=prime_client.Post("/assistant/prime?x=1","{}","application/json");
 prime_server.stop();prime_worker.join();check(prime_ok&&prime_ok->status==200&&json::parse(prime_ok->body)["secure"]==true&&prime_bad&&prime_bad->status==400&&prime_params&&prime_params->status==400&&prime_calls==3&&count_records()==before_prime);
 model_server.stop();model_worker.join();
 check(real_reply["message"]=="Mock local reply"&&redirect_reply["status"]=="failed"&&model_calls==9&&external_calls==1&&responses_reply["message"]=="Private reply");
 chmod(credential.c_str(),0644);rejected=false;
 try{spark::validate_assistant_config(config);}catch(...){rejected=true;}
 check(rejected);
 std::filesystem::remove_all(root);
 std::cout<<"Assistant validation, isolation, retry, restart and provider failure tests passed\n";
}
