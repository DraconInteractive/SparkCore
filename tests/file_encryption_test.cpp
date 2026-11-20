#include "api.hpp"
#include "files.hpp"
#include "timers.hpp"
#include "notifications.hpp"
#include <fstream>
#include <iostream>
#include <thread>
using namespace spark;using json=nlohmann::json;
void check(bool ok,const char* why){if(!ok)throw std::runtime_error(why);}
json expect(const httplib::Result& r,int status){if(!r||r->status!=status)throw std::runtime_error("Expected HTTP "+std::to_string(status)+", got "+(r?std::to_string(r->status)+" "+r->body:"no response"));return r->body.empty()?json(nullptr):json::parse(r->body);}
int main(){auto config=DatabaseConfig::from_environment();if(config.database.rfind("spark_test_",0)!=0||!getenv("SPARK_FILE_DIR"))return 77;httplib::Server server;std::thread worker;
 try{
  auto store=std::make_shared<FileStore>(config);auto storage=std::make_shared<FileStorage>(store,FileStorage::default_root());storage->initialize();configure_api(server,std::make_shared<DeviceStore>(config),std::make_shared<TimerStore>(config),47684,nullptr,nullptr,nullptr,storage);
  int port=server.bind_to_any_port("127.0.0.1");check(port>0,"bind");worker=std::thread([&]{server.listen_after_bind();});server.wait_until_ready();httplib::Client client("127.0.0.1",port);client.set_read_timeout(30,0);
  const std::string password="Password with unicode 🔐",original="Secret text 🌳\r\n",updated="Changed secret 🌳\r\n";
  auto record=expect(client.Post("/files?name=secret.txt",original,"application/octet-stream"),201);std::string id=record["id"],path="/files/"+id;auto originalPath=storage->content_path(record);
  auto info=client.Get(path+"/encryption");check(expect(info,200)["encrypted"]==false,"plaintext metadata");auto etag=info->get_header_value("ETag");httplib::Headers headers={{"If-Match",etag}};
  expect(client.Post(path+"/encrypt",json{{"password",password}}.dump(),"application/json"),428);
  expect(client.Post(path+"/encrypt",headers,json{{"password","short"}}.dump(),"application/json"),400);
  auto lease=expect(client.Post(path+"/leases",json{{"expires_at","2099-01-01T00:00:00Z"}}.dump(),"application/json"),201);
  auto encrypted=client.Post(path+"/encrypt",headers,json{{"password",password}}.dump(),"application/json");record=expect(encrypted,200);auto cipher=client.Get(path+"/content");check(cipher&&cipher->body!=original&&cipher->body.rfind("SPARKENC",0)==0,"encrypted on disk/download");check(!std::filesystem::exists(originalPath),"old plaintext revision removed");expect(client.Get(lease["download_url"].get<std::string>()),404);
  headers={{"If-Match",encrypted->get_header_value("ETag")}};auto revision=record["content_revision"];
  expect(client.Put(path+"/content",headers,"plaintext overwrite","text/plain"),409);
  expect(client.Post(path+"/encrypt",headers,json{{"password",password}}.dump(),"application/json"),409);
  for(const auto* mode:{"view","edit","full"}){expect(client.Post(path+"/decrypt",headers,json{{"password","wrong"},{"mode",mode}}.dump(),"application/json"),422);check(client.Get(path+"/content")->body==cipher->body,"wrong password unchanged");}
  for(const auto* mode:{"view","edit"}){auto r=client.Post(path+"/decrypt",headers,json{{"password",password},{"mode",mode}}.dump(),"application/json");check(r&&r->status==200&&r->body==original,"temporary plaintext returned");check(r->get_header_value("Cache-Control").find("no-store")!=std::string::npos,"no plaintext caching");check(store->get(id)->at("content_revision")==revision,"temporary unlock leaves revision unchanged");}
  expect(client.Put(path+"/encrypted-content",headers,json{{"password","wrong"},{"text",updated}}.dump(),"application/json"),422);
  auto saved=client.Put(path+"/encrypted-content",headers,json{{"password",password},{"text",updated}}.dump(),"application/json");record=expect(saved,200);check(record["content_revision"]!=revision,"encrypted edit new revision");check(client.Get(path+"/content")->body.find(updated)==std::string::npos,"edit stored ciphertext only");
  expect(client.Post(path+"/decrypt",headers,json{{"password",password},{"mode","full"}}.dump(),"application/json"),412);
  headers={{"If-Match",saved->get_header_value("ETag")}};
  auto reopened=client.Post(path+"/decrypt",headers,json{{"password",password},{"mode","view"}}.dump(),"application/json");check(reopened&&reopened->status==200&&reopened->body==updated,"reopen encrypted edit");
  auto cipherPath=storage->content_path(record);std::string good=client.Get(path+"/content")->body;{std::fstream f(cipherPath,std::ios::in|std::ios::out|std::ios::binary);f.seekp(good.size()-1);char b=good.back()^1;f.write(&b,1);}
  auto damaged=client.Post(path+"/decrypt",headers,json{{"password",password},{"mode","view"}}.dump(),"application/json");expect(damaged,422);check(damaged->body.find(updated)==std::string::npos,"tampered preview exposes no plaintext");expect(client.Post(path+"/decrypt",headers,json{{"password",password},{"mode","full"}}.dump(),"application/json"),422);check(store->get(id)->at("content_revision")==record["content_revision"],"damaged full decrypt does not commit");{std::ofstream f(cipherPath,std::ios::binary|std::ios::trunc);f.write(good.data(),good.size());}
  auto plain=client.Post(path+"/decrypt",headers,json{{"password",password},{"mode","full"}}.dump(),"application/json");expect(plain,200);check(client.Get(path+"/content")->body==updated,"permanent plaintext restored");check(!std::filesystem::exists(cipherPath),"old ciphertext revision removed");
  check(expect(client.Get(path+"/encryption"),200)["encrypted"]==false,"full decrypt state");
  // Real body limit covers escaped JSON edits, beyond the normal JSON request cap.
  std::string big(1048577,'z');auto large=expect(client.Post("/files?name=large.txt",big,"application/octet-stream"),201);std::string lp="/files/"+large["id"].get<std::string>();auto li=client.Get(lp+"/encryption");httplib::Headers lh={{"If-Match",li->get_header_value("ETag")}};auto le=client.Post(lp+"/encrypt",lh,json{{"password",password}}.dump(),"application/json");expect(le,200);lh={{"If-Match",le->get_header_value("ETag")}};expect(client.Put(lp+"/encrypted-content",lh,json{{"password",password},{"text",big+"new"}}.dump(),"application/json"),200);
  // Two files only: failed decryptions leave no orphan plaintext revisions.
  for(const auto& entry:std::filesystem::directory_iterator(FileStorage::default_root()))if(entry.is_regular_file()&&entry.path().filename().string().front()!='.'){std::ifstream f(entry.path(),std::ios::binary);std::string data((std::istreambuf_iterator<char>(f)),{});check(data==updated||data.rfind("SPARKENC",0)==0,"unexpected plaintext staging file");}
  NotificationStore notifications(config);notifications.sequence_events();int changes=0;for(const auto& e:notifications.after(0))if(e["type"]=="file.updated")changes++;check(changes>=5,"encryption changes notify clients");
  server.stop();worker.join();std::cout<<"File encryption API: password failures, preview isolation, encrypted edit, stale revisions, full decrypt, tampering, lease invalidation and notifications passed.\n";return 0;
 }catch(const std::exception& e){server.stop();if(worker.joinable())worker.join();std::cerr<<e.what()<<'\n';return 1;}}
