#include "api.hpp"
#include "notifications.hpp"
#include "timers.hpp"
#include <mysql.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <poll.h>
#include <unistd.h>
#include <thread>
#include <iostream>
#include <set>
using nlohmann::json;
void check(bool value,const char* message){if(!value)throw std::runtime_error(message);}
struct SQL {
    MYSQL* db;
    explicit SQL(const spark::DatabaseConfig& c):db(mysql_init(nullptr)){check(db && mysql_real_connect(db,c.host.c_str(),c.user.c_str(),c.password.c_str(),c.database.c_str(),c.port,c.socket.empty()?nullptr:c.socket.c_str(),0),"SQL connect");}
    ~SQL(){mysql_close(db);}
    void run(const std::string& s){if(mysql_query(db,s.c_str()))throw std::runtime_error(mysql_error(db));}
    unsigned long long count(const std::string& s){run(s);auto result=mysql_store_result(db);check(result,"SQL result");auto row=mysql_fetch_row(result);auto n=std::stoull(row[0]);mysql_free_result(result);return n;}
};
struct Peer {
    int fd;std::string input;json history=json::array();
    Peer(int port):fd(socket(AF_INET,SOCK_STREAM,0)){sockaddr_in a{};a.sin_family=AF_INET;a.sin_port=htons(port);inet_pton(AF_INET,"127.0.0.1",&a.sin_addr);check(fd>=0&&connect(fd,reinterpret_cast<sockaddr*>(&a),sizeof(a))==0,"TCP connect");}
    ~Peer(){close(fd);}
    void send(const json& j){auto s=j.dump()+"\n";check(::send(fd,s.data(),s.size(),MSG_NOSIGNAL)==static_cast<ssize_t>(s.size()),"TCP send");}
    json until(const std::string& type,const std::string& resource=""){
        for(int i=0;i<2000;++i){
            while(input.find('\n')==std::string::npos){pollfd p{fd,POLLIN,0};check(poll(&p,1,5000)>0,"TCP timeout");char b[8192];auto n=recv(fd,b,sizeof(b),0);check(n>0,"TCP closed");input.append(b,n);}
            auto end=input.find('\n');auto j=json::parse(input.substr(0,end));input.erase(0,end+1);
            if(j["type"]=="ping")send({{"type","pong"}});
            if(j.contains("event_id"))history.push_back(j);
            if(j["type"]==type&&(resource.empty()||j.value("resource",std::string())==resource))return j;
        }throw std::runtime_error("Missing notification");
    }
};
json expect(const httplib::Result& r,int status){if(!r||r->status!=status)throw std::runtime_error("HTTP expected "+std::to_string(status)+" got "+(r?std::to_string(r->status)+r->body:httplib::to_string(r.error())));return status==204?json(nullptr):json::parse(r->body);}
int main(){
    const auto config=spark::DatabaseConfig::from_environment();if(config.database.rfind("spark_test_",0)!=0)return 77;
    try{
        SQL db(config);auto store=std::make_shared<spark::NotificationStore>(config);
        while(db.count("SELECT COUNT(*) FROM events e LEFT JOIN notification_log n ON e.event_id=n.event_id WHERE n.event_id IS NULL"))store->sequence_events();
        spark::NotificationOptions options;options.host="127.0.0.1";options.port=0;
        spark::NotificationServer tcp(store,options);httplib::Server server;spark::configure_api(server,std::make_shared<spark::DeviceStore>(config),std::make_shared<spark::TimerStore>(config),tcp.port());
        auto port=server.bind_to_any_port("127.0.0.1");check(port>0,"HTTP bind");std::thread worker([&]{server.listen_after_bind();});server.wait_until_ready();int result=0;
        try{
            httplib::Client client("127.0.0.1",port);Peer live(tcp.port());live.send({{"type","hello"},{"device_id","coverage-observer"}});auto welcome=live.until("welcome");
            check(welcome["resources"].dump().find("/files/directories")!=std::string::npos && welcome["resources"].dump().find("/devices/coverage-observer")!=std::string::npos,"new resync resources");
            auto count=[&]{return db.count("SELECT COUNT(*) FROM events");};
            auto event_count=[&](const std::string& type,const std::string& resource){return db.count("SELECT COUNT(*) FROM events WHERE type='"+type+"' AND resource='"+resource+"'");};
            auto post=[&](const std::string& path,const json& body){return expect(client.Post(path,body.dump(),"application/json"),201);};
            auto reject=[&](const std::string& type){db.run("CREATE TRIGGER reject_coverage BEFORE INSERT ON events FOR EACH ROW BEGIN IF NEW.type='"+type+"' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='notification failure'; END IF; END");};
            auto restore=[&]{db.run("DROP TRIGGER reject_coverage");};
            const std::string device="/devices/coverage-device";
            post(device,{{"local_ip","192.168.4.99"}});live.until("device.created",device);
            expect(client.Patch(device,R"({"local_ip":"192.168.4.98"})","application/json"),200);live.until("device.updated",device);
            auto before=count();expect(client.Patch(device,R"({"local_ip":"192.168.4.98"})","application/json"),200);check(count()==before,"device no-op notification suppression");
            reject("device.updated");expect(client.Patch(device,R"({"local_ip":"192.168.4.97"})","application/json"),503);restore();check(expect(client.Get(device),200)["local_ip"]=="192.168.4.98"&&count()==before,"device event atomicity");
            expect(client.Post("/pulse/subscriptions/coverage-device"),200);check(live.until("device.subscription_updated",device)["subscribed"]==true,"subscription payload broadcast to non-subscriber");
            before=count();expect(client.Post("/pulse/subscriptions/coverage-device"),200);check(count()==before,"subscription no-op");
            reject("device.subscription_updated");expect(client.Delete("/pulse/subscriptions/coverage-device"),503);restore();check(expect(client.Get(device),200)["subscribed"]==true,"subscription event rollback");
            expect(client.Delete("/pulse/subscriptions/coverage-device"),200);check(live.until("device.subscription_updated",device)["subscribed"]==false,"unsubscribe delivered despite filtering");
            expect(client.Post("/files/directories",R"({"directory":"/coverage"})","application/json"),200);check(live.until("files.directories_updated")["drive_id"]==expect(client.Get("/files/directories"),200)["drive_id"],"directory notification identifies drive");
            before=count();expect(client.Post("/files/directories",R"({"directory":"/coverage"})","application/json"),200);check(count()==before,"mkdir no-op");
            reject("files.directories_updated");expect(client.Post("/files/directories",R"({"directory":"/coverage-failed"})","application/json"),503);restore();check(expect(client.Get("/files/directories"),200).dump().find("coverage-failed")==std::string::npos,"directory event rollback");
            auto file=expect(client.Post("/files?name=coverage.txt&directory=%2Fcoverage","initial","application/octet-stream"),201);const auto fp="/files/"+file["id"].get<std::string>();const auto content=fp+"/content";
            live.until("file.created",fp);check(client.Get(content)->body=="initial","created event means readable bytes");
            auto tag=client.Get(content)->get_header_value("ETag");expect(client.Put(content,{{"If-Match",tag}},"edited","text/plain"),200);live.until("file.updated",fp);
            before=count();reject("file.updated");expect(client.Put(content,{{"If-Match",client.Get(content)->get_header_value("ETag")}},"failed","text/plain"),503);restore();check(count()==before && client.Get(content)->body=="edited","edit event failure preserves bytes and metadata");
            expect(client.Patch(fp,R"({"directory":"/coverage/moved"})","application/json"),200);live.until("file.updated",fp);
            before=count();expect(client.Patch(fp,R"({"directory":"/coverage/moved"})","application/json"),200);check(count()==before,"move no-op");
            reject("file.created");expect(client.Post("/files?name=failed-coverage.txt","x","application/octet-stream"),503);restore();check(expect(client.Get("/files"),200).dump().find("failed-coverage.txt")==std::string::npos,"failed publication stays hidden");
            reject("file.deleted");expect(client.Delete(fp),503);restore();check(client.Get(content)->body=="edited","delete event rollback preserves file");expect(client.Delete(fp),204);live.until("file.deleted",fp);
            auto project=post("/projects",{{"name","Coverage root"},{"description",""}});const auto pp="/projects/"+project["id"].get<std::string>();live.until("project.created",pp);
            expect(client.Patch(pp,R"({"description":"updated"})","application/json"),200);live.until("project.updated",pp);
            before=count();expect(client.Patch(pp,R"({"description":"updated"})","application/json"),200);check(count()==before,"project no-op");
            reject("project.updated");expect(client.Patch(pp,R"({"description":"rollback"})","application/json"),503);restore();check(expect(client.Get(pp),200)["description"]=="updated","project edit rollback");
            auto child=post("/projects",{{"name","Child"},{"description",""},{"parent_id",project["id"]}});auto cp="/projects/"+child["id"].get<std::string>();live.until("project.created",cp);check(event_count("project.updated",pp)>=2,"parent notified about children");
            before=count();reject("project.updated");expect(client.Post(cp+"/boards",R"({"name":"Failed board"})","application/json"),503);restore();check(count()==before&&expect(client.Get(cp+"/boards"),200)["boards"].empty(),"owning project notification failure rolls back board and event");
            auto board=post(cp+"/boards",{{"name","Board"}});const auto bp=cp+"/boards/"+board["id"].get<std::string>();live.until("project.updated",cp);
            auto nested=expect(client.Post("/files?name=nested.txt&directory=%2Fprojects%2FCoverage%20root%2FChild","nested","application/octet-stream"),201);auto np="/files/"+nested["id"].get<std::string>();live.until("file.created",np);
            before=count();reject("project.deleted");expect(client.Delete(pp,R"({"destroy_directory":true})","application/json"),503);restore();check(count()==before&&client.Get(cp)->status==200&&client.Get(np)->status==200&&client.Get(bp)->status==200,"cascade notifications and state roll back together");
            expect(client.Delete(pp,R"({"destroy_directory":true})","application/json"),204);live.until("project.deleted",pp);
            check(event_count("project.deleted",cp)==1&&event_count("file.deleted",np)==1&&event_count("board.deleted",bp)==1,"cascade coverage exactly once");
            auto keep=post("/projects",{{"name","Coverage keep"},{"description",""}});auto kp="/projects/"+keep["id"].get<std::string>();
            auto kept=expect(client.Post("/files?name=keep.txt&directory=%2Fprojects%2FCoverage%20keep","keep","application/octet-stream"),201);auto kf="/files/"+kept["id"].get<std::string>();
            expect(client.Delete(kp,R"({"destroy_directory":false})","application/json"),204);live.until("project.deleted",kp);check(event_count("file.deleted",kf)==0&&client.Get(kf)->status==200,"retained files not reported deleted");expect(client.Delete(kf),204);live.until("file.deleted",kf);
            expect(client.Delete(device),204);live.until("device.deleted",device);
            db.run("INSERT INTO events(type,resource,payload) VALUES ('test.coverage','/coverage/end','{}')");live.until("test.coverage");
            Peer replay(tcp.port());replay.send({{"type","hello"},{"device_id","coverage-observer"},{"after_sequence",welcome["sequence"]}});check(replay.until("welcome")["resync_required"]==false,"coverage replay accepted");replay.until("catchup_complete");
            check(replay.history==live.history,"all new notifications replay identically and in order");
            std::set<std::string> types;for(const auto& e:live.history)types.insert(e["type"]);
            for(const auto* type:{"file.created","file.updated","file.deleted","files.directories_updated","project.created","project.updated","project.deleted","device.created","device.updated","device.deleted","device.subscription_updated"})check(types.count(type),"missing live notification coverage");
            std::cout<<"Notification coverage, no-ops, atomic rollback, cascades, subscriptions and replay passed\n";
        }catch(const std::exception& e){std::cerr<<e.what()<<'\n';result=1;}
        server.stop();worker.join();return result;
    }catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}
}
