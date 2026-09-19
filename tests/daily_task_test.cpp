#include "api.hpp"
#include "daily_tasks.hpp"
#include "timers.hpp"
#include "notifications.hpp"
#include <mysql.h>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <poll.h>
#include <unistd.h>
#include <iostream>
#include <thread>
#include <vector>
using nlohmann::json;
void check(bool b,const char* message) {if(!b) throw std::runtime_error(message);}
json expect(const httplib::Result& result,int status,const char* error=nullptr) {
    check(result && result->status==status,"daily task HTTP status");
    if(status==204) {check(result->body.empty(),"delete body");return nullptr;}
    auto value=json::parse(result->body);if(error) check(value["error"]==error,"daily task error");return value;
}
struct DB {
    MYSQL* db;
    explicit DB(const spark::DatabaseConfig& c):db(mysql_init(nullptr)) {check(db && mysql_real_connect(db,c.host.c_str(),c.user.c_str(),c.password.c_str(),c.database.c_str(),c.port,c.socket.empty()?nullptr:c.socket.c_str(),0),"test SQL connect");}
    ~DB(){mysql_close(db);}
    void run(const std::string& sql){check(mysql_query(db,sql.c_str())==0,"test SQL statement");}
    std::string value(const std::string& sql){run(sql);auto result=std::unique_ptr<MYSQL_RES,decltype(&mysql_free_result)>(mysql_store_result(db),mysql_free_result);check(bool(result),"SQL result");auto row=mysql_fetch_row(result.get());check(row && row[0],"SQL scalar");return row[0];}
};
struct Peer {
    int fd;std::string input;
    explicit Peer(int port):fd(socket(AF_INET,SOCK_STREAM,0)) {sockaddr_in addr{};addr.sin_family=AF_INET;addr.sin_port=htons(port);inet_pton(AF_INET,"127.0.0.1",&addr.sin_addr);check(fd>=0 && connect(fd,reinterpret_cast<sockaddr*>(&addr),sizeof(addr))==0,"TCP connect");const std::string hello="{\"type\":\"hello\",\"device_id\":\"task-client\"}\n";check(send(fd,hello.data(),hello.size(),MSG_NOSIGNAL)==static_cast<ssize_t>(hello.size()),"hello send");until("welcome");}
    ~Peer(){close(fd);}
    json until(const std::string& type){
        const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(5);
        while(std::chrono::steady_clock::now()<deadline){
            auto pos=input.find('\n');
            if(pos!=std::string::npos){auto frame=json::parse(input.substr(0,pos));input.erase(0,pos+1);if(frame["type"]==type)return frame;continue;}
            pollfd p{fd,POLLIN,0};if(poll(&p,1,100)>0){char bytes[4096];auto n=recv(fd,bytes,sizeof(bytes),0);check(n>0,"TCP read");input.append(bytes,n);}
        }
        throw std::runtime_error("Missing TCP "+type);
    }
};
int main(){
    auto config=spark::DatabaseConfig::from_environment();if(config.database.rfind("spark_test_",0)!=0)return 77;
    auto tasks=std::make_shared<spark::DailyTaskStore>(config);tasks->verify();
    auto events=std::make_shared<spark::NotificationStore>(config);events->sequence_events();
    spark::NotificationOptions options;options.host="127.0.0.1";options.port=0;
    spark::NotificationServer tcp(events,options);Peer peer(tcp.port());
    httplib::Server server;spark::configure_api(server,std::make_shared<spark::DeviceStore>(config),std::make_shared<spark::TimerStore>(config),tcp.port(),nullptr,nullptr,tasks);
    int port=server.bind_to_any_port("127.0.0.1");if(port<0)return 1;std::thread listener([&]{server.listen_after_bind();});server.wait_until_ready();
    int result=0;
    try {
        DB db(config);httplib::Client client("127.0.0.1",port);client.set_read_timeout(10,0);
        // UTC 16:00 is precisely midnight of the following GMT+8 day.
        check(db.value("SELECT DATE(TIMESTAMP('2026-09-19 15:59:59')+INTERVAL 8 HOUR)")=="2026-09-19","before Perth midnight");
        check(db.value("SELECT DATE(TIMESTAMP('2026-09-19 16:00:00')+INTERVAL 8 HOUR)")=="2026-09-20","Perth midnight boundary");
        auto list=expect(client.Get("/daily-tasks"),200);check(list["tasks"].empty() && list["timezone"]=="+08:00","empty day list");
        check(list["day"]==db.value("SELECT DATE(UTC_TIMESTAMP()+INTERVAL 8 HOUR)"),"current Perth day");
        auto task=expect(client.Post("/daily-tasks",R"({"name":"Water","description":"A daily habit","source_device":"phone-1"})","application/json"),201);
        const auto id=task["id"].get<std::string>(),path="/daily-tasks/"+id;
        check(task["checked"]==false && task["checked_at"].is_null(),"new task unchecked");
        check(peer.until("daily_tasks.updated")["resource"]=="/daily-tasks","task creation notification");
        auto checked=expect(client.Put(path+"/checked",R"({"checked":true})","application/json"),200);
        check(checked["checked"]==true && !checked["checked_at"].is_null(),"check task");peer.until("daily_tasks.updated");
        check(expect(client.Put(path+"/checked",R"({"checked":true})","application/json"),200)==checked,"idempotent check");
        auto modified=expect(client.Patch(path,R"({"name":"Drink water"})","application/json"),200);
        check(modified["checked"]==true && modified["description"]==task["description"],"modify preserves check and omitted fields");peer.until("daily_tasks.updated");
        auto unchecked=expect(client.Put(path+"/checked",R"({"checked":false})","application/json"),200);
        check(unchecked["checked"]==false && unchecked["checked_at"].is_null(),"uncheck task");peer.until("daily_tasks.updated");
        check(db.value("SELECT COUNT(*) FROM events WHERE type='daily_tasks.updated'")=="4","no event for identical check");
        expect(client.Put(path+"/checked",R"({"checked":1})","application/json"),400,"invalid_checked");
        expect(client.Put(path+"/checked","{}","application/json"),400,"missing_fields");
        expect(client.Patch(path,R"({"checked":true})","application/json"),400,"unknown_field");
        expect(client.Post("/daily-tasks","{}","application/json"),400,"missing_fields");
        expect(client.Post("/daily-tasks",R"({"name":" "})","application/json"),400,"invalid_name");
        expect(client.Patch(path,"[]","application/json"),400,"invalid_packet");
        expect(client.Put(path+"/checked",R"({"checked":true})","text/plain"),415,"unsupported_media_type");
        expect(client.Get("/daily-tasks/bad"),400,"invalid_task_id");
        const std::string missing="/daily-tasks/00000000-0000-0000-0000-000000000000";
        expect(client.Get(missing),404,"task_not_found");expect(client.Delete(missing),404,"task_not_found");
        expect(client.Put(missing+"/checked",R"({"checked":true})","application/json"),404,"task_not_found");
        auto options_result=client.Options(path+"/checked");check(options_result && options_result->status==204 && options_result->get_header_value("Allow")=="PUT, OPTIONS","check preflight");
        auto head=client.Head(path);check(head && head->status==200 && head->body.empty(),"task HEAD");
        // Background rollover with no REST request to trigger it.
        tasks->update(id,{{"checked",true}},true);peer.until("daily_tasks.updated");
        db.run("UPDATE daily_task_day SET local_day=DATE(UTC_TIMESTAMP()+INTERVAL 8 HOUR)-INTERVAL 1 DAY WHERE id=1");
        {spark::DailyTaskScheduler scheduler(tasks);peer.until("daily_tasks.reset");}
        auto reset=expect(client.Get(path),200);check(reset["checked"]==false && reset["checked_at"].is_null() && reset["name"]=="Drink water","reset retains task");
        check(db.value("SELECT COUNT(*) FROM events WHERE type='daily_tasks.reset'")=="1","one background reset");
        // A check concurrent with multiple catch-up workers belongs to the new day.
        db.run("UPDATE daily_task_day SET local_day=DATE(UTC_TIMESTAMP()+INTERVAL 8 HOUR)-INTERVAL 3 DAY WHERE id=1");
        std::vector<std::exception_ptr> failures(6);std::vector<std::thread> workers;
        for(int i=0;i<6;++i)workers.emplace_back([&,i]{try{if(i==0)tasks->update(id,{{"checked",true}},true);else tasks->tick();}catch(...){failures[i]=std::current_exception();}});
        for(auto& worker:workers)worker.join();for(auto failure:failures)check(!failure,"concurrent day reset");
        check((*tasks->get(id))["checked"]==true,"new-day check survives concurrent reset");
        check(db.value("SELECT COUNT(*) FROM events WHERE type='daily_tasks.reset'")=="2","downtime coalesces resets");
        check(std::make_shared<spark::DailyTaskStore>(config)->get(id)==tasks->get(id),"same-day restart preserves check");
        // Fail event insertion and prove mutations and reset are rolled back together.
        db.run("CREATE TRIGGER reject_task_event BEFORE INSERT ON events FOR EACH ROW BEGIN IF NEW.resource='/daily-tasks' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test task event failure'; END IF; END");
        expect(client.Post("/daily-tasks",R"({"name":"Rollback"})","application/json"),503,"database_unavailable");
        check(db.value("SELECT COUNT(*) FROM daily_tasks WHERE name='Rollback'")=="0","create rollback");
        expect(client.Put(path+"/checked",R"({"checked":false})","application/json"),503,"database_unavailable");
        expect(client.Patch(path,R"({"name":"Rollback"})","application/json"),503,"database_unavailable");
        expect(client.Delete(path),503,"database_unavailable");
        check((*tasks->get(id))["checked"]==true,"mutation rollback preserves task");
        db.run("UPDATE daily_task_day SET local_day=DATE(UTC_TIMESTAMP()+INTERVAL 8 HOUR)-INTERVAL 1 DAY WHERE id=1");
        expect(client.Get("/daily-tasks"),503,"database_unavailable");
        check(db.value("SELECT checked FROM daily_tasks WHERE id='"+id+"'")=="1","failed reset preserves checks");
        db.run("DROP TRIGGER reject_task_event");
        check(expect(client.Get(path),200)["checked"]==false,"GET catches up failed rollover");
        expect(client.Delete(path),204);expect(client.Get(path),404,"task_not_found");
        tasks->create({{"name","Persistent daily task"}});
        std::cout<<"Daily tasks CRUD, checks, GMT+8 rollover, concurrency, atomic events, and TCP notifications passed\n";
    }catch(const std::exception& error){std::cerr<<error.what()<<'\n';result=1;}
    server.stop();listener.join();return result;
}
