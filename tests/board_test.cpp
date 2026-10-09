#include "api.hpp"
#include "boards.hpp"
#include "timers.hpp"
#include "notifications.hpp"
#include <mysql.h>
#include <thread>
#include <iostream>
using nlohmann::json;
void check(bool v,const char* message){if(!v)throw std::runtime_error(message);}
json expect(const httplib::Result& r,int status,const char* error=nullptr){
    if(!r||r->status!=status)throw std::runtime_error("Expected "+std::to_string(status)+" got "+(r?std::to_string(r->status)+" "+r->body:"no response"));
    if(status==204)return nullptr;auto body=json::parse(r->body);if(error)check(body["error"]==error,"wrong error");return body;
}
struct DB{
 MYSQL* db;
 explicit DB(const spark::DatabaseConfig& c):db(mysql_init(nullptr)){check(db&&mysql_real_connect(db,c.host.c_str(),c.user.c_str(),c.password.c_str(),c.database.c_str(),c.port,c.socket.empty()?nullptr:c.socket.c_str(),0),"connect DB");}
 ~DB(){mysql_close(db);}
 void run(const std::string& sql){if(mysql_query(db,sql.c_str()))throw std::runtime_error(mysql_error(db));}
 unsigned long long count(const std::string& sql){run(sql);auto* result=mysql_store_result(db);auto row=mysql_fetch_row(result);auto n=std::stoull(row[0]);mysql_free_result(result);return n;}
};
int main(){
 auto config=spark::DatabaseConfig::from_environment();if(config.database.rfind("spark_test_",0)!=0||!std::getenv("SPARK_FILE_DIR"))return 77;
 spark::BoardStore(config).verify();DB db(config);
 auto storage=std::make_shared<spark::FileStorage>(std::make_shared<spark::FileStore>(config),spark::FileStorage::default_root());storage->initialize();
 httplib::Server server;spark::configure_api(server,std::make_shared<spark::DeviceStore>(config),std::make_shared<spark::TimerStore>(config),47684,nullptr,nullptr,nullptr,storage);
 const int port=server.bind_to_any_port("127.0.0.1");if(port<0)return 1;std::thread worker([&]{server.listen_after_bind();});server.wait_until_ready();int result=0;
 try{
    httplib::Client client("127.0.0.1",port);client.set_read_timeout(10,0);
    auto post=[&](const std::string& path,const json& body){return expect(client.Post(path,body.dump(),"application/json"),201);};
    auto project=post("/projects",{{"name","Board test"},{"description",""}});auto other=post("/projects",{{"name","Other board test"},{"description",""}});
    const auto pp="/projects/"+project["id"].get<std::string>(),op="/projects/"+other["id"].get<std::string>();
    auto board=post(pp+"/boards",{{"name","Development"}});auto second=post(pp+"/boards",{{"name","Planning"}});
    const auto path=pp+"/boards/"+board["id"].get<std::string>(),path2=pp+"/boards/"+second["id"].get<std::string>();
    check(board["columns"].empty()&&board["project_id"]==project["id"],"empty board creation");
    check(expect(client.Get(pp+"/boards"),200)["boards"].size()==2,"multiple boards");
    expect(client.Get(op+"/boards/"+board["id"].get<std::string>()),404,"board_not_found");
    expect(client.Post(pp+"/boards",R"({"name":"Development"})","application/json"),409,"name_conflict");
    auto todo=post(path+"/columns",{{"name","To do"}}),done=post(path+"/columns",{{"name","Done"}}),foreign=post(path2+"/columns",{{"name","Done"}});
    auto task=post(path+"/columns/"+todo["id"].get<std::string>()+"/tasks",{{"name","Implement <client> 🌳"},{"description","Long description\nNext line"}});
    const auto tp=path+"/tasks/"+task["id"].get<std::string>();
    check(expect(client.Get(path),200)["columns"][0]["tasks"][0]==task,"full board task detail");
    expect(client.Delete(path+"/columns/"+todo["id"].get<std::string>()),409,"column_not_empty");
    expect(client.Patch(tp,json{{"column_id",foreign["id"]}}.dump(),"application/json"),404,"column_not_found");
    expect(client.Get(path2+"/tasks/"+task["id"].get<std::string>()),404,"task_not_found");
    auto moved=expect(client.Patch(tp,json{{"column_id",done["id"]},{"description","Moved"}}.dump(),"application/json"),200);
    check(moved["id"]==task["id"]&&moved["column_id"]==done["id"]&&moved["name"]==task["name"],"move preserves identity and omitted fields");
    const auto count=db.count("SELECT COUNT(*) FROM events WHERE type='board.updated'");
    check(expect(client.Patch(tp,json{{"column_id",done["id"]}}.dump(),"application/json"),200)==moved && db.count("SELECT COUNT(*) FROM events WHERE type='board.updated'")==count,"idempotent move emits no event");
    expect(client.Delete(path+"/columns/"+todo["id"].get<std::string>()),204);
    expect(client.Patch(path+"/columns/"+done["id"].get<std::string>(),R"({"name":"Complete"})","application/json"),200);
    expect(client.Patch(path,R"({"name":"Delivery"})","application/json"),200);
    expect(client.Get(path+"?unknown=1"),400,"invalid_parameters");
    expect(client.Post(path+"/columns","{}","application/json"),400,"missing_fields");
    expect(client.Post(path+"/columns",R"({"name":" "})","application/json"),400,"invalid_name");
    expect(client.Patch(tp,R"({"column_id":"bad"})","application/json"),400,"invalid_column_id");
    expect(client.Patch(tp,R"({"status":"done"})","application/json"),400,"unknown_field");
    expect(client.Post(pp+"/boards","{}","text/plain"),415,"unsupported_media_type");
    check(client.Head(path)->status==200&&client.Head(path)->body.empty(),"board HEAD");
    check(client.Options(path)->get_header_value("Allow")=="GET, HEAD, PATCH, DELETE, OPTIONS","board OPTIONS");
    expect(client.Post(path,"{}","application/json"),405,"method_not_allowed");
    db.run("CREATE TRIGGER reject_board_event BEFORE INSERT ON events FOR EACH ROW BEGIN IF NEW.type LIKE 'board.%' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test board rollback'; END IF; END");
    expect(client.Patch(tp,R"({"name":"Should rollback"})","application/json"),503,"database_unavailable");
    check(expect(client.Get(tp),200)==moved,"task rollback with notification failure");
    expect(client.Delete(path),503,"database_unavailable");
    check(expect(client.Get(path),200)["columns"][0]["tasks"].size()==1,"board cascade rollback");
    expect(client.Post(pp+"/boards",R"({"name":"Rollback"})","application/json"),503,"database_unavailable");
    db.run("DROP TRIGGER reject_board_event");
    expect(client.Delete(tp),204);expect(client.Get(tp),404,"task_not_found");
    // Race a move against deletion of its destination: exactly one can succeed.
    auto source=post(path+"/columns",{{"name","Source"}}),target=post(path+"/columns",{{"name","Target"}});
    auto raced=post(path+"/columns/"+source["id"].get<std::string>()+"/tasks",{{"name","Race"},{"description",""}});
    int move_status=0,delete_status=0;
    std::thread mover([&]{httplib::Client c("127.0.0.1",port);auto r=c.Patch(path+"/tasks/"+raced["id"].get<std::string>(),json{{"column_id",target["id"]}}.dump(),"application/json");move_status=r?r->status:0;});
    std::thread remover([&]{httplib::Client c("127.0.0.1",port);auto r=c.Delete(path+"/columns/"+target["id"].get<std::string>());delete_status=r?r->status:0;});mover.join();remover.join();
    check((move_status==200&&delete_status==409)||(move_status==404&&delete_status==204),"move/delete race serialized");
    expect(client.Delete(path),204);expect(client.Get(path),404,"board_not_found");
    check(db.count("SELECT COUNT(*) FROM project_tasks WHERE board_id='"+board["id"].get<std::string>()+"'")==0,"board deletion cascades tasks");
    auto child=post("/projects",{{"name","Child"},{"description",""},{"parent_id",project["id"]}});
    auto cb=post("/projects/"+child["id"].get<std::string>()+"/boards",{{"name","Child board"}});
    auto cpath="/projects/"+child["id"].get<std::string>()+"/boards/"+cb["id"].get<std::string>();
    auto cc=post(cpath+"/columns",{{"name","Todo"}});post(cpath+"/columns/"+cc["id"].get<std::string>()+"/tasks",{{"name","Nested"},{"description",""}});
    expect(client.Delete(pp,R"({"destroy_directory":false})","application/json"),204);expect(client.Delete(op,R"({"destroy_directory":false})","application/json"),204);
    check(db.count("SELECT COUNT(*) FROM project_boards")==0 && db.count("SELECT COUNT(*) FROM project_columns")==0 && db.count("SELECT COUNT(*) FROM project_tasks")==0,"project subtree deletes all boards and tasks regardless of file retention");
    spark::NotificationStore notifications(config);while(db.count("SELECT COUNT(*) FROM events e LEFT JOIN notification_log n ON n.event_id=e.event_id WHERE n.event_id IS NULL"))notifications.sequence_events();unsigned long long cursor=0;bool created=false,updated=false,deleted=false;
    for(;;){auto frames=notifications.after(cursor);if(frames.empty())break;for(const auto& frame:frames){cursor=frame["sequence"];if(frame["resource"]==path){created|=frame["type"]=="board.created";updated|=frame["type"]=="board.updated";deleted|=frame["type"]=="board.deleted";}}}
    check(created&&updated&&deleted,"board changes use notification frames");
    auto root=expect(client.Get("/projects?parent_id=null"),200)["projects"][0];auto persistent=post("/projects/"+root["id"].get<std::string>()+"/boards",{{"name","Persistent board"}});
    auto persistent_path="/projects/"+root["id"].get<std::string>()+"/boards/"+persistent["id"].get<std::string>();
    auto pc=post(persistent_path+"/columns",{{"name","Todo"}});post(persistent_path+"/columns/"+pc["id"].get<std::string>()+"/tasks",{{"name","Persistent task"},{"description","Survives restart"}});
    std::cout<<"Boards, columns, task moves, ownership, deletion, rollback, concurrency and notifications passed\n";
 }catch(const std::exception& e){std::cerr<<e.what()<<'\n';result=1;}
 server.stop();worker.join();return result;
}
