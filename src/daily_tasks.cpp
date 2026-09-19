#include "bounded_body.hpp"
#include "daily_tasks.hpp"
#include <chrono>
#include <iostream>
#include <regex>
namespace spark {
using nlohmann::json;
DailyTaskScheduler::DailyTaskScheduler(std::shared_ptr<DailyTaskStore> store) {
    worker_=std::thread([this,store]{
        bool failed=false; std::unique_lock<std::mutex> lock(mutex_);
        while(!stopping_) {
            lock.unlock();
            try { store->tick(); if(failed) std::cerr<<"Daily task scheduler recovered\n"; failed=false; }
            catch(const std::exception& error) { if(!failed) std::cerr<<"Daily task scheduler will retry: "<<error.what()<<'\n'; failed=true; }
            lock.lock(); wake_.wait_for(lock,std::chrono::seconds(1),[this]{return stopping_;});
        }
    });
}
DailyTaskScheduler::~DailyTaskScheduler() { {std::lock_guard<std::mutex> lock(mutex_); stopping_=true;} wake_.notify_all(); worker_.join(); }
void configure_daily_tasks(httplib::Server& server,std::shared_ptr<DailyTaskStore> store) {
    auto handler=[store](const httplib::Request& request,httplib::Response& response){
        auto reply=[&](int status,const json& body){response.status=status;response.set_content(body.dump(2)+"\n","application/json");};
        auto bad=[&](const char* code){reply(400,{{"error",code}});};
        const bool collection=request.path=="/daily-tasks", checking=request.method=="PUT";
        std::string id;
        if(!collection) {
            id=request.matches[1].str();
            if(!std::regex_match(id,std::regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"))) {bad("invalid_task_id");return;}
        }
        json packet=json::object();
        if(request.method=="POST" || request.method=="PATCH" || checking) {
            const auto type=request.get_header_value("Content-Type");
            if(type.substr(0,type.find(';'))!="application/json") {reply(415,{{"error","unsupported_media_type"}});return;}
            packet=json::parse(request.body,nullptr,false);
            if(!packet.is_object()) {bad("invalid_packet");return;}
            for(const auto& field:packet.items()) {
                if(field.key()=="source_device") continue;
                if(checking?field.key()!="checked":(field.key()!="name" && field.key()!="description")) {bad("unknown_field");return;}
            }
            if((checking && !packet.contains("checked")) || (request.method=="POST" && !packet.contains("name")) ||
               (request.method=="PATCH" && !packet.contains("name") && !packet.contains("description"))) {bad("missing_fields");return;}
            if(checking && !packet["checked"].is_boolean()) {bad("invalid_checked");return;}
            if(packet.contains("name") && (!packet["name"].is_string() || packet["name"].get<std::string>().size()>200 || packet["name"].get<std::string>().find_first_not_of(" \t\r\n")==std::string::npos)) {bad("invalid_name");return;}
            if(packet.contains("description") && (!packet["description"].is_string() || packet["description"].get<std::string>().size()>4000)) {bad("invalid_description");return;}
            if(packet.contains("source_device") && !packet["source_device"].is_null() && (!packet["source_device"].is_string() || !std::regex_match(packet["source_device"].get<std::string>(),std::regex("[A-Za-z0-9_-]{1,64}")))) {bad("invalid_source_device");return;}
        }
        try {
            if(request.method=="POST") {auto task=store->create(packet);response.set_header("Location","/daily-tasks/"+task["id"].get<std::string>());reply(201,task);}
            else if(request.method=="DELETE") {if(store->remove(id)) response.status=204;else reply(404,{{"error","task_not_found"}});}
            else if(collection) reply(200,store->list());
            else {auto task=(checking || request.method=="PATCH")?store->update(id,packet,checking):store->get(id);if(task) reply(200,*task);else reply(404,{{"error","task_not_found"}});}
        } catch(const DatabaseError&) {reply(503,{{"error","database_unavailable"}});}
    };
    server.Get("/daily-tasks",handler);body_route(server,"POST","/daily-tasks",handler);
    server.Get(R"(/daily-tasks/([^/]+))",handler);body_route(server,"PATCH",R"(/daily-tasks/([^/]+))",handler);body_route(server,"DELETE",R"(/daily-tasks/([^/]+))",handler);
    body_route(server,"PUT",R"(/daily-tasks/([^/]+)/checked)",handler);
}
json daily_task_endpoints() {
    const json task={{"id","12345678-1234-1234-1234-123456789abc"},{"name","Drink water"},{"description",""},{"checked",false},{"checked_at",nullptr},{"day","2026-09-19"},{"created_at","2026-09-19T00:00:00.000000Z"},{"updated_at","2026-09-19T00:00:00.000000Z"}};
    const json fields={{"id","lowercase UUID"},{"name","string"},{"description","string"},{"checked","boolean for the returned GMT+8 day"},{"checked_at","UTC ISO 8601 timestamp or null"},{"day","YYYY-MM-DD in fixed GMT+8"},{"created_at","UTC timestamp"},{"updated_at","UTC timestamp"}};
    json entries=json::array();
    for(const std::string op:{"create","list","get","modify","delete","checked"}) {
        const bool create=op=="create",list=op=="list",remove=op=="delete",checking=op=="checked",write=create || op=="modify" || checking;
        entries.push_back({{"method",create?"POST":remove?"DELETE":checking?"PUT":op=="modify"?"PATCH":"GET"},
            {"path",create || list?"/daily-tasks":checking?"/daily-tasks/{id}/checked":"/daily-tasks/{id}"},
            {"description",create?"Create a recurring daily task, initially unchecked. Name required; description optional." : list?"List all tasks in creation order with the current GMT+8 day. Checks reset at local midnight." : remove?"Delete a task permanently; missing IDs return 404." : checking?"Set checked true or false for the current GMT+8 day. Repeated identical requests are no-ops." : op=="modify"?"Modify name/description without changing checked state. At least one field required." : "Read one task with the current GMT+8 checked state."},
            {"input",{{"path",create || list?json::object():json{{"id","lowercase UUID"}}},{"query",json::object()},{"headers",write?json{{"Content-Type","application/json"}}:json::object()},
                {"body",!write?json(nullptr):checking?json{{"checked",true}}:json{{"name","Drink water"},{"description",""}}},
                {"fields",!write?json::object():checking?json{{"checked","required boolean"},{"source_device","optional device ID or null"}}:json{{"name","nonblank string, max 200 UTF-8 bytes"},{"description","string, max 4000 UTF-8 bytes; default empty on creation"},{"source_device","optional device ID or null"}}},
                {"validation","Unknown body fields rejected. A source_device uses the existing 1-64 character device ID syntax. No authentication."}}},
            {"output",{{"status",create?201:remove?204:200},{"headers",remove?json{{"Cache-Control","no-store"}}:json{{"Content-Type","application/json"},{"Cache-Control","no-store"}}},
                {"fields",remove?json::object():list?json{{"day","current GMT+8 day"},{"timezone","+08:00"},{"tasks",fields}}:fields},
                {"body",remove?json(nullptr):list?json{{"day","2026-09-19"},{"timezone","+08:00"},{"tasks",json::array({task})}}:task}}},
            {"errors",{{"400","invalid_task_id, invalid_packet, unknown_field, missing_fields, invalid_name, invalid_description, invalid_checked, invalid_source_device"},{"404","task_not_found"},{"415","unsupported_media_type"},{"503","database_unavailable"}}}
        });
    }
    return entries;
}
}
