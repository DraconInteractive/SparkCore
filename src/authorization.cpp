#include "authorization.hpp"
namespace spark {
namespace {
bool deny(httplib::Response& response,int status,const char* error){
    response.status=status;response.set_header("Connection","close");
    if(status==401)response.set_header("WWW-Authenticate","Bearer realm=\"SPARK\"");
    response.set_content(nlohmann::json{{"error",error}}.dump(),"application/json");return false;
}
const RoutePolicy* policy(const std::string& method,const std::string& path,std::smatch& match){
    for(const auto& rule:route_policies())if(rule.method==(method=="HEAD"?"GET":method)&&std::regex_match(path,match,rule.pattern))return &rule;
    return nullptr;
}
bool authorize_route(const Identity& identity,const httplib::Request& req,httplib::Response& res){
    std::smatch match;const auto* rule=policy(req.method,req.path,match);
    if(!rule)return deny(res,403,"forbidden");
    for(const auto& permission:rule->permissions)if(!permitted(identity,permission))return deny(res,403,"forbidden");
    if(rule->own_device&&!acts_as(identity,match[1].str()))return deny(res,403,"device_identity_mismatch");
    return true;
}
}
const std::vector<RoutePolicy>& route_policies(){
    static const auto rules=[] {
        std::vector<RoutePolicy> out;
        auto add=[&](std::string method,std::string path,std::vector<std::string> permissions,bool own=false){
            auto pattern=std::regex_replace(path,std::regex(R"(\{[^}]+\})"),"([^/]+)");
            out.push_back({method,path,std::move(permissions),std::regex(pattern),own});
        };
        for(auto path:{"/","/health","/version","/api","/api/docs"})add("GET",path,{"discovery.read"});
        add("GET","/admin/status",{"admin.restart","admin.backup"});
        add("GET","/admin/backup-locations",{"admin.backup"});
        add("POST","/admin/restart",{"admin.restart"});add("POST","/admin/backup",{"admin.backup"});
        add("GET","/devices/{id}",{"devices.read"});
        for(auto method:{"POST","PATCH","DELETE"})add(method,"/devices/{id}",{"devices.write"},true);
        add("GET","/devices/{id}/actions",{"commands.read"});add("PUT","/devices/{id}/actions",{"commands.advertise"},true);
        add("POST","/devices/{id}/commands/claim",{"commands.execute"},true);
        for(auto path:{"/commands","/commands/devices","/commands/explorer","/commands/{id}"})add("GET",path,{"commands.read"});
        add("POST","/commands",{"commands.invoke"});add("POST","/commands/{id}/cancel",{"commands.invoke"});
        for(auto path:{"/commands/{id}/renew","/commands/{id}/complete"})add("POST",path,{"commands.execute"});
        add("GET","/pulse",{"pulse.read"});add("PATCH","/pulse",{"pulse.write"});
        for(auto method:{"POST","DELETE"})add(method,"/pulse/subscriptions/{id}",{"pulse.write"},true);
        for(auto path:{"/timers","/timers/{id}"})add("GET",path,{"timers.read"});
        for(auto path:{"/timers","/timers/{id}/confirm","/timers/{id}/stop"})add("POST",path,{"timers.write"});
        for(auto family:{"events","daily-tasks"}){
            std::string path="/"+std::string(family),permission=std::string(family)=="events"?"events":"tasks";
            add("GET",path,{permission+".read"});add("GET",path+"/{id}",{permission+".read"});
            add("POST",path,{permission+".write"});add("PATCH",path+"/{id}",{permission+".write"});add("DELETE",path+"/{id}",{permission+".write"});
        }
        add("GET","/events/occurrences",{"events.read"});add("PUT","/daily-tasks/{id}/checked",{"tasks.write"});
        for(auto path:{"/files","/files/explorer","/files/drives","/files/directories","/files/{id}","/files/{id}/content"})add("GET",path,{"files.read"});
        add("POST","/files/drives",{"admin.storage"});add("DELETE","/files/drives/{letter}",{"admin.storage"});
        for(auto path:{"/files","/files/directories"})add("POST",path,{"files.write"});
        add("PUT","/files/{id}/content",{"files.write"});add("PATCH","/files/{id}",{"files.write"});add("DELETE","/files/{id}",{"files.write"});
        for(auto path:{"/projects","/projects/explorer","/projects/{id}"})add("GET",path,{"projects.read"});
        add("POST","/projects",{"projects.write","files.write"});add("PATCH","/projects/{id}",{"projects.write"});
        // Deletion cascades into all descendant boards/tasks, even when files are retained.
        add("DELETE","/projects/{id}",{"projects.write","boards.write"});
        add("GET","/project/{id}/files",{"projects.read","files.read"});
        for(auto path:{"/projects/{id}/boards","/projects/{id}/boards/{board}","/projects/{id}/boards/{board}/tasks/{task}"})add("GET",path,{"boards.read"});
        for(auto path:{"/projects/{id}/boards","/projects/{id}/boards/{board}/columns","/projects/{id}/boards/{board}/columns/{column}/tasks"})add("POST",path,{"boards.write"});
        for(auto path:{"/projects/{id}/boards/{board}","/projects/{id}/boards/{board}/columns/{column}","/projects/{id}/boards/{board}/tasks/{task}"})for(auto method:{"PATCH","DELETE"})add(method,path,{"boards.write"});
        return out;
    }();return rules;
}
std::optional<Identity> Authorization::resolve(const httplib::Request& req,httplib::Response& res)const{
    // Open provider deliberately ignores headers to preserve existing behavior.
    if(!req.has_header("Authorization")&&req.has_header("Cookie")&&sessions_->enabled())return sessions_->resolve(req,res);
    std::string token;
    if(req.get_header_value_count("Authorization")==1){auto value=req.get_header_value("Authorization");if(value.size()>7&&std::regex_match(value.substr(0,7),std::regex("[Bb][Ee][Aa][Rr][Ee][Rr] ")))token=value.substr(7);}
    auto identity=provider_->authenticate(token);
    if(!identity)deny(res,401,"unauthorized");return identity;
}
bool Authorization::request(const httplib::Request& req,httplib::Response& res)const{
    auto identity=resolve(req,res);if(!identity)return false;if(identity->kind=="open")return true;
    return authorize_route(*identity,req,res);
}

bool Authorization::body(const httplib::Request& req,httplib::Response& res)const{
    auto identity=resolve(req,res);if(!identity)return false;if(identity->kind=="open")return true;
    if(!authorize_route(*identity,req,res))return false;
    // Parse using the same JSON semantics as domain handlers. Invalid JSON remains
    // their validation responsibility; no handler mutates from an invalid packet.
    auto packet=nlohmann::json::parse(req.body,nullptr,false);
    if(!packet.is_object())return true;
    for(auto field:{"source_device","device_id"})if(packet.contains(field)&&!packet[field].is_null()){
        if(!packet[field].is_string()||!acts_as(*identity,packet[field].get<std::string>()))return deny(res,403,"device_identity_mismatch");
    }
    if(req.method=="DELETE"&&std::regex_match(req.path,std::regex(R"(/projects/[^/]+)"))&&packet.value("destroy_directory",nlohmann::json(false))==true&&!permitted(*identity,"files.write"))return deny(res,403,"forbidden");
    return true;
}
bool notification_permitted(const Identity& who,const nlohmann::json& event){
    if(who.kind=="open")return true;
    if(!permitted(who,"notifications.read"))return false;
    const auto type=event.value("type",std::string{});
    std::string permission;
    if(type.rfind("timer.",0)==0)permission="timers.read";
    else if(type.rfind("pulse.",0)==0)permission="pulse.read";
    else if(type.rfind("calendar.",0)==0)permission="events.read";
    else if(type.rfind("daily_tasks.",0)==0)permission="tasks.read";
    else if(type.rfind("file.",0)==0||type=="files.directories_updated"||type=="drives.updated")permission="files.read";
    else if(type.rfind("project.",0)==0)permission="projects.read";
    else if(type.rfind("board.",0)==0)permission="boards.read";
    else if(type=="device.actions_updated")permission="commands.read";
    else if(type.rfind("device.",0)==0)permission="devices.read";
    else if(type=="command.available")return permitted(who,"commands.read")&&permitted(who,"commands.execute");
    else if(type=="command.updated")permission="commands.read";
    return !permission.empty()&&permitted(who,permission);
}
}
