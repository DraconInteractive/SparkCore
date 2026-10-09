#include "projects.hpp"
#include "project_explorer.hpp"
#include "bounded_body.hpp"
#include <charconv>
#include <regex>
namespace spark {
namespace {
using nlohmann::json;
bool name_ok(const json& value){
    if(!value.is_string())return false;const auto name=value.get<std::string>();
    if(name.empty() || name.size()>200 || name=="." || name==".." || name.find_first_not_of(' ')==std::string::npos)return false;
    for(unsigned char c:name)if(c<32 || c==127 || c=='/' || c=='\\')return false;
    return true;
}
bool valid_id(const std::string& id){return std::regex_match(id,std::regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"));}
int number(const std::string& s,int min,int max){int n=0;auto r=std::from_chars(s.data(),s.data()+s.size(),n);return r.ec==std::errc{} && r.ptr==s.data()+s.size() && n>=min && n<=max?n:-1;}
}
void annotate_project(json& record,const std::shared_ptr<FileStorage>& storage){
    record["drive_online"]=record["drive_id"].is_string()&&storage->drive_online(record["drive_id"].get<std::string>());
    for(auto& child:record["subprojects"])if(child.is_object())annotate_project(child,storage);
}
void configure_projects(httplib::Server& server,std::shared_ptr<ProjectStore> store,std::shared_ptr<FileStorage> storage, std::shared_ptr<Authorization> auth){
    auto handler=[store,storage](const httplib::Request& req,httplib::Response& res){
        auto reply=[&](int status,const json& body){res.status=status;res.set_content(body.dump(2)+"\n","application/json");};
        auto error=[&](int status,const char* code){reply(status,{{"error",code}});};
        const bool collection=req.path=="/projects",write=req.method=="POST" || req.method=="PATCH" || req.method=="DELETE";
        std::string id;
        if(!collection){id=req.matches[1].str();if(!std::regex_match(id,std::regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"))){error(400,"invalid_project_id");return;}}
        int limit=100,offset=0;bool deep=false;std::optional<std::string> parent_filter;
        for(const auto& [key,value]:req.params){
            if(write || req.get_param_value_count(key)!=1){error(400,"invalid_parameters");return;}
            if(!collection && key=="depth"){
                if(value!="shallow" && value!="deep"){error(400,"invalid_depth");return;}deep=value=="deep";continue;
            }
            if(collection && key=="parent_id"){
                if(value!="null" && !valid_id(value)){error(400,"invalid_parent_id");return;}parent_filter=value=="null"?"":value;continue;
            }
            if(!collection || (key!="limit" && key!="offset")){error(400,"invalid_parameters");return;}
            int n=number(value,key=="limit"?1:0,key=="limit"?500:1000000);if(n<0){error(400,"invalid_pagination");return;}(key=="limit"?limit:offset)=n;
        }
        json packet;
        if(write){
            const auto type=req.get_header_value("Content-Type");if(type.substr(0,type.find(';'))!="application/json"){error(415,"unsupported_media_type");return;}
            packet=json::parse(req.body,nullptr,false);if(!packet.is_object()){error(400,"invalid_packet");return;}
            if(req.method=="DELETE"){
                if(packet.size()!=1 || !packet.contains("destroy_directory") || !packet["destroy_directory"].is_boolean()){error(400,"invalid_destroy_directory");return;}
            }else{
                for(const auto& item:packet.items())if(item.key()!="name" && item.key()!="description" && item.key()!="github_url" && !(req.method=="POST" && (item.key()=="parent_id"||item.key()=="drive"))){error(400,"unknown_field");return;}
                if(packet.empty() || (req.method=="POST" && (!packet.contains("name") || !packet.contains("description")))){error(400,"missing_fields");return;}
                if(packet.contains("drive")&&(!packet["drive"].is_string()||!std::regex_match(packet["drive"].get<std::string>(),std::regex("[A-Z]+")))){error(400,"invalid_drive");return;}
                if(packet.contains("parent_id") && !packet["parent_id"].is_null() && (!packet["parent_id"].is_string() || !valid_id(packet["parent_id"].get<std::string>()))){error(400,"invalid_parent_id");return;}
                if(packet.contains("name") && !name_ok(packet["name"])){error(400,"invalid_name");return;}
                if(packet.contains("description") && (!packet["description"].is_string() || packet["description"].get<std::string>().size()>4000)){error(400,"invalid_description");return;}
                if(packet.contains("github_url") && !packet["github_url"].is_null() && (!packet["github_url"].is_string() || packet["github_url"].get<std::string>().size()>2048 || !std::regex_match(packet["github_url"].get<std::string>(),std::regex(R"(https://github\.com/[A-Za-z0-9_-]+/[A-Za-z0-9_.-]+/?)")))){error(400,"invalid_github_url");return;}
            }
        }
        try{
            std::unique_lock<std::shared_mutex> gate(storage->project_gate,std::defer_lock);
            std::shared_lock<std::shared_mutex> read_gate(storage->project_gate,std::defer_lock);if(write)gate.lock();else read_gate.lock();
            storage->initialize();
            if(req.method=="POST"){
                std::string drive=storage->store->default_drive();
                if(packet.contains("parent_id")&&!packet["parent_id"].is_null()){
                    auto parent=store->get(packet["parent_id"]);if(!parent){error(404,"parent_not_found");return;}drive=(*parent)["drive_id"];
                }
                if(packet.contains("drive")){
                    const auto chosen=storage->resolve_drive(packet["drive"]);
                    if(packet.contains("parent_id")&&!packet["parent_id"].is_null()&&chosen!=drive){error(409,"parent_drive_mismatch");return;}drive=chosen;
                }
                storage->require_drive_online(drive);packet.erase("drive");packet["drive_id"]=drive;
                auto record=store->create(packet);annotate_project(record,storage);res.set_header("Location","/projects/"+record["id"].get<std::string>());reply(201,record);
            }else if(req.method=="DELETE"){
                storage->initialize();if(packet["destroy_directory"]==true){auto project=store->get(id);if(project)storage->require_drive_online((*project)["drive_id"].get<std::string>());}
                auto files=store->remove(id,packet["destroy_directory"]);if(!files){error(404,"project_not_found");return;}
                for(const auto& file:*files){storage->erase_bytes(file.get<std::string>());storage->store->forget(file);}
                res.status=204;
            }else if(collection){auto page=store->list(limit,offset,parent_filter);for(auto& project:page["projects"])annotate_project(project,storage);reply(200,page);}
            else {auto record=req.method=="PATCH"?store->patch(id,packet):store->get(id,deep);if(!record){error(404,"project_not_found");return;}annotate_project(*record,storage);reply(200,*record);}
        }catch(const std::invalid_argument& e){const std::string code=e.what();error((code=="parent_not_found"||code=="drive_not_found")?404:code=="directory_too_long"?400:409,e.what());}
        catch(const DatabaseError&){error(503,"database_unavailable");}
        catch(const std::exception&){error(503,"storage_unavailable");}
    };
    server.Get(R"(/project/([^/]+)/files)",[store,storage](const httplib::Request& req,httplib::Response& res){
        auto reply=[&](int status,const json& body){res.status=status;res.set_content(body.dump(2)+"\n","application/json");};
        auto error=[&](int status,const char* code){reply(status,{{"error",code}});};
        const auto id=req.matches[1].str();if(!valid_id(id)){error(400,"invalid_project_id");return;}
        int limit=100,offset=0;
        for(const auto& [key,value]:req.params){
            if((key!="limit" && key!="offset") || req.get_param_value_count(key)!=1){error(400,"invalid_parameters");return;}
            int parsed=number(value,key=="limit"?1:0,key=="limit"?500:1000000);
            if(parsed<0){error(400,"invalid_pagination");return;}(key=="limit"?limit:offset)=parsed;
        }
        try{
            std::shared_lock<std::shared_mutex> gate(storage->project_gate);
            auto project=store->get(id);if(!project){error(404,"project_not_found");return;}
            storage->initialize();auto page=storage->store->list(limit,offset,(*project)["directory"].get<std::string>(),(*project)["drive_id"].get<std::string>());page["drive_id"]=(*project)["drive_id"];page["drive_letter"]=(*project)["drive_letter"];page["drive_online"]=storage->drive_online((*project)["drive_id"]);reply(200,page);
        }catch(const DatabaseError&){error(503,"database_unavailable");}
        catch(const std::exception&){error(503,"storage_unavailable");}
    });
    server.Get("/projects/explorer",[](const auto&,auto& response){response.set_content(std::string(project_explorer_html),"text/html; charset=utf-8");});
    server.Get("/projects",handler);server.Get(R"(/projects/([^/]+))",handler);
    body_route(server,"POST","/projects",handler,auth);body_route(server,"PATCH",R"(/projects/([^/]+))",handler,auth);body_route(server,"DELETE",R"(/projects/([^/]+))",handler,auth);
}
json project_endpoints(){
    json record={{"id","12345678-1234-1234-1234-123456789abc"},{"name","SPARK"},{"description","Cross-device intelligence hub"},{"directory","/projects/SPARK"},{"drive_id","0123456789abcdef0123456789abcdef"},{"drive_letter","A"},{"drive_online",true},{"parent_id",nullptr},{"subprojects",json::array()},{"github_url",nullptr},{"created_at","2026-09-22T00:00:00.000000Z"},{"updated_at","2026-09-22T00:00:00.000000Z"}};
    json endpoints=json::array();
    for(const std::string op:{"create","list","get","patch","delete"}){
        bool create=op=="create",list=op=="list",remove=op=="delete",write=create || remove || op=="patch";
        endpoints.push_back({{"method",create?"POST":remove?"DELETE":op=="patch"?"PATCH":"GET"},{"path",create || list?"/projects":"/projects/{id}"},
            {"description",create?"Create a root or subproject atomically. Optional parent_id puts its directory at {parent.directory}/{name}; roots use /projects/{name}. Parent is immutable. Existing directory contents retained. Optional drive letter selects storage for roots (default persistent drive); subprojects inherit their parent drive and reject a different one. Drive must be online and is immutable. Names unique among siblings on a drive; directories unique per drive, case-sensitive; max path 1024 UTF-8 bytes." :list?"List shallow project records ordered by name then ID; optional parent_id UUID selects immediate children, parent_id=null selects roots, omitted lists all projects. Follow pagination." :remove?"Delete project and ALL descendant project records; explicit destroy_directory boolean required. True requires the project drive online and recursively deletes its directory and files on that drive only; false retains them and is allowed offline. Already-open downloads may finish. Interrupted byte cleanup resumes on server restart." :op=="patch"?"Modify name, description or github_url. Directory stays fixed when renamed. null clears github_url. Omitted fields retained." :"Retrieve a project: depth=shallow (default) returns immediate child IDs in subprojects; depth=deep recursively returns full child records in subprojects, with no pagination or truncation. Each record includes parent_id (null for roots); children ordered by name then ID. Files are referenced via drive_id, drive_letter and directory. drive_online reports live availability; project details and boards remain usable offline."},
            {"input",{{"path",create || list?json::object():json{{"id","lowercase project UUID"}}},{"query",list?json{{"limit","1..500, default 100"},{"offset","0..1000000, default 0"},{"parent_id","optional parent UUID for immediate children; literal null for roots; omit for all"}}:op=="get"?json{{"depth","shallow (default) or deep"}}:json::object()},{"headers",write?json{{"Content-Type","application/json"}}:json::object()},
                {"body",remove?json{{"destroy_directory",false}}:write?json{{"name","SPARK"},{"description","Cross-device intelligence hub"},{"github_url",nullptr}}:json(nullptr)},
                {"fields",{{"name","required on create; nonblank UTF-8, max 200 bytes, no /, backslash, control characters, . or .."},{"description","required on create; string max 4000 UTF-8 bytes, may be empty"},{"github_url","optional null or HTTPS github.com/owner/repository URL, max 2048 bytes; stored only, not fetched or cloned"},{"drive","POST only: uppercase drive letter; roots default to persistent storage, children inherit their parent. Immutable after creation."},{"drive_online","read-only live drive availability, including on nested records"},{"parent_id","POST only: existing parent UUID or null for root; immutable thereafter"},{"subprojects","read-only immediate child IDs; full nested child records on GET depth=deep; empty array at leaves"},{"directory","server-managed, read-only logical path"},{"destroy_directory","DELETE only: required JSON boolean, recursively destroy directory and files if true"}}}}},
            {"output",{{"status",create?201:remove?204:200},{"body",remove?json(nullptr):list?json{{"projects",json::array({record})},{"limit",100},{"offset",0},{"has_more",false}}:record}}},
            {"errors",{{"400","invalid_project_id, invalid_parameters, invalid_pagination, invalid_packet, unknown_field, missing_fields, invalid_name, invalid_description, invalid_github_url, invalid_destroy_directory, invalid_parent_id, invalid_depth, directory_too_long, invalid_drive"},{"404","project_not_found, parent_not_found, drive_not_found"},{"409","project_conflict: sibling name or directory belongs to another project on this drive; parent_drive_mismatch"},{"415","unsupported_media_type"},{"503","database_unavailable, storage_unavailable; deletion may already be committed, pending bytes are recovered on restart"}}}});
    }
    endpoints.push_back({{"method","GET"},{"path","/projects/explorer"},{"description","Browser project explorer: view, create, edit and delete projects, choose directory retention, and open project files or GitHub links."},{"input",{{"body",nullptr},{"query",json::object()}}},{"output",{{"status",200},{"headers",{{"Content-Type","text/html; charset=utf-8"}}},{"body","HTML project explorer; JavaScript required"}}}});
    endpoints.push_back({{"method","GET"},{"path","/project/{id}/files"},{"description","List completed files directly in the project's stored directory. Filters by both project drive and directory, like GET /files?drive={project.drive_letter}&directory={project.directory}; returns drive_id, drive_letter and drive_online. Metadata remains readable when offline; excludes nested directories and subproject files. Uses the stable directory after renaming."},
        {"input",{{"path",{{"id","lowercase project UUID"}}},{"query",{{"limit","1..500; default 100"},{"offset","0..1000000; default 0"}}},{"body",nullptr}}},
        {"output",{{"status",200},{"headers",{{"Content-Type","application/json"}}},{"body",{{"files",json::array()},{"limit",100},{"offset",0},{"has_more",false}}},{"fields",{{"files","standard file records: id, name, directory, size_bytes, content_type, download_url, created_at; ordered by created_at then id"},{"has_more","another page exists; increment offset by limit"}}}}},
        {"errors",{{"400","invalid_project_id, invalid_parameters, invalid_pagination"},{"404","project_not_found"},{"503","database_unavailable, storage_unavailable"}}}});
    return endpoints;
}
}
