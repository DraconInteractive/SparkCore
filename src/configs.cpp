#include "configs.hpp"
#include "config_explorer.hpp"
#include "bounded_body.hpp"
#include <charconv>
#include <set>
namespace spark {
namespace {
using json=nlohmann::json;
void valid(bool ok,const char* error){if(!ok)throw std::invalid_argument(error);}
uint64_t integer(const std::string& text,uint64_t minimum,uint64_t maximum,const char* error){
    uint64_t value=0;auto r=std::from_chars(text.data(),text.data()+text.size(),value);
    valid(r.ec==std::errc()&&r.ptr==text.data()+text.size()&&value>=minimum&&value<=maximum,error);return value;
}
void text(const json& value,size_t maximum,const char* error,bool nonempty=false){
    valid(value.is_string(),error);auto s=value.get<std::string>();
    valid(s.size()<=maximum&&s.find('\0')==std::string::npos&&(!nonempty||s.find_first_not_of(" \r\n\t")!=std::string::npos),error);
}
json tags(const json& values){
    valid(values.is_array()&&values.size()<=32,"invalid_tags");std::set<std::string> unique;
    for(const auto& value:values){
        valid(value.is_string()&&std::regex_match(value.get<std::string>(),std::regex("[A-Za-z0-9][A-Za-z0-9_.:-]{0,63}")),"invalid_tags");
        valid(unique.insert(value.get<std::string>()).second,"invalid_tags");
    }
    return unique;
}
json parse(const std::string& body){
    std::vector<std::set<std::string>> keys;
    auto callback=[&](int depth,json::parse_event_t event,json& value){
        valid(depth<=64,"json_too_deep");
        if(event==json::parse_event_t::object_start)keys.emplace_back();
        else if(event==json::parse_event_t::object_end)keys.pop_back();
        else if(event==json::parse_event_t::key)valid(keys.back().insert(value.get<std::string>()).second,"duplicate_json_key");
        return true;
    };
    return json::parse(body,callback);
}
}
void configure_configs(httplib::Server& server,std::shared_ptr<ConfigStore> store,std::shared_ptr<Authorization> auth){
    server.Get("/configs/explorer",[](const auto&,auto& response){response.set_content(config_explorer_html,"text/html; charset=utf-8");});
    auto handler=[store](const httplib::Request& req,httplib::Response& res){
        auto reply=[&](int status,const json& body){res.status=status;res.set_header("Cache-Control","no-store");res.set_content(body.dump(),"application/json");};
        try{
            const bool collection=req.path=="/configs",get=req.method=="GET"||req.method=="HEAD";
            const bool content=!collection&&req.path.size()>8&&req.path.substr(req.path.size()-8)=="/content";
            std::string id;
            if(!collection){
                id=req.matches[1].str();
                valid(std::regex_match(id,std::regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")),"invalid_config_id");
            }
            if(get&&collection){
                json filters=json::object();json selected=json::array();
                for(const auto& p:req.params){
                    if(p.first=="tag"){selected.push_back(p.second);continue;}
                    valid(p.first=="name"||p.first=="limit"||p.first=="offset","unknown_filter");
                    valid(req.get_param_value_count(p.first)==1,"duplicate_filter");
                    if(p.first=="name"){text(p.second,200,"invalid_name",true);filters["name"]=p.second;}
                    else filters[p.first]=integer(p.second,p.first=="limit"?1:0,p.first=="limit"?100:1000000,"invalid_pagination");
                }
                filters["tags"]=tags(selected);reply(200,store->list(filters));return;
            }
            if(req.method=="DELETE"){
                valid(req.params.size()==1&&req.get_param_value_count("revision")==1,"revision_required");
                valid(req.body.empty(),"unexpected_body");
                const auto revision=integer(req.get_param_value("revision"),1,9007199254740991ULL,"invalid_revision");
                if(store->remove(id,revision))res.status=204;else reply(404,{{"error","config_not_found"}});
                return;
            }
            valid(req.params.empty(),"unknown_filter");
            if(get){
                auto record=store->get(id);if(!record){reply(404,{{"error","config_not_found"}});return;}
                const auto etag="\"config-"+id+"-"+std::to_string((*record)["revision"].get<uint64_t>())+(content?"-content\"":"-record\"");
                res.set_header("ETag",etag);
                if(req.get_header_value("If-None-Match")==etag){res.status=304;return;}
                reply(200,content?(*record)["data"]:*record);return;
            }
            auto type=req.get_header_value("Content-Type");
            if(type.substr(0,type.find(';'))!="application/json"){reply(415,{{"error","unsupported_media_type"}});return;}
            auto p=parse(req.body);valid(p.is_object(),"invalid_packet");
            for(const auto& f:p.items())valid(f.key()=="name"||f.key()=="description"||f.key()=="tags"||f.key()=="data"||f.key()=="source_device"||(!collection&&f.key()=="revision"),"unknown_field");
            if(collection)valid(p.contains("name")&&p.contains("data"),"missing_fields");
            else{
                valid(p.contains("revision"),"revision_required");
                valid(p["revision"].is_number_integer()&&p["revision"]>=1&&p["revision"]<=9007199254740991ULL,"invalid_revision");
                valid(p.contains("name")||p.contains("description")||p.contains("tags")||p.contains("data"),"missing_fields");
            }
            if(p.contains("name"))text(p["name"],200,"invalid_name",true);
            if(p.contains("description"))text(p["description"],4000,"invalid_description");
            if(p.contains("tags"))p["tags"]=tags(p["tags"]);
            if(p.contains("data")){
                valid(p["data"].is_object(),"invalid_data");
                if(p["data"].dump().size()>65536){reply(413,{{"error","config_too_large"}});return;}
            }
            if(p.contains("source_device")&&!p["source_device"].is_null())valid(p["source_device"].is_string()&&std::regex_match(p["source_device"].get<std::string>(),std::regex("[A-Za-z0-9_-]{1,64}")),"invalid_source_device");
            if(collection){
                auto record=store->create(p);res.set_header("Location","/configs/"+record["id"].get<std::string>());reply(201,record);
            }else{
                auto record=store->update(id,p);if(record)reply(200,*record);else reply(404,{{"error","config_not_found"}});
            }
        }catch(const ConfigConflict& e){reply(409,{{"error","revision_conflict"},{"current_revision",e.revision}});}
        catch(const std::invalid_argument& e){reply(std::string(e.what())=="revision_exhausted"?409:400,{{"error",e.what()}});}
        catch(const json::exception&){reply(400,{{"error","invalid_json"}});}
        catch(const DatabaseError&){reply(503,{{"error","database_unavailable"}});}
    };
    server.Get("/configs",handler);server.Get(R"(/configs/([^/]+)/content)",handler);server.Get(R"(/configs/([^/]+))",handler);
    body_route(server,"POST","/configs",handler,auth,131072);
    body_route(server,"PATCH",R"(/configs/([^/]+))",handler,auth,131072);
    body_route(server,"DELETE",R"(/configs/([^/]+))",handler,auth);
}
json config_endpoints(){
    json record={{"id","12345678-1234-1234-1234-123456789abc"},{"name","watch-display"},{"description","Watch settings"},{"tags",{"production","watch"}},{"revision",1},{"data",{{"brightness",70},{"clock_format","24h"}}},{"created_at","2026-10-27T00:00:00.000000Z"},{"updated_at","2026-10-27T00:00:00.000000Z"}};
    json metadata=record;metadata.erase("data");json out=json::array();
    auto add=[&](const char* method,const char* path,const char* description,json input,json output,int status=200){
        out.push_back({{"method",method},{"path",path},{"description",description},
            {"input",{{"headers",{{"Content-Type","application/json"}}},{"body",input}}},
            {"output",{{"status",status},{"body",output}}},
            {"errors",{{"400","invalid_config_id, invalid_json, duplicate_json_key, json_too_deep, invalid_packet, missing_fields, unknown_field, invalid_name, invalid_description, invalid_tags, invalid_data, invalid_source_device, revision_required, invalid_revision, unknown_filter, duplicate_filter, invalid_pagination, unexpected_body"},
                {"401","unauthorized"},{"403","forbidden, device_identity_mismatch"},{"404","config_not_found"},{"409","revision_conflict (includes current_revision), revision_exhausted"},{"413","config_too_large, payload_too_large"},{"415","unsupported_media_type"},{"503","database_unavailable"}}}});
    };
    add("POST","/configs","Declare a config with default JSON. Creates a new stable UUID; names/tags need not be unique. Devices should discover/reuse an existing ID on subsequent starts, rather than re-posting or overwriting human edits. configs.write required.",{{"name","watch-display"},{"description","Watch settings"},{"tags",{"watch","production"}},{"data",record["data"]}},record,201);
    out.back()["input"]["fields"]={{"name","required nonblank text, max 200 bytes"},{"description","optional text, max 4000 bytes; default empty"},{"tags","optional array of up to 32 unique case-sensitive tags, 1–64 characters each: letters, digits, _, ., :, -; first character alphanumeric. Stored sorted."},{"data","required JSON object; max 64 KiB compact UTF-8 JSON; request max 128 KiB; nesting max 64 levels; duplicate object keys rejected"},{"source_device","optional device attribution; existing ownership/delegation checks apply"}};
    add("GET","/configs","List metadata without data, oldest first. Exact name and all requested tags must match; multiple matches are returned explicitly. configs.read required.",nullptr,{{"configs",json::array({metadata})},{"limit",50},{"offset",0},{"has_more",false}});
    out.back()["input"]["query"]={{"name","optional exact, case-sensitive name"},{"tag","repeat for AND matching, e.g. tag=watch&tag=production"},{"limit","1..100; default 50"},{"offset","0..1000000; default 0"}};
    add("GET","/configs/{id}","Read full config with revision and ETag. Exact If-None-Match returns 304 when unchanged. configs.read required.",nullptr,record);
    add("PATCH","/configs/{id}","Revision-checked update: name, description, tags and/or data. data replaces the entire object; no deep merge. Stale revision returns 409. Identical edits leave revision and notifications unchanged. configs.write required.",{{"revision",1},{"data",{{"brightness",80},{"clock_format","24h"}}}},record);
    out.back()["output"]["body"]["revision"]=2;out.back()["output"]["body"]["data"]["brightness"]=80;
    out.back()["input"]["fields"]={{"revision","required current integer revision, 1..9007199254740991"},{"name, description, tags, data","optional editable fields, same validation as POST; at least one required"},{"source_device","optional device attribution"}};
    add("DELETE","/configs/{id}","Delete only if the supplied revision is current. Emits config.deleted with final revision (previous + 1). configs.write required. No body.",nullptr,nullptr,204);
    out.back()["input"]["query"]={{"revision","required current revision"}};
    add("GET","/configs/{id}/content","Raw JSON data only, application/json. ETag and exact If-None-Match supported. Uses configs.read, not files.read. Metadata/revision available from GET /configs/{id}.",nullptr,record["data"]);
    add("GET","/configs/explorer","Human config manager: tag/name filters, JSON editor, revision-safe save/delete, JSON data import/export. configs.read required; controls require configs.write.",nullptr,"HTML");
    return out;
}
}
