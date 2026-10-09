#include "commands.hpp"
#include "command_explorer.hpp"
#include "bounded_body.hpp"
#include <regex>
#include <set>
#include <charconv>
#include <iostream>
namespace spark {
namespace {
using nlohmann::json;
bool matches(const json& value,const char* pattern){return value.is_string()&&std::regex_match(value.get<std::string>(),std::regex(pattern));}
bool device_id(const json& value){return matches(value,"[A-Za-z0-9_-]{1,64}");}
void require(bool condition,const char* error){if(!condition)throw CommandError(400,error);}
void fields(const json& p,const std::set<std::string>& allowed){for(const auto& f:p.items())require(allowed.count(f.key()),"unknown_field");}
void lease(const json& p){if(p.contains("lease_seconds"))require(p["lease_seconds"].is_number_integer()&&p["lease_seconds"]>=1&&p["lease_seconds"]<=3600,"invalid_lease_seconds");}
void reply(httplib::Response& r,int status,const json& body){r.status=status;r.set_content(body.dump(2)+"\n","application/json");}
}
CommandScheduler::CommandScheduler(std::shared_ptr<CommandStore> store):worker_([this,store]{
    std::unique_lock<std::mutex> lock(mutex_);bool failed=false;
    while(!stopping_){lock.unlock();try{store->expire();if(failed)std::cerr<<"Command scheduler recovered\n";failed=false;}catch(const std::exception& e){if(!failed)std::cerr<<"Command scheduler will retry: "<<e.what()<<'\n';failed=true;}lock.lock();wake_.wait_for(lock,std::chrono::seconds(1),[this]{return stopping_;});}
}){}
CommandScheduler::~CommandScheduler(){{std::lock_guard<std::mutex> lock(mutex_);stopping_=true;}wake_.notify_all();worker_.join();}
void configure_commands(httplib::Server& server,std::shared_ptr<CommandStore> store, std::shared_ptr<Authorization> auth){
    server.Get("/commands/explorer",[](const auto&,auto& response){response.set_content(command_explorer_html,"text/html; charset=utf-8");});
    server.Get("/commands/devices",[store](const httplib::Request& req,httplib::Response& res){
        try {
            int limit=100,offset=0;
            for(const auto& [key,value]:req.params){
                require((key=="limit"||key=="offset")&&req.get_param_value_count(key)==1,"invalid_query");
                int number=0;auto parsed=std::from_chars(value.data(),value.data()+value.size(),number);
                require(parsed.ec==std::errc{}&&parsed.ptr==value.data()+value.size()&&number>=(key=="limit"?1:0)&&number<=(key=="limit"?100:1000000000),"invalid_pagination");
                (key=="limit"?limit:offset)=number;
            }
            reply(res,200,store->providers(limit,offset));
        }catch(const CommandError& e){reply(res,e.status,{{"error",e.what()}});}catch(const DatabaseError&){reply(res,503,{{"error","database_unavailable"}});}
    });
    auto handler=[store,auth](const httplib::Request& req,httplib::Response& res){
        try{
            const bool read=req.method=="GET"||req.method=="HEAD";
            const bool actions=req.path.size()>=8&&req.path.substr(req.path.size()-8)=="/actions";
            const bool claim=req.path.size()>=15&&req.path.substr(req.path.size()-15)=="/commands/claim";
            const bool collection=req.path=="/commands";
            std::string id;if(!collection){id=req.matches[1].str();require(actions||claim?device_id(id):matches(id,"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),"invalid_id");}
            if(!(collection&&read))require(req.params.empty(),"invalid_query");
            json p=json::object();
            if(!read){const auto type=req.get_header_value("Content-Type");if(type.substr(0,type.find(';'))!="application/json")throw CommandError(415,"unsupported_media_type");p=json::parse(req.body,nullptr,false);require(p.is_object(),"invalid_packet");}
            std::optional<std::string> administrator;
            if(auth&&!read&&!p.contains("source_device")&&(collection||(!actions&&!claim&&req.matches.size()>2&&req.matches[2].str()=="cancel"))){
                auto who=auth->identity(req,res);if(!who)return;
                if(who->kind=="administrator")administrator=who->subject;
            }
            if(actions){
                if(!read){fields(p,{"actions"});require(p.contains("actions")&&p["actions"].is_array()&&p["actions"].size()<=128,"invalid_actions");std::set<std::string> seen;for(const auto& action:p["actions"]){require(matches(action,"[A-Za-z0-9][A-Za-z0-9_.:-]{0,99}"),"invalid_action");require(seen.insert(action.get<std::string>()).second,"duplicate_action");}}
                reply(res,200,store->actions(id,read?json(nullptr):p["actions"]));
            }else if(claim){
                fields(p,{"lease_seconds","command_id"});lease(p);
                std::optional<std::string> command_id;
                if(p.contains("command_id")){
                    require(matches(p["command_id"],"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"),"invalid_command_id");
                    command_id=p["command_id"].get<std::string>();
                }
                reply(res,200,store->claim(id,p.value("lease_seconds",60),command_id));
            }
            else if(collection&&!read){
                fields(p,{"source_device","target_device","action","input","idempotency_key","expires_in_seconds"});
                require(administrator.has_value()||(p.contains("source_device")&&device_id(p["source_device"])),"invalid_source_device");
                require(!p.contains("target_device")||p["target_device"].is_null()||device_id(p["target_device"]),"invalid_target_device");
                require(p.contains("action")&&matches(p["action"],"[A-Za-z0-9][A-Za-z0-9_.:-]{0,99}"),"invalid_action");
                require(!p.contains("input")||p["input"].is_object(),"invalid_input");
                require(p.contains("idempotency_key")&&matches(p["idempotency_key"],"[A-Za-z0-9_.:-]{1,128}"),"invalid_idempotency_key");
                if(p.contains("expires_in_seconds"))require(p["expires_in_seconds"].is_number_integer()&&p["expires_in_seconds"]>=1&&p["expires_in_seconds"]<=604800,"invalid_expiry");
                auto record=store->create(p,administrator);res.set_header("Location","/commands/"+record["id"].get<std::string>());reply(res,202,record);
            }else if(collection){
                for(const auto& [key,value]:req.params){require(req.get_param_value_count(key)==1,"duplicate_parameter");if(key=="limit"||key=="offset"){int number=0;auto parsed=std::from_chars(value.data(),value.data()+value.size(),number);require(parsed.ec==std::errc{}&&parsed.ptr==value.data()+value.size()&&number>=(key=="limit"?1:0)&&number<=(key=="limit"?100:1000000000),"invalid_pagination");p[key]=number;}
                    else if(key=="source_device"||key=="target_device"||key=="executing_device"){require(device_id(value),"invalid_device_id");p[key]=value;}
                    else if(key=="status"){require(std::set<std::string>{"queued","running","succeeded","failed","expired","cancelled","outcome_unknown"}.count(value),"invalid_status");p[key]=value;}
                    else require(false,"invalid_query");}
                reply(res,200,store->list(p));
            }else if(read)reply(res,200,store->get(id));
            else {
                std::string operation=req.matches[2].str();
                if(operation=="cancel"){fields(p,{"source_device"});require(administrator.has_value()||(p.contains("source_device")&&device_id(p["source_device"])),"invalid_source_device");}
                else {
                    fields(p,operation=="renew"?std::set<std::string>{"device_id","claim_token","lease_seconds"}:std::set<std::string>{"device_id","claim_token","status","result","error"});
                    require(p.contains("device_id")&&device_id(p["device_id"]),"invalid_device_id");
                    require(p.contains("claim_token")&&matches(p["claim_token"],"[0-9a-f]{64}"),"invalid_claim_token");
                    if(operation=="renew")lease(p);
                    else {require(p.contains("status")&&(p["status"]=="succeeded"||p["status"]=="failed"),"invalid_status");
                        if(p["status"]=="succeeded")require(!p.contains("error")||p["error"].is_null(),"invalid_error");
                        else {require(!p.contains("result")||p["result"].is_null(),"invalid_result");require(p.contains("error")&&p["error"].is_object()&&p["error"].contains("code")&&matches(p["error"]["code"],"[A-Za-z0-9_.:-]{1,100}"),"invalid_error");}}
                }
                reply(res,200,store->transition(id,operation,p,administrator));
            }
        }catch(const CommandError& e){reply(res,e.status,{{"error",e.what()}});}catch(const DatabaseError&){reply(res,503,{{"error","database_unavailable"}});}
    };
    server.Get(R"(/devices/([^/]+)/actions)",handler);body_route(server,"PUT",R"(/devices/([^/]+)/actions)",handler,auth);
    body_route(server,"POST",R"(/devices/([^/]+)/commands/claim)",handler,auth);
    server.Get("/commands",handler);body_route(server,"POST","/commands",handler,auth);
    server.Get(R"(/commands/([^/]+))",handler);
    body_route(server,"POST",R"(/commands/([^/]+)/(renew|complete|cancel))",handler,auth);
}
json command_endpoints(){
    json result=json::array();
    const json command={{"id","12345678-1234-1234-1234-123456789abc"},{"source_device","watch"},{"source_admin",nullptr},{"target_device",nullptr},{"executing_device",nullptr},{"action","myservice.status"},{"input",json::object()},{"idempotency_key","watch-request-1042"},{"expires_in_seconds",300},{"status","queued"},{"result",nullptr},{"error",nullptr},{"created_at","2026-10-06T00:00:00.000000Z"},{"updated_at","2026-10-06T00:00:00.000000Z"},{"expires_at","2026-10-06T00:05:00.000000Z"},{"lease_expires_at",nullptr}};
    auto add=[&](const char* method,const char* path,const char* description,json input,json output,int status=200,json fields=json::object(),json query=json::object()){
        result.push_back({{"method",method},{"path",path},{"description",description},{"input",{{"headers",std::string(method)=="GET"?json::object():json{{"Content-Type","application/json"}}},{"query",query},{"body",input},{"fields",fields},{"validation","16 KiB JSON body maximum; unknown top-level fields rejected. Device IDs: 1-64 ASCII letters, digits, underscores or hyphens. Command IDs: lowercase UUID."}}},{"output",{{"status",status},{"headers",{{"Content-Type","application/json"},{"Cache-Control","no-store"}}},{"body",output}}},{"errors",{{"400","invalid_packet, unknown_field, invalid_id, invalid_command_id, invalid_action, invalid_actions, duplicate_action, invalid_source_device, invalid_target_device, invalid_device_id, invalid_input, invalid_idempotency_key, invalid_expiry, invalid_lease_seconds, invalid_claim_token, invalid_status, invalid_result, invalid_error, invalid_query, duplicate_parameter, invalid_pagination"},{"403","invalid_claim, source_device_mismatch"},{"404","device_not_found, command_not_found"},{"409","idempotency_conflict, invalid_command_state, completion_conflict"},{"413","payload_too_large"},{"415","unsupported_media_type"},{"503","database_unavailable"}}}});
    };
    add("GET","/devices/{id}/actions","Read the registered device's sorted action names.",nullptr,{{"device_id","windows-pc"},{"actions",json::array({"myservice.status"})}});
    add("PUT","/devices/{id}/actions","Atomically replace action names. Empty list withdraws all actions. After registering, claim pending work even without a TCP notification.",{{"actions",json::array({"myservice.start","myservice.stop","myservice.status"})}},{{"device_id","windows-pc"},{"actions",json::array({"myservice.start","myservice.status","myservice.stop"})}},200,{{"actions","Required array: 0..128 unique names, case sensitive, [A-Za-z0-9][A-Za-z0-9_.:-]{0,99}. Registration required; deletion of device removes advertisements."}});
    add("POST","/commands","Queue an action. Omit target_device or use null for first eligible claimant. Targeted commands can only be claimed by that registered device advertising the action. 202 means accepted, not executed. Same source/key and normalized packet returns original command; changed packet returns 409.",{{"source_device","watch"},{"action","myservice.status"},{"input",json::object()},{"idempotency_key","watch-request-1042"},{"expires_in_seconds",300}},command,202,{{"source_device","Registered requester ID for devices; administrators may omit to use their authenticated subject. source_admin is server-derived and read-only."},{"target_device","Optional registered target ID or null"},{"action","Required case-sensitive advertised action name"},{"input","Optional object; default {}. Executor validates action-specific input."},{"idempotency_key","Required [A-Za-z0-9_.:-]{1,128}; unique per requester; retain across retries."},{"expires_in_seconds","Optional integer 1..604800; default 300. Deadline for claiming only; running work uses lease expiry."}});
    add("GET","/commands","List command history, newest first. Filters combine with AND. Refresh requester history after reconnect; offset pagination is not a snapshot.",nullptr,{{"commands",json::array({command})},{"limit",100},{"offset",0}},200,json::object(),{{"source_device","optional requester ID"},{"target_device","optional explicit target ID"},{"executing_device","optional actual executor ID"},{"status","queued, running, succeeded, failed, expired, cancelled, outcome_unknown"},{"limit","1..100, default 100"},{"offset","0..1000000000, default 0"}});
    add("GET","/commands/{id}","Read state and result. claim_token is never included. Stored history survives device deletion.",nullptr,command);
    auto claimed=command;claimed["status"]="running";claimed["executing_device"]="windows-pc";claimed["lease_expires_at"]="2026-10-06T00:01:00.000000Z";claimed["claim_token"]=std::string(64,'a');
    add("POST","/devices/{id}/commands/claim","Atomically claim the specified command_id, or the oldest eligible queued command when omitted. Eligibility and target restrictions always apply. Returns {command:null} if requested work is unavailable, ineligible, expired, already claimed or nonexistent; never falls back to another command when an ID is supplied. Only winner executes. Save token before executing; lost claim response is not reissued. Check after reconnect, action registration and periodically.",{{"command_id",command["id"]},{"lease_seconds",60}},{{"command",claimed}},200,{{"command_id","Optional lowercase UUID. Claim only this command; omit for oldest eligible work. Null is invalid."},{"lease_seconds","Optional integer 1..3600, default 60"}});
    json credentials={{"device_id","windows-pc"},{"claim_token",std::string(64,'a')}};
    auto renewal=credentials;renewal["lease_seconds"]=60;
    auto running=claimed;running.erase("claim_token");
    add("POST","/commands/{id}/renew","Extend an active lease from server time. Token and executor must match. Expired leases become outcome_unknown and cannot renew or complete; commands are never automatically reassigned.",renewal,running,200,{{"device_id","Required executor ID"},{"claim_token","Required 64 lowercase hex characters from claim"},{"lease_seconds","Optional integer 1..3600, default 60"}});
    auto completion=credentials;completion["status"]="succeeded";completion["result"]={{"state","running"}};auto completed=running;completed["status"]="succeeded";completed["result"]=completion["result"];
    add("POST","/commands/{id}/complete","Complete a running command with its token. Identical completion retries are idempotent; differing results return 409. Unsupported actions should fail with error.code=unsupported_action.",completion,completed,200,{{"device_id","Required executor ID"},{"claim_token","Required claim token"},{"status","Required succeeded or failed"},{"result","Any JSON result on success; default null"},{"error","On failure required object with code matching [A-Za-z0-9_.:-]{1,100}; optional message/details. Result must be null. On success error must be absent or null."}});
    auto cancelled=command;cancelled["status"]="cancelled";
    add("POST","/commands/{id}/cancel","Requester cancels queued work only. Already cancelled is idempotent; running and other terminal states return 409. In token mode, source_device must match the caller or require devices.delegate.",{{"source_device","watch"}},cancelled);
    add("GET","/commands/devices","List registered devices advertising at least one action, ordered by device ID. Advertisements do not imply the device is online.",nullptr,{{"devices",json::array({{{"id","windows-pc"},{"local_ip","192.168.4.10"},{"external_ip",nullptr},{"actions",json::array({"myservice.status"})}}})},{"limit",100},{"offset",0}},200,json::object(),{{"limit","1..100, default 100"},{"offset","0..1000000000, default 0"}});
    add("GET","/commands/explorer","Human command explorer: advertised devices, JSON input, targeted one-shot submission and result polling. In token mode uses the signed-in administrator; open mode registers a browser requester with local_ip 0.0.0.0.",nullptr,"HTML command explorer");
    result.back()["output"]["headers"]["Content-Type"]="text/html; charset=utf-8";
    return result;
}
}
