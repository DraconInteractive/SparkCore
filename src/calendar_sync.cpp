#include "calendar_sync.hpp"
#include "bounded_body.hpp"
#include <openssl/rand.h>
#include <charconv>
#include <set>
namespace spark {
namespace {
using json=nlohmann::json;
std::string operation_id() {
    unsigned char bytes[16];if(RAND_bytes(bytes,sizeof(bytes))!=1)throw std::runtime_error("random_unavailable");
    const char* hex="0123456789abcdef";std::string out;
    for(int i=0;i<16;++i){if(i==4||i==6||i==8||i==10)out+='-';out+=hex[bytes[i]>>4];out+=hex[bytes[i]&15];}return out;
}
uint64_t number(const httplib::Request& req,const char* key) {
    if(!req.has_param(key))return 0;
    if(req.get_param_value_count(key)!=1)throw std::invalid_argument("invalid_cursor");
    auto s=req.get_param_value(key);uint64_t value=0;auto parsed=std::from_chars(s.data(),s.data()+s.size(),value);
    if(parsed.ec!=std::errc()||parsed.ptr!=s.data()+s.size())throw std::invalid_argument("invalid_cursor");return value;
}
}
json calendar_sync_status(const json& state) {
    auto out=state;out.erase("pending");
    out["pending_operation"]=!state.value("pending",json(nullptr)).is_null();
    out["conflict_policy"]="coordinator_wins";return out;
}
void run_calendar_sync(const CalendarStore& store,const std::string& connection,const CalendarRemote& remote) {
    auto state=store.sync_state(connection);
    if(!state.value("enabled",false))throw std::invalid_argument("calendar_sync_disabled");
    try {
        const auto local=store.sync_info();const auto peer=remote("GET","/calendar-sync",nullptr);
        if(peer.at("protocol")!=1||peer.at("instance_id")==local.at("instance_id"))throw std::invalid_argument("invalid_sync_peer");
        if(state.contains("peer_instance")&&state["peer_instance"]!=peer["instance_id"])throw std::invalid_argument("peer_instance_changed");
        state["peer_instance"]=peer["instance_id"];state["status"]="running";state["last_started_at"]=time(nullptr);state.erase("last_error");store.sync_save(connection,state);
        auto finish_pending=[&]{
            if(state["pending"].is_null())return;
            auto pending=state["pending"];auto remote_result=remote("POST","/calendar-sync/apply",pending["remote_request"]);
            if(!remote_result.at("applied").get<bool>()){
                state["pending"]=nullptr;store.sync_save(connection,state);throw std::runtime_error("concurrent_changes_retry");
            }
            auto local_result=store.sync_apply(pending["local_request"]);
            // Even if a local edit won the CAS race, remember what reached the peer.
            // The next cycle sees that newer edit as a fresh local change.
            json baseline={{"data",pending["local_request"]["data"]},{"remote",remote_result["item"]["revision"]},
                {"local",local_result["applied"]==true?local_result["item"]["revision"]:pending["local_request"]["expected_revision"]}};
            auto next_state=state;next_state["pending"]=nullptr;next_state["processed"]=state.value("processed",0)+1;
            if(!pending["conflict"].is_null())next_state["conflict_count"]=state.value("conflict_count",0)+1;
            store.sync_finish(connection,pending["local_request"]["sync_id"],baseline,next_state,pending["conflict"]);
            state=std::move(next_state);
            if(!local_result.at("applied").get<bool>())throw std::runtime_error("concurrent_changes_retry");
        };
        finish_pending();
        std::set<std::string> ids;
        auto local_cursor=state.value("local_cursor",uint64_t(0)),remote_cursor=state.value("remote_cursor",uint64_t(0));
        uint64_t local_through=store.sync_info()["sequence"],remote_through=peer.at("sequence");
        auto gather=[&](bool other,uint64_t cursor,uint64_t through){
            if(cursor>through)throw std::runtime_error("sync_history_rewound");
            while(cursor<through){
                auto page=other?remote("GET","/calendar-sync/changes?after="+std::to_string(cursor)+"&through="+std::to_string(through),nullptr):store.sync_changes(cursor,through);
                if(page.at("instance_id")!=(other?peer["instance_id"]:local["instance_id"]))throw std::runtime_error("peer_instance_changed");
                for(const auto& item:page.at("items"))ids.insert(item.at("sync_id").get<std::string>());
                auto next=page.at("next").get<uint64_t>();if(next<=cursor||next>through)throw std::runtime_error("invalid_sync_page");cursor=next;
                if(!page.at("has_more").get<bool>())break;
            }
        };
        gather(false,local_cursor,local_through);gather(true,remote_cursor,remote_through);
        state["processed"]=0;state["pending_changes"]=ids.size();store.sync_save(connection,state);
        for(const auto& id:ids){
            auto ours=store.sync_item(id),theirs=remote("GET","/calendar-sync/items/"+id,nullptr),base=store.sync_baseline(connection,id);
            bool local_changed=ours["data"]!=base["data"],remote_changed=theirs["data"]!=base["data"];
            auto chosen=local_changed?ours["data"]:remote_changed?theirs["data"]:ours["data"];
            auto op=operation_id();json conflict=nullptr;
            if(local_changed&&remote_changed&&ours["data"]!=theirs["data"])conflict={{"operation_id",op},{"sync_id",id},{"recorded_at",time(nullptr)},{"policy","coordinator_wins"},{"coordinator",ours},{"peer",theirs},{"baseline",base["data"]}};
            auto request=[&](const json& instance,const json& revision){
                return json{{"instance_id",instance},{"coordinator_id",local["instance_id"]},{"operation_id",op},{"sync_id",id},{"expected_revision",revision},{"data",chosen}};
            };
            // Write ahead before either side is modified. Receipts replay the exact result.
            state["pending"]={{"remote_request",request(peer["instance_id"],theirs["revision"])},{"local_request",request(local["instance_id"],ours["revision"])},{"conflict",conflict}};
            store.sync_save(connection,state);finish_pending();
            state["pending_changes"]=state["pending_changes"].get<size_t>()-1;store.sync_save(connection,state);
        }
        state["local_cursor"]=local_through;state["remote_cursor"]=remote_through;state["status"]="succeeded";state["last_success_at"]=time(nullptr);state["pending_changes"]=0;store.sync_save(connection,state);
    }catch(const std::exception& e){
        state["status"]="failed";state["last_error"]=e.what();store.sync_save(connection,state);throw;
    }
}
void configure_calendar_sync(httplib::Server& server,std::shared_ptr<CalendarStore> store,std::shared_ptr<Authorization> auth) {
    auto handle=[store](const httplib::Request& req,httplib::Response& res){
        auto reply=[&](int status,const json& data){res.status=status;res.set_header("Cache-Control","no-store");res.set_content(data.dump(),"application/json");};
        try{
            if(req.path=="/calendar-sync/changes"){
                for(const auto& p:req.params)if(p.first!="after"&&p.first!="through")throw std::invalid_argument("unknown_filter");
                reply(200,store->sync_changes(number(req,"after"),number(req,"through")));return;
            }
            if(!req.params.empty())throw std::invalid_argument("unknown_filter");
            if(req.path=="/calendar-sync"){reply(200,store->sync_info());return;}
            if(req.path=="/calendar-sync/apply"){
                const auto type=req.get_header_value("Content-Type");
                if(type.substr(0,type.find(';'))!="application/json"){reply(415,{{"error","unsupported_media_type"}});return;}
                reply(200,store->sync_apply(json::parse(req.body)));return;
            }
            auto id=req.matches[1].str();
            if(!std::regex_match(id,std::regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")))throw std::invalid_argument("invalid_sync_id");
            reply(200,store->sync_item(id));
        }catch(const std::invalid_argument& e){reply(400,{{"error",e.what()}});}
        catch(const json::exception&){reply(400,{{"error","invalid_packet"}});}
        catch(const DatabaseError&){reply(503,{{"error","database_unavailable"}});}
    };
    for(auto path:{"/calendar-sync","/calendar-sync/changes",R"(/calendar-sync/items/([^/]+))"})server.Get(path,handle);
    body_route(server,"POST","/calendar-sync/apply",handle,auth,65536);
}
json calendar_sync_endpoints() {
    json out=json::array();
    auto add=[&](const char* method,const char* path,const char* description,json input,json output){
        out.push_back({{"method",method},{"path",path},{"description",description},{"input",{{"body",input}}},{"output",{{"status",200},{"body",output}}},
            {"errors",{{"400","invalid_sync_packet, invalid_sync_data, invalid_cursor, instance_identity_mismatch, operation_id_reused"},{"401","unauthorized"},{"403","forbidden"},{"503","database_unavailable"}}}});
    };
    add("GET","/calendar-sync","Sync protocol version, persistent instance ID and committed shared-calendar sequence. Requires calendar.sync.read.",nullptr,{{"protocol",1},{"instance_id","UUID"},{"sequence",0}});
    add("GET","/calendar-sync/changes","Shared changes and tombstones only, 50 per page. after is exclusive; through pins the scan upper bound. Persist the checkpoint only after applying changes. Requires calendar.sync.read.",nullptr,{{"instance_id","UUID"},{"items",json::array()},{"next",0},{"through",0},{"has_more",false}});
    out.back()["input"]["query"]={{"after","unsigned cursor; default 0"},{"through","optional upper bound from sequence; default latest"}};
    add("GET","/calendar-sync/items/{id}","Latest shared sync item; unknown ID returns revision=null and data=null. Sync IDs differ from local event IDs. Requires calendar.sync.read.",nullptr,{{"sync_id","UUID"},{"revision",nullptr},{"sequence",0},{"data",nullptr}});
    add("POST","/calendar-sync/apply","Apply one shared change atomically with notification and durable idempotency receipt. Requires calendar.sync.write. expected_revision is an optimistic concurrency guard; applied=false means retry after fetching current data. data=null is deletion. Reminder delivery history never crosses instances.",{{"instance_id","target UUID"},{"coordinator_id","coordinator UUID"},{"operation_id","unique UUID"},{"sync_id","shared UUID"},{"expected_revision",nullptr},{"data",nullptr}},{{"applied",true},{"item",{{"sync_id","UUID"},{"revision","UUID"},{"sequence",1},{"data",nullptr}}}});
    out.back()["input"]["headers"]={{"Content-Type","application/json"}};
    out.back()["input"]["fields"]={{"data","null or full event fields: name, description, occurs_at, place, recurrence, reminder_minutes. Same validation as POST /events; excludes local ID, calendar, timestamps and reminder history."}};
    return out;
}
}
