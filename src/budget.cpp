#include "budget.hpp"
#include "budget_explorer.hpp"
#include "bounded_body.hpp"
#include "logging.hpp"
#include <charconv>
#include <ctime>
#include <set>
namespace spark {
using json=nlohmann::json;
namespace {
void require(bool ok,const std::string& field){if(!ok)throw std::invalid_argument(field);}
void text(const json& j,size_t max,const char* field,bool nonempty=false){require(j.is_string(),field);auto s=j.get<std::string>();require(s.size()<=max&&s.find('\0')==std::string::npos&&(!nonempty||s.find_first_not_of(" \r\n\t")!=std::string::npos),field);}
int64_t number(const json& j,int64_t lo,int64_t hi,const char* field){require(j.is_number_integer()&&j>=lo&&j<=hi,field);return j.get<int64_t>();}
int64_t query_number(const std::string& s,int64_t lo,int64_t hi){int64_t v=0;auto r=std::from_chars(s.data(),s.data()+s.size(),v);require(r.ec==std::errc()&&r.ptr==s.data()+s.size()&&v>=lo&&v<=hi,"invalid_integer");return v;}
int month_days(int y,int m){static const int d[]={31,28,31,30,31,30,31,31,30,31,30,31};return d[m-1]+(m==2&&y%4==0&&(y%100!=0||y%400==0));}
std::string make_date(int y,int m,int d){char out[16];std::snprintf(out,sizeof(out),"%04d-%02d-%02d",y,m,d);return out;}
}
int64_t budget_day(const std::string& s){
 require(std::regex_match(s,std::regex("[0-9]{4}-[0-9]{2}-[0-9]{2}")),"invalid_date");
 int y=std::stoi(s.substr(0,4)),m=std::stoi(s.substr(5,2)),d=std::stoi(s.substr(8,2));
 require(y>=1970&&y<=2199&&m>=1&&m<=12&&d>=1&&d<=month_days(y,m),"invalid_date");
 std::tm t{};t.tm_year=y-1900;t.tm_mon=m-1;t.tm_mday=d;return timegm(&t)/86400;
}
std::string budget_date(int64_t day){time_t v=day*86400;std::tm t{};gmtime_r(&v,&t);return make_date(t.tm_year+1900,t.tm_mon+1,t.tm_mday);}
std::string budget_today(){return budget_date((std::time(nullptr)+8*3600)/86400);}
std::string budget_next(const json& s){
 auto date=s.at("next_billing_date").get<std::string>();auto day=budget_day(date);auto cadence=s.at("cadence").get<std::string>();
 if(cadence=="weekly"||cadence=="fortnightly")return budget_date(day+(cadence=="weekly"?7:14));
 int y=std::stoi(date.substr(0,4)),m=std::stoi(date.substr(5,2));int add=cadence=="monthly"?1:cadence=="quarterly"?3:12;
 m+=add;y+=(m-1)/12;m=(m-1)%12+1;return make_date(y,m,std::min(s.at("billing_day").get<int>(),month_days(y,m)));
}
json budget_validate(json p,bool sub){
 text(p.at("name"),200,"invalid_name",true);text(p.at("category"),100,"invalid_category");text(p.at("notes"),4000,"invalid_notes");
 number(p.at("amount_cents"),1,100000000LL,"invalid_amount_cents");
 require(p.at("currency").is_string()&&std::regex_match(p["currency"].get<std::string>(),std::regex("[A-Z]{3}")),"invalid_currency");
 if(!p.at("gst_cents").is_null())number(p["gst_cents"],0,p["amount_cents"].get<int64_t>(),"invalid_gst_cents");
 auto date_key=sub?"next_billing_date":"spent_on";require(p.at(date_key).is_string(),"invalid_date");budget_day(p[date_key]);
 if(sub){
  require(p["cadence"]=="weekly"||p["cadence"]=="fortnightly"||p["cadence"]=="monthly"||p["cadence"]=="quarterly"||p["cadence"]=="yearly","invalid_cadence");
  require(p["status"]=="active"||p["status"]=="paused"||p["status"]=="cancelled","invalid_status");
  number(p.at("billing_day"),1,31,"invalid_billing_day");
  if(!p["end_date"].is_null()){require(p["end_date"].is_string(),"invalid_end_date");budget_day(p["end_date"]);}
  if(!p["reminder_days"].is_null())number(p["reminder_days"],0,365,"invalid_reminder_days");
 }else if(!p["subscription_id"].is_null())require(p["subscription_id"].is_string()&&std::regex_match(p["subscription_id"].get<std::string>(),std::regex("[0-9a-f-]{36}")),"invalid_subscription_id");
 return p;
}
BudgetScheduler::BudgetScheduler(std::shared_ptr<BudgetStore> store){worker_=std::thread([this,store]{RetryLog retry("budget.scheduler");std::unique_lock<std::mutex> lock(mutex_);while(!stopping_){lock.unlock();try{store->tick();retry.success();}catch(const std::exception& e){retry.failure(e);}lock.lock();wake_.wait_for(lock,std::chrono::seconds(30),[this]{return stopping_;});}});}
BudgetScheduler::~BudgetScheduler(){{std::lock_guard<std::mutex> lock(mutex_);stopping_=true;}wake_.notify_all();worker_.join();}
void configure_budget(httplib::Server& server,std::shared_ptr<BudgetStore> store,std::shared_ptr<Authorization> auth){
 server.Get("/budget/explorer",[](const auto&,auto& res){res.set_header("Cache-Control","no-store");res.set_content(budget_explorer_html,"text/html; charset=utf-8");});
 auto handler=[store](const httplib::Request& req,httplib::Response& res){
  auto reply=[&](int status,const json& body){res.status=status;res.set_header("Cache-Control","no-store");res.set_content(body.dump(),"application/json");};
  try{
   bool sub=req.path.rfind("/budget/subscriptions",0)==0,get=req.method=="GET"||req.method=="HEAD";
   bool forecast=req.path=="/budget/forecast",summary=req.path=="/budget/summary";
   bool collection=req.path=="/budget/expenses"||req.path=="/budget/subscriptions";
   bool payment=req.path.size()>9&&req.path.substr(req.path.size()-9)=="/payments";
   std::string id;if(!collection&&!forecast&&!summary){id=req.matches[1].str();require(std::regex_match(id,std::regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")),"invalid_id");}
   json filters=json::object();
   if(get&&(collection||forecast||summary)){
    std::set<std::string> allowed=forecast?std::set<std::string>{"from","to","mode","history_days","currency"}:summary?std::set<std::string>{"from","to","currency","category"}:sub?std::set<std::string>{"status","category","currency","limit","offset"}:std::set<std::string>{"from","to","category","currency","subscription_id","limit","offset"};
    for(const auto& f:req.params){require(allowed.count(f.first)&&req.get_param_value_count(f.first)==1,"invalid_filter");
     if(f.first=="limit"||f.first=="offset"||f.first=="history_days")filters[f.first]=query_number(f.second,f.first=="offset"?0:1,f.first=="limit"?100:f.first=="history_days"?365:1000000);
     else {text(f.second,100,"invalid_filter");filters[f.first]=f.second;}
    }
    for(const auto* key:{"from","to"})if(filters.contains(key))budget_day(filters[key]);
    if(filters.contains("from")&&filters.contains("to"))require(filters["from"]<=filters["to"],"invalid_date_range");
    if(forecast){filters["from"]=filters.value("from",budget_today());filters["to"]=filters.value("to",budget_date(budget_day(filters["from"])+29));require(filters["from"]>=budget_today(),"forecast_in_past");auto days=budget_day(filters["to"])-budget_day(filters["from"])+1;require(days>0&&days<=366,"invalid_date_range");require(!filters.contains("mode")||filters["mode"]=="subscriptions"||filters["mode"]=="patterns","invalid_mode");reply(200,store->forecast(filters));}
    else reply(200,summary?store->summary(filters):store->list(sub,filters));return;
   }
   if(req.method=="DELETE"){
    require(req.params.size()==1&&req.has_param("revision")&&req.body.empty(),"revision_required");
    if(store->remove(sub,id,query_number(req.get_param_value("revision"),1,9007199254740990LL)))res.status=204;else reply(404,{{"error","budget_item_not_found"}});return;
   }
   require(req.params.empty(),"invalid_filter");
   if(get){auto v=store->get(sub,id);if(v)reply(200,*v);else reply(404,{{"error","budget_item_not_found"}});return;}
   auto ct=req.get_header_value("Content-Type");if(ct.substr(0,ct.find(';'))!="application/json"){reply(415,{{"error","unsupported_media_type"}});return;}
   std::vector<std::set<std::string>> keys;
   auto p=json::parse(req.body,[&](int depth,json::parse_event_t event,json& value){require(depth<16,"json_too_deep");if(event==json::parse_event_t::object_start)keys.emplace_back();else if(event==json::parse_event_t::object_end)keys.pop_back();else if(event==json::parse_event_t::key)require(keys.back().insert(value.get<std::string>()).second,"duplicate_json_key");return true;});
   require(p.is_object(),"invalid_packet");
   std::set<std::string> allowed=payment?std::set<std::string>{"revision","scheduled_date","spent_on","amount_cents","gst_cents","source_device"}:std::set<std::string>{"name","amount_cents","currency","gst_cents","category","notes","source_device"};
   if(!payment){if(!collection)allowed.insert("revision");for(auto k:sub?std::vector<std::string>{"cadence","next_billing_date","billing_day","status","end_date","reminder_days"}:std::vector<std::string>{"spent_on","subscription_id"})allowed.insert(k);}
   for(const auto& f:p.items())require(allowed.count(f.key()),"unknown_field:"+f.key());
   if(!collection)number(p.at("revision"),1,9007199254740990LL,"invalid_revision");
   if(p.contains("source_device")&&!p["source_device"].is_null())require(p["source_device"].is_string()&&std::regex_match(p["source_device"].get<std::string>(),std::regex("[A-Za-z0-9_-]{1,64}")),"invalid_source_device");
   if(payment){require(p.at("scheduled_date").is_string(),"invalid_date");budget_day(p["scheduled_date"]);reply(200,store->payment(id,p));}
   else if(collection){auto v=store->create(sub,p);res.set_header("Location",req.path+"/"+v["id"].get<std::string>());reply(201,v);}
   else{auto v=store->update(sub,id,p);if(v)reply(200,*v);else reply(404,{{"error","budget_item_not_found"}});}
  }catch(const BudgetConflict&){reply(409,{{"error","revision_conflict"}});}
  catch(const std::invalid_argument& e){std::string error=e.what();reply(error=="too_many_records"||error=="payment_schedule_changed"||error=="subscription_inactive"?409:error=="subscription_not_found"?404:400,{{"error",error}});}
  catch(const json::exception&){reply(400,{{"error","invalid_or_missing_field"}});}
  catch(const DatabaseError&){reply(503,{{"error","database_unavailable"}});}
 };
 for(const auto& base:{std::string("/budget/expenses"),std::string("/budget/subscriptions")}){
  server.Get(base,handler);server.Get(base+"/([^/]+)",handler);body_route(server,"POST",base,handler,auth);body_route(server,"PATCH",base+"/([^/]+)",handler,auth);body_route(server,"DELETE",base+"/([^/]+)",handler,auth);
 }
 server.Get("/budget/forecast",handler);server.Get("/budget/summary",handler);body_route(server,"POST","/budget/subscriptions/([^/]+)/payments",handler,auth);
}
}
namespace spark {
nlohmann::json budget_endpoints(){
 json expense={{"id","12345678-1234-1234-1234-123456789abc"},{"name","Groceries"},{"amount_cents",5500},{"currency","AUD"},{"gst_cents",500},{"category","Food"},{"notes",""},{"spent_on","2026-11-20"},{"subscription_id",nullptr},{"revision",1},{"created_at","2026-11-20T01:00:00.000000Z"},{"updated_at","2026-11-20T01:00:00.000000Z"}};
 json subscription=expense;subscription.erase("spent_on");subscription.erase("subscription_id");subscription.update({{"name","Music"},{"category","Entertainment"},{"amount_cents",1200},{"gst_cents",nullptr},{"cadence","monthly"},{"next_billing_date","2026-11-30"},{"billing_day",30},{"end_date",nullptr},{"status","active"},{"reminder_days",2}});
 json out=json::array();
 auto add=[&](const std::string& method,const std::string& path,const std::string& description,const json& body,const json& result,int status=200){out.push_back({{"method",method},{"path",path},{"description",description},{"input",{{"body",body}}},{"output",{{"status",status},{"body",result}}},{"access",{{"permission",method=="GET"?"budget.read":"budget.write"}}},{"errors",{{"400","invalid_or_missing_field, unknown_field:<name>, invalid_amount_cents, invalid_gst_cents, invalid_date, invalid_date_range, invalid_currency, invalid_filter, invalid_mode, forecast_in_past, revision_required, duplicate_json_key"},{"401","unauthorized"},{"403","forbidden, device_identity_mismatch"},{"404","budget_item_not_found, subscription_not_found"},{"409","revision_conflict, payment_schedule_changed, subscription_inactive, too_many_records"},{"413","payload_too_large"},{"415","unsupported_media_type"},{"503","database_unavailable"}}}});};
 for(bool sub:{false,true}){
  std::string base=sub?"/budget/subscriptions":"/budget/expenses",label=sub?"subscription":"expense";auto record=sub?subscription:expense;auto input=record;for(auto k:{"id","revision","created_at","updated_at"})input.erase(k);
  add("POST",base,"Create "+label+". Monetary amounts are integer cents (two decimal places for every currency); gross total includes optional GST. AUD default; no inferred GST rate or currency conversion. Accounting dates are YYYY-MM-DD in Australia/Perth. JSON body limit 16 KiB.",input,record,201);
  out.back()["input"]["headers"]={{"Content-Type","application/json"}};
  out.back()["input"]["fields"]={{"name","required nonblank text, up to 200 bytes"},{"amount_cents","required integer 1..100000000 (maximum 1,000,000 currency units)"},{"currency","optional uppercase 3-letter currency code; default AUD; cents always denote 1/100 unit"},{"gst_cents","optional null or integer 0..amount_cents, already included in the total; null means unspecified"},{"category","optional text up to 100 bytes, default empty"},{"notes","optional text up to 4000 bytes"},{"source_device","optional notification attribution; existing delegation checks apply"}};
  if(sub)out.back()["input"]["fields"].update({{"cadence","required weekly, fortnightly, monthly, quarterly or yearly"},{"next_billing_date","required date, 1970..2199; not automatically advanced by time"},{"billing_day","optional 1..31, defaults to next_billing_date day. Preserved across month-end clamping. Editing next_billing_date resets anchor unless billing_day supplied."},{"status","active (default), paused or cancelled"},{"end_date","optional inclusive last billing date, or null; charges after it are excluded"},{"reminder_days","optional null (disabled) or 0..365; one TCP reminder per next billing occurrence at/after Perth midnight on the reminder date, checked every 30s, catches up after downtime. No acknowledgement required; does not record payment."}});
  else out.back()["input"]["fields"].update({{"spent_on","optional accounting date, 1970..2199; defaults to today in Perth"},{"subscription_id","optional existing subscription ID of same currency. Prefer Record payment endpoint to also advance schedule. Link remains on history after subscription deletion."}});
  add("GET",base,"List "+label+" records ordered by accounting date, then ID. Exact category/currency filters. Requires budget.read.",nullptr,{{sub?"subscriptions":"expenses",json::array({record})},{"limit",50},{"offset",0},{"has_more",false}});
  out.back()["input"]["query"]={{"currency","optional exact currency"},{"category","optional exact category"},{"limit","1..100, default 50"},{"offset","0..1000000, default 0"}};
  if(sub)out.back()["input"]["query"]["status"]="optional active, paused or cancelled";
  else out.back()["input"]["query"].update({{"from","inclusive spent_on lower bound"},{"to","inclusive spent_on upper bound"},{"subscription_id","optional exact linked subscription"}});
  add("GET",base+"/{id}","Retrieve a "+label+" and its revision.",nullptr,record);
  add("PATCH",base+"/{id}","Update fields from POST with required current revision. GST must remain within the new gross total. No-op changes emit no notification. Stale revision returns 409; reload before retrying.",{{"revision",1},{"notes","Updated note"}},record);
  add("DELETE",base+"/{id}",sub?"Delete subscription only. Recorded expenses and payment deduplication receipts remain.":"Delete an expense; actual-spending totals update. This does not rewind a subscription or erase a payment deduplication receipt.",nullptr,nullptr,204);out.back()["input"]["query"]={{"revision","required current integer revision"}};
 }
 add("POST","/budget/subscriptions/{id}/payments","Explicitly record an actual payment and advance ONE billing period atomically. Does not charge money. Requires active subscription and next scheduled date within end_date. Same subscription+scheduled_date is idempotent: retries return already_recorded=true even with stale revision; a deleted expense returns null and is not recreated. Use PATCH to correct an expense. Optional spent_on, amount_cents and gst_cents override defaults for this payment only.",{{"revision",1},{"scheduled_date","2026-11-30"},{"spent_on","2026-11-30"}},{{"already_recorded",false},{"expense_id",expense["id"]},{"expense",expense},{"subscription",subscription}});
 add("GET","/budget/summary","Actual expense totals and category subtotals, grouped by currency; GST totals include only specified GST and show an unspecified count. No forecast amounts included. Query limited to 100,000 matching expenses; narrow dates if too_many_records.",nullptr,{{"currencies",{{"AUD",{{"amount_cents",5500},{"gst_cents",500},{"gst_unspecified_count",0},{"count",1},{"categories",{{"Food",5500}}}}}}},{"timezone","Australia/Perth"},{"filters",json::object()}});out.back()["input"]["query"]={{"from","optional inclusive accounting date"},{"to","optional inclusive accounting date"},{"currency","optional exact currency"},{"category","optional exact category"}};
 add("GET","/budget/forecast","Estimate scheduled subscription expenditure, or extrapolate total spending patterns. Never creates expenses or advances schedules. Pattern mode adds upcoming active subscription charges to historical non-subscription gross spend / history_days * forecast days, rounded to cents. Zero-spend days included. History ends yesterday Perth; excludes future-dated and subscription-linked expenses. No seasonality or currency conversion. Sparse history and unlinked subscription expenses can distort estimates. Overdue dates before window are reported separately, not added. End dates inclusive; month-end dates clamp while retaining billing_day. Bounded to 100,000 records/charges; narrow scope if too_many_records.",nullptr,{{"mode","subscriptions"},{"from","2026-11-20"},{"to","2026-12-19"},{"days",30},{"currencies",{{"AUD",{{"subscription_cents",1200},{"pattern_cents",0},{"total_cents",1200},{"history_non_subscription_cents",0},{"history_expense_count",0}}}}},{"charges",json::array({{{"subscription_id",subscription["id"]},{"name","Music"},{"date","2026-11-30"},{"amount_cents",1200},{"currency","AUD"}}})},{"overdue_subscriptions",json::array()},{"assumptions",{{"timezone","Australia/Perth"},{"history_days",90},{"history_from","2026-08-22"},{"history_to","2026-11-19"},{"method","See description"},{"warning","Estimate only"}}}});
 out.back()["input"]["query"]={{"mode","subscriptions (default) or patterns"},{"from","today Perth or later, default today"},{"to","inclusive end, default from + 29 days; window 1..366 days"},{"history_days","1..365, default 90; trailing days ending yesterday"},{"currency","optional exact currency"}};
 add("GET","/budget/explorer","Human expense/subscription editor, explicit Record payment, actual totals and subscription-only or pattern forecast. budget.read required; modifications require budget.write. Financial data is not exposed to assistant tools.",nullptr,"HTML");
 return out;
}
}
