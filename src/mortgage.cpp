#include "mortgage.hpp"
#include "bounded_body.hpp"
#include <charconv>
#include <set>
namespace spark {
using json=nlohmann::json;
namespace {
void require(bool ok,const std::string& error){if(!ok)throw std::invalid_argument(error);}
int64_t integer(const json& v,int64_t max,const char* field){require(v.is_number_integer()&&v>=0&&v<=max,std::string("invalid_")+field);return v.get<int64_t>();}
int64_t interest(int64_t principal,int64_t offset,int64_t bps){return (std::max(int64_t(0),principal-offset)*bps+60000)/120000;}
json project(int64_t principal,int64_t offset,int64_t bps,int64_t payment){
 json result={{"status","paid_off"},{"months_remaining",0},{"remaining_interest_cents",0},{"remaining_payments_cents",0},{"final_payment_cents",0}};
 if(principal==0)return result;
 result["months_remaining"]=nullptr;result["remaining_interest_cents"]=nullptr;result["remaining_payments_cents"]=nullptr;result["final_payment_cents"]=nullptr;
 if(payment<=interest(principal,offset,bps)){result["status"]="payment_too_low";return result;}
 int64_t total_interest=0,total_paid=0,last=0;
 for(int month=1;month<=1200;++month){auto cost=interest(principal,offset,bps);total_interest+=cost;last=std::min(payment,principal+cost);total_paid+=last;principal=principal+cost-last;
  if(principal==0){result.update({{"status","amortizing"},{"months_remaining",month},{"remaining_interest_cents",total_interest},{"remaining_payments_cents",total_paid},{"final_payment_cents",last}});return result;}}
 result["status"]="horizon_exceeded";return result;
}
}
json validate_mortgage(json p){
 require(p.is_object(),"invalid_packet");
 const std::set<std::string> allowed={"remaining_principal_cents","annual_interest_rate_bps","monthly_payment_cents","offset_balance_cents","currency","as_of"};
 for(const auto& f:p.items())require(allowed.count(f.key()),"unknown_field:"+f.key());
 for(const auto* key:{"remaining_principal_cents","annual_interest_rate_bps","monthly_payment_cents","offset_balance_cents"}){require(p.contains(key),std::string("missing_field:")+key);integer(p[key],std::string(key)=="annual_interest_rate_bps"?10000:1000000000000LL,key);}
 if(!p.contains("currency"))p["currency"]="AUD";
 require(p["currency"].is_string()&&std::regex_match(p["currency"].get<std::string>(),std::regex("[A-Z]{3}")),"invalid_currency");
 if(!p.contains("as_of"))p["as_of"]=budget_today();require(p["as_of"].is_string(),"invalid_as_of");budget_day(p["as_of"].get<std::string>());
 return p;
}
json mortgage_stats(const json& input){
 auto p=validate_mortgage(input);auto principal=p["remaining_principal_cents"].get<int64_t>(),offset=p["offset_balance_cents"].get<int64_t>(),bps=p["annual_interest_rate_bps"].get<int64_t>(),payment=p["monthly_payment_cents"].get<int64_t>();
 auto result=project(principal,offset,bps,payment),baseline=project(principal,0,bps,payment);auto first_interest=interest(principal,offset,bps);
 result["interest_bearing_balance_cents"]=std::max(int64_t(0),principal-offset);result["first_month_interest_cents"]=first_interest;result["first_month_principal_reduction_cents"]=std::min(payment,principal+first_interest)-first_interest;
 result["first_month_offset_interest_saving_cents"]=interest(principal,0,bps)-first_interest;
 result["years_remaining"]=nullptr;result["additional_months_remaining"]=nullptr;
 if(!result["months_remaining"].is_null()){int months=result["months_remaining"];result["years_remaining"]=months/12;result["additional_months_remaining"]=months%12;}
 result["without_offset"]=baseline;result["interest_saved_by_offset_cents"]=nullptr;result["months_saved_by_offset"]=nullptr;
 if(!result["months_remaining"].is_null()&&!baseline["months_remaining"].is_null()){result["interest_saved_by_offset_cents"]=baseline["remaining_interest_cents"].get<int64_t>()-result["remaining_interest_cents"].get<int64_t>();result["months_saved_by_offset"]=baseline["months_remaining"].get<int>()-result["months_remaining"].get<int>();}
 result["as_of"]=p["as_of"];result["currency"]=p["currency"];
 result["assumptions"]={{"method","Monthly approximation: interest on max(principal minus offset, 0), annual rate / 12, rounded to cents, followed by an end-of-month payment. First payment one month after snapshot."},{"offset","100% offset, constant balance, never spent or automatically applied to principal."},{"fixed_inputs",true},{"fees_included",false},{"maximum_months",1200},{"warning","Estimate from the saved snapshot, not a lender payout quote. Daily accrual, fees and changing rates/balances are not modelled. No payments or expenses are created."}};
 return result;
}
void configure_mortgage(httplib::Server& server,std::shared_ptr<BudgetStore> store,std::shared_ptr<Authorization> auth){
 auto handler=[store](const httplib::Request& req,httplib::Response& res){auto reply=[&](int code,const json& data){res.status=code;res.set_header("Cache-Control","no-store");res.set_content(data.dump(),"application/json");};
 try{
  if(req.method=="GET"||req.method=="HEAD"){require(req.params.empty(),"unknown_filter");reply(200,store->mortgage());return;}
  if(req.method=="DELETE"){
   require(req.params.size()==1&&req.has_param("revision")&&req.body.empty(),"revision_required");auto value=req.get_param_value("revision");uint64_t revision=0;auto parsed=std::from_chars(value.data(),value.data()+value.size(),revision);require(parsed.ec==std::errc()&&parsed.ptr==value.data()+value.size()&&revision<=9007199254740990ULL,"invalid_revision");reply(200,store->save_mortgage(nullptr,revision));return;
  }
  require(req.params.empty(),"unknown_filter");auto ct=req.get_header_value("Content-Type");if(ct.substr(0,ct.find(';'))!="application/json"){reply(415,{{"error","unsupported_media_type"}});return;}
  std::set<std::string> keys;auto p=json::parse(req.body,[&](int depth,json::parse_event_t event,json& value){require(depth<=2,"invalid_packet");if(event==json::parse_event_t::key)require(keys.insert(value.get<std::string>()).second,"duplicate_json_key");return true;});require(p.is_object(),"invalid_packet");
  if(req.path=="/budget/mortgage/estimate"){reply(200,{{"mortgage",validate_mortgage(p)},{"stats",mortgage_stats(p)}});return;}
  require(p.contains("revision"),"revision_required");auto revision=integer(p["revision"],9007199254740990LL,"revision");p.erase("revision");p=validate_mortgage(p);reply(200,store->save_mortgage(p,revision));
 }catch(const BudgetConflict&){reply(409,{{"error","revision_conflict"}});}
 catch(const std::invalid_argument& e){reply(400,{{"error",e.what()}});}
 catch(const json::exception&){reply(400,{{"error","invalid_json"}});}
 catch(const DatabaseError&){reply(503,{{"error","database_unavailable"}});}
 };
 server.Get("/budget/mortgage",handler);body_route(server,"PUT","/budget/mortgage",handler,auth);body_route(server,"DELETE","/budget/mortgage",handler,auth);body_route(server,"POST","/budget/mortgage/estimate",handler,auth);
}
json mortgage_endpoints(){
 json p={{"remaining_principal_cents",50000000},{"annual_interest_rate_bps",600},{"monthly_payment_cents",350000},{"offset_balance_cents",5000000},{"currency","AUD"},{"as_of","2026-11-23"}};
 json result={{"mortgage",p},{"revision",1},{"updated_at","2026-11-23T00:00:00.000000Z"},{"stats",mortgage_stats(p)}};json out=json::array();
 for(const auto& method:{"GET","PUT","DELETE","POST"}){bool estimate=std::string(method)=="POST";std::string path=estimate?"/budget/mortgage/estimate":"/budget/mortgage";json input=nullptr;if(estimate||std::string(method)=="PUT"){input=p;if(!estimate)input["revision"]=0;}
  out.push_back({{"method",method},{"path",path},{"description",estimate?"Preview a mortgage scenario without saving, notifications or expenses; budget.read required.":std::string(method)=="GET"?"Read the single mortgage snapshot and derived stats; mortgage/stats are null until configured. Estimates are anchored to as_of, never auto-advanced. budget.read required.":std::string(method)=="DELETE"?"Clear the saved mortgage with revision check; returns empty state with advanced revision. Keeps a revision tombstone to reject stale writes. budget.write required.":"Set all four mortgage inputs, with required current revision (initially 0). Currency defaults AUD and as_of defaults today Perth. Writes a snapshot only, no actual repayments/expenses. budget.write required. Emits budget.mortgage.updated."},{"input",{{"body",input}}},{"output",{{"status",200},{"body",estimate?json{{"mortgage",p},{"stats",mortgage_stats(p)}}:std::string(method)=="DELETE"?json{{"mortgage",nullptr},{"stats",nullptr},{"revision",2},{"updated_at","2026-11-23T00:00:00.000000Z"}}:result}}},{"access",{{"permission",estimate||std::string(method)=="GET"?"budget.read":"budget.write"}}},{"errors",{{"400","invalid_json, invalid_packet, duplicate_json_key, unknown_field:<name>, missing_field:<name>, invalid_<field>, revision_required, unknown_filter"},{"401","unauthorized"},{"403","forbidden"},{"409","revision_conflict"},{"413","payload_too_large"},{"415","unsupported_media_type"},{"503","database_unavailable"}}}});
  if(std::string(method)=="DELETE")out.back()["input"]["query"]={{"revision","required current revision"}};
  if(input.is_object())out.back()["input"]["fields"]={{"remaining_principal_cents, monthly_payment_cents, offset_balance_cents","required nonnegative integer cents, max 1000000000000 each"},{"annual_interest_rate_bps","required integer 0..10000; 600 = 6.00% nominal annual rate"},{"currency","optional three uppercase letters, default AUD; amounts always use 1/100 units"},{"as_of","optional YYYY-MM-DD snapshot date, default today Perth"},{"revision","PUT only: required current integer revision, 0..9007199254740990"}};
 }
 return out;
}
}
