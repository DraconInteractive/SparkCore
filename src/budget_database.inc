// Included by database.cpp to reuse its prepared statements and transaction/event helpers.
namespace spark {
namespace {
std::string budget_table(bool sub){return sub?"budget_subscriptions":"budget_expenses";}
const std::string budget_projection="JSON_SET(data,'$.id',id,'$.revision',revision,'$.created_at',DATE_FORMAT(created_at,'%Y-%m-%dT%H:%i:%s.%fZ'),'$.updated_at',DATE_FORMAT(updated_at,'%Y-%m-%dT%H:%i:%s.%fZ'))";
std::optional<json> budget_read(Connection& c,bool sub,const std::string& id,bool lock=false){Statement q(c,"SELECT "+budget_projection+" FROM "+budget_table(sub)+" WHERE id=?"+(lock?" FOR UPDATE":""),{id});q.execute();auto r=q.rows();return r.empty()?std::nullopt:std::optional<json>(r[0]);}
void budget_event(Connection& c,bool sub,const char* action,const json& v,const json& source=nullptr){append_event(c,std::string(sub?"budget.subscription.":"budget.expense.")+action,std::string(sub?"/budget/subscriptions/":"/budget/expenses/")+v["id"].get<std::string>(),source,{{"id",v["id"]},{"revision",v["revision"]}});}
json budget_defaults(bool sub,const json& input){
 json v={{"currency","AUD"},{"gst_cents",nullptr},{"category",""},{"notes",""}};
 if(sub){v["status"]="active";v["end_date"]=nullptr;v["reminder_days"]=nullptr;}
 else{v["spent_on"]=budget_today();v["subscription_id"]=nullptr;}
 v.update(input);v.erase("source_device");
 if(sub&&!v.contains("billing_day")){budget_day(v.at("next_billing_date").get<std::string>());v["billing_day"]=std::stoi(v["next_billing_date"].get<std::string>().substr(8,2));}
 return budget_validate(v,sub);
}
void budget_link(Connection& c,const json& v){if(!v["subscription_id"].is_null()){auto s=budget_read(c,true,v["subscription_id"].get<std::string>());if(!s)throw std::invalid_argument("subscription_not_found");if((*s)["currency"]!=v["currency"])throw std::invalid_argument("subscription_currency_mismatch");}}
json budget_insert(Connection& c,bool sub,const json& data){const auto id=sync_uuid(c);Statement q(c,"INSERT INTO "+budget_table(sub)+"(id,data,accounting_date) VALUES (?,?,?)",{id,data.dump(),data.at(sub?"next_billing_date":"spent_on").get<std::string>()});q.execute();return *budget_read(c,sub,id);}
void budget_save(Connection& c,bool sub,const std::string& id,json data){for(auto key:{"id","revision","created_at","updated_at"})data.erase(key);Statement q(c,"UPDATE "+budget_table(sub)+" SET data=?,accounting_date=?,revision=revision+1,updated_at=CURRENT_TIMESTAMP(6) WHERE id=?",{data.dump(),data.at(sub?"next_billing_date":"spent_on").get<std::string>(),id});q.execute();}
json budget_rows(Connection& c,bool sub,const json& f,int limit,int offset=0){std::string sql="SELECT "+budget_projection+" FROM "+budget_table(sub)+" WHERE 1=1";Parameters p;
 for(auto key:{"from","to"})if(f.contains(key)){sql+=std::string(" AND accounting_date")+(std::string(key)=="from"?">=":"<=")+"?";p.push_back(f[key].get<std::string>());}
 for(auto key:{"category","currency","subscription_id","status"})if(f.contains(key)){sql+=" AND BINARY JSON_UNQUOTE(JSON_EXTRACT(data,'$."+std::string(key)+"'))=BINARY ?";p.push_back(f[key].get<std::string>());}
 sql+=" ORDER BY accounting_date,id LIMIT ? OFFSET ?";p.push_back(std::to_string(limit));p.push_back(std::to_string(offset));Statement q(c,sql,p);q.execute();return q.rows();
}
json budget_all(Connection& c,bool sub,const json& f){auto rows=budget_rows(c,sub,f,100001);if(rows.size()>100000)throw std::invalid_argument("too_many_records");return rows;}
}
void BudgetStore::verify()const{Connection c(config_);{Statement q(c,"SELECT JSON_OBJECT('data',JSON_EXTRACT(data,'$'),'revision',revision,'updated_at',updated_at) FROM budget_mortgage WHERE id=1");q.execute();if(q.rows().empty())throw DatabaseError("Mortgage schema is not provisioned");}for(bool sub:{false,true}){Statement q(c,"SELECT "+budget_projection+" FROM "+budget_table(sub)+" LIMIT 0");q.execute();q.rows();}for(auto table:{"budget_payments","budget_reminders"}){Statement q(c,std::string("SELECT JSON_OBJECT('id',subscription_id,'date',scheduled_date)")+(std::string(table)=="budget_payments"?", expense_id":"")+" FROM "+table+" LIMIT 0");q.execute();}}
json BudgetStore::create(bool sub,const json& p)const{auto v=budget_defaults(sub,p);Connection c(config_);Transaction tx(c);if(!sub)budget_link(c,v);auto r=budget_insert(c,sub,v);budget_event(c,sub,"created",r,p.value("source_device",json(nullptr)));tx.commit();return r;}
std::optional<json> BudgetStore::get(bool sub,const std::string& id)const{Connection c(config_);return budget_read(c,sub,id);}
json BudgetStore::list(bool sub,const json& f)const{Connection c(config_);int limit=f.value("limit",50),offset=f.value("offset",0);auto rows=budget_rows(c,sub,f,limit+1,offset);bool more=rows.size()>size_t(limit);if(more)rows.erase(rows.end()-1);return {{sub?"subscriptions":"expenses",rows},{"limit",limit},{"offset",offset},{"has_more",more}};}
std::optional<json> BudgetStore::update(bool sub,const std::string& id,const json& p)const{Connection c(config_);Transaction tx(c);auto old=budget_read(c,sub,id,true);if(!old)return std::nullopt;if(old->at("revision")!=p.at("revision"))throw BudgetConflict();auto v=*old;for(auto& field:p.items())if(field.key()!="source_device"&&field.key()!="revision")v[field.key()]=field.value();
 if(sub&&p.contains("next_billing_date")&&!p.contains("billing_day")){budget_day(v["next_billing_date"].get<std::string>());v["billing_day"]=std::stoi(v["next_billing_date"].get<std::string>().substr(8,2));}
 budget_validate(v,sub);if(!sub&&(v["subscription_id"]!=(*old)["subscription_id"]||v["currency"]!=(*old)["currency"]))budget_link(c,v);
 if(v!=*old){budget_save(c,sub,id,v);old=budget_read(c,sub,id);budget_event(c,sub,"updated",*old,p.value("source_device",json(nullptr)));}tx.commit();return old;}
bool BudgetStore::remove(bool sub,const std::string& id,uint64_t revision)const{Connection c(config_);Transaction tx(c);auto old=budget_read(c,sub,id,true);if(!old)return false;if(old->at("revision")!=revision)throw BudgetConflict();Statement q(c,"DELETE FROM "+budget_table(sub)+" WHERE id=?",{id});q.execute();(*old)["revision"]=revision+1;budget_event(c,sub,"deleted",*old);tx.commit();return true;}
json BudgetStore::payment(const std::string& id,const json& p)const{
 Connection c(config_);Transaction tx(c);auto sub=budget_read(c,true,id,true);auto date=p.at("scheduled_date").get<std::string>();
 Statement prior(c,"SELECT JSON_OBJECT('expense_id',expense_id) FROM budget_payments WHERE subscription_id=? AND scheduled_date=?",{id,date});prior.execute();auto rows=prior.rows();
 if(!rows.empty()){auto eid=rows[0]["expense_id"].get<std::string>();auto expense=budget_read(c,false,eid);tx.commit();return {{"already_recorded",true},{"expense_id",eid},{"expense",expense?*expense:json(nullptr)},{"subscription",sub?*sub:json(nullptr)}};}
 if(!sub)throw std::invalid_argument("subscription_not_found");if(sub->at("revision")!=p.at("revision"))throw BudgetConflict();if((*sub)["next_billing_date"]!=date)throw std::invalid_argument("payment_schedule_changed");
 if((*sub)["status"]!="active"||(!(*sub)["end_date"].is_null()&&(*sub)["end_date"]<date))throw std::invalid_argument("subscription_inactive");
 json e={{"name",(*sub)["name"]},{"currency",(*sub)["currency"]},{"category",(*sub)["category"]},{"notes",""},{"subscription_id",id},{"spent_on",p.value("spent_on",json(budget_today()))},{"amount_cents",p.value("amount_cents",(*sub)["amount_cents"])},{"gst_cents",p.value("gst_cents",(*sub)["gst_cents"])}};
 e=budget_validate(e,false);auto expense=budget_insert(c,false,e);Statement receipt(c,"INSERT INTO budget_payments(subscription_id,scheduled_date,expense_id) VALUES (?,?,?)",{id,date,expense["id"].get<std::string>()});receipt.execute();
 (*sub)["next_billing_date"]=budget_next(*sub);budget_day((*sub)["next_billing_date"]);budget_save(c,true,id,*sub);sub=budget_read(c,true,id);
 budget_event(c,false,"created",expense,p.value("source_device",json(nullptr)));budget_event(c,true,"updated",*sub,p.value("source_device",json(nullptr)));tx.commit();return {{"already_recorded",false},{"expense_id",expense["id"]},{"expense",expense},{"subscription",*sub}};
}
json BudgetStore::summary(const json& f)const{Connection c(config_);auto rows=budget_all(c,false,f);json currencies=json::object();for(const auto& e:rows){auto code=e["currency"].get<std::string>();if(!currencies.contains(code))currencies[code]={{"amount_cents",0},{"gst_cents",0},{"gst_unspecified_count",0},{"count",0},{"categories",json::object()}};auto& a=currencies[code];a["amount_cents"]=a["amount_cents"].get<int64_t>()+e["amount_cents"].get<int64_t>();a["count"]=a["count"].get<int>()+1;if(e["gst_cents"].is_null())a["gst_unspecified_count"]=a["gst_unspecified_count"].get<int>()+1;else a["gst_cents"]=a["gst_cents"].get<int64_t>()+e["gst_cents"].get<int64_t>();auto cat=e["category"].get<std::string>();a["categories"][cat]=a["categories"].value(cat,int64_t(0))+e["amount_cents"].get<int64_t>();}return {{"currencies",currencies},{"timezone","Australia/Perth"},{"filters",f}};}
json BudgetStore::forecast(const json& f)const{
 Connection c(config_);Transaction tx(c);const auto from=f.at("from").get<std::string>(),to=f.at("to").get<std::string>();auto days=budget_day(to)-budget_day(from)+1;auto mode=f.value("mode",std::string("subscriptions"));json currencies=json::object(),charges=json::array(),overdue=json::array();
 auto ensure=[&](const std::string& code){if(!currencies.contains(code))currencies[code]={{"subscription_cents",0},{"pattern_cents",0},{"total_cents",0},{"history_non_subscription_cents",0},{"history_expense_count",0}};};
 json sf={{"status","active"}};if(f.contains("currency"))sf["currency"]=f["currency"];
 size_t iterations=0;
 for(auto sub:budget_all(c,true,sf)){
  if(sub["next_billing_date"]<from&& (sub["end_date"].is_null()||sub["next_billing_date"]<=sub["end_date"]))overdue.push_back({{"subscription_id",sub["id"]},{"name",sub["name"]},{"next_billing_date",sub["next_billing_date"]}});
  while(sub["next_billing_date"]<=to&&(sub["end_date"].is_null()||sub["next_billing_date"]<=sub["end_date"])){
   if(++iterations>1000000)throw std::invalid_argument("too_many_records");
   if(sub["next_billing_date"]>=from){if(charges.size()>=100000)throw std::invalid_argument("too_many_records");auto code=sub["currency"].get<std::string>();ensure(code);currencies[code]["subscription_cents"]=currencies[code]["subscription_cents"].get<int64_t>()+sub["amount_cents"].get<int64_t>();charges.push_back({{"subscription_id",sub["id"]},{"name",sub["name"]},{"date",sub["next_billing_date"]},{"amount_cents",sub["amount_cents"]},{"currency",code}});}
   sub["next_billing_date"]=budget_next(sub);
  }
 }
 std::sort(charges.begin(),charges.end(),[](const auto& a,const auto& b){return a["date"]==b["date"]?a["subscription_id"]<b["subscription_id"]:a["date"]<b["date"];});
 // Anchor history to today, never include future-dated records or pretend the forecast start is history.
 auto history=f.value("history_days",90);auto end=budget_day(budget_today())-1;auto start=end-history+1;
 if(mode=="patterns"){
  json ef={{"from",budget_date(start)},{"to",budget_date(end)}};if(f.contains("currency"))ef["currency"]=f["currency"];
  for(const auto& e:budget_all(c,false,ef))if(e["subscription_id"].is_null()){auto code=e["currency"].get<std::string>();ensure(code);auto& a=currencies[code];a["history_non_subscription_cents"]=a["history_non_subscription_cents"].get<int64_t>()+e["amount_cents"].get<int64_t>();a["history_expense_count"]=a["history_expense_count"].get<int>()+1;}
 }
 for(auto& it:currencies.items()){auto& a=it.value();a["pattern_cents"]=(a["history_non_subscription_cents"].get<int64_t>()*days+history/2)/history;a["total_cents"]=a["pattern_cents"].get<int64_t>()+a["subscription_cents"].get<int64_t>();}
 tx.commit();return {{"mode",mode},{"from",from},{"to",to},{"days",days},{"currencies",currencies},{"charges",charges},{"overdue_subscriptions",overdue},{"assumptions",{{"timezone","Australia/Perth"},{"history_from",budget_date(start)},{"history_to",budget_date(end)},{"history_days",history},{"method","Scheduled active subscriptions plus (patterns mode only) trailing daily average of non-subscription expenses. Zero-spend days included; no seasonality or FX. GST is included in gross amounts. Overdue charges before the forecast window are listed, not added."},{"warning","Estimate only. Sparse history and unlinked subscription payments may distort the pattern forecast."}}}};
}
void BudgetStore::tick()const{
 Connection c(config_);auto today=budget_today();auto subs=budget_all(c,true,{{"status","active"},{"to",budget_date(budget_day(today)+365)}});
 for(const auto& item:subs){if(item["reminder_days"].is_null()||budget_day(item["next_billing_date"])-item["reminder_days"].get<int>()>budget_day(today)||(!item["end_date"].is_null()&&item["next_billing_date"]>item["end_date"]))continue;Transaction tx(c);auto current=budget_read(c,true,item["id"].get<std::string>(),true);if(!current){tx.commit();continue;}auto s=*current;
  if(s["status"]=="active"&&!s["reminder_days"].is_null()&&(s["end_date"].is_null()||s["next_billing_date"]<=s["end_date"])&&budget_day(s["next_billing_date"])-s["reminder_days"].get<int>()<=budget_day(today)){
   Statement insert(c,"INSERT INTO budget_reminders(subscription_id,scheduled_date) VALUES (?,?)",{s["id"].get<std::string>(),s["next_billing_date"].get<std::string>()});if(insert.execute(true))append_event(c,"budget.subscription.reminder","/budget/subscriptions/"+s["id"].get<std::string>(),nullptr,{{"subscription_id",s["id"]},{"scheduled_date",s["next_billing_date"]}});
  }tx.commit();
 }
}
}

namespace spark {
namespace {
json read_mortgage(Connection& c,bool lock=false){Statement q(c,"SELECT JSON_OBJECT('mortgage',JSON_EXTRACT(data,'$'),'revision',revision,'updated_at',DATE_FORMAT(updated_at,'%Y-%m-%dT%H:%i:%s.%fZ')) FROM budget_mortgage WHERE id=1"+std::string(lock?" FOR UPDATE":""));q.execute();auto rows=q.rows();if(rows.empty())throw DatabaseError("Mortgage schema is not provisioned");return rows[0];}
json with_mortgage_stats(json value){value["stats"]=value["mortgage"].is_null()?json(nullptr):mortgage_stats(value["mortgage"]);return value;}
}
json BudgetStore::mortgage()const{Connection c(config_);return with_mortgage_stats(read_mortgage(c));}
json BudgetStore::save_mortgage(const json& input,uint64_t revision)const{
 auto data=input.is_null()?json(nullptr):validate_mortgage(input);Connection c(config_);Transaction tx(c);auto prior=read_mortgage(c,true);if(prior["revision"]!=revision)throw BudgetConflict();
 if(prior["mortgage"]==data){tx.commit();return with_mortgage_stats(prior);}
 if(revision>=9007199254740991ULL)throw std::invalid_argument("revision_exhausted");
 Statement update(c,"UPDATE budget_mortgage SET data=?,revision=revision+1,updated_at=CURRENT_TIMESTAMP(6) WHERE id=1",{data.is_null()?std::nullopt:std::optional<std::string>(data.dump())});update.execute();auto saved=read_mortgage(c);append_event(c,"budget.mortgage.updated","/budget/mortgage",nullptr,{{"revision",saved["revision"]},{"configured",!data.is_null()}});tx.commit();return with_mortgage_stats(saved);
}
}
