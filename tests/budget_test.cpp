#include "budget.hpp"
#include "notifications.hpp"
#include <iostream>
#include <future>
#include <set>
using namespace spark;
using json=nlohmann::json;
void check(bool ok,const char* why){if(!ok)throw std::runtime_error(why);}
template<class F>void rejects(F f){bool failed=false;try{f();}catch(const std::exception&){failed=true;}check(failed,"expected rejection");}
int main(){
 auto config=DatabaseConfig::from_environment();if(config.database.rfind("spark_test_",0)!=0)return 77;
 httplib::Server server;std::thread worker;
 try{
  BudgetStore store(config);store.verify();auto today=budget_today();auto day=budget_day(today);
  rejects([]{budget_day("2027-02-29");});rejects([]{budget_day("2026-13-01");});check(budget_date(budget_day("2028-02-29"))=="2028-02-29","leap date");
  json month={{"next_billing_date","2027-01-31"},{"cadence","monthly"},{"billing_day",31}};check(budget_next(month)=="2027-02-28","month clamp");month["next_billing_date"]=budget_next(month);check(budget_next(month)=="2027-03-31","anchor preserved");month.update({{"next_billing_date","2028-02-29"},{"cadence","yearly"},{"billing_day",29}});check(budget_next(month)=="2029-02-28","yearly leap clamp");month.update({{"next_billing_date","2026-11-30"},{"cadence","quarterly"},{"billing_day",31}});check(budget_next(month)=="2027-02-28","quarterly clamp");month.update({{"next_billing_date","2026-12-30"},{"cadence","fortnightly"}});check(budget_next(month)=="2027-01-13","fortnightly year rollover");
  json draft={{"name","Groceries"},{"amount_cents",9000},{"gst_cents",700},{"category","Food"},{"spent_on",budget_date(day-1)}};
  auto expense=store.create(false,draft);check(expense["currency"]=="AUD"&&expense["revision"]==1,"expense defaults");auto id=expense["id"].get<std::string>();
  rejects([&]{auto p=draft;p["gst_cents"]=9001;store.create(false,p);});rejects([&]{auto p=draft;p["amount_cents"]=1.1;store.create(false,p);});rejects([&]{auto p=draft;p["amount_cents"]=-1;store.create(false,p);});
  auto changed=store.update(false,id,{{"revision",1},{"notes","Receipt checked"}});check(changed&&(*changed)["revision"]==2,"update revision");rejects([&]{store.update(false,id,{{"revision",1},{"name","Stale"}});});rejects([&]{store.update(false,id,{{"revision",2},{"amount_cents",600}});});check(store.get(false,id)->at("amount_cents")==9000,"invalid cross-field edit rolls back");
  check(store.list(false,{{"category","Food"},{"limit",1}})["expenses"].size()==1,"filtered listing");
  auto sub=store.create(true,{{"name","Subscription"},{"amount_cents",1200},{"cadence","monthly"},{"next_billing_date",today},{"end_date",today},{"reminder_days",0}});auto sid=sub["id"].get<std::string>();
  auto forecast=store.forecast({{"from",today},{"to",today},{"mode","patterns"},{"history_days",90}});check(forecast["currencies"]["AUD"]["pattern_cents"]==100&&forecast["currencies"]["AUD"]["subscription_cents"]==1200&&forecast["currencies"]["AUD"]["total_cents"]==1300,"pattern forecast plus scheduled subscription");
  store.create(false,{{"name","Unspecified tax"},{"amount_cents",3000},{"currency","USD"},{"spent_on",budget_date(day-1)}});
  store.create(false,{{"name","Linked historical payment"},{"amount_cents",6000},{"subscription_id",sid},{"spent_on",budget_date(day-1)}});
  store.create(false,{{"name","Future entry"},{"amount_cents",90000},{"spent_on",budget_date(day+1)}});
  auto f=store.forecast({{"from",today},{"to",today},{"mode","patterns"},{"history_days",90}});check(f["currencies"]["AUD"]["pattern_cents"]==100,"exclude linked and future payments from pattern");check(f["currencies"]["USD"]["pattern_cents"]==33,"separate currency rounding");
  auto summary=store.summary({{"to",budget_date(day-1)}});check(summary["currencies"]["AUD"]["amount_cents"]==15000&&summary["currencies"]["AUD"]["gst_cents"]==700&&summary["currencies"]["AUD"]["gst_unspecified_count"]==1,"actual totals and partial GST");
  NotificationStore notifications(config);notifications.sequence_events();auto before=notifications.latest();store.tick();store.tick();notifications.sequence_events();auto events=notifications.after(before);int reminders=0;for(const auto& e:events)if(e["type"]=="budget.subscription.reminder")reminders++;check(reminders==1,"reminder deduplication");
  json payment={{"revision",1},{"scheduled_date",today},{"spent_on",today}};
  auto concurrent=std::async(std::launch::async,[&]{return store.payment(sid,payment);});auto paid=store.payment(sid,payment);auto other=concurrent.get();check(paid["expense_id"]==other["expense_id"]&&paid["already_recorded"]!=other["already_recorded"],"atomic concurrent payment deduplication");
  check(store.get(true,sid)->at("revision")==2,"payment advances once");check(store.forecast({{"from",today},{"to",budget_date(day+60)}})["charges"].empty(),"inclusive subscription end date respected");
  auto eid=paid["expense_id"].get<std::string>();check(store.remove(false,eid,1),"delete recorded expense");auto retry=store.payment(sid,payment);check(retry["already_recorded"]==true&&retry["expense"].is_null(),"deleted expense is not recreated by retry");
  rejects([&]{store.remove(true,sid,1);});check(store.remove(true,sid,2),"delete subscription preserves historical expenses");check(store.payment(sid,payment)["already_recorded"]==true,"receipt survives subscription deletion");
  auto paused=store.create(true,{{"name","Paused"},{"amount_cents",500},{"cadence","weekly"},{"next_billing_date",today},{"status","paused"}});check(store.forecast({{"from",today},{"to",today}})["charges"].empty(),"paused excluded");rejects([&]{store.payment(paused["id"],{{"revision",1},{"scheduled_date",today}});});
  auto active=store.create(true,{{"name","Weekly"},{"amount_cents",500},{"cadence","weekly"},{"next_billing_date",budget_date(day-7)}});auto scheduled=store.forecast({{"from",today},{"to",budget_date(day+7)},{"mode","subscriptions"}});check(scheduled["charges"].size()==2&&scheduled["currencies"]["AUD"]["total_cents"]==1000&&scheduled["overdue_subscriptions"].size()==1,"inclusive weekly schedule and overdue excluded");check(store.get(true,active["id"])->at("next_billing_date")==budget_date(day-7),"forecast does not mutate schedule");
  configure_budget(server,std::make_shared<BudgetStore>(config),nullptr);auto port=server.bind_to_any_port("127.0.0.1");check(port>0,"bind");worker=std::thread([&]{server.listen_after_bind();});server.wait_until_ready();httplib::Client client("127.0.0.1",port);
  auto response=client.Get("/budget/forecast?from=2020-01-01");check(response&&response->status==400,"past forecast rejected");
  response=client.Get("/budget/forecast?mode=bogus");check(response&&response->status==400,"invalid forecast mode");response=client.Get("/budget/expenses?limit=0");check(response&&response->status==400,"pagination validation");
  response=client.Post("/budget/expenses",R"({"name":"Test","amount_cents":123,"gst_cents":124})","application/json");check(response&&response->status==400,"GST HTTP validation");response=client.Post("/budget/expenses",R"({"name":"Test","amount_cents":123,"amount_cents":456})","application/json");check(response&&response->status==400,"duplicate keys rejected");
  response=client.Post("/budget/expenses",R"({"name":"Test","amount_cents":123,"oops":1})","application/json");check(response&&response->status==400&&json::parse(response->body)["error"]=="unknown_field:oops","unknown field named");
  response=client.Post("/budget/expenses",R"({"name":"Test","amount_cents":123})","application/json");check(response&&response->status==201,"HTTP creation");auto record=json::parse(response->body);response=client.Get("/budget/expenses/"+record["id"].get<std::string>());check(response&&response->status==200&&json::parse(response->body)==record,"HTTP get");
  response=client.Patch("/budget/expenses/"+record["id"].get<std::string>(),R"({"revision":99,"name":"Edit"})","application/json");check(response&&response->status==409,"HTTP stale conflict");
  response=client.Get("/budget/explorer");check(response&&response->status==200&&response->body.find("Record payment")!=std::string::npos,"explorer route");
  response=client.Get("/budget/expenses/00000000-0000-0000-0000-000000000000");check(response&&response->status==404,"missing expense");
  notifications.sequence_events();auto all=notifications.after(0);std::set<std::string> types;for(const auto& e:all)types.insert(e["type"]);for(auto type:{"budget.expense.created","budget.expense.updated","budget.expense.deleted","budget.subscription.created","budget.subscription.updated","budget.subscription.deleted","budget.subscription.reminder"})check(types.count(type),"transactional change notification missing");
  server.stop();worker.join();std::cout<<"Budget CRUD, GST, currencies, forecasts, recurrence, concurrent payments, reminders, HTTP validation and notification tests passed.\n";return 0;
 }catch(const std::exception& e){server.stop();if(worker.joinable())worker.join();std::cerr<<e.what()<<'\n';return 1;}
}
