#include "mortgage.hpp"
#include "notifications.hpp"
#include <iostream>
#include <future>
#include <cmath>
using namespace spark;using json=nlohmann::json;
void check(bool ok,const char* why){if(!ok)throw std::runtime_error(why);}
template<class F>void fails(F f){bool bad=false;try{f();}catch(const std::exception&){bad=true;}check(bad,"expected validation failure");}
json expect(const httplib::Result& r,int status){check(r&&r->status==status,"HTTP status");return json::parse(r->body);}
int main(){httplib::Server server;std::thread worker;try{
 json p={{"remaining_principal_cents",50000000},{"annual_interest_rate_bps",600},{"monthly_payment_cents",350000},{"offset_balance_cents",5000000},{"as_of","2026-11-23"}};
 auto stats=mortgage_stats(p);check(stats["first_month_interest_cents"]==225000&&stats["first_month_principal_reduction_cents"]==125000,"offset interest and principal");check(stats["interest_saved_by_offset_cents"]>0&&stats["months_saved_by_offset"]>0,"offset reduces time and interest");check(stats["remaining_payments_cents"].get<int64_t>()==50000000+stats["remaining_interest_cents"].get<int64_t>(),"payment accounting identity");
 auto nooffset=p;nooffset["offset_balance_cents"]=0;auto baseline=mortgage_stats(nooffset);double n=std::ceil(-std::log(1-50000000*.005/350000)/std::log(1.005));check(baseline["months_remaining"].get<int>()==int(n),"matches closed form no-offset amortization");
 auto zero=p;zero["annual_interest_rate_bps"]=0;zero["remaining_principal_cents"]=10001;zero["monthly_payment_cents"]=1000;check(mortgage_stats(zero)["months_remaining"]==11&&mortgage_stats(zero)["final_payment_cents"]==1,"zero rate final partial payment");
 auto covered=zero;covered["annual_interest_rate_bps"]=10000;check(mortgage_stats(covered)["months_remaining"]==11&&mortgage_stats(covered)["remaining_interest_cents"]==0,"fully offset still requires principal repayment");
 auto paid=p;paid["remaining_principal_cents"]=0;paid["monthly_payment_cents"]=0;check(mortgage_stats(paid)["status"]=="paid_off"&&mortgage_stats(paid)["months_remaining"]==0,"zero principal");
 auto low=nooffset;low["monthly_payment_cents"]=250000;check(mortgage_stats(low)["status"]=="payment_too_low"&&mortgage_stats(low)["months_remaining"].is_null(),"interest-only no payoff");low["monthly_payment_cents"]=1;check(mortgage_stats(low)["first_month_principal_reduction_cents"]<0,"negative amortization sign");
 low=covered;low["monthly_payment_cents"]=0;check(mortgage_stats(low)["status"]=="payment_too_low","zero payment fully offset");
 auto longloan=zero;longloan["remaining_principal_cents"]=1201;longloan["monthly_payment_cents"]=1;check(mortgage_stats(longloan)["status"]=="horizon_exceeded"&&mortgage_stats(longloan)["remaining_interest_cents"].is_null(),"horizon distinct from infinite");longloan["remaining_principal_cents"]=1200;check(mortgage_stats(longloan)["months_remaining"]==1200,"horizon boundary included");
 auto penny=nooffset;penny["remaining_principal_cents"]=6;penny["annual_interest_rate_bps"]=10000;penny["monthly_payment_cents"]=7;check(mortgage_stats(penny)["first_month_interest_cents"]==1&&mortgage_stats(penny)["months_remaining"]==1,"round half cent upward");
 for(auto field:{"remaining_principal_cents","annual_interest_rate_bps","monthly_payment_cents","offset_balance_cents"}){auto invalid=p;invalid[field]=-1;fails([&]{mortgage_stats(invalid);});invalid[field]=1.5;fails([&]{mortgage_stats(invalid);});invalid.erase(field);fails([&]{mortgage_stats(invalid);});}
 auto invalid=p;invalid["annual_interest_rate_bps"]=10001;fails([&]{mortgage_stats(invalid);});invalid=p;invalid["remaining_principal_cents"]=1000000000001LL;fails([&]{mortgage_stats(invalid);});invalid=p;invalid["as_of"]="2026-02-30";fails([&]{mortgage_stats(invalid);});invalid=p;invalid["unknown"]=0;fails([&]{mortgage_stats(invalid);});
 std::cout<<"Mortgage math: monthly amortization, zero rate, full offset, partial payments, insufficient payments, rounding, horizon and validation passed.\n";
 auto config=DatabaseConfig::from_environment();if(config.database.rfind("spark_test_",0)!=0)return 0;
 auto store=std::make_shared<BudgetStore>(config);store->verify();check(store->mortgage()["mortgage"].is_null(),"initial snapshot empty");auto saved=store->save_mortgage(p,0);check(saved["revision"]==1&&saved["stats"]==stats,"snapshot stored with estimates");check(store->save_mortgage(p,1)["revision"]==1,"no-op preserves revision");fails([&]{store->save_mortgage(p,0);});
 auto changed=p;changed["monthly_payment_cents"]=360000;
 auto attempt=[&]{try{store->save_mortgage(changed,1);return 1;}catch(const BudgetConflict&){return 0;}};auto future=std::async(std::launch::async,attempt);int successes=attempt()+future.get();check(successes==1,"concurrent writes serialize by revision");check(store->mortgage()["revision"]==2,"single revision advance");
 auto cleared=store->save_mortgage(nullptr,2);check(cleared["mortgage"].is_null()&&cleared["stats"].is_null()&&cleared["revision"]==3,"clear returns tombstone");fails([&]{store->save_mortgage(p,0);});check(store->save_mortgage(p,3)["revision"]==4,"recreate uses latest revision");
 configure_mortgage(server,store,nullptr);auto port=server.bind_to_any_port("127.0.0.1");check(port>0,"bind");worker=std::thread([&]{server.listen_after_bind();});server.wait_until_ready();httplib::Client client("127.0.0.1",port);
 auto before=store->mortgage();auto preview=expect(client.Post("/budget/mortgage/estimate",changed.dump(),"application/json"),200);check(preview["stats"]["months_remaining"]<before["stats"]["months_remaining"]&&store->mortgage()==before,"preview has no side effects");check(expect(client.Get("/budget/mortgage"),200)==before,"GET state");
 expect(client.Put("/budget/mortgage",p.dump(),"application/json"),400);auto packet=p;packet["revision"]=0;expect(client.Put("/budget/mortgage",packet.dump(),"application/json"),409);packet["revision"]=4;packet["offset_balance_cents"]=7000000;expect(client.Put("/budget/mortgage",packet.dump(),"application/json"),200);
 expect(client.Delete("/budget/mortgage?revision=4"),409);expect(client.Delete("/budget/mortgage?revision=5"),200);expect(client.Post("/budget/mortgage/estimate",R"({"remaining_principal_cents":1,"remaining_principal_cents":2})","application/json"),400);
 NotificationStore notifications(config);notifications.sequence_events();int count=0;for(const auto& event:notifications.after(0))if(event["type"]=="budget.mortgage.updated"){++count;check(!event.dump().empty(),"notification");}check(count==6,"notify only committed changes, not preview or noop");
 server.stop();worker.join();std::cout<<"Mortgage database/API: persistence, conflicts, concurrent saves, clear/recreate, preview isolation and notifications passed.\n";return 0;
 }catch(const std::exception& e){server.stop();if(worker.joinable())worker.join();std::cerr<<e.what()<<'\n';return 1;}}
