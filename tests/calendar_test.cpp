#include "api.hpp"
#include "calendar.hpp"
#include "timers.hpp"
#include "pulse.hpp"
#include "notifications.hpp"
#include <mysql.h>
#include <iostream>
#include <thread>
#include <arpa/inet.h>
#include <sys/socket.h>
#include <poll.h>
#include <unistd.h>
using nlohmann::json;
void check(bool value, const char* message) { if (!value) throw std::runtime_error(message); }
json expect(const httplib::Result& response, int status, const char* error = nullptr) {
    check(response && response->status == status, "calendar HTTP status");
    if (status == 204) { check(response->body.empty(), "delete body"); return nullptr; }
    auto body = json::parse(response->body);
    if (error) check(body["error"] == error, "calendar error");
    return body;
}
struct DB {
    MYSQL* handle;
    explicit DB(const spark::DatabaseConfig& c) : handle(mysql_init(nullptr)) {
        check(handle && mysql_real_connect(handle,c.host.c_str(),c.user.c_str(),c.password.c_str(),c.database.c_str(),c.port,c.socket.empty()?nullptr:c.socket.c_str(),0), "SQL connect");
    }
    ~DB() { mysql_close(handle); }
    void run(const std::string& sql) { check(mysql_query(handle,sql.c_str()) == 0,"SQL statement"); }
    unsigned long long count(const std::string& sql) {
        run(sql);
        auto result = std::unique_ptr<MYSQL_RES,decltype(&mysql_free_result)>(mysql_store_result(handle),mysql_free_result);
        check(bool(result),"SQL result"); auto row=mysql_fetch_row(result.get()); check(row && row[0],"SQL count"); return std::stoull(row[0]);
    }
};
struct Peer {
    int fd;std::string input;
    explicit Peer(int port):fd(socket(AF_INET,SOCK_STREAM,0)) {sockaddr_in addr{};addr.sin_family=AF_INET;addr.sin_port=htons(port);inet_pton(AF_INET,"127.0.0.1",&addr.sin_addr);check(fd>=0 && connect(fd,reinterpret_cast<sockaddr*>(&addr),sizeof(addr))==0,"TCP connect");const std::string hello="{\"type\":\"hello\",\"device_id\":\"task-client\"}\n";check(send(fd,hello.data(),hello.size(),MSG_NOSIGNAL)==static_cast<ssize_t>(hello.size()),"hello send");until("welcome");}
    ~Peer(){close(fd);}
    json until(const std::string& type){
        const auto deadline=std::chrono::steady_clock::now()+std::chrono::seconds(5);
        while(std::chrono::steady_clock::now()<deadline){
            auto pos=input.find('\n');
            if(pos!=std::string::npos){auto frame=json::parse(input.substr(0,pos));input.erase(0,pos+1);if(frame["type"]==type)return frame;continue;}
            pollfd p{fd,POLLIN,0};if(poll(&p,1,100)>0){char bytes[4096];auto n=recv(fd,bytes,sizeof(bytes),0);check(n>0,"TCP read");input.append(bytes,n);}
        }
        throw std::runtime_error("Missing TCP "+type);
    }
};
int main() {
    const auto config = spark::DatabaseConfig::from_environment();
    if (config.database.rfind("spark_test_",0) != 0) return 77;
    auto store=std::make_shared<spark::CalendarStore>(config); store->verify();
    httplib::Server server;
    spark::configure_api(server,std::make_shared<spark::DeviceStore>(config),std::make_shared<spark::TimerStore>(config),47684,std::make_shared<spark::PulseStore>(config),store);
    const auto port=server.bind_to_any_port("127.0.0.1"); if(port<0) return 1;
    std::thread thread([&]{server.listen_after_bind();}); server.wait_until_ready();
    int result=0;
    try {
        httplib::Client client("127.0.0.1",port); client.set_read_timeout(10,0);
        auto create = [&](const json& body) { return expect(client.Post("/events",body.dump(),"application/json"),201); };
        const json input={{"name","Walk 🌳"},{"description","Meet by the gate"},{"occurs_at","2026-09-19T08:00:00+08:00"},{"place","Park"},{"source_device","watch-1"}};
        auto first=create(input); const auto path="/events/"+first["id"].get<std::string>();
        check(first["occurs_at"]=="2026-09-19T00:00:00Z","UTC normalization");
        check(expect(client.Get(path),200)==first,"get created event");
        auto head=client.Head(path); check(head && head->status==200 && head->body.empty(),"event HEAD");
        auto options=client.Options(path); check(options && options->status==204 && options->get_header_value("Allow").find("PATCH")!=std::string::npos,"event OPTIONS");
        expect(client.Post(path,"{}","application/json"),405,"method_not_allowed");
        auto second_input=input; second_input["name"]="Lunch"; second_input["occurs_at"]="2026-09-19T12:00:00Z";
        auto second=create(second_input);
        auto third_input=input; third_input["name"]="Walk tomorrow"; third_input["place"]="Office"; third_input["occurs_at"]="2026-09-20T00:00:00Z";
        auto third=create(third_input);
        auto fourth_input=input; fourth_input["name"]="Night"; fourth_input["place"]="park"; fourth_input["occurs_at"]="2026-09-18T23:59:59Z";
        create(fourth_input);
        check(expect(client.Get("/events?day=2026-09-19"),200)["events"].size()==2,"UTC day boundaries");
        check(expect(client.Get("/events?from=2026-09-19T00:00:00Z&to=2026-09-20T00:00:00Z"),200)["events"].size()==2,"half-open range");
        check(expect(client.Get("/events?from=2026-09-19T08:00:00%2B08:00&to=2026-09-19T13:00:00Z&place=Park&name=Walk"),200)["events"].size()==1,"combined offset, place and name filters");
        check(expect(client.Get("/events?place=park"),200)["events"].size()==1,"case-sensitive place");
        check(expect(client.Get("/events?name=%25"),200)["events"].empty(),"literal substring not SQL wildcard");
        check(expect(client.Get("/events?place=%27%20OR%201%3D1%20--"),200)["events"].empty(),"bound filter parameters");
        auto page=expect(client.Get("/events?limit=2"),200); check(page["has_more"]==true && page["events"].size()==2,"pagination first page");
        auto last=expect(client.Get("/events?limit=2&offset=2"),200); check(last["has_more"]==false && last["events"].size()==2,"pagination last page");
        check(page["events"][0]["name"]=="Night" && last["events"][1]["id"]==third["id"],"chronological ordering");
        check(expect(client.Get("/events?offset=50"),200)["events"].empty(),"empty page");
        auto changed=expect(client.Patch(path,R"({"description":"New meeting point","place":null})","application/json"),200);
        check(changed["name"]==first["name"] && changed["place"].is_null(),"partial update and clear place");
        check(expect(client.Patch(path,R"({"description":"New meeting point","place":null})","application/json"),200)==changed,"idempotent patch");
        DB db(config);
        check(db.count("SELECT COUNT(*) FROM events WHERE resource='"+path+"'")==2,"one create and one update event");
        // Escaped control characters force the JSON result beyond the initial 8 KiB buffer.
        auto large=input; large["name"]="Large"; large["description"]=std::string(2000,'\1');
        const auto large_event=create(large);
        check(expect(client.Get("/events/"+large_event["id"].get<std::string>()),200)["description"]==large["description"],"large escaped JSON roundtrip");
        for(const std::string bad : {"2026-02-29T00:00:00Z","2026-09-19T24:00:00Z","2026-09-19T10:00:00","2026-09-19T00:00:00+15:00","0999-12-31T00:00:00Z"}) {
            auto invalid=input; invalid["occurs_at"]=bad; expect(client.Post("/events",invalid.dump(),"application/json"),400,"invalid_datetime");
        }
        auto leap=input; leap["occurs_at"]="2028-02-29T23:00:00-02:00";
        check(create(leap)["occurs_at"]=="2028-03-01T01:00:00Z","leap day and negative offset rollover");
        expect(client.Post("/events","{}","application/json"),400,"missing_fields");
        expect(client.Post("/events","[]","application/json"),400,"invalid_packet");
        expect(client.Post("/events","{}","text/plain"),415,"unsupported_media_type");
        expect(client.Patch(path,"{}","application/json"),400,"missing_fields");
        expect(client.Patch(path,R"({"name":"   "})","application/json"),400,"invalid_name");
        expect(client.Patch(path,R"({"extra":1})","application/json"),400,"unknown_field");
        expect(client.Get("/events?day=2026-02-29"),400,"invalid_day");
        expect(client.Get("/events?from=2026-09-20T00:00:00Z&to=2026-09-19T00:00:00Z"),400,"invalid_range");
        expect(client.Get("/events?place=Park&place=Office"),400,"duplicate_filter");
        expect(client.Get("/events?limit=501"),400,"invalid_pagination");
        expect(client.Get("/events?offset=-1"),400,"invalid_pagination");
        expect(client.Get("/events?unknown=value"),400,"unknown_filter");
        expect(client.Get("/events/bad"),400,"invalid_event_id");
        const auto missing="/events/00000000-0000-0000-0000-000000000000";
        expect(client.Get(missing),404,"event_not_found");
        expect(client.Patch(missing,R"({"name":"Missing"})","application/json"),404,"event_not_found");
        expect(client.Delete(path),204); expect(client.Get(path),404,"event_not_found"); expect(client.Delete(path),404,"event_not_found");
        check(db.count("SELECT COUNT(*) FROM events WHERE resource='"+path+"' AND type='calendar.deleted'")==1,"delete history");
        db.run("CREATE TRIGGER fail_calendar_event BEFORE INSERT ON events FOR EACH ROW BEGIN IF NEW.type LIKE 'calendar.%' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test event failure'; END IF; END");
        auto rejected=input; rejected["name"]="Rollback";
        expect(client.Post("/events",rejected.dump(),"application/json"),503,"database_unavailable");
        check(db.count("SELECT COUNT(*) FROM calendar_events WHERE name='Rollback'")==0,"create event rollback");
        const auto second_path="/events/"+second["id"].get<std::string>();
        expect(client.Patch(second_path,R"({"name":"Should rollback"})","application/json"),503,"database_unavailable");
        expect(client.Delete(second_path),503,"database_unavailable");
        check(expect(client.Get(second_path),200)==second,"mutation rollback");
        db.run("DROP TRIGGER fail_calendar_event");
        check(std::make_shared<spark::CalendarStore>(config)->get(second["id"])==second,"fresh store persistence");
        auto recurring=input;
        recurring["name"]="Fortnightly";
        recurring["occurs_at"]="2030-01-01T09:00:00+08:00";
        recurring["recurrence"]={{"weekdays",json::array({2,3})},{"interval_weeks",2},{"start_date","2030-01-01"},{"end_date","2030-02-16"}};
        auto series=create(recurring);
        check(series["recurrence"]["timezone"]=="+08:00","default recurrence timezone");
        check(expect(client.Get("/events?day=2030-01-02&name=Fortnightly"),200)["events"].size()==1,"Wednesday same recurrence week");
        check(expect(client.Get("/events?day=2030-01-08&name=Fortnightly"),200)["events"].empty(),"off week");
        check(expect(client.Get("/events?day=2030-01-15&name=Fortnightly"),200)["events"].size()==1,"next fortnight");
        check(expect(client.Get("/events?day=2030-02-26&name=Fortnightly"),200)["events"].empty(),"end bound");
        check(spark::calendar_occurrence(series,spark::calendar_instant("2030-01-02T01:00:01Z"))==spark::calendar_instant("2030-01-15T01:00:00Z"),"strict next occurrence");
        const std::string occurrence_path="/events/occurrences?from=2030-01-01T01:00:00Z&to=2030-02-17T00:00:00Z&name=Fortnightly";
        auto instances=expect(client.Get(occurrence_path),200);
        check(instances["occurrences"].size()==8 && instances["has_more"]==false,"expand alternate Tuesdays and Wednesdays through inclusive end date");
        check(instances["occurrences"][1]["occurrence_at"]=="2030-01-02T01:00:00Z" && instances["occurrences"][2]["occurrence_at"]=="2030-01-15T01:00:00Z","occurrence order and off-week exclusion");
        for(const auto& instance:instances["occurrences"]) check(instance["event_id"]==series["id"] && instance["event"]==series,"parent record and anchor preserved");
        auto occurrence_page=expect(client.Get(occurrence_path+"&limit=2&offset=2"),200);
        check(occurrence_page["has_more"]==true && occurrence_page["occurrences"][0]==instances["occurrences"][2],"pagination counts occurrences");
        check(expect(client.Get(occurrence_path+"&offset=8"),200)["occurrences"].empty(),"offset beyond occurrences");
        check(expect(client.Get(occurrence_path+"&limit=2&offset=6"),200)["has_more"]==false,"exact final page");
        auto single=recurring; single["recurrence"]=nullptr; single["occurs_at"]="2030-01-02T01:00:00Z";
        auto single_record=create(single);
        auto mixed=expect(client.Get("/events/occurrences?from=2030-01-02T09:00:00%2B08:00&to=2030-01-15T01:00:00Z&name=Fortnightly&place=Park"),200)["occurrences"];
        check(mixed.size()==2 && mixed[0]["event_id"].get<std::string>()<mixed[1]["event_id"].get<std::string>(),"one-off and recurring merge, offset normalization, inclusive lower/exclusive upper and ID tie-break");
        expect(client.Delete("/events/"+single_record["id"].get<std::string>()),204);
        check(expect(client.Get(occurrence_path+"&place=park"),200)["occurrences"].empty(),"case-sensitive occurrence place filter");
        expect(client.Get("/events/occurrences"),400,"missing_range");
        expect(client.Get("/events/occurrences?from=2030-01-01T00:00:00Z"),400,"missing_range");
        expect(client.Get("/events/occurrences?from=2030-01-01T00:00:00Z&to=2030-01-01T00:00:00Z"),400,"invalid_range");
        expect(client.Get(occurrence_path+"&from=2030-01-01T00:00:00Z"),400,"duplicate_filter");
        expect(client.Get(occurrence_path+"&day=2030-01-01"),400,"unknown_filter");
        expect(client.Get(occurrence_path+"&limit=501"),400,"invalid_pagination");
        expect(client.Get("/events/occurrences?from=bad&to=2030-01-01T00:00:00Z"),400,"invalid_datetime");
        check(client.Head(occurrence_path)->status==200,"occurrence HEAD");
        check(client.Options("/events/occurrences")->get_header_value("Allow")=="GET, HEAD, OPTIONS","occurrence OPTIONS");
        expect(client.Delete("/events/occurrences"),405,"method_not_allowed");
        auto boundary=series;
        boundary["occurs_at"]="2028-02-27T16:30:00Z";
        boundary["recurrence"]={{"weekdays",json::array({2})},{"interval_weeks",1},{"start_date","2028-02-28"},{"end_date","2028-02-29"}};
        check(spark::calendar_occurrence(boundary,spark::calendar_instant("2028-02-27T00:00:00Z"))==spark::calendar_instant("2028-02-28T16:30:00Z"),"inclusive leap day with Perth UTC crossover");
        boundary["recurrence"]["end_date"]="2028-02-28";
        check(!spark::calendar_occurrence(boundary,spark::calendar_instant("2028-02-27T00:00:00Z")),"date range without occurrence");
        auto invalid=recurring; invalid["recurrence"]["weekdays"]=json::array({2,2});
        expect(client.Post("/events",invalid.dump(),"application/json"),400,"invalid_recurrence");
        invalid=recurring; invalid["recurrence"]["end_date"]="2029-12-31";
        expect(client.Post("/events",invalid.dump(),"application/json"),400,"invalid_recurrence");
        invalid=input; invalid["reminder_minutes"]=-1;
        expect(client.Post("/events",invalid.dump(),"application/json"),400,"invalid_reminder_minutes");
        invalid["reminder_minutes"]=1.5;
        expect(client.Post("/events",invalid.dump(),"application/json"),400,"invalid_reminder_minutes");
        auto reminder=input; reminder["name"]="Reminder"; reminder["occurs_at"]=spark::calendar_timestamp(std::time(nullptr)+120); reminder["reminder_minutes"]=5;
        const auto reminded=create(reminder); const auto rid=reminded["id"].get<std::string>();
        db.run("CREATE TRIGGER fail_reminder BEFORE INSERT ON events FOR EACH ROW BEGIN IF NEW.type='calendar.reminder' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test'; END IF; END");
        bool failed=false; try {store->tick();} catch(const spark::DatabaseError&) {failed=true;}
        check(failed && store->get(rid)->at("last_reminder_occurs_at").is_null(),"reminder transaction rollback");
        db.run("DROP TRIGGER fail_reminder");
        std::thread a([&]{store->tick();}), b([&]{store->tick();}); a.join(); b.join();
        store->tick();
        check(db.count("SELECT COUNT(*) FROM events WHERE type='calendar.reminder' AND resource='/events/"+rid+"'")==1,"concurrent reminders deduplicated");
        spark::CalendarStore(config).tick();
        check(db.count("SELECT COUNT(*) FROM events WHERE type='calendar.reminder' AND resource='/events/"+rid+"'")==1,"reminder state survives new store");
        spark::NotificationStore notifications(config); notifications.sequence_events();
        bool found=false; unsigned long long cursor=0;
        for(;;) { auto frames=notifications.after(cursor); if(frames.empty()) break; for(const auto& frame:frames) {cursor=frame["sequence"]; if(frame["type"]=="calendar.reminder" && frame["resource"]=="/events/"+rid) {check(frame["occurrence_at"]==reminded["occurs_at"],"reminder notification occurrence"); found=true;}} }
        check(found,"reminder in notification channel");
        expect(client.Patch("/events/"+rid,R"({"reminder_minutes":null})","application/json"),200);
        auto channel=std::make_shared<spark::NotificationStore>(config); channel->sequence_events();
        spark::NotificationOptions notification_options; notification_options.host="127.0.0.1"; notification_options.port=0;
        spark::NotificationServer tcp(channel,notification_options); Peer peer(tcp.port());
        auto zero=input; zero["name"]="Zero minute reminder"; zero["occurs_at"]=spark::calendar_timestamp(std::time(nullptr)-2); zero["reminder_minutes"]=0;
        auto zero_event=create(zero);
        { spark::CalendarScheduler scheduler(store); const auto frame=peer.until("calendar.reminder");
          check(frame["resource"]=="/events/"+zero_event["id"].get<std::string>() && frame["occurrence_at"]==zero_event["occurs_at"],"scheduler TCP reminder including zero-minute grace"); }
        auto recurring_reminder=zero;
        const auto next_time=std::time(nullptr)+120;
        recurring_reminder["name"]="Recurring TCP reminder";
        recurring_reminder["occurs_at"]=spark::calendar_timestamp(next_time);
        recurring_reminder["reminder_minutes"]=5;
        const auto perth=next_time+28800;std::tm local{};gmtime_r(&perth,&local);
        recurring_reminder["recurrence"]={{"weekdays",json::array({(local.tm_wday+6)%7+1})},{"start_date",spark::calendar_timestamp(perth).substr(0,10)}};
        const auto recurring_due=create(recurring_reminder);
        { spark::CalendarScheduler scheduler(store);const auto frame=peer.until("calendar.reminder");
          check(frame["resource"]=="/events/"+recurring_due["id"].get<std::string>() && frame["occurrence_at"]==recurring_due["occurs_at"],"recurring advance reminder delivered over TCP"); }
        // The observer and scheduler must already be running before creation.
        // Previous TCP checks only exercised reminders due on the first sweep.
        {
            spark::CalendarScheduler scheduler(store);
            std::this_thread::sleep_for(std::chrono::milliseconds(1200));
            auto future=zero;future["name"]="Created after scheduler startup";
            future["occurs_at"]=spark::calendar_timestamp(std::time(nullptr)+3);
            const auto future_event=create(future);
            check(future_event["last_reminder_occurs_at"].is_null(),"future reminder initially unmarked");
            const auto frame=peer.until("calendar.reminder");
            check(frame["resource"]=="/events/"+future_event["id"].get<std::string>() && frame["occurrence_at"]==future_event["occurs_at"],"running scheduler discovers new future events");
            check(store->get(future_event["id"])->at("last_reminder_occurs_at")==future_event["occurs_at"],"future reminder marker committed");
        }
        auto stale=zero; stale["occurs_at"]=spark::calendar_timestamp(std::time(nullptr)-7200);
        auto stale_event=create(stale); store->tick();
        check(store->get(stale_event["id"])->at("last_reminder_occurs_at").is_null(),"skip stale reminders");

        auto disabled=expect(client.Patch("/events/"+series["id"].get<std::string>(),R"({"recurrence":null})","application/json"),200);
        check(disabled["recurrence"].is_null(),"disable recurrence");
        std::cout<<"Calendar CRUD, recurrence, Perth boundaries, reminder rollback/deduplication and TCP scheduler passed\n";
    } catch(const std::exception& error) { std::cerr<<error.what()<<'\n'; result=1; }
    server.stop(); thread.join(); return result;
}
