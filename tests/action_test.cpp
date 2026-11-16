#include "assistant.hpp"
#include "timers.hpp"
#include "calendar.hpp"
#include <mysql.h>
#include <fstream>
#include <thread>
#include <sys/stat.h>
#include <unistd.h>
#include <iostream>
using nlohmann::json;
void check(bool b,const char* m){if(!b)throw std::runtime_error(m);}
int main(){
 if(!getenv("SPARK_DB_SOCKET"))return 77;
 auto db=spark::DatabaseConfig::from_environment();
 MYSQL* conn=mysql_init(nullptr);check(mysql_real_connect(conn,db.host.c_str(),db.user.c_str(),db.password.c_str(),db.database.c_str(),db.port,db.socket.c_str(),0),"connect");
 auto sql=[&](const std::string& s){check(mysql_query(conn,s.c_str())==0,"test SQL");};
 sql("SET time_zone='+00:00'");
 auto actions=std::make_shared<spark::ActionStore>(db);actions->verify();spark::TimerStore timers(db);
 json packet={{"label","Assistant test"},{"duration_seconds",600}};
 std::string id(64,'a'),fingerprint(64,'f');
 auto action=actions->create_timer(id,"device:test",fingerprint,packet);
 check(action["after_state"]["status"]=="running","created");
 check(action["undo_available"]==true,"fresh action undo eligibility");
 auto repeat=actions->create_timer(id,"device:test",fingerprint,packet);check(repeat["resource"]==action["resource"],"idempotency");
 bool failed=false;try{actions->create_timer(id,"device:test",std::string(64,'b'),packet);}catch(...){failed=true;}check(failed,"fingerprint conflict");
 check(!actions->get(id,"device:other"),"owner isolation");
 sql("CREATE TRIGGER fail_undo_event BEFORE INSERT ON events FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test failure'");failed=false;try{actions->undo(id,"device:test");}catch(...){failed=true;}sql("DROP TRIGGER fail_undo_event");check(failed&&actions->get(id,"device:test")->at("status")=="completed","undo rollback");
 auto undone=actions->undo(id,"device:test");check(undone["status"]=="undone"&&undone["undo_state"]["after"]["status"]=="stopped","undo");
 check(actions->undo(id,"device:test")==undone,"idempotent undo");
 auto conflicting=actions->create_timer(std::string(64,'b'),"device:test",fingerprint,packet);
 timers.transition(conflicting["after_state"]["id"],"stop",nullptr);failed=false;try{actions->undo(std::string(64,'b'),"device:test");}catch(...){failed=true;}check(failed,"later stop conflict");
 actions->create_timer(std::string(64,'c'),"device:test",fingerprint,packet);
 sql("UPDATE action_history SET undo_until=TIMESTAMPADD(SECOND,-1,CURRENT_TIMESTAMP(6)) WHERE id='"+std::string(64,'c')+"'");failed=false;try{actions->undo(std::string(64,'c'),"device:test");}catch(...){failed=true;}check(failed,"undo expiry");
 auto expired=actions->create_timer(std::string(64,'d'),"device:test",fingerprint,{{"duration_seconds",1},{"label","expires"}});sleep(2);timers.expire();check(actions->undo(std::string(64,'d'),"device:test")["status"]=="undone","natural expiry undo");
 auto timer_count=timers.active()["timers"].size();
 sql("CREATE TRIGGER fail_assistant_event BEFORE INSERT ON events FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test failure'");failed=false;try{actions->create_timer(std::string(64,'e'),"device:test",fingerprint,packet);}catch(...){failed=true;}sql("DROP TRIGGER fail_assistant_event");check(failed&&!actions->get(std::string(64,'e'),"device:test"),"transaction rollback");check(timers.active()["timers"].size()==timer_count,"timer rolled back with failed event");
 json first,second;std::exception_ptr e1,e2;std::thread t1([&]{try{first=actions->create_timer(std::string(64,'1'),"device:test",fingerprint,packet);}catch(...){e1=std::current_exception();}});std::thread t2([&]{try{second=actions->create_timer(std::string(64,'1'),"device:test",fingerprint,packet);}catch(...){e2=std::current_exception();}});t1.join();t2.join();check(!e1&&!e2&&first["resource"]==second["resource"],"concurrent deduplication");
 char temp[]="/tmp/spark-action-harness-XXXXXX";check(mkdtemp(temp),"temp");std::filesystem::path root(temp),key=root/"key.json";{std::ofstream f(key);f<<R"({"api_key":"fake"})";}chmod(key.c_str(),0600);spark::AssistantConfig config{key,root};
 spark::Identity who{"test-token","assistant-test","device",{"assistant.use","timers.write","timers.read"}};
 int calls=0;std::string mode="create_timer";
 std::string model_instructions;json model_input;json overrides=json::object();
 auto transport=[&](const json& request){++calls;model_instructions=request["instructions"];model_input={{"history",json::array()},{"current_user_text",request["input"].back()["content"]}};for(size_t i=0;i+1<request["input"].size();i+=2){check(request["input"][i]["role"]=="user"&&request["input"][i+1]["role"]=="assistant","provider conversation roles");model_input["history"].push_back({{"user",request["input"][i]["content"]},{"result",json::parse(request["input"][i+1]["content"].get<std::string>())}});}json result={{"status",mode},{"message","proposed"},{"duration_seconds",mode=="create_timer"?json(300):json(nullptr)},{"label",mode=="create_timer"?json("Tea"):json(nullptr)}};for(auto key:{"calendar","name","description","occurs_at","place","from","to","reminder_minutes","target_event_id","target_name","scope","recurrence","project_id","project_name","search_query"})result[key]=nullptr;result["edit_fields"]=json::array();result.update(overrides);json content={{"type","output_text"},{"text",result.dump()}};json message={{"type","message"},{"role","assistant"},{"content",json::array({content})}};return json{{"status","completed"},{"output",json::array({message})}};};
 spark::Assistant assistant(config,transport,actions);json input={{"request_id","create"},{"text","Set a five minute tea timer"}};auto reply=assistant.query(who,input);check(reply["status"]=="completed"&&reply["undo_available"]==true,"tool create");check(assistant.query(who,input)==reply&&calls==1,"cached tool reply");
 for(auto& entry:std::filesystem::directory_iterator(root))if(entry.path().extension()==".json"&&entry.path()!=key)std::filesystem::remove(entry.path());
 auto recovered=assistant.query(who,input);check(recovered["action_id"]==reply["action_id"]&&calls==1,"DB reconciliation without receipt");
 assistant.action(who,reply["action_id"],true);check(assistant.get(who,"create")["undo_available"]==false,"undo reflected in receipt");
 who.permissions.erase("timers.write");input["request_id"]="denied";check(assistant.query(who,input)["error"]=="forbidden","permission enforcement");who.permissions.insert("timers.write");input["request_id"]="revoked";check(assistant.query(who,input,[]{return false;})["error"]=="authorization_changed","revocation before mutation");
 overrides["label"]=nullptr;input["request_id"]="unlabelled";auto unlabelled=assistant.query(who,input);check(unlabelled["status"]=="completed"&&assistant.action(who,unlabelled["action_id"],false)["after_state"]["label"]=="","optional timer label");assistant.action(who,unlabelled["action_id"],true);overrides=json::object();
 mode="list_timers";input["request_id"]="list";check(assistant.query(who,input)["timers"].is_array(),"tool listing");
 // Conversation context is caller scoped, durable, bounded, and permission checked before model disclosure.
 mode="needs_clarification";auto start=assistant.query(who,{{"request_id","conversation"},{"text","Set a timer"}});
 check(start["conversation_id"]=="conversation"&&!start.contains("transcript"),"new conversation");
 mode="create_timer";spark::Assistant restarted(config,transport,actions);
 json follow={{"request_id","follow"},{"conversation_id","conversation"},{"text","Five minutes"}};
 auto follow_reply=restarted.query(who,follow);check(follow_reply["status"]=="completed"&&model_input["history"].size()==1&&model_input["history"][0]["user"]=="Set a timer","follow-up after restart");
 auto call_count=calls;check(restarted.query(who,follow)==follow_reply&&calls==call_count,"follow-up deduplication");
 auto other=who;other.subject="other";failed=false;try{restarted.query(other,{{"request_id","foreign"},{"conversation_id","conversation"},{"text","Five minutes"}});}catch(...){failed=true;}check(failed&&calls==call_count,"foreign context denied before provider");
 who.permissions.erase("timers.write");failed=false;try{restarted.query(who,{{"request_id","lost-permission"},{"conversation_id","conversation"},{"text","What happened?"}});}catch(...){failed=true;}check(failed&&calls==call_count,"context permission recheck");who.permissions.insert("timers.write");
 restarted.action(who,follow_reply["action_id"],true);mode="completed";
 restarted.query(who,{{"request_id","after-undo"},{"conversation_id","conversation"},{"text","What happened?"}});
 check(model_input["history"][1]["result"]["undo_available"]==false,"context reflects undo");
 for(const auto& entry:std::filesystem::directory_iterator(root))if(entry.path().extension()==".json"&&entry.path()!=key){auto value=spark::read_json_object(entry.path(),"test",true);if(value["request_id"]=="conversation"){value["conversation_expires_at"]=time(nullptr)-1;std::ofstream f(entry.path());f<<value.dump();}}
 call_count=calls;failed=false;try{restarted.query(who,{{"request_id","expired-follow"},{"conversation_id","conversation"},{"text","Five minutes"}});}catch(...){failed=true;}check(failed&&calls==call_count,"expired context denied");
 // Calendar actions share notifications and sync transactions, and undo detects later edits.
 spark::CalendarStore calendar(db);
 json event={{"name","Meeting"},{"description",""},{"occurs_at","2030-01-02T00:30:00+08:00"},{"place",nullptr},{"recurrence",nullptr},{"reminder_minutes",15},{"calendar","shared"}};
 auto sync_start=calendar.sync_info()["sequence"].get<uint64_t>();
 auto calendar_action=actions->create_event(std::string(64,'2'),"device:test",fingerprint,event);
 auto event_id=calendar_action["after_state"]["id"].get<std::string>();check(calendar_action["after_state"]["occurs_at"]=="2030-01-01T16:30:00Z","Perth normalization");
 check(actions->create_event(std::string(64,'2'),"device:test",fingerprint,event)["resource"]==calendar_action["resource"],"calendar deduplication");
 auto changes=calendar.sync_changes(sync_start);check(changes["items"].size()==1&&!changes["items"][0]["data"].is_null(),"calendar creation synced");
 actions->undo(std::string(64,'2'),"device:test");check(!calendar.get(event_id)&&calendar.sync_changes(sync_start)["items"][0]["data"].is_null(),"undo sync tombstone");
 event["calendar"]="local";auto changed=actions->create_event(std::string(64,'3'),"device:test",fingerprint,event);calendar.patch(changed["after_state"]["id"],{{"name","Edited"}});failed=false;try{actions->undo(std::string(64,'3'),"device:test");}catch(...){failed=true;}check(failed,"calendar edit conflict");
 auto sequence=calendar.sync_info()["sequence"];event["calendar"]="shared";
 sql("CREATE TRIGGER fail_calendar_event BEFORE INSERT ON events FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test failure'");failed=false;try{actions->create_event(std::string(64,'4'),"device:test",fingerprint,event);}catch(...){failed=true;}sql("DROP TRIGGER fail_calendar_event");check(failed&&!actions->get(std::string(64,'4'),"device:test")&&calendar.sync_info()["sequence"]==sequence,"calendar and sync rollback");
 auto recurring=spark::calendar_sync_data({{"name","Weekly"},{"description",""},{"occurs_at","2031-01-06T09:00:00+08:00"},{"place",nullptr},{"reminder_minutes",nullptr},{"recurrence",{{"weekdays",{1}},{"start_date","2031-01-06"}}}});recurring["calendar"]="shared";calendar.create(recurring);
 auto occurrences=actions->occurrences({{"calendar","shared"},{"from","2031-01-06T00:00:00Z"},{"to","2031-01-20T00:00:00Z"},{"limit",100}});check(occurrences["occurrences"].size()==2,"recurring occurrence reads");
 // Model proposals are validated; calendar permissions do not depend on timer permissions.
 mode="create_event";overrides={{"calendar","local"},{"name","Assistant meeting"},{"description",""},{"occurs_at","2030-01-02T00:30:00+08:00"}};
 input={{"request_id","event-denied"},{"text","Create meeting in local calendar"}};check(assistant.query(who,input)["error"]=="forbidden","calendar write permission");
 who.permissions.insert("events.write");who.permissions.erase("timers.write");input["request_id"]="event-created";auto event_reply=assistant.query(who,input);check(event_reply["status"]=="completed","calendar tool creation");check(assistant.action(who,event_reply["action_id"],false)["kind"]=="calendar.create","calendar action inspection without timer permission");
 mode="list_events";overrides={{"calendar","local"},{"from","2030-01-02T00:00:00+08:00"},{"to","2030-01-03T00:00:00+08:00"}};input["request_id"]="read-denied";check(assistant.query(who,input)["error"]=="forbidden","calendar read permission");who.permissions.insert("events.read");input["request_id"]="events-list";auto listing=assistant.query(who,input);check(listing["occurrences"].size()==2&&listing["range"]["from"]=="2030-01-01T16:00:00Z","occurrences and Perth midnight range");
 // Combined reads preserve source calendars, global ordering and a shared result cap.
 event["calendar"]="shared";event["occurs_at"]="2030-01-01T16:15:00Z";calendar.create(event);
 overrides["calendar"]="both";input["request_id"]="both-list";auto both=assistant.query(who,input);
 check(both["status"]=="completed"&&both["calendar"]=="both"&&both["occurrences"].size()==3&&!both["has_more"].get<bool>(),"both calendars read");
 check(both["occurrences"][0]["event"]["calendar"]=="shared"&&both["occurrences"][1]["event"]["calendar"]=="local","joint list chronological and labelled");
 who.permissions.erase("events.read");input["request_id"]="both-denied";check(assistant.query(who,input)["error"]=="forbidden","both calendar read permission");who.permissions.insert("events.read");
 recurring["calendar"]="local";calendar.create(recurring);
 overrides={{"calendar","both"},{"from","2031-01-01T00:00:00Z"},{"to","2032-01-01T00:00:00Z"}};input["request_id"]="both-limit";auto limited=assistant.query(who,input);
 check(limited["occurrences"].size()==100&&limited["has_more"]==true,"combined global cap and has_more");
 for(size_t i=1;i<limited["occurrences"].size();++i)check(limited["occurrences"][i-1]["occurrence_at"].get<std::string>()<=limited["occurrences"][i]["occurrence_at"].get<std::string>(),"combined occurrence ordering");
 mode="create_event";input["request_id"]="both-create-rejected";check(assistant.query(who,input)["error"]=="invalid_calendar","both cannot be a creation destination");
 mode="needs_clarification";overrides=json::object();auto weekday=assistant.query(who,{{"request_id","weekday"},{"text","Add an event on Wednesday called In Office"}});
 check(weekday["status"]=="needs_clarification"&&!weekday.contains("action_id"),"date clarification does not mutate");
 check(model_instructions.find("server-computed Perth date map")!=std::string::npos&&model_instructions.find("Do not create until the user accepts")!=std::string::npos,"inferred weekday confirmation instructions");
 mode="create_event";overrides={{"calendar","local"},{"name","In Office"},{"description",""},{"occurs_at","2030-01-02T09:00:00+08:00"}};
 auto accepted=assistant.query(who,{{"request_id","weekday-accepted"},{"conversation_id","weekday"},{"text","Yes, at 9 am in local"}});
 check(accepted["status"]=="completed"&&model_input["history"].size()==1,"date clarification accepts follow-up");assistant.action(who,accepted["action_id"],true);
 mode="list_events";overrides={{"calendar","local"},{"from","2030-01-02T00:00:00+08:00"},{"to","2030-01-03T00:00:00+08:00"}};
 overrides["calendar"]="unknown";input["request_id"]="bad-calendar";check(assistant.query(who,input)["error"]=="invalid_calendar","invalid calendar selection");overrides["calendar"]="local";
 overrides["from"]="not a date";input["request_id"]="bad-range";check(assistant.query(who,input)["error"]=="invalid_calendar_range","invalid calendar range");
 assistant.action(who,event_reply["action_id"],true);
 // Project tools use only read permissions and preserve authoritative board/task data.
 sql("INSERT INTO projects(id,name,description,directory) VALUES ('project-one','SPARK','Central hub','/projects/spark'),('project-two','SPARK Mobile','Phone','/projects/mobile')");
 sql("INSERT INTO project_boards(id,project_id,name) VALUES ('board-one','project-one','Development')");
 sql("INSERT INTO project_columns(id,board_id,name) VALUES ('column-one','board-one','Doing')");
 sql("INSERT INTO project_tasks(id,board_id,column_id,name,description,severity) VALUES ('task-one','board-one','column-one','Read projects','Read-only assistant','High')");
 mode="list_projects";overrides=json::object();input={{"request_id","projects-denied"},{"text","List projects"}};
 check(assistant.query(who,input)["error"]=="forbidden","project read denied");who.permissions.insert("projects.read");input["request_id"]="projects-list";auto project_list=assistant.query(who,input);
 check(project_list["projects"].size()==2&&!project_list["projects"][0].contains("directory"),"project list without storage details");
 mode="read_project";overrides={{"project_name","SPARK"}};input["request_id"]="boards-denied";check(assistant.query(who,input)["error"]=="forbidden","board permission required");who.permissions.insert("boards.read");input["request_id"]="project-ambiguous";
 auto ambiguous=assistant.query(who,input);check(ambiguous["status"]=="needs_clarification"&&ambiguous["projects"].size()==2&&!ambiguous.contains("project"),"ambiguous project selection");
 overrides={{"project_id","project-one"},{"message",""}};auto detail=assistant.query(who,{{"request_id","project-selected"},{"conversation_id","project-ambiguous"},{"text","The first one"}});
 check(detail["status"]=="completed"&&detail["project"]["boards"][0]["tasks"][0]["severity"]=="High"&&detail["message"].get<std::string>().find("Doing")!=std::string::npos,"project board tasks with column and severity");
 check(!detail.contains("action_id")&&!detail["undo_available"].get<bool>(),"read has no mutation action");
 who.permissions.erase("boards.read");failed=false;try{assistant.get(who,"project-selected");}catch(...){failed=true;}check(failed,"project receipt permission recheck");
 auto calls_before=calls;failed=false;try{assistant.query(who,{{"request_id","project-revoked-followup"},{"conversation_id","project-ambiguous"},{"text","What next?"}});}catch(...){failed=true;}check(failed&&calls==calls_before,"revoked board context not sent to model");who.permissions.insert("boards.read");
 overrides={{"project_id","missing"}};input["request_id"]="project-missing";check(assistant.query(who,input)["status"]=="needs_clarification","missing project");
 mode="list_projects";overrides=json::object();for(int i=0;i<101;++i)sql("INSERT INTO projects(id,name,description,directory) VALUES ('extra-"+std::to_string(i)+"','Extra "+std::to_string(i)+"','','/extra/"+std::to_string(i)+"')");
 input["request_id"]="project-limit";auto capped=assistant.query(who,input);check(capped["projects"].size()==100&&capped["has_more"]==true,"bounded project listing");
 mode="read_project";overrides={{"project_id","project-one"}};sql("UPDATE project_tasks SET description=REPEAT('x',50000) WHERE id='task-one'");input["request_id"]="project-oversized";auto oversized=assistant.query(who,input);check(oversized["error"]=="project_result_too_large"&&!oversized.contains("project")&&!oversized.contains("projects"),"oversized project rejected without oversized receipt");
 config.allow_open_access=true;auto http_assistant=std::make_shared<spark::Assistant>(config,transport,actions);httplib::Server server;spark::configure_assistant(server,http_assistant,nullptr);int port=server.bind_to_any_port("127.0.0.1");check(port>0,"bind setup test");std::thread http([&]{server.listen_after_bind();});server.wait_until_ready();httplib::Client client("127.0.0.1",port);auto setup=client.Get("/assistant/setup");server.stop();http.join();check(setup&&setup->status==200,"setup HTTP endpoint");auto guide=json::parse(setup->body);check(guide["tools"].size()==10&&guide["conversations"]["ttl_seconds"]==86400&&guide["request"]["follow_up_example"]["conversation_id"]=="phone-turn-1","setup contract");
 std::filesystem::remove_all(root);mysql_close(conn);std::cout<<"Action atomicity, deduplication, undo, isolation, conflicts, recovery and tool permissions passed\n";
}
