#include "assistant.hpp"
#include "calendar.hpp"
#include <mysql.h>
#include <fstream>
#include <thread>
#include <sys/stat.h>
#include <unistd.h>
#include <iostream>
using nlohmann::json;
void check(bool ok,const char* message){if(!ok)throw std::runtime_error(message);}
template<class F> void rejects(F f,const char* message){bool bad=false;try{f();}catch(...){bad=true;}check(bad,message);}
std::string cid(char c){return std::string(32,'9')+std::string(32,c);}
int main(){
 if(!getenv("SPARK_DB_SOCKET"))return 77;
 auto db=spark::DatabaseConfig::from_environment();MYSQL* conn=mysql_init(nullptr);check(mysql_real_connect(conn,db.host.c_str(),db.user.c_str(),db.password.c_str(),db.database.c_str(),db.port,db.socket.c_str(),0),"connect");
 auto sql=[&](const std::string& q){check(mysql_query(conn,q.c_str())==0,"fixture SQL");};sql("SET time_zone='+00:00'");
 auto actions=std::make_shared<spark::ActionStore>(db);actions->verify();spark::CalendarStore calendar(db);
 json data={{"name","Confirmation appointment"},{"description",""},{"occurs_at","2032-02-06T06:00:00Z"},{"place",nullptr},{"recurrence",nullptr},{"reminder_minutes",15},{"calendar","shared"}};
 auto event=calendar.create(data);std::string event_id=event["id"],owner="device:confirm",fp(64,'f');
 auto propose=[&](char key,const std::string& kind="calendar.update",const std::string& conversation="confirm-test"){
  return actions->propose_event(cid(key),owner,fp,conversation,event_id,kind,kind=="calendar.update"?json{{"occurs_at","2032-02-08T06:00:00Z"}}:json::object(),"event",nullptr);
 };
 auto preview=propose('a');check(preview["status"]=="pending"&&calendar.get(event_id)->at("occurs_at")==data["occurs_at"],"preview does not mutate");
 check(propose('a')==preview,"proposal retry");check(!actions->confirmation(cid('a'),"device:other"),"owner isolation");rejects([&]{actions->decide(cid('a'),"device:other",true);},"foreign confirmation");
 auto confirmed=actions->decide(cid('a'),owner,true,cid('1'),fp);check(confirmed["status"]=="confirmed"&&calendar.get(event_id)->at("occurs_at")=="2032-02-08T06:00:00Z","confirmed edit");
 check(actions->decide(cid('a'),owner,true)==confirmed,"confirmation idempotency");actions->undo(cid('a'),owner);check(calendar.get(event_id)->at("occurs_at")==data["occurs_at"],"edit undo");
 propose('b');calendar.patch(event_id,{{"name","Changed elsewhere"}});check(actions->decide(cid('b'),owner,true)["status"]=="conflict"&&!actions->get(cid('b'),owner),"concurrent edit conflict");
 propose('c');sql("UPDATE assistant_confirmations SET expires_at=TIMESTAMPADD(SECOND,-1,CURRENT_TIMESTAMP(6)) WHERE id='"+cid('c')+"'");check(actions->decide(cid('c'),owner,true)["status"]=="expired","expiry");
 propose('d');check(actions->decide(cid('d'),owner,false)["status"]=="cancelled","cancel");check(actions->decide(cid('d'),owner,true)["status"]=="cancelled","cancel cannot execute");
 propose('e');propose('f');check(actions->decide(cid('e'),owner,true)["status"]=="superseded","replacement invalidates old approval");
 sql("CREATE TRIGGER fail_confirmation BEFORE INSERT ON events FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test failure'");rejects([&]{actions->decide(cid('f'),owner,true);},"notification failure");sql("DROP TRIGGER fail_confirmation");check(actions->confirmation(cid('f'),owner)->at("status")=="pending"&&!actions->get(cid('f'),owner)&&calendar.get(event_id)->at("occurs_at")==data["occurs_at"],"confirmation atomic rollback");
 json one,two;std::exception_ptr e1,e2;std::thread a([&]{try{one=actions->decide(cid('f'),owner,true);}catch(...){e1=std::current_exception();}}),b([&]{try{two=actions->decide(cid('f'),owner,true);}catch(...){e2=std::current_exception();}});a.join();b.join();check(!e1&&!e2&&one==two&&one["status"]=="confirmed","concurrent confirmation");actions->undo(cid('f'),owner);
 auto sync_before=calendar.sync_changes(0);std::string sync_id;for(const auto& item:sync_before["items"])if(!item["data"].is_null()&&item["data"]["name"]=="Changed elsewhere")sync_id=item["sync_id"];check(!sync_id.empty(),"find sync identity");
 propose('2',"calendar.delete");actions->decide(cid('2'),owner,true);check(!calendar.get(event_id)&&calendar.sync_item(sync_id)["data"].is_null(),"delete and sync tombstone");
 sql("CREATE TRIGGER fail_restore BEFORE INSERT ON events FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test failure'");rejects([&]{actions->undo(cid('2'),owner);},"undo notification failure");sql("DROP TRIGGER fail_restore");check(!calendar.get(event_id),"undo rollback");
 auto restored=actions->undo(cid('2'),owner);check(calendar.get(event_id).has_value()&&!calendar.sync_item(sync_id)["data"].is_null()&&restored["undo_state"]["after"]["id"]==event_id,"delete undo preserves event and sync identity");check(actions->undo(cid('2'),owner)==restored,"delete undo idempotent");
 propose('3',"calendar.delete");actions->decide(cid('3'),owner,true);sql("UPDATE calendar_sync_items SET revision=UUID() WHERE sync_id='"+sync_id+"'");rejects([&]{actions->undo(cid('3'),owner);},"changed remote tombstone blocks undo");
 auto recurring=data;recurring["name"]="Weekly confirmation";recurring["occurs_at"]="2033-01-03T01:00:00Z";recurring["recurrence"]={{"frequency","weekly"},{"interval_weeks",1},{"weekdays",{1}},{"start_date","2033-01-03"},{"end_date",nullptr},{"timezone","+08:00"}};auto series=calendar.create(recurring);
 rejects([&]{actions->propose_event(cid('4'),owner,fp,"series",series["id"],"calendar.delete",json::object(),"occurrence",nullptr);},"occurrence deletion rejected");
 rejects([&]{actions->propose_event(cid('4'),owner,fp,"series",series["id"],"calendar.delete",json::object(),"event",nullptr);},"series scope mandatory");
 actions->propose_event(cid('4'),owner,fp,"series",series["id"],"calendar.delete",json::object(),"series",nullptr);actions->decide(cid('4'),owner,true);actions->undo(cid('4'),owner);check(calendar.get(series["id"])->at("recurrence")==recurring["recurrence"],"series restore");
 // End-to-end assistant: only a saved proposal can be confirmed; voice decisions do not call the model.
 event=calendar.create(data);event_id=event["id"];
 char temp[]="/tmp/spark-confirmation-harness-XXXXXX";check(mkdtemp(temp),"temp directory");std::filesystem::path root(temp),key=root/"key.json";{std::ofstream out(key);out<<R"({"api_key":"fake"})";}chmod(key.c_str(),0600);spark::AssistantConfig config{key,root};
 spark::Identity who{"confirmation-test","confirm-phone","device",{"assistant.use","events.read","events.write"}};
 int calls=0;std::string mode="edit_event",new_time="2032-02-08T06:00:00Z";bool use_name=false;
 auto transport=[&](const json&){++calls;json r={{"status",mode},{"message","proposal"},{"edit_fields",mode=="edit_event"?json::array({"occurs_at"}):json::array()}};for(auto field:{"duration_seconds","label","calendar","name","description","occurs_at","place","from","to","reminder_minutes","target_event_id","target_name","scope","recurrence","project_id","project_name","search_query"})r[field]=nullptr;r["calendar"]="shared";r["scope"]="event";r["target_event_id"]=use_name?json(nullptr):json(event_id);r["target_name"]=use_name?json("Duplicate confirmation"):json(nullptr);r["occurs_at"]=new_time;return json{{"status","completed"},{"output",json::array({{{"type","message"},{"role","assistant"},{"content",json::array({{{"type","output_text"},{"text",r.dump()}}})}}})}};};
 spark::Assistant assistant(config,transport,actions);auto reply=assistant.query(who,{{"request_id","edit"},{"text","Move my appointment to Friday"}});check(reply["status"]=="needs_confirmation"&&calls==2&&calendar.get(event_id)->at("occurs_at")==data["occurs_at"],"assistant preview only");auto confirmation_id=reply["confirmation_id"].get<std::string>();
 auto unbound=assistant.query(who,{{"request_id","unbound-yes"},{"conversation_id","edit"},{"text","Yes"}});check(unbound["status"]=="needs_confirmation"&&!unbound.contains("action_id")&&calendar.get(event_id)->at("occurs_at")==data["occurs_at"],"unbound yes cannot execute");
 auto denied=who;denied.permissions.erase("events.write");rejects([&]{assistant.confirmation(denied,confirmation_id,"confirm");},"permission revoked before confirm");
 json yes={{"request_id","yes"},{"conversation_id","edit"},{"confirmation_id",confirmation_id},{"text","Yes please."}};auto done=assistant.query(who,yes);check(done["status"]=="completed"&&done["action_id"]==confirmation_id&&calls==2,"voice confirmation no model call");
 auto correction=assistant.query(who,{{"request_id","new-proposal"},{"conversation_id","edit"},{"text","Actually Saturday"}}); // model proposal still Friday => no_changes; switch the fixture below.
 new_time="2032-02-09T06:00:00Z";correction=assistant.query(who,{{"request_id","saturday"},{"conversation_id","edit"},{"text","Actually Saturday"}});check(correction["status"]=="needs_confirmation","new proposal");
 // Lose the voice receipt: the durable decision must NOT approve the newer Saturday preview.
 for(const auto& entry:std::filesystem::directory_iterator(root))if(entry.path().extension()==".json"&&entry.path()!=key){auto r=spark::read_json_object(entry.path(),"fixture",true);if(r["request_id"]=="yes")std::filesystem::remove(entry.path());}
 auto recovered=assistant.query(who,yes);check(recovered["action_id"]==confirmation_id&&calendar.get(event_id)->at("occurs_at")=="2032-02-08T06:00:00Z"&&actions->confirmation(correction["confirmation_id"],"device:confirm-phone")->at("status")=="pending","lost receipt cannot confirm newer proposal");
 new_time="2032-02-10T06:00:00Z";auto replaced=assistant.query(who,{{"request_id","sunday"},{"conversation_id","edit"},{"text","Actually Sunday"}});check(replaced["status"]=="needs_confirmation"&&actions->confirmation(correction["confirmation_id"],"device:confirm-phone")->at("status")=="superseded","correction invalidates old preview");
 auto stale=assistant.query(who,{{"request_id","delayed-approval"},{"conversation_id","edit"},{"confirmation_id",correction["confirmation_id"]},{"text","Yes"}});check(stale["error"]=="confirmation_superseded"&&actions->confirmation(replaced["confirmation_id"],"device:confirm-phone")->at("status")=="pending","delayed approval cannot select newer preview");
 auto cancelled=assistant.query(who,{{"request_id","cancel"},{"conversation_id","edit"},{"confirmation_id",replaced["confirmation_id"]},{"text","No thanks"}});check(cancelled["confirmation_status"]=="cancelled","spoken cancellation");
 mode="needs_clarification";auto question=assistant.query(who,{{"request_id","new-question"},{"conversation_id","edit"},{"text","Prepare another change"}});check(question["status"]=="needs_clarification","new question");mode="edit_event";auto answered=assistant.query(who,{{"request_id","answer-question"},{"conversation_id","edit"},{"text","Yes"}});check(answered["status"]=="needs_confirmation","yes to new clarification does not target old cancellation");assistant.confirmation(who,answered["confirmation_id"],"cancel");
 auto duplicate=data;duplicate["name"]="Duplicate confirmation";calendar.create(duplicate);calendar.create(duplicate);use_name=true;auto ambiguous=assistant.query(who,{{"request_id","ambiguous"},{"text","Remove duplicate confirmation"}});check(ambiguous["status"]=="needs_clarification"&&ambiguous["candidates"].size()==2&&!ambiguous.contains("confirmation_id"),"ambiguous target not proposed");use_name=false;
 mode="delete_event";auto removal=assistant.query(who,{{"request_id","remove"},{"text","Delete my appointment"}});check(removal["status"]=="needs_confirmation"&&calendar.get(event_id).has_value(),"removal preview");assistant.confirmation(who,removal["confirmation_id"],"confirm");check(!calendar.get(event_id),"typed confirmation removal");assistant.action(who,removal["confirmation_id"],true);check(calendar.get(event_id).has_value(),"assistant removal undo");
 config.allow_open_access=true;auto open=std::make_shared<spark::Assistant>(config,transport,actions);spark::Identity local{"open","local","open",{}};auto http_preview=open->query(local,{{"request_id","http-preview"},{"text","Remove appointment"}});
 httplib::Server server;spark::configure_assistant(server,open,nullptr);int port=server.bind_to_any_port("127.0.0.1");check(port>0,"HTTP bind");std::thread worker([&]{server.listen_after_bind();});server.wait_until_ready();httplib::Client client("127.0.0.1",port);auto path="/assistant/confirmations/"+http_preview["confirmation_id"].get<std::string>();auto bad=client.Post(path+"/confirm",R"({"occurs_at":"2030-01-01T00:00:00Z"})","application/json");auto cancel=client.Post(path+"/cancel","{ }","application/json");auto late=client.Post(path+"/confirm","{}","application/json");server.stop();worker.join();check(bad&&bad->status==400&&cancel&&cancel->status==200&&late&&late->status==409,"HTTP cannot modify proposal; cancellation enforced");
 std::filesystem::remove_all(root);mysql_close(conn);std::cout<<"Confirmation previews, decisions, expiry, conflicts, rollback, sync undo, voice retries, corrections and HTTP tests passed\n";
}
