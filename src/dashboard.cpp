#include "dashboard.hpp"
#include "files.hpp"
#include <filesystem>
#include <fstream>
#include <cstdlib>
#include <vector>
#include <cerrno>
#include <spawn.h>
#include <sys/wait.h>
#include <unistd.h>
extern char **environ;
namespace spark {
namespace {
std::string history_path() {
    const char* value=std::getenv("SPARK_BACKUP_HISTORY");
    return value ? value : SPARK_SOURCE_DIR "/storage/backup-locations.tsv";
}
constexpr const char* html=R"HTML(<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>SPARK</title>
<style>body{margin:0;background:#101820;color:#e5edf3;font:16px system-ui}main{max-width:950px;margin:auto;padding:48px 24px}h1{font-size:48px;margin-bottom:8px}p{color:#adc1ce}nav{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin:32px 0}nav a,section{background:#1a2935;border:1px solid #324957;border-radius:12px;padding:24px}a{color:#78dfcc;text-decoration:none}nav a strong{display:block;font-size:22px;margin-bottom:8px}button,input,select{font:inherit;padding:10px;border-radius:6px;border:1px solid #56717f}button{background:#78dfcc;color:#102029;cursor:pointer}button:disabled{opacity:.5}label{display:block;margin:16px 0 6px}input,select{box-sizing:border-box;width:100%;background:#101820;color:white}dialog{max-width:520px;width:80%;background:#1a2935;color:white;border:1px solid #56717f;border-radius:12px}dialog::backdrop{background:#0009}.actions{display:flex;gap:12px;margin-top:20px}pre{white-space:pre-wrap;overflow-wrap:anywhere;max-height:320px;overflow:auto}#status{color:#78dfcc}</style>
<main><h1>SPARK</h1><p>Your devices, projects and shared files.</p><div id="status" role="status">Connecting…</div>
<nav><a href="/projects/explorer"><strong>Projects</strong>Projects, boards and tasks</a><a href="/files/explorer"><strong>Files</strong>Browse, edit and share files</a><a href="/commands/explorer"><strong>Commands</strong>Run device actions and inspect results</a><a href="/api/docs"><strong>API reference</strong>Explore endpoints and packets</a></nav>
<section><h2>Server controls</h2><p>Restart briefly disconnects devices. Backup pauses REST and TCP while files and database are compressed and verified, then brings SPARK back online.</p><div class="actions"><button id="restart">Restart server</button><button id="backup">Back up storage</button></div><pre id="result" aria-live="polite"></pre></section></main>
<dialog id="dialog"><form id="form"><h2>Back up storage</h2><p>Connect and mount the external drive first. Existing backups are kept.</p><label for="previous">Backup location</label><select id="previous"><option value="">New location</option></select><label for="mount">Drive mount path</label><input id="mount" required placeholder="/media/demo/backup-drive"><label for="destination">Backup directory (optional)</label><input id="destination" placeholder="Defaults to mount/spark-backups"><div class="actions"><button type="submit">Start backup</button><button type="button" id="cancel">Cancel</button></div></form></dialog>
<script>
const $=id=>document.getElementById(id);let waiting=false,locations=[],localError='';
async function refresh(){try{const r=await fetch('/admin/status');if(!r.ok)throw Error('Unavailable');const s=await r.json();$('status').textContent='Online · SPARK '+s.version;const busy=s.state==='queued';$('restart').disabled=$('backup').disabled=busy;if(!busy)waiting=false;$('result').textContent=localError||s.message|| (busy?'Maintenance accepted. Waiting for SPARK to return…':'');}catch(e){$('status').textContent=waiting?'Maintenance in progress · reconnecting…':'Server unavailable · reconnecting…';}}
async function action(path,body){$('restart').disabled=$('backup').disabled=true;try{const r=await fetch(path,{method:'POST',headers:{'Content-Type':'application/json','X-SPARK-Admin':'dashboard'},body:JSON.stringify(body)});const s=await r.json();if(!r.ok)throw Error(s.error);localError='';waiting=true;$('result').textContent='Maintenance accepted. This page will reconnect automatically.';$('dialog').close();}catch(e){localError=e.message;$('result').textContent=e.message;$('restart').disabled=$('backup').disabled=false;}}
$('restart').onclick=()=>{if(confirm('Restart SPARK and disconnect devices briefly?'))action('/admin/restart',{});};
$('backup').onclick=async()=>{try{const r=await fetch('/admin/backup-locations');if(!r.ok)throw Error('Cannot load backup locations');locations=(await r.json()).locations;$('previous').replaceChildren(new Option('New location',''));locations.forEach((l,i)=>$('previous').add(new Option(l.mount+' → '+l.destination,String(i))));$('mount').value=$('destination').value='';$('dialog').showModal();}catch(e){localError=e.message;$('result').textContent=e.message;}};
$('previous').onchange=()=>{const l=locations[$('previous').value];$('mount').value=l?l.mount:'';$('destination').value=l?l.destination:'';};$('cancel').onclick=()=>$('dialog').close();$('form').onsubmit=e=>{e.preventDefault();action('/admin/backup',{mount:$('mount').value,destination:$('destination').value});};refresh();setInterval(refresh,2000);
</script></html>)HTML";
}
void Maintenance::execute() {
    const int action=requested.load();
    nlohmann::json outcome={{"state","succeeded"},{"message","Server restarted."}};
    if(action==2) {
        int pipes[2];
        if(pipe(pipes)!=0) outcome={{"state","failed"},{"message","Cannot start backup: pipe failed."}};
        else {
            posix_spawn_file_actions_t actions;posix_spawn_file_actions_init(&actions);
            posix_spawn_file_actions_adddup2(&actions,pipes[1],STDOUT_FILENO);
            posix_spawn_file_actions_adddup2(&actions,pipes[1],STDERR_FILENO);
            posix_spawn_file_actions_addclose(&actions,pipes[0]);
            posix_spawn_file_actions_addclose(&actions,pipes[1]);
            std::string script=SPARK_SOURCE_DIR "/scripts/backup.sh";
            std::vector<std::string> args={script,"--mount",mount,"--destination",destination.empty()?mount+"/spark-backups":destination};
            for(const auto& drive:FileStore(DatabaseConfig::from_environment()).drives()){args.push_back("--file-dir");args.push_back(drive["path"].is_string()?drive["path"].get<std::string>():"/nonexistent-spark-drive-"+drive["id"].get<std::string>());}
            std::vector<char*> argv;for(auto& arg:args)argv.push_back(arg.data());argv.push_back(nullptr);
            pid_t pid;int rc=posix_spawn(&pid,script.c_str(),&actions,nullptr,argv.data(),environ);
            posix_spawn_file_actions_destroy(&actions);close(pipes[1]);
            std::string output;char buffer[4096];ssize_t count;
            while((count=read(pipes[0],buffer,sizeof(buffer)))>0){output.append(buffer,count);if(output.size()>32768)output.erase(0,output.size()-32768);}
            close(pipes[0]);int status=0;
            if(rc==0){while(waitpid(pid,&status,0)<0){if(errno!=EINTR){rc=-1;break;}}}
            const bool ok=rc==0 && WIFEXITED(status) && WEXITSTATUS(status)==0;
            outcome={{"state",ok?"succeeded":"failed"},{"message",std::string(ok?"Backup completed.\n":"Backup failed.\n")+output}};
        }
    }
    std::lock_guard<std::mutex> guard(mutex);result=outcome;requested=0;
}
void configure_dashboard(httplib::Server& server,std::shared_ptr<Maintenance> state) {
    server.Get("/",[](const auto&,auto& res){res.set_content(html,"text/html; charset=utf-8");});
    server.Get("/admin/status",[state](const auto&,auto& res){nlohmann::json body;{std::lock_guard<std::mutex> guard(state->mutex);body=state->result;}body["version"]=SPARK_VERSION;res.set_content(body.dump(-1,' ',false,nlohmann::json::error_handler_t::replace),"application/json");});
    server.Get("/admin/backup-locations",[](const auto&,auto& res){nlohmann::json locations=nlohmann::json::array();std::ifstream input(history_path());std::string line;while(locations.size()<20 && std::getline(input,line)){auto tab=line.find('\t');if(tab!=std::string::npos)locations.push_back({{"mount",line.substr(0,tab)},{"destination",line.substr(tab+1)}});}res.set_content(nlohmann::json({{"locations",locations}}).dump(),"application/json");});
    for(int action:{1,2})server.Post(action==1?"/admin/restart":"/admin/backup",[state,action](const httplib::Request& req,httplib::Response& res,const httplib::ContentReader& read){
        auto fail=[&](int code,const char* message){res.status=code;res.set_header("Connection","close");res.set_content(nlohmann::json({{"error",message}}).dump(),"application/json");};
        // Non-simple header plus no CORS preflight support prevents cross-site forms/fetch.
        if(req.get_header_value("X-SPARK-Admin")!="dashboard" || (req.has_header("Sec-Fetch-Site") && req.get_header_value("Sec-Fetch-Site")!="same-origin" && req.get_header_value("Sec-Fetch-Site")!="none")){fail(403,"same_origin_dashboard_required");return;}
        const auto content_type=req.get_header_value("Content-Type");
        if(content_type!="application/json" && content_type.rfind("application/json;",0)!=0){fail(415,"json_content_type_required");return;}
        std::string payload;
        bool oversized=false;
        if(!read([&](const char* data,std::size_t size){
            if(size>16384-payload.size()){oversized=true;return false;}
            payload.append(data,size);return true;
        })){fail(oversized?413:400,oversized?"payload_too_large":"invalid_body");return;}
        auto body=nlohmann::json::parse(payload,nullptr,false);
        if(!body.is_object()){fail(400,"invalid_json");return;}
        std::string mount,dest;
        if(action==2){if(!body.contains("mount") || !body["mount"].is_string() || (body.contains("destination")&&!body["destination"].is_string())){fail(400,"invalid_backup_location");return;}mount=body["mount"].get<std::string>();dest=body.value("destination",std::string{});
            auto valid=[](const std::string& s){return s.size()<4096 && s.find_first_of(std::string("\0\t\r\n",4))==std::string::npos;};
            if(mount.empty() || mount[0]!='/' || (!dest.empty()&&dest[0]!='/') || !valid(mount)||!valid(dest)){fail(400,"invalid_backup_location");return;}}
        std::lock_guard<std::mutex> guard(state->mutex);
        if(state->requested){fail(409,"maintenance_in_progress");return;}
        state->mount=mount;state->destination=dest;state->result={{"state","queued"}};state->requested=action;
        res.status=202;res.set_content("{\"state\":\"queued\"}","application/json");
    });
}
nlohmann::json dashboard_endpoints() {
    using nlohmann::json;
    json endpoints=json::array();
    auto add=[&](const char* path,const char* description,json body,bool post=false,json input=nullptr) {
        endpoints.push_back({
            {"method",post?"POST":"GET"}, {"path",path}, {"description",description},
            {"input",{{"headers",post?json({{"X-SPARK-Admin","dashboard"},{"Content-Type","application/json"}}):json::object()},
                      {"query",json::object()}, {"body",input}}},
            {"output",{{"status",post?202:200},
                       {"headers",{{"Content-Type",std::string(path)=="/"?"text/html; charset=utf-8":"application/json"}}},
                       {"body",body}}}
        });
    };
    add("/","Human dashboard with explorer links and maintenance controls.","HTML dashboard");
    add("/admin/status","Latest maintenance state: idle, queued, succeeded or failed. Unavailable during maintenance; poll again after reconnecting. Result survives service restart, not process exit.",
        {{"state","succeeded"},{"message","Server restarted."},{"version",SPARK_VERSION}});
    add("/admin/backup-locations","Up to 20 previously successful backup mount and destination pairs. Drives must be mounted before use.",
        {{"locations",json::array({{{"mount","/media/demo/drive"},{"destination","/media/demo/drive/spark-backups"}}})}});
    add("/admin/restart","Recreate REST, TCP and scheduler services within the running process. Does not reload the executable. HTTP 409 if maintenance is already queued; 403 for missing dashboard header or cross-site browser requests; 400 for invalid JSON; 415 for non-JSON content type.",
        {{"state","queued"}},true,json::object());
    add("/admin/backup","Pause all services, run compressed verified backup, then resume even on backup failure. Mount must be an external mounted filesystem. Optional destination defaults to mount/spark-backups. HTTP 400 for invalid absolute paths or JSON, 403 for missing dashboard header or cross-site requests, 409 if busy, 415 for non-JSON content type. Poll /admin/status for completion; 202 only means accepted.",
        {{"state","queued"}},true,{{"mount","/media/demo/drive"},{"destination","/media/demo/drive/spark-backups"}});
    return endpoints;
}
}
