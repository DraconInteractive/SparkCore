#include "logging.hpp"
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
#include <poll.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <algorithm>
#include <sstream>
#include <iostream>
#include <chrono>
extern char **environ;
namespace spark {
namespace {
// Host-wide Linux statistics. No shell commands, database access or file paths exposed.
nlohmann::json system_stats() {
    using json=nlohmann::json;
    static std::mutex mutex;
    static uint64_t previous_total=0,previous_idle=0;
    static auto sampled=std::chrono::steady_clock::time_point{};
    static json cpu=nullptr;
    std::lock_guard<std::mutex> lock(mutex);
    const auto now=std::chrono::steady_clock::now();
    if(now-sampled>=std::chrono::seconds(1)) {
        std::ifstream input("/proc/stat");std::string name;
        uint64_t user=0,nice=0,system=0,idle=0,iowait=0,irq=0,softirq=0,steal=0;
        if(input>>name>>user>>nice>>system>>idle>>iowait>>irq>>softirq>>steal && name=="cpu") {
            const auto total=user+nice+system+idle+iowait+irq+softirq+steal;
            const auto inactive=idle+iowait;
            cpu=nullptr;
            if(previous_total && total>previous_total && inactive>=previous_idle) {
                const double percent=100.0*(1.0-double(inactive-previous_idle)/double(total-previous_total));
                cpu=std::clamp(percent,0.0,100.0);
            }
            previous_total=total;previous_idle=inactive;
        } else {cpu=nullptr;previous_total=previous_idle=0;}
        sampled=now;
    }
    json memory=nullptr,storage=nullptr;
    std::ifstream mem("/proc/meminfo");std::string line;uint64_t total=0,available=0;bool has_available=false;
    while(std::getline(mem,line)){std::istringstream row(line);std::string key;uint64_t kb=0;
        if(row>>key>>kb){if(key=="MemTotal:")total=kb*1024;if(key=="MemAvailable:"){available=kb*1024;has_available=true;}}}
    if(total && has_available){available=std::min(available,total);memory={{"total_bytes",total},{"used_bytes",total-available},{"available_bytes",available},{"used_percent",100.0*double(total-available)/double(total)}};}
    struct statvfs disk{};
    if(statvfs(FileStorage::default_root().c_str(),&disk)==0 && disk.f_blocks) {
        const uint64_t size=uint64_t(disk.f_blocks)*disk.f_frsize;
        const uint64_t free=uint64_t(disk.f_bfree)*disk.f_frsize;
        const uint64_t usable=uint64_t(disk.f_bavail)*disk.f_frsize;
        if(size && free<=size)storage={{"total_bytes",size},{"used_bytes",size-free},{"available_bytes",usable},{"used_percent",100.0*double(size-free)/double(size)}};
    }
    return {{"cpu",{{"usage_percent",cpu}}},{"memory",memory},{"storage",storage}};
}
std::string history_path() {
    const char* value=std::getenv("SPARK_BACKUP_HISTORY");
    return value ? value : SPARK_SOURCE_DIR "/storage/backup-locations.tsv";
}
constexpr const char* html=R"HTML(<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>SPARK</title>
<style>body{margin:0;background:#101820;color:#e5edf3;font:16px system-ui}main{max-width:950px;margin:auto;padding:48px 24px}h1{font-size:48px;margin-bottom:8px}p{color:#adc1ce}nav{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:16px;margin:32px 0}nav a,section{background:#1a2935;border:1px solid #324957;border-radius:12px;padding:24px}a{color:#78dfcc;text-decoration:none}nav a strong{display:block;font-size:22px;margin-bottom:8px}button,input,select{font:inherit;padding:10px;border-radius:6px;border:1px solid #56717f}button{background:#78dfcc;color:#102029;cursor:pointer}button:disabled{opacity:.5}label{display:block;margin:16px 0 6px}input,select{box-sizing:border-box;width:100%;background:#101820;color:white}dialog{max-width:520px;width:80%;background:#1a2935;color:white;border:1px solid #56717f;border-radius:12px}dialog::backdrop{background:#0009}.actions{display:flex;gap:12px;margin-top:20px}pre{white-space:pre-wrap;overflow-wrap:anywhere;max-height:320px;overflow:auto}#status{color:#78dfcc}.stats{display:grid;grid-template-columns:repeat(auto-fit,minmax(180px,1fr));gap:16px;margin:0 0 24px}.stats section{padding:16px}.stats h2{font-size:16px;margin:0 0 8px}.stats strong{font-size:20px}.stats small{display:block;color:#adc1ce;margin-top:6px}</style>
<main><h1>SPARK</h1><p>Your devices, projects and shared files.</p><div id="status" role="status">Connecting…</div>
<nav><a href="/budget/explorer" data-spark-permission="budget.read"><strong>Budget</strong>Expenses, subscriptions and forecasts</a><a id="google-link" href="/admin/google/explorer" data-spark-permission="admin.credentials"><strong>Google login</strong>Enrol your account and manage Google sessions</a><a href="/configs/explorer" data-spark-permission="configs.read"><strong>Configs</strong>Device configuration and JSON editor</a><a href="/backups/explorer" data-spark-permission="admin.backup"><strong>Backups</strong>Manage and transfer backups</a><a href="/connections/explorer" data-spark-permission="admin.connections"><strong>Connections</strong>Manage other SPARK instances</a><a href="/assistant/explorer" data-spark-permission="assistant.use">Assistant</a> <a href="/projects/explorer"><strong>Projects</strong>Projects, boards and tasks</a><a href="/files/explorer"><strong>Files</strong>Browse, edit and share files</a><a href="/commands/explorer"><strong>Commands</strong>Run device actions and inspect results</a><a href="/api/docs"><strong>API reference</strong>Explore endpoints and packets</a><a id="credentials-link" href="/admin/credentials/explorer" data-spark-permission="admin.credentials"><strong>Device access</strong>Provision and revoke device tokens</a></nav>
<div class="stats" aria-label="Server statistics"><section><h2>CPU</h2><strong id="cpu-stat">Loading…</strong><small>Overall server usage</small></section><section><h2>Memory</h2><strong id="memory-stat">Loading…</strong><small id="memory-detail">Used / total RAM</small></section><section><h2>Storage</h2><strong id="storage-stat">Loading…</strong><small id="storage-detail">Persistent storage filesystem</small></section></div>
<section><h2>Server controls</h2><p>Restart briefly disconnects devices. Backup pauses ordinary REST and TCP while files and database are compressed and verified. The authenticated backup progress page stays available.</p><div class="actions"><button id="restart" data-spark-permission="admin.restart">Restart server</button><button id="backup" data-spark-permission="admin.backup">Back up storage</button></div><p><a href="/backups/progress" data-spark-permission="admin.backup">View backup progress and log</a></p><pre id="result" aria-live="polite"></pre></section></main>
<script src="/auth/client.js"></script>
<script>
sparkSession.ready.then(s=>{if(s.mode==='open'){document.getElementById('credentials-link').hidden=true;document.getElementById('google-link').hidden=true;}}).catch(()=>{});
const $=id=>document.getElementById(id);let waiting=false,locations=[],localError='';
function bytes(n){const units=['B','KiB','MiB','GiB','TiB'];let i=0;while(n>=1024&&i<units.length-1){n/=1024;i++;}return n.toFixed(i?1:0)+' '+units[i];}
async function refreshStats(){try{await sparkSession.ready;const r=await fetch('/system/stats',{cache:'no-store'});if(!r.ok)throw Error('Unavailable');const s=await r.json();$('cpu-stat').textContent=s.cpu.usage_percent===null?'Sampling…':s.cpu.usage_percent.toFixed(1)+'%';for(const kind of ['memory','storage']){const v=s[kind];$(kind+'-stat').textContent=v?v.used_percent.toFixed(1)+'%':'Unavailable';$(kind+'-detail').textContent=v?bytes(v.used_bytes)+' / '+bytes(v.total_bytes)+(kind==='storage'?' · persistent filesystem':' RAM'):'Unavailable';}}catch(e){for(const kind of ['cpu','memory','storage'])$(kind+'-stat').textContent='Unavailable';$('memory-detail').textContent='Used / total RAM';$('storage-detail').textContent='Persistent storage filesystem';}finally{setTimeout(refreshStats,5000);}}
refreshStats();
async function refresh(){try{const session=await sparkSession.ready;if(session.mode==='session'&&!['admin.restart','admin.backup'].every(p=>session.permissions.includes(p))){const r=await fetch('/version');if(!r.ok)throw Error('Unavailable');$('status').textContent='Online · SPARK '+(await r.json()).server_version;$('restart').disabled=$('backup').disabled=false;return;}const r=await fetch('/admin/status');if(!r.ok)throw Error('Unavailable');const s=await r.json();$('status').textContent='Online · SPARK '+s.version;const busy=s.state==='queued'||s.state==='running';$('restart').disabled=$('backup').disabled=busy;if(!busy)waiting=false;$('result').textContent=localError||s.message|| (busy?'Maintenance accepted. Waiting for SPARK to return…':'');}catch(e){$('status').textContent=waiting?'Maintenance in progress · reconnecting…':'Server unavailable · reconnecting…';}}
async function action(path,body){$('restart').disabled=$('backup').disabled=true;try{const r=await fetch(path,{method:'POST',headers:{'Content-Type':'application/json','X-SPARK-Admin':'dashboard'},body:JSON.stringify(body)});const s=await r.json();if(!r.ok)throw Error(s.error);localError='';waiting=true;$('result').textContent='Maintenance accepted. This page will reconnect automatically.';}catch(e){localError=e.message;$('result').textContent=e.message;$('restart').disabled=$('backup').disabled=false;}}
$('restart').onclick=()=>{if(confirm('Restart SPARK and disconnect devices briefly?'))action('/admin/restart',{});};
$('backup').onclick=()=>location.assign('/backups/new');refresh();setInterval(refresh,2000);
</script></html>)HTML";
}
void Maintenance::execute() {
    const int action=requested.load();
    nlohmann::json outcome={{"state","succeeded"},{"message","Server restarted."}};
    if(action==2) {
        initialize();
        try {auto fresh=estimate(mount,destination);progress({{"estimate",fresh}});if(!fresh["space_sufficient"].get<bool>())throw std::runtime_error("insufficient_backup_space");}
        catch(const std::exception& e){outcome={{"state","failed"},{"message",std::string("Backup preflight failed: ")+e.what()}};progress({{"state","failed"},{"stage","preflight"},{"message",outcome["message"]},{"finished_at",time(nullptr)},{"process_alive",false}});std::lock_guard<std::mutex> lock(mutex);result=outcome;requested=0;return;}
        int pipes[2];
        if(pipe(pipes)!=0) outcome={{"state","failed"},{"message","Cannot start backup: pipe failed."}};
        else {
            posix_spawn_file_actions_t actions;posix_spawn_file_actions_init(&actions);
            posix_spawn_file_actions_adddup2(&actions,pipes[1],STDOUT_FILENO);
            posix_spawn_file_actions_adddup2(&actions,pipes[1],STDERR_FILENO);
            posix_spawn_file_actions_addclose(&actions,pipes[0]);
            posix_spawn_file_actions_addclose(&actions,pipes[1]);
            std::string script=SPARK_SOURCE_DIR "/scripts/backup.sh";
            std::vector<std::string> args={script};
            if(mount.empty())args.push_back("--local");else{args.insert(args.end(),{"--mount",mount,"--destination",destination.empty()?mount+"/spark-backups":destination});}
            for(const auto& drive:FileStore(DatabaseConfig::from_environment()).drives()){args.push_back("--file-dir");args.push_back(drive["path"].is_string()?drive["path"].get<std::string>():"/nonexistent-spark-drive-"+drive["id"].get<std::string>());}
            std::vector<char*> argv;for(auto& arg:args)argv.push_back(arg.data());argv.push_back(nullptr);
            pid_t pid;int rc=posix_spawn(&pid,script.c_str(),&actions,nullptr,argv.data(),environ);
            posix_spawn_file_actions_destroy(&actions);close(pipes[1]);
            auto initial=snapshot();const auto log_path=initial["log_path"].get<std::string>();int log_fd=open(log_path.c_str(),O_WRONLY|O_APPEND|O_CREAT|O_CLOEXEC|O_NOFOLLOW,0600);
            std::string output,pending,archive;char buffer[4096];bool eof=false;uint64_t previous_ticks=0,previous_bytes=0;int verified=0;
            auto observe=[&]{
                uint64_t ticks=0;bool alive=false;std::vector<int> pids;if(rc==0)pids.push_back(pid);
                for(size_t i=0;i<pids.size()&&i<256;++i){auto base="/proc/"+std::to_string(pids[i]);std::ifstream children(base+"/task/"+std::to_string(pids[i])+"/children");int child;while(children>>child)pids.push_back(child);std::ifstream in(base+"/stat");std::string line;std::getline(in,line);auto end=line.rfind(')');if(end!=std::string::npos){if(i==0&&end+2<line.size())alive=line[end+2]!='Z'&&line[end+2]!='X';std::istringstream fields(line.substr(end+2));std::string part;for(int j=0;j<=12&&fields>>part;++j)if(j==11||j==12)try{ticks+=std::stoull(part);}catch(...){} }}
                uint64_t bytes=0;std::error_code error;if(!archive.empty())for(auto& f:std::filesystem::directory_iterator(archive,error)){if(f.is_regular_file(error))bytes+=f.file_size(error);}
                nlohmann::json patch={{"state","running"},{"process_alive",rc==0&&!eof&&alive},{"process_id",rc==0?int(pid):0},{"output_bytes",bytes},{"cpu_ticks",ticks},{"verified_files",verified},{"log_tail",output}};
                if(ticks!=previous_ticks||bytes!=previous_bytes)patch["last_activity_at"]=time(nullptr);previous_ticks=ticks;previous_bytes=bytes;progress(patch);
            };
            fcntl(pipes[0],F_SETFL,fcntl(pipes[0],F_GETFL)|O_NONBLOCK);
            auto next=std::chrono::steady_clock::now();
            while(!eof){pollfd descriptor{pipes[0],POLLIN,0};poll(&descriptor,1,500);ssize_t count;
                while((count=read(pipes[0],buffer,sizeof(buffer)))>0){
                    if(log_fd>=0){size_t offset=0;while(offset<size_t(count)){auto n=write(log_fd,buffer+offset,size_t(count)-offset);if(n<0&&errno==EINTR)continue;if(n<=0)break;offset+=n;}}
                    output.append(buffer,count);if(output.size()>32768)output.erase(0,output.size()-32768);pending.append(buffer,count);size_t newline;
                    while((newline=pending.find('\n'))!=std::string::npos){auto line=pending.substr(0,newline);pending.erase(0,newline+1);
                        if(line.rfind("SPARK_STAGE ",0)==0){auto stage=line.substr(12);auto space=stage.find(' ');progress({{"stage",stage.substr(0,space)},{"stage_detail",space==std::string::npos?"":stage.substr(space+1)},{"last_activity_at",time(nullptr)}});}
                        else if(line.rfind("SPARK_BACKUP_DIRECTORY ",0)==0)archive=line.substr(23);
                        else if(line.rfind("Backup complete: ",0)==0)archive=line.substr(17);
                        if(line.size()>4&&line.substr(line.size()-4)==": OK")++verified;
                    }
                }
                if(count==0)eof=true;else if(count<0&&errno!=EAGAIN&&errno!=EINTR)eof=true;
                if(std::chrono::steady_clock::now()>=next||eof){try{observe();}catch(const std::exception& e){log_exception("warning","backup","Backup activity observation failed",e);}next=std::chrono::steady_clock::now()+std::chrono::seconds(1);}
            }
            if(log_fd>=0){fsync(log_fd);close(log_fd);}
            close(pipes[0]);int status=0;
            if(rc==0){while(waitpid(pid,&status,0)<0){if(errno!=EINTR){rc=-1;break;}}}
            const bool ok=rc==0 && WIFEXITED(status) && WEXITSTATUS(status)==0;
            progress({{"state",ok?"succeeded":"failed"},{"stage",ok?"complete":"failed"},{"finished_at",time(nullptr)},{"process_alive",false},{"message",ok?"Backup completed and verified.":"Backup failed. Review the log; incomplete files may remain."},{"log_tail",output}});
            if(ok)try{record_success();}catch(const std::exception& e){log_exception("warning","backup","Backup completed but timing history could not be saved",e);}
            outcome={{"state",ok?"succeeded":"failed"},{"message",std::string(ok?"Backup completed.\n":"Backup failed.\n")+output}};
        }
    }
    if(action==2){auto status=snapshot();if(status["state"]=="queued"||status["state"]=="running")progress({{"state","failed"},{"message",outcome["message"]},{"finished_at",time(nullptr)},{"process_alive",false}});}
    std::lock_guard<std::mutex> guard(mutex);result=outcome;requested=0;
}
void configure_dashboard(httplib::Server& server,std::shared_ptr<Maintenance> state, std::shared_ptr<Authorization> auth) {
    server.Get("/system/stats",[](const auto&,auto& res){res.set_header("Cache-Control","no-store");res.set_content(system_stats().dump(),"application/json");});
    server.Get("/",[](const auto&,auto& res){res.set_content(html,"text/html; charset=utf-8");});
    server.Get("/admin/status",[state](const auto&,auto& res){nlohmann::json body;{std::lock_guard<std::mutex> guard(state->mutex);body=state->result;}body["version"]=SPARK_VERSION;res.set_content(body.dump(-1,' ',false,nlohmann::json::error_handler_t::replace),"application/json");});
    server.Get("/admin/backup-locations",[](const auto&,auto& res){nlohmann::json locations=nlohmann::json::array();std::ifstream input(history_path());std::string line;while(locations.size()<20 && std::getline(input,line)){auto tab=line.find('\t');if(tab!=std::string::npos)locations.push_back({{"mount",line.substr(0,tab)},{"destination",line.substr(tab+1)}});}res.set_content(nlohmann::json({{"locations",locations}}).dump(),"application/json");});
    for(int action:{1,2})server.Post(action==1?"/admin/restart":"/admin/backup",[state,action,auth](const httplib::Request& req,httplib::Response& res,const httplib::ContentReader& read){
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
        if(auth&&!auth->request(req,res))return;
        auto body=nlohmann::json::parse(payload,nullptr,false);
        if(!body.is_object()){fail(400,"invalid_json");return;}
        std::string mount,dest;
        if(action==2){if((body.contains("mount") && !body["mount"].is_string()) || (body.contains("destination")&&!body["destination"].is_string())){fail(400,"invalid_backup_location");return;}mount=body.value("mount",std::string{});dest=body.value("destination",std::string{});
            auto valid=[](const std::string& s){return s.size()<4096 && s.find_first_of(std::string("\0\t\r\n",4))==std::string::npos;};
            if((!mount.empty()&&mount[0]!='/') || (mount.empty()&&!dest.empty()) || (!dest.empty()&&dest[0]!='/') || !valid(mount)||!valid(dest)){fail(400,"invalid_backup_location");return;}}
        nlohmann::json estimate;
        if(action==2){try{state->initialize();estimate=state->estimate(mount,dest);}catch(const std::exception& e){estimate={{"error",e.what()}};}}
        std::lock_guard<std::mutex> guard(state->mutex);
        if(state->requested||state->transfer_active){fail(409,"maintenance_in_progress");return;}
        if(action==2){try{state->begin_backup(estimate);}catch(const std::exception&){fail(503,"backup_status_unavailable");return;}}
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
    add("/system/stats","Host-wide CPU usage across all cores, memory used excluding available cache, and filesystem usage for persistent SPARK storage (not just SPARK files). CPU is sampled at most once per second and is null until a second sample. Unavailable memory/storage are null. Requires discovery.read; no-store.",
        {{"cpu",{{"usage_percent",12.5}}},{"memory",{{"total_bytes",8589934592LL},{"used_bytes",4294967296LL},{"available_bytes",4294967296LL},{"used_percent",50.0}}},{"storage",{{"total_bytes",100000000000LL},{"used_bytes",40000000000LL},{"available_bytes",55000000000LL},{"used_percent",40.0}}}});
    endpoints.back()["access"]={{"permission","discovery.read"}};
    add("/admin/status","Latest maintenance state: idle, queued, succeeded or failed. During backup use /admin/backup-status for durable stage, activity and logs. A minimal status listener stays available during backup.",
        {{"state","succeeded"},{"message","Server restarted."},{"version",SPARK_VERSION}});
    add("/admin/backup-locations","Up to 20 previously successful backup mount and destination pairs. Drives must be mounted before use.",
        {{"locations",json::array({{{"mount","/media/demo/drive"},{"destination","/media/demo/drive/spark-backups"}}})}});
    add("/admin/restart","Recreate REST, TCP and scheduler services within the running process. Does not reload the executable. HTTP 409 if maintenance is already queued; 403 for missing dashboard header or cross-site browser requests; 400 for invalid JSON; 415 for non-JSON content type.",
        {{"state","queued"}},true,json::object());
    add("/admin/backup","Pause ordinary services, keep authenticated backup status available, run compressed verified backup, then resume even on backup failure. Use POST /admin/backup-estimate before confirmation; space is checked again before execution. Omit mount and destination to save under persistent storage/.spark-backups. An explicit mount must be an external mounted filesystem; its destination defaults to mount/spark-backups. Local backups and private connection credentials are excluded from archives. HTTP 400 for invalid absolute paths or JSON, 403 for missing dashboard header or cross-site requests, 409 if busy, 415 for non-JSON content type. Poll /admin/status for completion; 202 only means accepted.",
        {{"state","queued"}},true,{{"mount","/media/demo/drive"},{"destination","/media/demo/drive/spark-backups"}});
    return endpoints;
}
}
