#include "inter_spark.hpp"
#include "bounded_body.hpp"
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <filesystem>
#include <fstream>
#include <thread>
#include <functional>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <spawn.h>
#include <unistd.h>
extern char **environ;
namespace spark {
namespace {
using json=nlohmann::json;namespace fs=std::filesystem;
struct Failure:std::runtime_error{int status;Failure(int s,const std::string& m):runtime_error(m),status(s){}};
void require(bool ok,int status,const char* message){if(!ok)throw Failure(status,message);}
std::string random_id(){unsigned char data[16];require(RAND_bytes(data,sizeof(data))==1,503,"random_unavailable");const char* hex="0123456789abcdef";std::string out;for(auto c:data){out+=hex[c>>4];out+=hex[c&15];}return out;}
void private_dir(const fs::path& path){if(!fs::exists(path))require(mkdir(path.c_str(),0700)==0,503,"private_storage_unavailable");struct stat s{};require(lstat(path.c_str(),&s)==0&&S_ISDIR(s.st_mode)&&s.st_uid==geteuid()&&(s.st_mode&077)==0,503,"unsafe_private_storage");}
// External backups follow the attached drive's access policy. Control data stays private.
struct BackupDirectory {
    int fd=-1;bool shared=false;fs::path path;
    explicit BackupDirectory(const fs::path& requested){
        if(mkdir(requested.c_str(),0700)!=0&&errno!=EEXIST)throw Failure(503,"destination_not_writable");
        fd=open(requested.c_str(),O_RDONLY|O_DIRECTORY|O_CLOEXEC|O_NOFOLLOW);
        require(fd>=0,409,"unsafe_backup_directory");
        struct stat st{};if(fstat(fd,&st)!=0){close(fd);fd=-1;throw Failure(503,"destination_unavailable");}
        shared=st.st_uid!=geteuid()||(st.st_mode&077)!=0;
        auto probe=".spark-write-test-"+random_id();int test=openat(fd,probe.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC|O_NOFOLLOW,0600);
        if(test<0){close(fd);fd=-1;throw Failure(503,"destination_not_writable");}
        close(test);if(unlinkat(fd,probe.c_str(),0)!=0){close(fd);fd=-1;throw Failure(503,"destination_not_writable");}
        path=fs::path("/proc")/std::to_string(getpid())/"fd"/std::to_string(fd);
    }
    ~BackupDirectory(){if(fd>=0)close(fd);}
    BackupDirectory(const BackupDirectory&)=delete;
    void still_at(const fs::path& original)const{struct stat current{},opened{};require(lstat(original.c_str(),&current)==0&&fstat(fd,&opened)==0&&S_ISDIR(current.st_mode)&&current.st_dev==opened.st_dev&&current.st_ino==opened.st_ino,409,"destination_changed");}
};
void save(const fs::path& path,const json& value){auto tmp=path.string()+"."+random_id();int fd=open(tmp.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0600);require(fd>=0,503,"state_write_failed");auto bytes=value.dump();size_t at=0;bool ok=true;while(at<bytes.size()){auto n=write(fd,bytes.data()+at,bytes.size()-at);if(n<0&&errno==EINTR)continue;if(n<=0){ok=false;break;}at+=n;}if(fsync(fd))ok=false;close(fd);if(!ok||rename(tmp.c_str(),path.c_str())){unlink(tmp.c_str());throw Failure(503,"state_write_failed");}fd=open(path.parent_path().c_str(),O_RDONLY|O_DIRECTORY|O_CLOEXEC);if(fd>=0){fsync(fd);close(fd);}}
json load(const fs::path& p,json fallback=json::object()){return fs::exists(p)?read_json_object(p,"SPARK private state",true):fallback;}
// No shell, no token in argv, bounded diagnostics. Consume large responses as streams.
int process(std::vector<std::string> args,const std::function<void(const char*,size_t)>& consume={}){
    int pipes[2];require(pipe2(pipes,O_CLOEXEC)==0,503,"process_failed");
    posix_spawn_file_actions_t actions;posix_spawn_file_actions_init(&actions);posix_spawn_file_actions_adddup2(&actions,pipes[1],STDOUT_FILENO);posix_spawn_file_actions_addopen(&actions,STDERR_FILENO,"/dev/null",O_WRONLY,0);posix_spawn_file_actions_addclose(&actions,pipes[0]);posix_spawn_file_actions_addclose(&actions,pipes[1]);
    std::vector<char*> argv;for(auto& a:args)argv.push_back(a.data());argv.push_back(nullptr);pid_t pid;int rc=posix_spawnp(&pid,args[0].c_str(),&actions,nullptr,argv.data(),environ);posix_spawn_file_actions_destroy(&actions);close(pipes[1]);
    if(rc){close(pipes[0]);throw Failure(503,"process_unavailable");}
    std::exception_ptr error;char buffer[65536];for(;;){auto n=read(pipes[0],buffer,sizeof(buffer));if(n<0&&errno==EINTR)continue;if(n<=0)break;if(consume&&!error)try{consume(buffer,size_t(n));}catch(...){error=std::current_exception();kill(pid,SIGTERM);}}
    close(pipes[0]);int status=0;while(waitpid(pid,&status,0)<0)if(errno!=EINTR)throw Failure(503,"process_wait_failed");if(error)std::rethrow_exception(error);return WIFEXITED(status)?WEXITSTATUS(status):128;
}
struct Digest{EVP_MD_CTX* ctx=EVP_MD_CTX_new();Digest(){require(ctx&&EVP_DigestInit_ex(ctx,EVP_sha256(),nullptr)==1,503,"digest_failed");}~Digest(){EVP_MD_CTX_free(ctx);}void add(const char* b,size_t n){require(EVP_DigestUpdate(ctx,b,n)==1,503,"digest_failed");}std::string finish(){unsigned char data[32];unsigned int n;require(EVP_DigestFinal_ex(ctx,data,&n)==1,503,"digest_failed");const char* h="0123456789abcdef";std::string s;for(auto c:data){s+=h[c>>4];s+=h[c&15];}return s;}};
std::string checksum(const fs::path& path){require(fs::is_regular_file(fs::symlink_status(path)),409,"unsafe_backup_member");std::ifstream in(path,std::ios::binary);require(bool(in),503,"backup_read_failed");Digest d;char bytes[65536];while(in){in.read(bytes,sizeof(bytes));d.add(bytes,size_t(in.gcount()));}require(in.eof(),503,"backup_read_failed");return d.finish();}
// Do not preserve POSIX modes: FAT/exFAT permissions are controlled by the mount.
void copy_backup_member(const fs::path& source,const fs::path& destination){
    int in=open(source.c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW);require(in>=0,503,"backup_read_failed");
    int out=open(destination.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC|O_NOFOLLOW,0600);
    if(out<0){close(in);throw Failure(503,"destination_write_failed");}
    bool ok=true;char bytes[65536];while(ok){auto n=read(in,bytes,sizeof(bytes));if(n<0&&errno==EINTR)continue;if(n==0)break;if(n<0){ok=false;break;}ssize_t at=0;while(at<n){auto written=write(out,bytes+at,size_t(n-at));if(written<0&&errno==EINTR)continue;if(written<=0){ok=false;break;}at+=written;}}
    if(close(out)!=0)ok=false;close(in);require(ok,503,"destination_write_failed");
}
std::string encode(const std::string& s){std::string out;const char* h="0123456789ABCDEF";for(unsigned char c:s){if(std::isalnum(c)||c=='-'||c=='_'||c=='.'||c=='~')out+=c;else{out+='%';out+=h[c>>4];out+=h[c&15];}}return out;}
std::string quoted(const std::string& s){std::string out="\"";for(char c:s){require(c!='\r'&&c!='\n'&&c!='\0',400,"invalid_connection");if(c=='\\'||c=='\"')out+='\\';out+=c;}return out+'\"';}
void reply(httplib::Response& r,int status,const json& value){r.status=status;r.set_content(value.dump(),"application/json");}
void fields(const json& p,std::initializer_list<const char*> allowed){require(p.is_object(),400,"invalid_packet");for(auto& item:p.items()){bool ok=false;for(auto name:allowed)if(item.key()==name)ok=true;require(ok,400,"unknown_field");}}
std::string string_field(const json& p,const char* key,size_t max=4096){require(p.contains(key)&&p[key].is_string(),400,"missing_or_invalid_field");auto value=p[key].get<std::string>();require(!value.empty()&&value.size()<=max&&value.find_first_of(std::string("\0\r\n",3))==std::string::npos,400,"invalid_field");return value;}
json public_connection(json p){p.erase("token");return p;}
struct Hub {
    std::shared_ptr<FileStorage> storage;std::shared_ptr<Maintenance> maintenance;
    std::mutex mutex;std::thread worker;fs::path root,control,backups;json connections,jobs;
    Hub(std::shared_ptr<FileStorage> s,std::shared_ptr<Maintenance> m):storage(s),maintenance(m){
        for(const auto& d:s->store->drives())if(d["persistent"]==true){storage->require_drive_online(d["id"]);root=d["path"].get<std::string>();}
        require(!root.empty(),503,"persistent_drive_unavailable");control=root/".spark-control";backups=root/".spark-backups";private_dir(control);private_dir(backups);
        connections=load(control/"connections.json");jobs=load(control/"jobs.json");for(auto& j:jobs.items())if(j.value()["state"]=="running"||j.value()["state"]=="queued"){j.value()["state"]="interrupted";j.value()["error"]="server_restarted_retry_transfer";}save(control/"jobs.json",jobs);
    }
    ~Hub(){if(worker.joinable())worker.join();}
    fs::path backup(const std::string& id){require(std::regex_match(id,std::regex(R"((\.incomplete-)?spark-[A-Za-z0-9-]+)")),400,"invalid_backup_id");auto p=backups/id;require(fs::is_directory(fs::symlink_status(p)),404,"backup_not_found");return p;}
    std::vector<fs::path> members(const fs::path& p){std::vector<fs::path> out;for(const auto& e:fs::directory_iterator(p)){auto n=e.path().filename().string();require(std::regex_match(n,std::regex(R"(manifest\.txt|SHA256SUMS|drives\.tsv|database\.sql\.gz\.part-[0-9]{6}|files\.tar\.gz\.part-[0-9]{6}|drive-[0-9a-f]{32}\.tar\.gz\.part-[0-9]{6})"))&&fs::is_regular_file(e.symlink_status()),409,"unsafe_backup_member");out.push_back(e.path());}std::sort(out.begin(),out.end());return out;}
    void verify(const fs::path& p){require(p.filename().string().rfind(".incomplete-",0)!=0,409,"backup_incomplete");members(p);require(process({SPARK_SOURCE_DIR "/scripts/verify-backup.sh",p.string()})==0,409,"backup_verification_failed");}
    json catalog(){json result=json::array();for(const auto& e:fs::directory_iterator(backups)){auto id=e.path().filename().string();if(!fs::is_directory(e.symlink_status())||!std::regex_match(id,std::regex(R"((\.incomplete-)?spark-[A-Za-z0-9-]+)")))continue;uint64_t bytes=0;json files=json::array();bool safe=true;for(const auto& part:fs::directory_iterator(e)){if(!fs::is_regular_file(part.symlink_status())){safe=false;continue;}auto size=part.file_size();bytes+=size;files.push_back({{"name",part.path().filename().string()},{"size_bytes",size}});}std::string manifest,line;std::ifstream in;if(fs::is_regular_file(fs::symlink_status(e.path()/"manifest.txt")))in.open(e.path()/"manifest.txt");while(std::getline(in,line)&&manifest.size()<16384)manifest+=line+"\n";result.push_back({{"id",id},{"state",id.rfind(".incomplete-",0)==0?"incomplete":safe?"complete":"invalid"},{"verification","checked during creation; checked again before transfer"},{"size_bytes",bytes},{"manifest",manifest},{"files",files}});}return result;}
    std::string remote(const json& connection,const std::string& method,const std::string& path,const std::string& body="",const fs::path& upload={},Digest* digest=nullptr){
        auto config=control/("request-"+random_id());std::string text="header = "+quoted("Authorization: Bearer "+connection["token"].get<std::string>())+"\n";
        int fd=open(config.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0600);require(fd>=0,503,"private_storage_unavailable");auto n=write(fd,text.data(),text.size());close(fd);struct Remove{fs::path p;~Remove(){std::error_code e;fs::remove(p,e);}}cleanup{config};require(n==ssize_t(text.size()),503,"private_storage_unavailable");
        std::vector<std::string> args={"curl","-q","--config",config.string(),"--proto","=https","--connect-timeout","10","--max-time",upload.empty()&&!digest?"30":"3600","--fail","--silent","--request",method,"--url",connection["url"].get<std::string>()+path};
        if(!upload.empty())args.insert(args.end(),{"--header","Content-Type: application/octet-stream","--upload-file",upload.string()});else if(method!="GET")args.insert(args.end(),{"--header","Content-Type: application/json","--data-binary",body});
        std::string response;int status=process(args,[&](const char* b,size_t count){if(digest)digest->add(b,count);else{require(response.size()+count<=1048576,502,"remote_response_too_large");response.append(b,count);}});require(status==0,502,"remote_request_failed_check_tls_token_permissions_and_address");return response;
    }
    json test(json& connection){auto answer=json::parse(remote(connection,"POST","/auth/verify",json{{"device_id",connection["device_id"]}}.dump()));require(answer.value("authenticated",false)&&answer.value("device_id",std::string{})==connection["device_id"]&&answer.contains("permissions")&&answer["permissions"].is_array(),502,"remote_identity_not_verified");connection["permissions"]=answer["permissions"];connection["status"]="connected";connection["checked_at"]=time(nullptr);return public_connection(connection);}
    void update(const std::string& id,const json& patch){std::lock_guard<std::mutex> lock(mutex);jobs[id].update(patch);save(control/"jobs.json",jobs);}
    void transfer(std::string id,json request){
        try{
            std::shared_lock<std::shared_mutex> gate(storage->project_gate);auto source=backup(request["backup_id"]);update(id,{{"state","running"},{"stage","verifying source"}});verify(source);auto parts=members(source);uint64_t total=0;for(auto& p:parts)total+=fs::file_size(p);update(id,{{"total_bytes",total},{"completed_bytes",0}});uint64_t completed=0;
            if(request["action"]=="send"){
                json connection;{std::lock_guard<std::mutex> lock(mutex);require(connections.contains(request["connection_id"]),404,"connection_not_found");connection=connections[request["connection_id"]];}
                test(connection);auto permissions=connection["permissions"];require(std::find(permissions.begin(),permissions.end(),"files.read")!=permissions.end()&&std::find(permissions.begin(),permissions.end(),"files.write")!=permissions.end(),403,"remote_files_permissions_required");
                {std::lock_guard<std::mutex> lock(mutex);connections[request["connection_id"]]=connection;save(control/"connections.json",connections);}
                std::string instance;{std::ifstream in(root/".spark-drive");in>>instance;}auto directory=connection["directory"].get<std::string>()+"/"+instance+"/"+source.filename().string();auto drive=connection["drive"].get<std::string>();
                remote(connection,"POST","/files/directories",json{{"drive",drive},{"directory",directory}}.dump());
                json existing=json::object();for(size_t offset=0;;offset+=100){auto page=json::parse(remote(connection,"GET","/files?drive="+encode(drive)+"&directory="+encode(directory)+"&limit=100&offset="+std::to_string(offset)));for(auto& f:page.at("files")){auto name=f["name"].get<std::string>();require(!existing.contains(name),409,"remote_duplicate_backup_member");existing[name]=f;}if(!page.at("has_more").get<bool>())break;}
                for(auto& p:parts){auto name=p.filename().string(),hash=checksum(p);update(id,{{"stage","sending "+name}});std::string file_id;
                    if(existing.contains(name))file_id=existing[name]["id"];else{auto file=json::parse(remote(connection,"POST","/files?drive="+encode(drive)+"&directory="+encode(directory)+"&name="+encode(name),"",p));file_id=file.at("id");}
                    require(std::regex_match(file_id,std::regex("[0-9a-f-]{36}")),502,"invalid_remote_file_id");Digest d;remote(connection,"GET","/files/"+file_id+"/content","",{},&d);require(d.finish()==hash,409,"remote_checksum_mismatch");completed+=fs::file_size(p);update(id,{{"completed_bytes",completed}});
                }
                // Publish the completion marker only after every remote byte was checked.
                if(!existing.contains("SPARK-BACKUP-COMPLETE.json")){auto marker=control/("complete-"+id);save(marker,{{"backup_id",request["backup_id"]},{"source_instance",instance},{"verified_at",time(nullptr)}});try{remote(connection,"POST","/files?drive="+encode(drive)+"&directory="+encode(directory)+"&name=SPARK-BACKUP-COMPLETE.json","",marker);}catch(...){fs::remove(marker);throw;}fs::remove(marker);}
                update(id,{{"remote_directory",directory}});
            }else{
                update(id,{{"stage","checking destination"}});auto drive=storage->resolve_drive(request["drive"]);storage->require_drive_online(drive);fs::path target;for(auto& d:storage->store->drives())if(d["id"]==drive)target=fs::path(d["path"].get<std::string>())/".spark-backups";require(target!=backups,409,"destination_is_source");BackupDirectory target_directory(target);auto original_target=target;target=target_directory.path;auto final=target/source.filename();auto temporary=target/(".incomplete-"+source.filename().string());
                auto notice=[&](bool shared){if(shared)update(id,{{"warning","Destination uses shared or mount-controlled permissions. Other local users may be able to read or modify this backup."}});};notice(target_directory.shared);
                if(fs::exists(final)){require(fs::is_directory(fs::symlink_status(final)),409,"unsafe_backup_directory");BackupDirectory existing(final);notice(existing.shared);verify(existing.path);for(auto& p:parts)require(checksum(p)==checksum(existing.path/p.filename()),409,"destination_conflict");existing.still_at(final);}
                else{BackupDirectory temporary_directory(temporary);notice(temporary_directory.shared);for(auto& p:parts){auto destination=temporary_directory.path/p.filename();require(!fs::is_symlink(fs::symlink_status(destination)),409,"unsafe_backup_member");update(id,{{"stage","copying "+p.filename().string()}});if(!fs::exists(destination)||checksum(destination)!=checksum(p)){auto scratch=temporary_directory.path/(".copy-"+random_id());try{copy_backup_member(p,scratch);fs::rename(scratch,destination);}catch(...){std::error_code ec;fs::remove(scratch,ec);throw;}}require(checksum(destination)==checksum(p),409,"destination_checksum_mismatch");completed+=fs::file_size(p);update(id,{{"completed_bytes",completed}});}require(process({SPARK_SOURCE_DIR "/scripts/verify-backup.sh",temporary_directory.path.string()})==0,409,"destination_verification_failed");int rc=syncfs(temporary_directory.fd);require(rc==0,503,"destination_sync_failed");temporary_directory.still_at(temporary);fs::rename(temporary,final);}
                target_directory.still_at(original_target);storage->require_drive_online(drive);update(id,{{"stage","flushing destination"}});int durable=open(target.c_str(),O_RDONLY|O_DIRECTORY|O_CLOEXEC);require(durable>=0,503,"destination_unavailable");int synced=syncfs(durable);close(durable);require(synced==0,503,"destination_sync_failed");
                if(request["action"]=="move")fs::remove_all(source);
            }
            update(id,{{"state","succeeded"},{"stage","complete"},{"completed_bytes",total},{"finished_at",time(nullptr)}});
        }catch(const std::exception& e){try{update(id,{{"state","failed"},{"error",dynamic_cast<const Failure*>(&e)?e.what():"transfer_failed"},{"finished_at",time(nullptr)}});}catch(...){} }
        maintenance->transfer_active=false;
    }
    json start(const json& request){std::lock_guard<std::mutex> maintenance_lock(maintenance->mutex);require(!maintenance->requested&&!maintenance->transfer_active,409,"maintenance_or_transfer_in_progress");if(worker.joinable())worker.join();auto id=random_id();{std::lock_guard<std::mutex> lock(mutex);require(jobs.size()<1000,409,"job_history_full");jobs[id]={{"id",id},{"request",request},{"state","queued"},{"created_at",time(nullptr)}};save(control/"jobs.json",jobs);}maintenance->transfer_active=true;try{worker=std::thread([this,id,request]{transfer(id,request);});}catch(...){maintenance->transfer_active=false;throw;}return {{"job_id",id},{"state","queued"}};}
};
constexpr const char* page=R"HTML(<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>SPARK management</title><style>body{font:16px system-ui;background:#101820;color:#e5edf3;margin:0}main{max-width:1050px;margin:auto;padding:32px}a{color:#78dfcc}button,input,select{font:inherit;padding:9px;margin:5px;border-radius:5px}button{cursor:pointer}article,section{border:1px solid #456;padding:18px;margin:15px 0;border-radius:8px}p,pre{overflow-wrap:anywhere;white-space:pre-wrap}label{display:block;margin:10px 0}input,select{max-width:90%;background:#20303b;color:white;border:1px solid #678}dialog{background:#182732;color:white;max-width:600px;width:90%;border:1px solid #678}dialog::backdrop{background:#0009}dialog label input{display:block;width:90%}[hidden]{display:none!important}.error{color:#ffb0a0}</style><main><a href="/">SPARK dashboard</a> · <a href="/backups/explorer">Backups</a> · <a href="/connections/explorer">Connections</a><h1 id="title"></h1><p id="intro"></p><button id="new"></button><p id="status" role="status"></p><div id="rows"></div><section id="jobs"><h2>Transfers</h2><div id="job-actions"></div><p>Dismissal removes history only; backup files are retained.</p><div id="job-rows"></div></section></main>
<dialog id="dialog"><form id="form"><h2 id="dialog-title"></h2><div id="fields"></div><p id="error" class="error" role="alert"></p><button type="submit" id="save">Save</button><button type="button" id="cancel">Cancel</button></form></dialog><script src="/auth/client.js"></script><script>
const $=id=>document.getElementById(id),connectionPage=location.pathname.startsWith('/connections');let pending=false,mode='',record=null;
async function api(path,method='GET',body){const r=await fetch(path,{method,headers:body?{'Content-Type':'application/json','X-SPARK-Admin':'dashboard'}:{},...(body?{body:JSON.stringify(body)}:{})});const data=r.status===204?null:await r.json();if(!r.ok)throw Error(data.error||'Request failed');return data;}
function element(tag,text,parent){const n=document.createElement(tag);if(text!==undefined)n.textContent=text;parent.append(n);return n;}
function button(parent,label,action){const b=element('button',label,parent);b.onclick=async()=>{b.disabled=true;try{await action();await refresh();}catch(e){$('status').textContent=e.message;}finally{b.disabled=false;}};return b;}
function field(name,label,value='',type='text'){const l=element('label',label,$('fields'));const i=element('input',undefined,l);i.name=name;i.value=value;i.type=type;i.autocomplete=type==='password'?'new-password':'off';return i;}
function select(name,label,items){const l=element('label',label,$('fields')),s=element('select',undefined,l);s.name=name;for(const [value,text] of items)s.add(new Option(text,value));return s;}
async function dialog(kind,item){if(kind==='create'){location.assign('/backups/new');return;}mode=kind;record=item;$('fields').replaceChildren();$('error').textContent='';$('dialog-title').textContent=kind==='connection'?'SPARK connection':kind==='create'?'Create backup':'Transfer backup';
if(kind==='connection'){field('name','Connection name',item?.name||'').required=true;field('url','HTTPS address',item?.url||'https://').required=true;field('device_id','Assigned device ID on the remote instance',item?.device_id||'').required=true;const t=field('token',item?'New token (leave blank to retain)':'Provisioned token','','password');t.required=!item;field('drive','Remote destination drive',item?.drive||'A').required=true;field('directory','Remote base directory',item?.directory||'/backups').required=true;}
else if(kind==='create'){field('mount','External mount (blank = persistent storage)');field('destination','External directory (optional)');element('p','SPARK briefly pauses while creating a consistent backup. This page reconnects automatically.',$('fields'));}
else{select('action','Action',[['copy','Copy to drive'],['move','Move to drive'],['send','Send to SPARK']]);const drives=await api('/backups/drives');select('drive','Destination drive',drives.drives.filter(d=>!d.persistent&&d.online).map(d=>[d.letter,d.letter+': '+d.path]));let connections=[];try{connections=(await api('/connections')).connections;}catch{}select('connection_id','SPARK connection',connections.map(c=>[c.id,c.name+' — '+c.status]));element('p','Copy and Send retain the local backup. Move deletes it only after the destination is verified. External drive permissions apply; FAT/exFAT or shared mounts may allow other local users to access backups.',$('fields'));}
$('dialog').showModal();}
$('cancel').onclick=()=>{$('fields').querySelectorAll('input[type=password]').forEach(i=>i.value='');$('dialog').close();};window.addEventListener('pagehide',()=>{$('fields').querySelectorAll('input[type=password]').forEach(i=>i.value='');});$('form').onsubmit=async e=>{e.preventDefault();if(pending)return;pending=true;$('save').disabled=true;try{const body=Object.fromEntries(new FormData($('form')));if(mode==='connection'){if(record&&!body.token)delete body.token;await api('/connections'+(record?'/'+record.id:''),record?'PATCH':'POST',body);}else if(mode==='create')await api('/admin/backup','POST',body);else{if(body.action==='send')delete body.drive;else delete body.connection_id;await api('/backups/'+record.id+'/transfers','POST',body);}$('fields').querySelectorAll('input[type=password]').forEach(i=>i.value='');$('dialog').close();await refresh();}catch(e){$('error').textContent=e.message;}finally{pending=false;$('save').disabled=false;}};
async function refresh(){try{$('rows').replaceChildren();if(connectionPage){for(const c of (await api('/connections')).connections){const a=element('article',undefined,$('rows'));element('h2',c.name,a);element('p',c.url+' · device '+c.device_id+' · '+c.status,a);element('p','Permissions: '+(c.permissions||[]).join(', '),a);element('p','Last checked: '+(c.checked_at?new Date(c.checked_at*1000).toLocaleString():'Never'),a);button(a,'Edit',()=>dialog('connection',c));button(a,'Test connection',()=>api('/connections/'+c.id+'/test','POST',{}));button(a,'Delete',async()=>{if(confirm('Delete this saved connection? The remote token is not revoked.'))await api('/connections/'+c.id,'DELETE');});}}
else{for(const b of (await api('/backups')).backups){const a=element('article',undefined,$('rows'));element('h2',b.id,a);element('p',b.state+' · '+b.size_bytes.toLocaleString()+' bytes · '+b.verification,a);const details=element('details',undefined,a);element('summary','Contents and download links',details);element('pre',b.manifest,details);for(const f of b.files){const row=element('p',undefined,details),link=element('a',f.name,row);link.href='/backups/'+encodeURIComponent(b.id)+'/files/'+encodeURIComponent(f.name);link.download=f.name;}if(b.state==='complete')button(a,'Copy / Move / Send',()=>dialog('transfer',b));button(a,'Delete',async()=>{if(confirm('Permanently delete this local backup and free its storage?'))await api('/backups/'+b.id,'DELETE');});} $('job-rows').replaceChildren();const jobs=(await api('/backups/jobs')).jobs;$('job-actions').replaceChildren();button($('job-actions'),'Clear completed',()=>api('/backups/jobs/completed','DELETE')).disabled=!jobs.some(j=>j.state==='succeeded');for(const j of jobs){const a=element('article',undefined,$('job-rows'));element('p',j.request.backup_id+' · '+j.request.action+' · '+j.state,a);element('p',(j.stage||'')+' '+(j.completed_bytes||0)+' / '+(j.total_bytes||0)+' bytes',a);if(j.error)element('p',j.error,a);if(j.warning)element('p',j.warning,a);if(['succeeded','failed','interrupted'].includes(j.state))button(a,'Dismiss',()=>api('/backups/jobs/'+j.id,'DELETE'));if(['failed','interrupted'].includes(j.state))button(a,'Retry',()=>api('/backups/jobs/'+j.id+'/retry','POST',{}));}}
$('status').textContent='';}catch(e){$('status').textContent=e.message+' — reconnecting…';}}
$('title').textContent=connectionPage?'SPARK connections':'Local backups';$('intro').textContent=connectionPage?'Private, reusable credentials for other SPARK instances. Permissions are refreshed when tested and before transfers.':'Backups on persistent storage. Creation and transfers are separate operations. External copies are not listed here.';$('new').textContent=connectionPage?'Register connection':'Create backup';$('new').onclick=()=>dialog(connectionPage?'connection':'create',null).catch(e=>$('status').textContent=e.message);$('jobs').hidden=connectionPage;refresh();setInterval(()=>{if(!$('dialog').open&&!pending)refresh();},4000);
</script></html>)HTML";
}
void configure_inter_spark(httplib::Server& server,std::shared_ptr<FileStorage> storage,std::shared_ptr<Maintenance> maintenance,std::shared_ptr<Authorization> auth){
    struct Lazy{std::mutex mutex;std::shared_ptr<Hub> hub;};auto lazy=std::make_shared<Lazy>();
    auto handle=[storage,maintenance,auth,lazy](const httplib::Request& req,httplib::Response& res){
        try{
            require(req.params.empty(),400,"invalid_parameters");
            if(req.method!="GET"&&req.method!="HEAD"){
                if(req.method!="DELETE"){auto type=req.get_header_value("Content-Type");require(type.substr(0,type.find(';'))=="application/json",415,"unsupported_media_type");}
                require(req.get_header_value("Sec-Fetch-Site")!="cross-site",403,"cross_site_request");
                if(!auth||dynamic_cast<OpenIdentityProvider*>(auth->provider().get())){auto origin=req.get_header_value("Origin");require(origin.empty()||origin=="http://"+req.get_header_value("Host")||origin=="https://"+req.get_header_value("Host"),403,"cross_site_request");}
            }
            res.set_header("Cache-Control","no-store");
            if(req.path=="/auth/verify"){
                auto p=json::parse(req.body,nullptr,false);fields(p,{"device_id"});auto device=string_field(p,"device_id",64);
                if(!auth||dynamic_cast<OpenIdentityProvider*>(auth->provider().get())){reply(res,200,{{"authenticated",false},{"auth_mode","open"},{"permissions",json::array()}});return;}
                require(req.get_header_value_count("Authorization")==1,401,"bearer_token_required");auto who=auth->identity(req,res);if(!who)return;require(who->kind=="device"&&who->subject==device,403,"device_identity_mismatch");reply(res,200,{{"authenticated",true},{"auth_mode","required"},{"device_id",device},{"permissions",who->permissions}});return;
            }
            if(auth){auto who=auth->identity(req,res);if(!who)return;require(who->kind=="open"||who->kind=="administrator",403,"administrator_required");}
            if(req.path=="/backups/explorer"||req.path=="/connections/explorer"){res.set_content(page,"text/html; charset=utf-8");return;}
            std::shared_ptr<Hub> hub;{std::lock_guard<std::mutex> lock(lazy->mutex);if(!lazy->hub){std::shared_lock<std::shared_mutex> gate(storage->project_gate);storage->initialize();lazy->hub=std::make_shared<Hub>(storage,maintenance);}hub=lazy->hub;}
            bool get=req.method=="GET"||req.method=="HEAD";
            if(req.path=="/backups/drives"){reply(res,200,storage->drives());return;}
            if(req.method=="DELETE"&&(req.path=="/backups/jobs/completed"||std::regex_match(req.path,std::regex(R"(/backups/jobs/[0-9a-f]{32})")))){
                std::lock_guard<std::mutex> lock(hub->mutex);auto next=hub->jobs;size_t removed=0;
                if(req.path=="/backups/jobs/completed"){
                    for(auto it=next.begin();it!=next.end();)if(it.value()["state"]=="succeeded"){it=next.erase(it);++removed;}else ++it;
                }else{
                    auto id=req.path.substr(std::string("/backups/jobs/").size());require(next.contains(id),404,"job_not_found");auto state=next[id]["state"];
                    require(state=="succeeded"||state=="failed"||state=="interrupted",409,"job_in_progress");next.erase(id);removed=1;
                }
                save(hub->control/"jobs.json",next);hub->jobs=std::move(next);reply(res,200,{{"removed",removed}});return;
            }
            if(req.path=="/backups/jobs"){std::lock_guard<std::mutex> lock(hub->mutex);json all=json::array();for(auto& j:hub->jobs.items())all.push_back(j.value());reply(res,200,{{"jobs",all}});return;}
            if(req.path=="/backups"){std::lock_guard<std::mutex> lock(hub->mutex);reply(res,200,{{"backups",hub->catalog()}});return;}
            std::smatch match;
            if(std::regex_match(req.path,match,std::regex(R"(/backups/jobs/([0-9a-f]{32})/retry)"))){json job;{std::lock_guard<std::mutex> lock(hub->mutex);require(hub->jobs.contains(match[1].str()),404,"job_not_found");job=hub->jobs[match[1].str()];if(job["request"]["action"]=="send"&&auth){auto who=auth->identity(req,res);if(!who)return;require(permitted(*who,"admin.connections"),403,"forbidden");}require(job["state"]=="failed"||job["state"]=="interrupted",409,"job_not_retryable");}reply(res,202,hub->start(job["request"]));return;}
            if(req.path.rfind("/connections",0)==0){
                std::lock_guard<std::mutex> lock(hub->mutex);
                if(req.path=="/connections"&&get){json all=json::array();for(auto& c:hub->connections.items())all.push_back(public_connection(c.value()));reply(res,200,{{"connections",all}});return;}
                std::string id;if(req.path!="/connections"){require(std::regex_match(req.path,match,std::regex(R"(/connections/([0-9a-f]{32})(/test)?)")),404,"connection_not_found");id=match[1];require(hub->connections.contains(id),404,"connection_not_found");}
                require(!maintenance->transfer_active,409,"transfer_in_progress");
                if(req.method=="DELETE"){hub->connections.erase(id);save(hub->control/"connections.json",hub->connections);res.status=204;return;}
                json c=id.empty()?json::object():hub->connections[id];
                bool test_only=!id.empty()&&req.path=="/connections/"+id+"/test";
                if(!test_only){auto p=json::parse(req.body,nullptr,false);fields(p,{"name","url","device_id","token","drive","directory"});c.update(p);c["name"]=string_field(c,"name",200);auto url=string_field(c,"url",300);if(url.back()=='/')url.pop_back();require(std::regex_match(url,std::regex(R"(https://([A-Za-z0-9][A-Za-z0-9.-]*|\[[0-9A-Fa-f:]+\])(:[0-9]{1,5})?)")),400,"https_origin_required");c["url"]=url;require(std::regex_match(string_field(c,"device_id",64),std::regex("[A-Za-z0-9_-]{1,64}")),400,"invalid_device_id");require(std::regex_match(string_field(c,"token",100),std::regex("spark1_[0-9a-f]{64}")),400,"invalid_token");require(std::regex_match(string_field(c,"drive",8),std::regex("[A-Z]+")),400,"invalid_drive");auto directory=string_field(c,"directory",700);require(directory[0]=='/'&&directory.back()!='/'&&directory.find("//")==std::string::npos&&directory.find('\\')==std::string::npos,400,"invalid_directory");for(auto& component:fs::path(directory))require(component!="."&&component!="..",400,"invalid_directory");c["directory"]=directory;}
                try{hub->test(c);}catch(...){if(test_only){hub->connections[id]["status"]="failed";hub->connections[id]["checked_at"]=time(nullptr);save(hub->control/"connections.json",hub->connections);}throw;}
                if(id.empty()){require(hub->connections.size()<100,409,"too_many_connections");id=random_id();}c["id"]=id;hub->connections[id]=c;save(hub->control/"connections.json",hub->connections);reply(res,req.path=="/connections"?201:200,public_connection(c));return;
            }
            require(std::regex_match(req.path,match,std::regex(R"(/backups/([^/]+)(/transfers|/files/([^/]+))?)")),404,"not_found");auto id=match[1].str();auto path=hub->backup(id);
            if(req.method=="DELETE"){
                std::lock_guard<std::mutex> lock(maintenance->mutex);require(!maintenance->transfer_active&&!maintenance->requested,409,"backup_in_use");std::lock_guard<std::mutex> catalog_lock(hub->mutex);fs::remove_all(path);res.status=204;return;
            }
            if(match[2]=="/transfers"){
                auto p=json::parse(req.body,nullptr,false);fields(p,{"action","drive","connection_id"});auto action=string_field(p,"action",16);require(action=="copy"||action=="move"||action=="send",400,"invalid_transfer_action");if(action=="send"){string_field(p,"connection_id",32);if(auth){auto who=auth->identity(req,res);if(!who)return;require(permitted(*who,"admin.connections"),403,"forbidden");}}else string_field(p,"drive",8);p["backup_id"]=id;reply(res,202,hub->start(p));return;
            }
            if(get&&match[3].matched){auto name=match[3].str();auto members=hub->members(path);auto member=path/name;require(std::find(members.begin(),members.end(),member)!=members.end(),404,"member_not_found");int fd=open(member.c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW);require(fd>=0,404,"member_not_found");struct stat st{};if(fstat(fd,&st)||!S_ISREG(st.st_mode)){close(fd);throw Failure(404,"member_not_found");}auto descriptor=std::shared_ptr<int>(new int(fd),[](int* p){close(*p);delete p;});res.set_header("Content-Disposition","attachment; filename=\""+name+"\"");res.set_content_provider(size_t(st.st_size),"application/octet-stream",[descriptor](size_t offset,size_t length,httplib::DataSink& sink){char b[65536];auto n=pread(*descriptor,b,std::min(length,sizeof(b)),off_t(offset));return n>0&&sink.write(b,size_t(n));});return;}
            throw Failure(404,"not_found");
        }catch(const Failure& e){reply(res,e.status,{{"error",e.what()}});}catch(const json::exception&){reply(res,400,{{"error","invalid_packet"}});}catch(const std::exception&){reply(res,503,{{"error","backup_or_connection_unavailable"}});}
    };
    for(auto path:{"/backups/explorer","/connections/explorer","/backups","/backups/drives","/backups/jobs","/connections",R"(/backups/([^/]+)/files/([^/]+))"})server.Get(path,handle);
    for(auto path:{"/auth/verify","/connections",R"(/connections/([0-9a-f]{32})/test)",R"(/backups/([^/]+)/transfers)",R"(/backups/jobs/([0-9a-f]{32})/retry)"})body_route(server,"POST",path,handle,auth);
    body_route(server,"PATCH",R"(/connections/([0-9a-f]{32}))",handle,auth);
    for(auto path:{"/backups/jobs/completed",R"(/backups/jobs/([0-9a-f]{32}))",R"(/connections/([0-9a-f]{32}))",R"(/backups/([^/]+))"})body_route(server,"DELETE",path,handle,auth);
}
json inter_spark_endpoints(){json out=json::array();
    auto add=[&](const char* method,const char* path,const char* description,json input,json output,int status=200){out.push_back({{"method",method},{"path",path},{"description",description},{"input",{{"headers",{{"Content-Type","application/json"}}},{"body",input}}},{"output",{{"status",status},{"body",output}}},{"errors",{{"400","invalid_packet, invalid_parameters, unknown_field, invalid_field"},{"401","unauthorized"},{"403","forbidden, administrator_required, device_identity_mismatch"},{"404","backup_not_found, connection_not_found, job_not_found"},{"409","backup_in_use, backup_incomplete, backup_verification_failed, maintenance_or_transfer_in_progress"},{"502","remote_request_failed_check_tls_token_permissions_and_address, remote_identity_not_verified"},{"503","backup_or_connection_unavailable"}}}});};
    add("POST","/auth/verify","Verify the bearer token's exact device subject and return its own current permissions. No permission grant required; administrator tokens rejected. Open mode returns authenticated=false, auth_mode=open.",{{"device_id","spark-a"}},{{"authenticated",true},{"device_id","spark-a"},{"permissions",json::array({"files.read","files.write"})}});
    out.back()["access"]="authenticated-self";
    add("GET","/backups/explorer","Administrator backup viewer: persistent-drive backups, download parts, copy/move, send, delete and transfer progress.",nullptr,"HTML");
    add("GET","/connections/explorer","Administrator connection manager. Register, test, edit and delete remote credentials. Tokens never returned.",nullptr,"HTML");
    add("GET","/backups","List backups in persistent storage, including incomplete directories, size, manifest and downloadable members. No external-copy inventory.",nullptr,{{"backups",json::array()}});
    add("GET","/backups/drives","List registered drives for backup copy/move. Requires admin.backup.",nullptr,{{"drives",json::array()}});
    add("DELETE","/backups/jobs/completed","Clear successful transfer history only. Retains failed, interrupted and active jobs and all backup files. Requires admin.backup.",nullptr,{{"removed",1}});
    add("DELETE","/backups/jobs/{id}","Dismiss a succeeded, failed or interrupted transfer history record only. Active jobs return 409 job_in_progress. Backup files are retained. Requires admin.backup.",nullptr,{{"removed",1}});
    add("GET","/backups/jobs","List persistent transfer jobs and byte progress. Interrupted transfers can be retried after restart.",nullptr,{{"jobs",json::array()}});
    add("POST","/backups/jobs/{id}/retry","Retry a failed/interrupted transfer using the same local backup. Remote members already uploaded are verified and reused.",json::object(),{{"job_id","identifier"},{"state","queued"}},202);
    add("GET","/backups/{id}/files/{name}","Download one backup member. Backup archives are split into parts; download all members for restore.",nullptr,"binary bytes");
    add("DELETE","/backups/{id}","Permanently delete a local backup, including incomplete backups. Blocked during a transfer or maintenance.",nullptr,nullptr,204);
    add("POST","/backups/{id}/transfers","Background copy/move to an attached drive, or send over verified HTTPS to a saved connection. action=copy|move requires drive; action=send requires connection_id and admin.connections. Move removes source only after verification; Send retains it. Requires admin.backup. One transfer at a time.",{{"action","send"},{"connection_id","identifier"}},{{"job_id","identifier"},{"state","queued"}},202);
    add("GET","/connections","List saved connections, remote device ID, destination, permissions and last test status. Requires admin.connections; never includes tokens.",nullptr,{{"connections",json::array()}});
    json packet={{"name","SPARK B"},{"url","https://spark.example.com"},{"device_id","spark-a"},{"token","spark1_..."},{"drive","A"},{"directory","/backups"}};
    add("POST","/connections","Verify and save a private HTTPS connection. Remote must implement auth/verify and require tokens. Credentials excluded from backups.",packet,{{"id","identifier"},{"status","connected"},{"permissions",json::array()}},201);
    packet.erase("token");add("PATCH","/connections/{id}","Update connection fields and verify before saving. Omitted token retains existing secret; supplied token rotates it.",{{"name","New name"}},packet);
    add("DELETE","/connections/{id}","Delete saved connection and token from this instance; does not revoke the remote credential.",nullptr,nullptr,204);
    add("POST","/connections/{id}/test","Refresh verification status and current permission snapshot; does not grant permissions.",json::object(),{{"status","connected"},{"permissions",json::array()}});
    return out;
}
}
