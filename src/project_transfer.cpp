#include "project_transfer.hpp"
#include "bounded_body.hpp"
#include "logging.hpp"
#include <archive.h>
#include <archive_entry.h>
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <array>
#include <atomic>
#include <fstream>
#include <map>
#include <set>
#include <regex>
#include <thread>
#include <condition_variable>
namespace spark { namespace {
using nlohmann::json;namespace fs=std::filesystem;
constexpr uint64_t text_limit=1024*1024,manifest_limit=8*1024*1024,space_reserve=64*1024*1024;
struct Error:std::runtime_error{int status;Error(int s,const std::string& e):std::runtime_error(e),status(s){}};
void need(bool b,int status,const std::string& e){if(!b)throw Error(status,e);}
std::string random_id(){unsigned char b[16];need(RAND_bytes(b,16)==1,503,"random_unavailable");std::string s;for(auto c:b){s+="0123456789abcdef"[c>>4];s+="0123456789abcdef"[c&15];}return s;}
bool uuid(const std::string& s){return std::regex_match(s,std::regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"));}
bool inside(const std::string& s,const std::string& root){return s==root||s.rfind(root+"/",0)==0;}
bool name_ok(const json& j,size_t limit,bool blank=false){if(!j.is_string())return false;const auto s=j.get<std::string>();return !s.empty()&&s.size()<=limit&&s!="."&&s!=".."&&(blank||s.find_first_not_of(' ')!=std::string::npos)&&s.find_first_of("/\\")==std::string::npos&&std::none_of(s.begin(),s.end(),[](unsigned char c){return c<32||c==127;});}
bool relative(const std::string& s){if(s.empty())return true;if(s.size()>1280||s.front()=='/'||s.back()=='/'||s.find("//")!=std::string::npos)return false;for(const auto& part:fs::path(s))if(!name_ok(part.string(),255,true))return false;return true;}
std::string join(const std::string& root,const std::string& rel){return root+(rel.empty()?"":"/"+rel);}
std::string read_text(const fs::path& p,uint64_t limit){need(fs::file_size(p)<=limit,413,"text_too_large");std::ifstream in(p,std::ios::binary);need(bool(in),503,"storage_unavailable");return std::string(std::istreambuf_iterator<char>(in),{});}
bool is_text(const std::string& s){for(unsigned char c:s)if((c<32&&c!='\t'&&c!='\n'&&c!='\r')||c==127)return false;try{json(s).dump();return true;}catch(...){return false;}}
void sync_file(const fs::path& p){int fd=open(p.c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW);need(fd>=0,503,"storage_unavailable");int result=fsync(fd);close(fd);need(result==0,503,"storage_unavailable");}
std::string hash_file(const fs::path& p){std::ifstream in(p,std::ios::binary);need(bool(in),409,"source_unavailable");std::unique_ptr<EVP_MD_CTX,decltype(&EVP_MD_CTX_free)> ctx(EVP_MD_CTX_new(),EVP_MD_CTX_free);need(ctx&&EVP_DigestInit_ex(ctx.get(),EVP_sha256(),nullptr)==1,503,"hash_unavailable");std::array<char,65536> b;while(in){in.read(b.data(),b.size());need(EVP_DigestUpdate(ctx.get(),b.data(),size_t(in.gcount()))==1,503,"hash_unavailable");}need(in.eof(),503,"storage_unavailable");unsigned char digest[32];unsigned n;need(EVP_DigestFinal_ex(ctx.get(),digest,&n)==1,503,"hash_unavailable");std::string s;for(auto c:digest){s+="0123456789abcdef"[c>>4];s+="0123456789abcdef"[c&15];}return s;}
struct Archive {archive* a;bool writer;explicit Archive(bool w):a(w?archive_write_new():archive_read_new()),writer(w){need(a,503,"archive_unavailable");}~Archive(){if(writer)archive_write_free(a);else archive_read_free(a);}};
struct Job {
 std::string id=random_id(),owner,kind,state="preparing",error,drive;time_t expires=time(nullptr)+3600;
 fs::path stage;json result=json::object(),manifest,expected,plan=json::array(),directories=json::array();std::atomic<uint64_t> bytes{0};std::thread worker;
 explicit Job(){std::string path=(fs::temp_directory_path()/"spark-project-XXXXXX").string();std::vector<char> b(path.begin(),path.end());b.push_back(0);char* p=mkdtemp(b.data());need(p,503,"staging_unavailable");stage=p;}
 ~Job(){if(worker.joinable()){if(worker.get_id()==std::this_thread::get_id())worker.detach();else worker.join();}std::error_code e;fs::remove_all(stage,e);}
};
json public_job(const Job& j){json out={{"id",j.id},{"kind",j.kind},{"state",j.state},{"expires_at",j.expires},{"processed_bytes",j.bytes.load()},{"result",j.result}};if(!j.error.empty())out["error"]=j.error;if(j.state=="review"){out["entries"]=j.plan;for(auto& e:out["entries"]){e.erase("data");e.erase("local_record");}}return out;}
json fields(const json& record,const std::vector<std::string>& names){json out=json::object();for(const auto& name:names)out[name]=record.at(name);return out;}
const std::map<std::string,std::vector<std::string>> editable={{"projects",{"name","description","github_url"}},{"boards",{"name"}},{"columns",{"name"}},{"tasks",{"name","description","severity","column_id"}}};
void validate(json& m){
 need(m.is_object()&&m.value("format","")=="spark-project"&&m.value("version",0)==1,400,"unsupported_project_archive");
 std::set<std::string> ids;std::map<std::string,size_t> depths;std::map<std::string,json> projects,boards,columns;std::set<std::string> paths;size_t count=0;
 for(auto kind:{"projects","boards","columns","tasks","files","directories"}){need(m.contains(kind)&&m[kind].is_array(),400,"invalid_manifest");count+=m[kind].size();}need(count<=10000&&!m["projects"].empty(),413,"manifest_limit");
 for(auto kind:{"projects","boards","columns","tasks","files"})for(auto& r:m[kind]){
  need(r.is_object()&&r.contains("id")&&r["id"].is_string()&&uuid(r["id"])&&ids.insert(r["id"]).second,400,"invalid_manifest_id");
  if(std::string(kind)!="files"){need(r.contains("name")&&r["name"].is_string(),400,"invalid_manifest_name");auto name=r["name"].get<std::string>();need(std::string(kind)=="projects"?name_ok(r["name"],200):name.size()<=200&&name.find_first_not_of(" \t\r\n")!=std::string::npos,400,"invalid_manifest_name");}
  if(std::string(kind)=="projects"||std::string(kind)=="tasks")need(r.contains("description")&&r["description"].is_string()&&r["description"].get<std::string>().size()<=4000,400,"invalid_manifest_description");
  if(std::string(kind)=="projects"){
   need(r.contains("directory")&&r["directory"].is_string()&&relative(r["directory"])&&paths.insert(r["directory"]).second&&r.contains("parent_id"),400,"invalid_manifest_directory");
   if(projects.empty())need(r["id"]==m.at("root")&&r["parent_id"].is_null()&&r["directory"]=="",400,"invalid_manifest_root");
   else {need(r["parent_id"].is_string()&&projects.count(r["parent_id"]),400,"invalid_manifest_parent");auto p=projects.at(r["parent_id"]);need(p["directory"]==""||inside(r["directory"],p["directory"]),400,"invalid_manifest_directory");}
   need(r.contains("github_url")&&(r["github_url"].is_null()||(r["github_url"].is_string()&&r["github_url"].get<std::string>().size()<=2048&&std::regex_match(r["github_url"].get<std::string>(),std::regex(R"(https://github\.com/[A-Za-z0-9_-]+/[A-Za-z0-9_.-]+/?)")))),400,"invalid_manifest_github");depths[r["id"]]=r["parent_id"].is_null()?0:depths.at(r["parent_id"])+1;need(depths.at(r["id"])<=64,400,"project_depth_limit");projects[r["id"]]=r;
  }else if(std::string(kind)=="boards"){need(r.contains("project_id")&&r["project_id"].is_string()&&projects.count(r["project_id"]),400,"invalid_manifest_parent");boards[r["id"]]=r;}
  else if(std::string(kind)=="columns"){need(r.contains("board_id")&&r["board_id"].is_string()&&boards.count(r["board_id"]),400,"invalid_manifest_parent");columns[r["id"]]=r;}
  else if(std::string(kind)=="tasks")need(r.contains("board_id")&&r["board_id"].is_string()&&boards.count(r["board_id"])&&r.contains("column_id")&&r["column_id"].is_string()&&columns.count(r["column_id"])&&columns.at(r["column_id"])["board_id"]==r["board_id"]&&r.contains("severity")&&r["severity"].is_string()&&std::set<std::string>{"Very Low","Low","Normal","High","Very High","Urgent"}.count(r["severity"]),400,"invalid_manifest_task");
  else need(r.contains("path")&&r["path"].is_string()&&!r["path"].get<std::string>().empty()&&relative(r["path"])&&r.contains("sha256")&&r["sha256"].is_string()&&std::regex_match(r["sha256"].get<std::string>(),std::regex("[0-9a-f]{64}"))&&r.contains("size_bytes")&&r["size_bytes"].is_number_unsigned(),400,"invalid_manifest_file");
 }
 paths.clear();for(auto& f:m["files"])need(paths.insert(f["path"]).second,409,"duplicate_file_path");for(auto& d:m["directories"])need(d.is_string()&&relative(d),400,"invalid_manifest_directory");
}
struct Transfers {
 std::shared_ptr<FileStorage> storage;ProjectTransferStore store{DatabaseConfig::from_environment()};std::mutex mutex;std::map<std::string,std::shared_ptr<Job>> jobs;std::condition_variable wake;bool stopping=false;std::thread janitor;
 explicit Transfers(std::shared_ptr<FileStorage> s):storage(std::move(s)){janitor=std::thread([this]{std::unique_lock<std::mutex> lock(mutex);while(!wake.wait_for(lock,std::chrono::seconds(10),[this]{return stopping;})){for(auto it=jobs.begin();it!=jobs.end();)if(it->second->expires<time(nullptr)&&it->second->state!="preparing"&&it->second->state!="applying")it=jobs.erase(it);else ++it;}});}
 ~Transfers(){{std::lock_guard<std::mutex> lock(mutex);stopping=true;}wake.notify_all();janitor.join();for(auto& pair:jobs)if(pair.second->worker.joinable())pair.second->worker.join();}
 std::shared_ptr<Job> get(const std::string& id,const std::string& owner){auto it=jobs.find(id);need(it!=jobs.end()&&it->second->owner==owner,404,"transfer_not_found");need(time(nullptr)<it->second->expires||it->second->state=="preparing"||it->second->state=="applying",410,"transfer_expired");return it->second;}
 void finish(const std::shared_ptr<Job>& j,const std::string& state){std::lock_guard<std::mutex> lock(mutex);j->state=state;}
 template<class F> json start(const std::string& owner,const std::string& kind,F work){
  std::lock_guard<std::mutex> lock(mutex);
  for(auto it=jobs.begin();it!=jobs.end();)if(it->second->expires<time(nullptr)&&it->second->state!="preparing"&&it->second->state!="applying")it=jobs.erase(it);else ++it;
  need(jobs.size()<16,409,"transfer_limit");size_t running=0;for(auto& pair:jobs)if(pair.second->state=="preparing"||pair.second->state=="applying")++running;need(running<2,409,"transfer_busy");
  auto j=std::make_shared<Job>();j->owner=owner;j->kind=kind;jobs[j->id]=j;
  j->worker=std::thread([this,j,work]{try{work(j);}catch(const Error& e){std::lock_guard<std::mutex> lock(mutex);j->error=e.what();j->state="failed";}catch(const std::exception& e){log_exception("warning","project.transfer","Project transfer failed",e,{{"transfer_id",j->id}});std::lock_guard<std::mutex> lock(mutex);j->error="transfer_failed";j->state="failed";}});return public_job(*j);
 }
 void export_project(const std::shared_ptr<Job>& j,const std::string& id){
  std::unique_lock<std::shared_mutex> gate(storage->project_gate);storage->initialize();ProjectStore projects(DatabaseConfig::from_environment());auto root=projects.get(id);need(bool(root),404,"project_not_found");j->drive=(*root)["drive_id"];storage->require_drive_online(j->drive);auto all=store.snapshot(j->drive);json m={{"format","spark-project"},{"version",1},{"root",id}};for(auto k:{"projects","boards","columns","tasks","files","directories"})m[k]=json::array();std::set<std::string> pids{id},bids;std::string base=(*root)["directory"];
  std::vector<std::string> ordered{id};for(size_t i=0;i<ordered.size();++i){need(ordered.size()<=10000,413,"manifest_limit");for(auto& p:all["projects"])if(p["id"]==ordered[i]){auto r=p;r["parent_id"]=i==0?json(nullptr):p["parent_id"];auto dir=p["directory"].get<std::string>();need(inside(dir,base),409,"invalid_project_directory");r["directory"]=dir==base?"":dir.substr(base.size()+1);m["projects"].push_back(r);}for(auto& p:all["projects"])if(p["parent_id"]==ordered[i]&&pids.insert(p["id"]).second)ordered.push_back(p["id"]);}
  for(auto& b:all["boards"])if(pids.count(b["project_id"])){m["boards"].push_back(b);bids.insert(b["id"]);}
  for(auto k:{"columns","tasks"})for(auto& r:all[k])if(bids.count(r["board_id"]))m[k].push_back(r);
  for(auto& dir:all["directories"]){auto s=dir.get<std::string>();if(inside(s,base))m["directories"].push_back(s==base?"":s.substr(base.size()+1));}
  for(auto& f:all["files"]){auto dir=f["directory"].get<std::string>();if(!inside(dir,base))continue;auto path=storage->content_path(f);need(fs::is_regular_file(fs::symlink_status(path)),409,"source_unavailable");m["files"].push_back({{"id",f["id"]},{"path",join(dir==base?"":dir.substr(base.size()+1),f["name"]).substr(dir==base?1:0)},{"size_bytes",f["size_bytes"]},{"sha256",hash_file(path)}});}
  validate(m);auto manifest=m.dump(2);need(manifest.size()<=manifest_limit,413,"manifest_limit");auto output=j->stage/"project.zip";Archive zip(true);need(archive_write_set_format_zip(zip.a)==ARCHIVE_OK&&archive_write_set_options(zip.a,"zip:compression=deflate")==ARCHIVE_OK&&archive_write_open_filename(zip.a,output.c_str())==ARCHIVE_OK,503,"archive_unavailable");
  auto add=[&](const std::string& name,uint64_t size,const fs::path& path,const std::string* bytes){auto e=archive_entry_new();archive_entry_set_pathname(e,name.c_str());archive_entry_set_filetype(e,AE_IFREG);archive_entry_set_perm(e,0600);archive_entry_set_size(e,size);int rc=archive_write_header(zip.a,e);archive_entry_free(e);need(rc==ARCHIVE_OK,503,"archive_write_failed");if(bytes)need(archive_write_data(zip.a,bytes->data(),bytes->size())==static_cast<la_ssize_t>(bytes->size()),503,"archive_write_failed");else{std::ifstream in(path,std::ios::binary);std::array<char,65536>b;uint64_t total=0;while(in){in.read(b.data(),b.size());size_t n=in.gcount();need(archive_write_data(zip.a,b.data(),n)==static_cast<la_ssize_t>(n),503,"archive_write_failed");total+=n;j->bytes+=n;}need(in.eof()&&total==size,409,"source_changed");}need(archive_write_finish_entry(zip.a)==ARCHIVE_OK,503,"archive_write_failed");};
  add("manifest.json",manifest.size(),{},&manifest);for(auto& f:m["files"]){auto original=std::find_if(all["files"].begin(),all["files"].end(),[&](const json& r){return r["id"]==f["id"];});add("data/"+f["id"].get<std::string>(),f["size_bytes"],storage->content_path(*original),nullptr);}need(archive_write_close(zip.a)==ARCHIVE_OK,503,"archive_write_failed");
  std::string letter;auto drives=storage->drives();for(auto& d:drives["drives"])if(d["persistent"]==true)letter=d["letter"];auto upload=storage->begin_upload("project-"+id+"-"+j->id+".zip","/project-exports",fs::file_size(output),letter);
  bool publishing=false;try{fs::copy_file(output,storage->path(upload,true),fs::copy_options::overwrite_existing);fs::permissions(storage->path(upload,true),fs::perms::owner_read|fs::perms::owner_write);sync_file(storage->path(upload,true));fs::rename(storage->path(upload,true),storage->path(upload));storage->sync_directory();publishing=true;auto file=storage->store->publish(upload,fs::file_size(output));{std::lock_guard<std::mutex> lock(mutex);j->result={{"file",file}};std::error_code cleanup;fs::remove(output,cleanup);j->state="complete";j->expires=time(nullptr)+3600;}}catch(...){if(!publishing){storage->erase_bytes(upload);storage->store->forget(upload);}throw;}
 }
 void unpack(const std::shared_ptr<Job>& j,const fs::path& input){
  Archive zip(false);archive_read_support_format_zip(zip.a);need(archive_read_open_filename(zip.a,input.c_str(),65536)==ARCHIVE_OK,400,"invalid_zip");std::set<std::string> names;uint64_t total=0;auto available=fs::space(j->stage).available;need(available>space_reserve,507,"insufficient_staging_space");const uint64_t budget=available-space_reserve;archive_entry* e;int rc;
  while((rc=archive_read_next_header(zip.a,&e))==ARCHIVE_OK){const char* raw=archive_entry_pathname(e);need(raw,400,"invalid_zip_entry");std::string name=raw;need((name=="manifest.json"||(name.rfind("data/",0)==0&&uuid(name.substr(5))))&&names.insert(name).second,400,"unsafe_zip_entry");need(archive_entry_filetype(e)==AE_IFREG&&!archive_entry_symlink(e)&&!archive_entry_hardlink(e)&&archive_entry_is_encrypted(e)==0,400,"unsafe_zip_entry");need(names.size()<=10001,413,"archive_entry_limit");auto path=j->stage/(name=="manifest.json"?name:name.substr(5));std::ofstream out(path,std::ios::binary);need(bool(out),503,"staging_unavailable");std::array<char,65536>b;la_ssize_t n;uint64_t size=0;while((n=archive_read_data(zip.a,b.data(),b.size()))>0){need(uint64_t(n)<=budget-total,507,"insufficient_staging_space");size+=n;total+=n;need(name!="manifest.json"||size<=manifest_limit,413,"manifest_limit");if(total%(16*1024*1024)<uint64_t(n))need(fs::space(j->stage).available>space_reserve,507,"insufficient_staging_space");out.write(b.data(),n);need(bool(out),503,"staging_unavailable");j->bytes+=n;}need(n==0,400,"invalid_zip_data");out.close();}
  need(rc==ARCHIVE_EOF&&names.count("manifest.json"),400,"invalid_zip");j->manifest=json::parse(read_text(j->stage/"manifest.json",manifest_limit),nullptr,false);validate(j->manifest);need(names.size()==j->manifest["files"].size()+1,400,"unexpected_zip_entries");for(auto& f:j->manifest["files"]){auto id=f["id"].get<std::string>();need(names.count("data/"+id)&&fs::file_size(j->stage/id)==f["size_bytes"].get<uint64_t>()&&hash_file(j->stage/id)==f["sha256"],400,"file_integrity_mismatch");}
 }
 void prepare(const std::shared_ptr<Job>& j,const json& packet){
  std::unique_lock<std::shared_mutex> gate(storage->project_gate);storage->initialize();j->drive=storage->resolve_drive(packet.at("drive"));storage->require_drive_online(j->drive);auto source=storage->store->get(packet.at("file_id"));need(bool(source),404,"file_not_found");storage->require_drive_online((*source)["drive_id"]);unpack(j,storage->content_path(*source));j->expected=store.snapshot(j->drive);const auto& m=j->manifest;
  std::map<std::string,std::string> mapping,parents;std::map<std::string,std::string> dirs;std::set<std::string> used;std::string rootdir;
  for(const auto& kind:std::vector<std::string>{"projects","boards","columns","tasks"})for(const auto& incoming:m[kind]){
   const auto sourceid=incoming["id"].get<std::string>();auto data=fields(incoming,editable.at(kind));std::string parentkey;json parent=nullptr;bool root=kind=="projects"&&incoming["id"]==m["root"];
   if(kind=="projects"){if(!root){parent=mapping.at(incoming["parent_id"]);parentkey="projects:"+incoming["parent_id"].get<std::string>();}data["parent_id"]=parent;}
   else {std::string key=kind=="boards"?"project_id":"board_id";parent=mapping.at(incoming[key]);data[key]=parent;parentkey=(kind=="boards"?"projects:":"boards:")+incoming[key].get<std::string>();if(kind=="tasks"){data["column_id"]=mapping.at(incoming["column_id"]);parentkey="columns:"+incoming["column_id"].get<std::string>();}}
   json local=nullptr;auto scope=[&](const json& row){if(kind=="projects")return row["parent_id"]==parent;return row[kind=="boards"?"project_id":"board_id"]==parent;};
   if(root&&packet.contains("target_project_id")){for(auto& row:j->expected[kind])if(row["id"]==packet["target_project_id"])local=row;need(!local.is_null(),404,"target_project_not_found");}
   else {for(auto& row:j->expected[kind])if(row["id"]==sourceid){need(scope(row)||root,409,"identity_scope_conflict");local=row;}if(local.is_null())for(auto& row:j->expected[kind])if(scope(row)&&row["name"]==incoming["name"]){need(local.is_null(),409,"ambiguous_name_match");local=row;}}
   std::string id=local.is_null()?sourceid:local["id"].get<std::string>();need(used.insert(id).second,409,"ambiguous_identity_match");mapping[sourceid]=id;
   if(kind=="projects"){
    if(root){rootdir=local.is_null()?"/projects/"+data["name"].get<std::string>():local["directory"].get<std::string>();data["parent_id"]=local.is_null()?json(nullptr):local["parent_id"];}
    std::string dir=rootdir;
    if(!root&&local.is_null()){auto parentSource=std::find_if(m["projects"].begin(),m["projects"].end(),[&](const json& p){return p["id"]==incoming["parent_id"];});std::string prefix=(*parentSource)["directory"],relativeDir=incoming["directory"];dir=join(dirs.at(prefix),prefix.empty()?relativeDir:relativeDir.substr(prefix.size()+1));}
    if(!local.is_null())dir=local["directory"];need(dir.size()<=1024,400,"directory_too_long");data["directory"]=dir;dirs[incoming["directory"]]=dir;
    for(auto& p:j->expected["projects"])need(p["id"]==id||p["directory"]!=dir,409,"project_directory_conflict");
   }
   bool same=!local.is_null()&&fields(local,editable.at(kind))==fields(data,editable.at(kind));
   json row={{"key",kind+":"+sourceid},{"kind",kind},{"source_id",sourceid},{"id",id},{"parent",parentkey},{"label",incoming["name"]},{"exists",!local.is_null()},{"conflict",!local.is_null()&&!same},{"default_action",local.is_null()?"import":"keep"},{"incoming",fields(incoming,editable.at(kind))},{"local",local.is_null()?json(nullptr):fields(local,editable.at(kind))},{"data",data},{"mergeable",false}};j->plan.push_back(row);
  }
  auto mapped_directory=[&](const std::string& path){std::string best="",dest=rootdir;for(const auto& [prefix,target]:dirs)if((prefix.empty()||inside(path,prefix))&&prefix.size()>=best.size()){best=prefix;dest=target;}return join(dest,best.empty()?path:path==best?"":path.substr(best.size()+1));};
  for(const auto& dir:m["directories"]){auto mapped=mapped_directory(dir);need(mapped.size()<=1024,400,"directory_too_long");j->directories.push_back(mapped);}
  for(const auto& incoming:m["files"]){auto rel=incoming["path"].get<std::string>();auto slash=rel.find_last_of('/');std::string dir=slash==std::string::npos?"":rel.substr(0,slash),name=slash==std::string::npos?rel:rel.substr(slash+1);auto destination=mapped_directory(dir);need(destination.size()<=1024,400,"directory_too_long");json local=nullptr;for(auto& f:j->expected["files"])if(f["directory"]==destination&&f["name"]==name){need(local.is_null(),409,"duplicate_file_path");local=f;}
   std::string sourceid=incoming["id"],id=local.is_null()?sourceid:local["id"].get<std::string>();need(used.insert(id).second,409,"ambiguous_identity_match");
   bool same=!local.is_null()&&local["size_bytes"]==incoming["size_bytes"]&&hash_file(storage->content_path(local))==incoming["sha256"];
   bool text=incoming["size_bytes"].get<uint64_t>()<=text_limit&&is_text(read_text(j->stage/sourceid,text_limit));if(!local.is_null())text=text&&local["size_bytes"].get<uint64_t>()<=text_limit&&is_text(read_text(storage->content_path(local),text_limit));
   std::string owner=m["root"],best="";for(auto& p:m["projects"]){std::string path=p["directory"];if((path.empty()||inside(dir,path))&&path.size()>=best.size()){best=path;owner=p["id"];}}
   j->plan.push_back({{"key","files:"+sourceid},{"kind","files"},{"source_id",sourceid},{"id",id},{"parent","projects:"+owner},{"label",rel},{"exists",!local.is_null()},{"conflict",!local.is_null()&&!same},{"default_action",local.is_null()?"import":"keep"},{"incoming",{{"size_bytes",incoming["size_bytes"]},{"sha256",incoming["sha256"]}}},{"local",local.is_null()?json(nullptr):json{{"size_bytes",local["size_bytes"]}}},{"local_record",local},{"data",{{"directory",destination},{"name",name},{"size_bytes",incoming["size_bytes"]}}},{"mergeable",text&&!local.is_null()&&!same}});
  }
  {std::lock_guard<std::mutex> lock(mutex);j->result={{"project_id",mapping.at(m["root"])},{"directory",rootdir},{"drive",packet["drive"]}};j->state="review";j->expires=time(nullptr)+3600;}
 }
 json text(const std::shared_ptr<Job>& j,const std::string& key){
  const auto it=std::find_if(j->plan.begin(),j->plan.end(),[&](const json& e){return e["key"]==key;});need(it!=j->plan.end()&&(*it)["mergeable"]==true,400,"text_merge_unavailable");std::unique_lock<std::shared_mutex> gate(storage->project_gate);need(store.snapshot(j->drive)==j->expected,409,"import_target_changed");return {{"local",read_text(storage->content_path((*it)["local_record"]),text_limit)},{"incoming",read_text(j->stage/(*it)["source_id"].get<std::string>(),text_limit)}};
 }
 void apply(const std::shared_ptr<Job>& j,const json& resolutions){
  std::unique_lock<std::shared_mutex> gate(storage->project_gate);storage->require_drive_online(j->drive);need(store.snapshot(j->drive)==j->expected,409,"import_target_changed");
  std::set<std::string> keys,skipped;for(auto& e:j->plan)keys.insert(e["key"]);for(auto& r:resolutions.items())need(keys.count(r.key()),400,"unknown_resolution");json ops=json::array(),dirs=json::array();std::vector<fs::path> candidates;bool committing=false;
  try{for(auto& e:j->plan){std::string key=e["key"],parent=e["parent"],kind=e["kind"],action=e["default_action"];json resolution=resolutions.value(key,json::object());need(resolution.is_object()&&resolution.size()<=2,400,"invalid_resolution");for(auto& field:resolution.items())need(field.key()=="action"||field.key()=="content",400,"invalid_resolution");if(resolution.contains("action")){need(resolution["action"].is_string(),400,"invalid_resolution");action=resolution["action"];}
   need(action=="keep"||action=="import"||action=="discard"||action=="merge",400,"invalid_resolution");if(skipped.count(parent)||action=="discard"||(action=="keep"&&e["exists"]==false)){skipped.insert(key);continue;}if(action=="keep")continue;auto data=e["data"];
   if(kind=="files"){
    fs::path source=j->stage/e["source_id"].get<std::string>();if(action=="merge"){need(e["mergeable"]==true&&resolution.contains("content")&&resolution["content"].is_string(),400,"text_merge_unavailable");auto content=resolution["content"].get<std::string>();need(content.size()<=text_limit&&is_text(content),400,"invalid_merge_text");source=j->stage/(e["source_id"].get<std::string>()+".merged");std::ofstream out(source,std::ios::binary);out.write(content.data(),content.size());out.close();need(bool(out),503,"staging_unavailable");}
    std::string revision=random_id();auto candidate=storage->content_path({{"id",e["id"]},{"drive_id",j->drive},{"content_revision",revision}});candidates.push_back(candidate);fs::copy_file(source,candidate);fs::permissions(candidate,fs::perms::owner_read|fs::perms::owner_write);sync_file(candidate);data["content_revision"]=revision;data["size_bytes"]=fs::file_size(candidate);
   }else need(action!="merge",400,"text_merge_only");
   ops.push_back({{"kind",kind},{"id",e["id"]},{"create",!e["exists"].get<bool>()},{"data",data}});
  }
  // Do not create directories beneath explicitly discarded project subtrees.
  for(const auto& d:j->directories){bool discard=false;for(const auto& e:j->plan)if(e["kind"]=="projects"&&skipped.count(e["key"])&&inside(d,e["data"]["directory"]))discard=true;if(!discard)dirs.push_back(d);}
  storage->sync_directory();committing=true;store.apply(j->drive,j->expected,ops,dirs);
  }catch(...){if(!committing)for(const auto& p:candidates){std::error_code ignored;fs::remove(p,ignored);}throw;}
  // Publication succeeded. Unlink replaced revisions; an already-open descriptor stays valid.
  for(const auto& op:ops)if(op["kind"]=="files"&&op["create"]==false){for(const auto& e:j->plan)if(e["kind"]=="files"&&e["id"]==op["id"]&&!e["local_record"].is_null()){std::error_code ignored;fs::remove(storage->content_path(e["local_record"]),ignored);}}
  {std::error_code ignored;fs::remove_all(j->stage,ignored);}
  {std::lock_guard<std::mutex> lock(mutex);j->result["applied_items"]=ops.size();if(!j->plan[0]["exists"].get<bool>()&&skipped.count(j->plan[0]["key"]))j->result["project_id"]=nullptr;j->state="complete";j->expires=time(nullptr)+3600;}
 }
};
void reply(httplib::Response& r,int status,const json& j){r.status=status;r.set_content(j.dump(),"application/json");}
}
void configure_project_transfers(httplib::Server& server,std::shared_ptr<FileStorage> storage,std::shared_ptr<Authorization> auth){
 auto transfers=std::make_shared<Transfers>(storage);
 auto handler=[transfers,auth](const httplib::Request& req,httplib::Response& res){try{
  std::string owner="open";if(auth){auto identity=auth->identity(req,res);if(!identity)return;owner=identity->kind=="open"?"open":identity->credential_id;}
  if(req.path=="/projects/imports"||req.path.find("/export")!=std::string::npos){need(req.params.empty(),400,"invalid_parameters");need(req.get_header_value("Content-Type").rfind("application/json",0)==0,415,"unsupported_media_type");auto p=json::parse(req.body,nullptr,false);need(p.is_object(),400,"invalid_packet");
   if(req.path=="/projects/imports"){need(p.contains("file_id")&&p["file_id"].is_string()&&uuid(p["file_id"])&&p.contains("drive")&&p["drive"].is_string(),400,"invalid_parameters");for(auto& f:p.items())need(f.key()=="file_id"||f.key()=="drive"||f.key()=="target_project_id",400,"unknown_field");if(p.contains("target_project_id"))need(p["target_project_id"].is_string()&&uuid(p["target_project_id"]),400,"invalid_target_project");reply(res,202,transfers->start(owner,"import",[manager=transfers.get(),p](auto j){manager->prepare(j,p);}));}
   else {need(p.empty(),400,"unknown_field");auto id=req.matches[1].str();need(uuid(id),400,"invalid_project_id");reply(res,202,transfers->start(owner,"export",[manager=transfers.get(),id](auto j){manager->export_project(j,id);}));}return;
  }
  std::shared_ptr<Job> job;{std::lock_guard<std::mutex> lock(transfers->mutex);job=transfers->get(req.matches[1].str(),owner);
   if(req.method=="GET"&&!req.has_param("text")){need(req.params.empty(),400,"invalid_parameters");reply(res,200,public_job(*job));return;}
   if(req.method=="DELETE"){need(req.params.empty()&&req.body.empty(),400,"invalid_parameters");need(job->state!="preparing"&&job->state!="applying",409,"transfer_busy");transfers->jobs.erase(job->id);res.status=204;return;}
   need(job->state=="review",409,"import_not_reviewable");
   if(req.method=="GET"){need(req.params.size()==1&&req.get_param_value_count("text")==1,400,"invalid_parameters");}
   else {need(req.params.empty(),400,"invalid_parameters");need(req.get_header_value("Content-Type").rfind("application/json",0)==0,415,"unsupported_media_type");}
  }
  if(req.method=="GET"){reply(res,200,transfers->text(job,req.get_param_value("text")));return;}
  auto packet=json::parse(req.body,nullptr,false);need(packet.is_object()&&packet.size()==1&&packet.contains("resolutions")&&packet["resolutions"].is_object(),400,"invalid_packet");
  {std::lock_guard<std::mutex> lock(transfers->mutex);need(transfers->get(job->id,owner)==job&&job->state=="review",409,"import_not_reviewable");job->state="applying";}
  // Applying is synchronous; the status endpoint remains available in another request.
  try{transfers->apply(job,packet["resolutions"]);}catch(...){std::lock_guard<std::mutex> lock(transfers->mutex);job->state="failed";job->error="import_failed_refresh_review";throw;}
  {std::lock_guard<std::mutex> lock(transfers->mutex);reply(res,200,public_job(*job));}
 }catch(const Error& e){reply(res,e.status,{{"error",e.what()}});}catch(const std::invalid_argument& e){reply(res,409,{{"error",e.what()}});}catch(const json::exception&){reply(res,400,{{"error","invalid_packet"}});}catch(const DatabaseError&){reply(res,503,{{"error","database_unavailable"}});}catch(const std::exception&){reply(res,503,{{"error","transfer_unavailable"}});}};
 body_route(server,"POST",R"(/projects/([^/]+)/export)",handler,auth);
 body_route(server,"POST","/projects/imports",handler,auth);
 server.Get(R"(/projects/transfers/([0-9a-f]{32}))",handler);
 body_route(server,"POST",R"(/projects/transfers/([0-9a-f]{32})/apply)",handler,auth,16*1024*1024);
 body_route(server,"DELETE",R"(/projects/transfers/([0-9a-f]{32}))",handler,auth);
}
json project_transfer_endpoints(){json endpoints=json::array();for(const auto& row:std::vector<std::vector<std::string>>{
 {"POST","/projects/{id}/export","Create a portable project ZIP in /project-exports on persistent storage. Includes descendants, boards, columns, task severity, empty directories and all files below the project directory. Returns a background job; poll until complete for result.file. Requires projects.read, boards.read, files.read, files.write."},
 {"POST","/projects/imports","Prepare a review from an existing uploaded ZIP file_id on an online target drive letter. Optional target_project_id explicitly selects an existing project. Otherwise match root ID then root name on that drive. No changes until apply. Upload the ZIP through POST /files first."},
 {"GET","/projects/transfers/{id}","Poll preparation/apply state and review entries. Optional text={entry.key} returns local and incoming UTF-8 text for mergeable file conflicts (up to 1 MiB each). Jobs belong to the initiating credential, expire after one hour, and do not survive restart."},
 {"POST","/projects/transfers/{id}/apply","Apply reviewed resolutions atomically. resolutions maps entry keys to {action: keep|import|discard|merge, content?: string}. Defaults keep existing items and import new items. Discard skips incoming item and descendants, never deletes local items. Merge only for text-file conflicts; content is the final edited UTF-8 text. Target changes since review cause 409 import_target_changed; prepare a fresh review. Local-only items are retained."},
 {"DELETE","/projects/transfers/{id}","Dismiss a finished or reviewed transfer and delete staging bytes. Does not delete the uploaded ZIP or completed export. Active jobs cannot be dismissed."}}){auto method=row[0],path=row[1];json body=nullptr;if(method=="POST")body=path=="/projects/imports"?json{{"file_id","uploaded-file-uuid"},{"drive","A"}}:path.find("apply")!=std::string::npos?json{{"resolutions",json::object()}}:json::object();endpoints.push_back({{"method",method},{"path",path},{"description",row[2]},{"input",{{"body",body},{"headers",method=="POST"?json{{"Content-Type","application/json"}}:json::object()}}},{"output",{{"status",method=="DELETE"?204:method=="POST"&&path.find("apply")==std::string::npos?202:200},{"body",method=="DELETE"?json(nullptr):json{{"id","transfer-id"},{"state","preparing|review|applying|complete|failed"},{"processed_bytes",0},{"expires_at",1790000000},{"result",json::object()}}}}},{"errors",{{"400","invalid_packet, unsafe_zip_entry, invalid_manifest, invalid_resolution, text_merge_only"},{"403","forbidden"},{"404","transfer_not_found"},{"409","import_target_changed, transfer_busy, duplicate_file_path, import_not_reviewable"},{"410","transfer_expired"},{"413","manifest_limit (8 MiB / 10000 items), text_too_large (1 MiB)"},{"503","transfer_unavailable, database_unavailable"},{"507","insufficient_staging_space"}}}});}return endpoints;}
}
