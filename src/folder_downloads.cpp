#include "folder_downloads.hpp"
#include "zip_stream.hpp"
#include "bounded_body.hpp"
#include <openssl/rand.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <fstream>
#include <set>
#include <thread>
namespace spark {
namespace {
using nlohmann::json;namespace fs=std::filesystem;
struct Error:std::runtime_error{int status;Error(int s,const char* e):std::runtime_error(e),status(s){}};
void need(bool b,int s,const char* e){if(!b)throw Error(s,e);}
struct FD{int fd;explicit FD(const fs::path& p):fd(open(p.c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW)){need(fd>=0,409,"source_unavailable");}~FD(){close(fd);}};
json stamp(int fd){struct stat s{};need(fstat(fd,&s)==0&&S_ISREG(s.st_mode)&&s.st_size>=0,409,"source_unavailable");return json::array({s.st_dev,s.st_ino,s.st_size,s.st_mtim.tv_sec,s.st_mtim.tv_nsec,s.st_ctim.tv_sec,s.st_ctim.tv_nsec});}
std::string id(){unsigned char b[16];need(RAND_bytes(b,sizeof(b))==1,503,"random_unavailable");std::string out;for(auto c:b){out+="0123456789abcdef"[c>>4];out+="0123456789abcdef"[c&15];}return out;}
void save(const fs::path& path,const json& value){auto temp=path.string()+".tmp";int fd=open(temp.c_str(),O_WRONLY|O_CREAT|O_TRUNC|O_CLOEXEC|O_NOFOLLOW,0600);need(fd>=0,503,"download_storage_unavailable");auto data=value.dump();size_t offset=0;while(offset<data.size()){auto n=write(fd,data.data()+offset,data.size()-offset);if(n<0&&errno==EINTR)continue;if(n<=0){close(fd);throw Error(503,"download_storage_unavailable");}offset+=n;}auto synced=fsync(fd);close(fd);need(synced==0&&rename(temp.c_str(),path.c_str())==0,503,"download_storage_unavailable");int dir=open(path.parent_path().c_str(),O_RDONLY|O_DIRECTORY|O_CLOEXEC);if(dir>=0){fsync(dir);close(dir);}}
bool inside(const std::string& path,const std::string& root){return root=="/"||path==root||path.rfind(root+"/",0)==0;}
bool safe_path(const std::string& path){if(path.empty()||path[0]!='/'||path.size()>4096||path.find('\\')!=std::string::npos)return false;if(path!="/"&&path.back()=='/')return false;for(auto& part:fs::path(path))if(part=="."||part=="..")return false;return path.find("//")==std::string::npos&&std::none_of(path.begin(),path.end(),[](unsigned char c){return c<32||c==127;});}
json public_job(json job){job.erase("entries");job.erase("owner");return job;}
struct Downloads {
    std::shared_ptr<FileStorage> storage;std::mutex mutex;std::thread worker;std::atomic<bool> stop{false};fs::path root;json jobs=json::object();
    explicit Downloads(std::shared_ptr<FileStorage> s):storage(std::move(s)){
        storage->initialize();auto drives=storage->drives();for(auto& d:drives["drives"])if(d["persistent"]==true)root=fs::path(d["path"].get<std::string>())/".spark-control";
        need(!root.empty(),503,"download_storage_unavailable");
        auto secure=[](const fs::path& p){if(mkdir(p.c_str(),0700)&&errno!=EEXIST)throw Error(503,"download_storage_unavailable");struct stat st{};need(lstat(p.c_str(),&st)==0&&S_ISDIR(st.st_mode)&&st.st_uid==geteuid()&&!(st.st_mode&077),503,"download_storage_unavailable");};
        secure(root);root/="downloads";secure(root);
        for(auto& f:fs::directory_iterator(root)){if(f.path().extension()!=".json")continue;need(fs::is_regular_file(f.symlink_status()),503,"download_storage_unavailable");std::ifstream in(f.path());json j;in>>j;if(j.at("expires_at").get<int64_t>()<=time(nullptr)){fs::remove(f.path());continue;}if(j["state"]=="preparing"){j["state"]="interrupted";j["error"]="server_restarted_prepare_again";save(f.path(),j);}auto key=j.at("id").get<std::string>();jobs[key]=std::move(j);}
    }
    ~Downloads(){stop=true;if(worker.joinable())worker.join();}
    void persist(const json& j){save(root/(j.at("id").get<std::string>()+".json"),j);}
    void update(const std::string& key,const json& patch){std::lock_guard<std::mutex> lock(mutex);jobs[key].update(patch);persist(jobs[key]);}
    json get(const std::string& key,const std::string& owner){std::lock_guard<std::mutex> lock(mutex);need(jobs.contains(key)&&jobs[key]["owner"]==owner,404,"download_not_found");need(jobs[key]["expires_at"].get<int64_t>()>time(nullptr),410,"download_expired");return jobs[key];}
    std::shared_ptr<FD> source(const json& entry){auto current=storage->store->get(entry["record"]["id"]);need(current.has_value(),409,"source_changed");for(auto field:{"name","directory","content_revision","drive_id","size_bytes"})need((*current)[field]==entry["record"][field],409,"source_changed");auto fd=std::make_shared<FD>(storage->content_path(*current));need(stamp(fd->fd)==entry["stamp"],409,"source_changed");return fd;}
    std::vector<ZipEntry> zip_entries(const json& j){std::vector<ZipEntry> out;for(auto& e:j["entries"])out.push_back({e["name"],e["size"],e["crc"]});return out;}
    void prepare(std::string key){try{
        json j;{std::lock_guard<std::mutex> lock(mutex);j=jobs[key];}
        std::shared_lock<std::shared_mutex> gate(storage->project_gate);auto drive=j["drive_id"].get<std::string>(),directory=j["directory"].get<std::string>();storage->require_drive_online(drive);
        auto dirs=storage->store->directories(drive)["directories"];need(std::find(dirs.begin(),dirs.end(),directory)!=dirs.end(),404,"directory_not_found");
        json entries=json::array();std::set<std::string> names;auto prefix=directory=="/"?1:directory.size()+1;
        for(auto& d:dirs){auto path=d.get<std::string>();if(path!=directory&&inside(path,directory)){auto name=path.substr(prefix)+"/";need(entries.size()<100000,413,"too_many_archive_entries");names.insert(name);entries.push_back({{"name",name},{"size",0},{"crc",0}});}}
        uint64_t total=0;
        for(int offset=0;;offset+=1000){need(!stop,409,"preparation_interrupted");auto page=storage->store->list(1000,offset,std::nullopt,drive);for(auto& record:page["files"]){auto dir=record["directory"].get<std::string>();if(!inside(dir,directory))continue;auto name=(dir==directory?"":dir.substr(prefix)+"/")+record["name"].get<std::string>();need(name.find('\\')==std::string::npos&&safe_path("/"+name),409,"unsafe_archive_name");need(names.insert(name).second&&!names.count(name+"/"),409,"duplicate_archive_name");need(entries.size()<100000,413,"too_many_archive_entries");auto file=std::make_shared<FD>(storage->content_path(record));auto signature=stamp(file->fd);auto size=record["size_bytes"].get<uint64_t>();need(signature[2]==size,409,"source_changed");need(size<=uint64_t(INT64_MAX)-total,413,"archive_too_large");total+=size;entries.push_back({{"name",name},{"size",size},{"crc",0},{"record",record},{"stamp",signature}});}if(!page["has_more"].get<bool>())break;}
        std::sort(entries.begin(),entries.end(),[](const json& a,const json& b){return a["name"].get<std::string>()<b["name"].get<std::string>();});
        update(key,{{"total_bytes",total},{"entry_count",entries.size()}});uint64_t done=0;auto last=std::chrono::steady_clock::now();std::array<char,262144> buffer{};
        for(auto& e:entries){if(!e.contains("record"))continue;auto file=source(e);uint32_t crc=0;uint64_t offset=0,size=e["size"];while(offset<size){need(!stop,409,"preparation_interrupted");auto n=pread(file->fd,buffer.data(),std::min<uint64_t>(buffer.size(),size-offset),off_t(offset));if(n<0&&errno==EINTR)continue;need(n>0,409,"source_unavailable");crc=zip_crc(crc,buffer.data(),size_t(n));offset+=n;done+=n;if(std::chrono::steady_clock::now()-last>std::chrono::seconds(1)){update(key,{{"prepared_bytes",done}});last=std::chrono::steady_clock::now();}}need(stamp(file->fd)==e["stamp"],409,"source_changed");e["crc"]=crc;}
        for(auto& e:entries)if(e.contains("record"))source(e);
        j["entries"]=entries;ZipLayout layout(zip_entries(j));update(key,{{"entries",entries},{"state","ready"},{"prepared_bytes",done},{"archive_bytes",layout.size}});
    }catch(const std::exception& e){try{update(key,{{"state",stop?"interrupted":"failed"},{"error",e.what()}});}catch(...) {}}}
    json create(const std::string& owner,const json& p){
        std::lock_guard<std::mutex> lock(mutex);for(auto it=jobs.begin();it!=jobs.end();)if(it.value()["expires_at"].get<int64_t>()<=time(nullptr)&&it.value()["state"]!="preparing"){fs::remove(root/(it.key()+".json"));it=jobs.erase(it);}else ++it;
        for(auto& j:jobs.items())need(j.value()["state"]!="preparing",409,"download_preparation_busy");need(jobs.size()<64,409,"download_session_limit");if(worker.joinable())worker.join();
        auto key=id();json j={{"id",key},{"owner",owner},{"state","preparing"},{"drive_id",storage->resolve_drive(p["drive"])},{"drive",p["drive"]},{"directory",p["directory"]},{"created_at",time(nullptr)},{"expires_at",time(nullptr)+86400},{"prepared_bytes",0},{"total_bytes",0},{"download_url","/files/downloads/"+key+"/content"}};
        persist(j);jobs[key]=j;try{worker=std::thread([this,key]{prepare(key);});}catch(...){jobs[key]["state"]="failed";jobs[key]["error"]="preparation_unavailable";persist(jobs[key]);throw;}return public_job(j);
    }
};
struct Lazy {std::mutex mutex;std::shared_ptr<Downloads> downloads;};
void reply(httplib::Response& res,int status,const json& j){res.status=status;res.set_content(j.dump(),"application/json");}
}
void configure_folder_downloads(httplib::Server& server,std::shared_ptr<FileStorage> storage,std::shared_ptr<Authorization> auth){
    auto lazy=std::make_shared<Lazy>();
    auto handle=[storage,auth,lazy](const httplib::Request& req,httplib::Response& res){try{
        std::string owner="open";if(auth){auto who=auth->identity(req,res);if(!who)return;owner=who->kind=="open"?"open":who->credential_id;}
        std::shared_lock<std::shared_mutex> gate(storage->project_gate);std::shared_ptr<Downloads> downloads;{std::lock_guard<std::mutex> lock(lazy->mutex);if(!lazy->downloads)lazy->downloads=std::make_shared<Downloads>(storage);downloads=lazy->downloads;}
        if(req.method=="POST"){need(req.get_header_value("Content-Type").rfind("application/json",0)==0,415,"unsupported_media_type");auto p=json::parse(req.body,nullptr,false);need(p.is_object()&&p.size()==2&&p.contains("drive")&&p["drive"].is_string()&&p.contains("directory")&&p["directory"].is_string()&&safe_path(p["directory"]),400,"invalid_parameters");reply(res,202,downloads->create(owner,p));return;}
        auto key=req.matches[1].str();auto job=downloads->get(key,owner);
        if(req.method=="DELETE"){std::lock_guard<std::mutex> lock(downloads->mutex);need(downloads->jobs[key]["state"]!="preparing",409,"download_preparation_busy");fs::remove(downloads->root/(key+".json"));downloads->jobs.erase(key);res.status=204;return;}
        if(req.path.size()<8||req.path.substr(req.path.size()-8)!="/content"){reply(res,200,public_job(job));return;}
        need(job["state"]=="ready",409,"download_not_ready");for(auto& e:job["entries"])if(e.contains("record"))downloads->source(e);
        auto layout=std::make_shared<ZipLayout>(downloads->zip_entries(job));auto etag="\"spark-zip-"+key+"\"";
        // The library applies byte ranges to providers. If-Range mismatch must serve the full representation.
        if(req.has_header("If-Range")&&req.get_header_value("If-Range")!=etag)const_cast<httplib::Request&>(req).ranges.clear();
        res.set_header("ETag",etag);res.set_header("Accept-Ranges","bytes");res.set_header("Content-Disposition","attachment; filename=\"spark-folder-"+key+".zip\"");res.set_header("Cache-Control","private, no-store, no-transform");res.set_header("X-Content-Type-Options","nosniff");
        auto transfer=std::make_shared<std::shared_lock<std::shared_mutex>>(std::move(gate));
        struct Cursor {int index=-1;std::shared_ptr<FD> fd;};auto cursor=std::make_shared<Cursor>();
        res.set_content_provider(size_t(layout->size),"application/zip",[layout,job,downloads,transfer,cursor](size_t offset,size_t length,httplib::DataSink& sink){try{
            auto it=std::upper_bound(layout->parts.begin(),layout->parts.end(),uint64_t(offset),[](uint64_t n,const ZipPart& p){return n<p.offset;});if(it==layout->parts.begin())return false;--it;auto relative=offset-it->offset;auto count=std::min<uint64_t>({length,it->size-relative,65536});if(!count)return false;
            if(it->file<0)return sink.write(it->bytes.data()+relative,size_t(count));
            const auto& entry=job["entries"][it->file];if(cursor->index!=it->file){cursor->fd=downloads->source(entry);cursor->index=it->file;}need(stamp(cursor->fd->fd)==entry["stamp"],409,"source_changed");std::array<char,65536> b{};ssize_t n;do{n=pread(cursor->fd->fd,b.data(),size_t(count),off_t(relative));}while(n<0&&errno==EINTR);return n>0&&stamp(cursor->fd->fd)==entry["stamp"]&&sink.write(b.data(),size_t(n));
        }catch(...){return false;}});
    }catch(const Error& e){reply(res,e.status,{{"error",e.what()}});}catch(const std::invalid_argument& e){reply(res,409,{{"error",e.what()}});}catch(const std::exception&){reply(res,503,{{"error","download_unavailable"}});}};
    body_route(server,"POST","/files/downloads",handle,auth);
    server.Get(R"(/files/downloads/([0-9a-f]{32}))",handle);server.Get(R"(/files/downloads/([0-9a-f]{32})/content)",handle);
    body_route(server,"DELETE",R"(/files/downloads/([0-9a-f]{32}))",handle,auth);
}
nlohmann::json folder_download_endpoints(){json out=json::array();for(auto method:{"POST","GET","CONTENT","DELETE"}){std::string kind=method,path=kind=="POST"?"/files/downloads":kind=="CONTENT"?"/files/downloads/{id}/content":"/files/downloads/{id}";out.push_back({{"method",kind=="CONTENT"?"GET":kind},{"path",path},{"description",kind=="POST"?"Prepare a recursive, uncompressed ZIP64 download. Background CRC32 scan; poll status until ready. Session is bound to the credential and expires after 24 hours. Duplicate archive paths are rejected. Requires files.read.":kind=="CONTENT"?"Stream the prepared ZIP. Supports HEAD, Range and strong ETag/If-Range. Source changes return 409 or abort an active stream. Resume using the same URL and authentication before expiry.":kind=="DELETE"?"Discard a finished download session, retaining source files. Preparing sessions return 409.":"Read preparation state, byte progress, expiry and stable download URL. Ready sessions survive server restarts; interrupted preparation must be started again."},{"input",{{"body",kind=="POST"?json{{"drive","A"},{"directory","/backups"}}:json(nullptr)}}},{"output",{{"status",kind=="POST"?202:kind=="DELETE"?204:200},{"body",kind=="CONTENT"?json("ZIP64 bytes"):kind=="DELETE"?json(nullptr):json{{"id","identifier"},{"state","ready"},{"prepared_bytes",123},{"total_bytes",123},{"expires_at",1790000000},{"download_url","/files/downloads/{id}/content"}}}}},{"errors",{{"403","forbidden"},{"404","download_not_found"},{"409","source_changed, download_not_ready, download_preparation_busy, download_session_limit"},{"410","download_expired"},{"416","unsatisfiable range"}}}});}return out;}
}
