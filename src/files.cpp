#include "files.hpp"
#include "file_explorer.hpp"
#include "bounded_body.hpp"
#include <sys/file.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <unistd.h>
#include <array>
#include <charconv>
#include <cerrno>
#include <cstdlib>
#include <limits>
#include <regex>
#include <iostream>
#include <random>
#include <algorithm>
#include <fstream>
namespace spark {
namespace {
using nlohmann::json;
struct StorageError : std::runtime_error {
    int status;
    explicit StorageError(int error):std::runtime_error(error==ENOSPC || error==EDQUOT?"insufficient_storage":"storage_unavailable"),status(error==ENOSPC || error==EDQUOT?507:503){}
};
struct Descriptor {
    int fd;
    explicit Descriptor(int value):fd(value){if(fd<0)throw StorageError(errno);}
    ~Descriptor(){close(fd);}
};
void sync_fd(int fd){if(fsync(fd))throw StorageError(errno);}
void reply(httplib::Response& response,int status,const json& body){response.status=status;response.set_content(body.dump(2)+"\n","application/json");}
void failure(httplib::Response& response,int status,const char* error){reply(response,status,{{"error",error}});}
bool valid_id(const std::string& id){return std::regex_match(id,std::regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"));}
bool valid_name(const std::string& name){
    if(name.empty() || name.size()>255 || name=="." || name=="..")return false;
    for(unsigned char c:name)if(c<32 || c==127 || c=='/' || c=='\\')return false;
    try {json(name).dump();}catch(const json::exception&){return false;}return true;
}
bool valid_directory(const std::string& directory){
    if(directory=="/")return true;
    if(directory.empty() || directory.size()>1024 || directory.front()!='/' || directory.back()=='/')return false;
    std::size_t start=1;
    for(std::size_t end=1;end<=directory.size();++end)if(end==directory.size() || directory[end]=='/'){
        if(!valid_name(directory.substr(start,end-start)))return false;start=end+1;
    }
    return true;
}
std::string encoded_name(const std::string& name){
    const char* hex="0123456789ABCDEF";std::string out;
    for(unsigned char c:name){if((c>='a'&&c<='z')||(c>='A'&&c<='Z')||(c>='0'&&c<='9')||c=='.'||c=='-'||c=='_')out+=c;else {out+='%';out+=hex[c>>4];out+=hex[c&15];}}
    return out;
}
int integer(const std::string& value,int low,int high){int parsed=0;auto result=std::from_chars(value.data(),value.data()+value.size(),parsed);return result.ec==std::errc{} && result.ptr==value.data()+value.size() && parsed>=low && parsed<=high?parsed:-1;}
}
FileStorage::FileStorage(std::shared_ptr<FileStore> metadata,std::filesystem::path root):FileStorage(std::move(metadata),std::vector<std::filesystem::path>{std::move(root)}){}
FileStorage::FileStorage(std::shared_ptr<FileStore> metadata,std::vector<std::filesystem::path> roots):store(std::move(metadata)){
    if(roots.empty())throw std::runtime_error("No storage drives configured");
    for(const auto& root:roots)drives_.push_back({std::filesystem::weakly_canonical(std::filesystem::absolute(root)),{}});
}
FileStorage::~FileStorage(){for(auto& drive:drives_){if(drive.lock>=0)close(drive.lock);if(drive.fd>=0)close(drive.fd);}}
std::filesystem::path FileStorage::default_root(){const char* value=std::getenv("SPARK_FILE_DIR");return value && *value?value:"storage/files";}
std::vector<std::filesystem::path> FileStorage::default_roots(){
    const char* value=std::getenv("SPARK_FILE_DRIVES");if(!value||!*value)return {default_root()};
    auto list=json::parse(value);if(!list.is_array()||list.empty())throw std::runtime_error("Invalid storage drive environment");
    std::vector<std::filesystem::path> roots;for(const auto& root:list)roots.emplace_back(root.get<std::string>());return roots;
}
std::filesystem::path FileStorage::drive_path(std::size_t index)const{
    const auto& drive=drives_.at(index);
    if(drive.fd<0 && drive.id.empty())return drive.root;
    if(drive.fd<0)throw StorageError(ENODEV); // Only used by legacy fixture/setup code before initialize.
    struct stat current{},held{};
    if(stat(drive.root.c_str(),&current)||fstat(drive.fd,&held)||current.st_dev!=held.st_dev||current.st_ino!=held.st_ino)throw StorageError(ENODEV);
    // Operations stay anchored to the opened filesystem even if a mount/path is
    // replaced between the identity check and open/rename/unlink.
    return std::filesystem::path("/proc/self/fd")/std::to_string(drive.fd);
}
std::size_t FileStorage::drive_index(const json& id)const{
    if(id.is_null())return 0;
    for(std::size_t i=0;i<drives_.size();++i)if(drives_[i].id==id)return i;
    throw StorageError(ENODEV);
}
std::filesystem::path FileStorage::path(const std::string& id,bool partial)const{
    const auto drive=store->location(id);
    return drive_path(drive_index(drive?json(*drive):json(nullptr)))/(id+(partial?".part":".blob"));
}
std::filesystem::path FileStorage::content_path(const json& record)const{
    const auto revision=record.at("content_revision").get<std::string>();
    return drive_path(drive_index(record.value("drive_id",json(nullptr))))/(record.at("id").get<std::string>()+(revision.empty()?"":"."+revision)+".blob");
}
void FileStorage::sync_directory(){for(std::size_t i=0;i<drives_.size();++i)if(online(i))sync_fd(drives_[i].fd);}
bool FileStorage::online(std::size_t i)const{if(drives_[i].fd<0)return false;try{drive_path(i);return true;}catch(...){return false;}}
void FileStorage::erase_bytes(const std::string& id){
    auto location=store->location(id);const auto root=drive_path(drive_index(location?json(*location):json(nullptr)));
    for(bool partial:{true,false})if(unlink((root/(id+(partial?".part":".blob"))).c_str())&&errno!=ENOENT)throw StorageError(errno);
    for(const auto& entry:std::filesystem::directory_iterator(root)){
        const auto name=entry.path().filename().string();
        if(std::regex_match(name,std::regex(id+R"(\.[0-9a-f]{32}\.blob)")))if(unlink(entry.path().c_str())&&errno!=ENOENT)throw StorageError(errno);
    }
    sync_directory();
}
std::string FileStorage::begin_upload(const std::string& name,const std::string& directory,std::optional<std::uint64_t> bytes,const std::optional<std::string>& letter){
    std::lock_guard<std::mutex> guard(allocation_);
    std::optional<std::string> selected=letter?resolve_drive(*letter):store->default_drive(directory);
    if(selected){bool found=false;for(std::size_t i=0;i<drives_.size();++i)if(drives_[i].id==*selected&&online(i))found=true;if(!found)throw StorageError(ENODEV);}
    for(std::size_t attempt=0;attempt<drives_.size();++attempt){
        const auto index=(next_drive_+attempt)%drives_.size();
        if(!online(index)||(selected&&drives_[index].id!=*selected))continue;
        std::error_code error;auto space=std::filesystem::space(drive_path(index),error);
        if(error)continue;
        if(space.available<bytes.value_or(1))continue;
        auto id=store->begin(name,directory,drives_[index].id);next_drive_=(index+1)%drives_.size();return id;
    }
    throw StorageError(ENOSPC);
}
void FileStorage::initialize(){
    std::call_once(initialized_,[this]{
        const auto registered=store->drives();
        // Explicit detach persists across restart, even if the old config lists it.
        drives_.erase(std::remove_if(drives_.begin(),drives_.end(),[&](const Drive& d){for(const auto& row:registered)if(row["path"]==d.root.string()&&row["attached"]==false)return true;return false;}),drives_.end());
        std::vector<std::string> configured_ids;for(const auto& d:drives_){std::ifstream marker(d.root/".spark-drive");std::string id;marker>>id;configured_ids.push_back(id);}
        for(const auto& row:registered)if(row["attached"]==true&&row["path"].is_string()&&std::find(configured_ids.begin(),configured_ids.end(),row["id"].get<std::string>())==configured_ids.end()){
            const auto root=std::filesystem::path(row["path"].get<std::string>());
            if(std::none_of(drives_.begin(),drives_.end(),[&](const Drive& d){return d.root==root;}))drives_.push_back({root,{}});
        }
        drives_.erase(std::remove_if(drives_.begin(),drives_.end(),[&](const Drive& d){
            std::ifstream marker(d.root/".spark-drive");std::string id;marker>>id;
            for(const auto& row:registered)if(row["id"]==id&&row["attached"]==false)return true;return false;
        }),drives_.end());
        // Keep the persistent root first, regardless of configuration ordering.
        for(const auto& row:registered)if(row["persistent"]==true){
            auto found=std::find_if(drives_.begin(),drives_.end(),[&](const Drive& d){std::ifstream marker(d.root/".spark-drive");std::string id;marker>>id;return row["id"]==id;});
            // A relocated root can instead be identified by its marker below.
            if(found!=drives_.end()&&std::filesystem::exists(found->root))std::iter_swap(drives_.begin(),found);
        }
        mount_roots(true);
    });
}
std::string FileStorage::resolve_drive(const std::string& letter)const{
    for(const auto& row:store->drives())if(row["letter"]==letter)return row["id"].get<std::string>();
    throw std::invalid_argument("drive_not_found");
}
json FileStorage::drives()const{
    auto rows=store->drives();for(auto& row:rows){row["online"]=false;row["available_bytes"]=nullptr;
        for(std::size_t i=0;i<drives_.size();++i)if(drives_[i].id==row["id"]&&online(i)){
            row["online"]=true;std::error_code error;auto capacity=std::filesystem::space(drive_path(i),error);if(!error)row["available_bytes"]=capacity.available;
        }
    }return {{"drives",rows}};
}
json FileStorage::attach(const std::filesystem::path& path){
    auto root=std::filesystem::canonical(path);
    std::string marker_id;{std::ifstream marker(root/".spark-drive");marker>>marker_id;}
    bool known=false;
    for(const auto& registered:store->drives()){
        if(registered["id"]==marker_id)known=true;
        if(registered["path"].is_string()){
            const auto existing=registered["path"].get<std::string>();
            if(existing!=root.string()&&((root.string()+"/").find(existing+"/")==0||(existing+"/").find(root.string()+"/")==0))throw std::invalid_argument("overlapping_drive_path");
        }
    }
    if(!known)for(const auto& entry:std::filesystem::directory_iterator(root)){
        const auto name=entry.path().filename().string();
        if(name!=".spark-drive"&&name!=".spark.lock")throw std::invalid_argument("drive_directory_not_empty");
    }
    for(std::size_t i=0;i<drives_.size();++i){
        const auto& current=drives_[i].root;
        if(root==current&&online(i))throw std::invalid_argument("drive_already_attached");
        const auto a=root.string()+"/",b=current.string()+"/";
        if(root!=current&&(a.find(b)==0||b.find(a)==0))throw std::invalid_argument("overlapping_drive_path");
    }
    if(std::count_if(drives_.begin(),drives_.end(),[](const Drive& d){return d.fd>=0;})>=64)throw std::invalid_argument("too_many_drives");
    {std::ifstream marker(root/".spark-drive");std::string id;marker>>id;for(std::size_t i=0;i<drives_.size();++i)if(drives_[i].id==id&&online(i))throw std::invalid_argument("duplicate_drive_identity");}
    for(auto it=drives_.begin();it!=drives_.end();)if((it->root==root||it->id==marker_id)&&!online(std::size_t(it-drives_.begin()))){if(it->lock>=0)close(it->lock);if(it->fd>=0)close(it->fd);it=drives_.erase(it);}else ++it;
    FileStorage candidate(store,root);candidate.mount_roots(false);
    auto& added=candidate.drives_.front();
    for(std::size_t i=0;i<drives_.size();++i)if(drives_[i].id==added.id&&online(i))throw std::invalid_argument("duplicate_drive_identity");
    for(auto it=drives_.begin();it!=drives_.end();)if(it->root==root||it->id==added.id){if(it->lock>=0)close(it->lock);if(it->fd>=0)close(it->fd);it=drives_.erase(it);}else ++it;
    drives_.push_back(added);added.fd=added.lock=-1;
    const auto status=drives();for(const auto& row:status["drives"])if(row["id"]==drives_.back().id)return row;
    throw std::runtime_error("Drive registration failed");
}
void FileStorage::detach(const std::string& letter){
    const auto id=resolve_drive(letter);
    for(const auto& row:store->drives())if(row["id"]==id&&row["persistent"]==true)throw std::invalid_argument("persistent_drive");
    for(std::size_t i=0;i<drives_.size();++i)if(drives_[i].id==id&&online(i))sync_fd(drives_[i].fd);
    store->detach_drive(id);
    for(auto it=drives_.begin();it!=drives_.end();)if(it->id==id){if(it->lock>=0)close(it->lock);if(it->fd>=0)close(it->fd);it=drives_.erase(it);}else ++it;
}
bool FileStorage::drive_online(const std::string& id)const{
    for(std::size_t i=0;i<drives_.size();++i)if(drives_[i].id==id)return online(i);return false;
}
void FileStorage::require_drive_online(const std::string& id)const{if(!drive_online(id))throw StorageError(ENODEV);}
void FileStorage::mount_roots(bool startup){
        store->verify();
        try {
            // Lock every root before recovery. Multi-drive roots must already exist;
            // provisioning a new mount is an explicit operator action.
            for(std::size_t i=0;i<drives_.size();++i){
                auto& drive=drives_[i];
                if(startup && i==0 && store->drives().empty() && std::filesystem::create_directories(drive.root))if(chmod(drive.root.c_str(),0700))throw StorageError(errno);
                drive.fd=open(drive.root.c_str(),O_RDONLY|O_DIRECTORY|O_CLOEXEC|O_NOFOLLOW);if(drive.fd<0){if(startup)continue;throw StorageError(errno);}
                if(startup){
                    const auto known=store->drives();bool remembered_path=false,recognized=false;
                    std::ifstream marker(drive.root/".spark-drive");std::string identity;marker>>identity;
                    for(const auto& row:known){if(row["path"]==drive.root.string())remembered_path=true;if(row["id"]==identity)recognized=true;}
                    // An empty mountpoint after unplugging is not a new disk.
                    // Only an explicit runtime attach may initialize a replacement.
                    if(remembered_path&&!recognized){close(drive.fd);drive.fd=-1;continue;}
                }
                drive.lock=openat(drive.fd,".spark.lock",O_CREAT|O_RDWR|O_CLOEXEC|O_NOFOLLOW,0600);if(drive.lock<0)throw StorageError(errno);
                if(flock(drive.lock,LOCK_EX|LOCK_NB))throw std::runtime_error("File storage directory is already in use");
                auto marker=drive_path(i)/".spark-drive";
                int marker_fd=open(marker.c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW);
                if(marker_fd<0 && errno==ENOENT){
                    std::random_device random;const char* hex="0123456789abcdef";std::string id;
                    for(int n=0;n<32;++n)id+=hex[random()&15];
                    // A completed marker is installed atomically. Interrupted temp
                    // markers are never treated as registered drive identities.
                    auto temp=drive_path(i)/(".spark-drive-"+id+".tmp");
                    Descriptor out(open(temp.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC|O_NOFOLLOW,0600));
                    auto data=id+"\n";std::size_t offset=0;
                    while(offset<data.size()){auto n=write(out.fd,data.data()+offset,data.size()-offset);if(n<0&&errno==EINTR)continue;if(n<=0)throw StorageError(errno);offset+=n;}
                    sync_fd(out.fd);if(rename(temp.c_str(),marker.c_str()))throw StorageError(errno);sync_fd(drive.fd);
                    marker_fd=open(marker.c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW);
                }
                Descriptor marker_file(marker_fd);struct stat info{};char data[34]{};
                if(fstat(marker_file.fd,&info)||!S_ISREG(info.st_mode)||info.st_size!=33||pread(marker_file.fd,data,33,0)!=33||data[32]!='\n')throw std::runtime_error("Invalid .spark-drive identity");
                drive.id=std::string(data,32);if(!std::regex_match(drive.id,std::regex("[0-9a-f]{32}")))throw std::runtime_error("Invalid .spark-drive identity");
                for(std::size_t j=0;j<i;++j)if(drives_[j].id==drive.id)throw std::runtime_error("Duplicate storage drive identity");
            }
            const auto records=store->layout();json assignments=json::object(),roots=json::array();std::vector<std::string> ids;
            for(const auto& drive:drives_)if(drive.fd>=0){ids.push_back(drive.id);roots.push_back({{"id",drive.id},{"path",drive.root.string()}});}
            if(ids.empty())throw std::runtime_error("No online storage drives");
            if(startup)for(const auto& known:store->drives())if(known["persistent"]==true&&std::find(ids.begin(),ids.end(),known["id"].get<std::string>())==ids.end())throw std::runtime_error("Persistent storage drive is unavailable");
            // Resolve legacy NULL drive IDs by locating their actual bytes, rather
            // than assuming the config's first drive is the original one.
            for(auto record:records){
                if(record["drive_id"].is_null()){
                    std::vector<std::size_t> candidates;
                    const auto id=record["id"].get<std::string>();const auto revision=record["content_revision"].get<std::string>();
                    for(std::size_t i=0;i<drives_.size();++i){if(!online(i))continue;auto root=drive_path(i);if(std::filesystem::exists(root/(id+(revision.empty()?"":"."+revision)+".blob"))||std::filesystem::exists(root/(id+".part")))candidates.push_back(i);}
                    if(candidates.empty()&&!startup)continue;
                    if(candidates.size()>1||(candidates.empty()&&record["state"]=="ready"))throw std::runtime_error("Legacy file drive is missing or ambiguous; recovery aborted");
                    auto index=candidates.empty()?0:candidates[0];record["drive_id"]=drives_[index].id;assignments[id]=record["drive_id"];
                }
                if(std::find(ids.begin(),ids.end(),record["drive_id"].get<std::string>())==ids.end())continue;
                if(record["state"]=="ready"){
                    Descriptor file(open(content_path(record).c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW));struct stat info{};
                    if(fstat(file.fd,&info)||!S_ISREG(info.st_mode)||info.st_size<0||static_cast<std::uint64_t>(info.st_size)!=record["size_bytes"].get<std::uint64_t>())throw std::runtime_error("Stored file missing or damaged; recovery aborted");
                }
            }
            store->register_drives(ids,assignments,roots,startup);
            // Recover only attached roots; offline records and bytes are preserved.
            for(const auto& id:store->unfinished()){auto where=store->location(id.get<std::string>());if(!where||std::find(ids.begin(),ids.end(),*where)==ids.end())continue;erase_bytes(id.get<std::string>());store->forget(id);}
            for(std::size_t i=0;i<drives_.size();++i)if(online(i))for(const auto& entry:std::filesystem::directory_iterator(drive_path(i))){
                const auto name=entry.path().filename().string();
                if(!std::regex_match(name,std::regex(R"([0-9a-f-]{36}(\.[0-9a-f]{32})?\.blob)")))continue;
                auto record=store->get(name.substr(0,36));
                if(!record || (*record)["drive_id"]!=drives_[i].id || content_path(*record)!=entry.path())if(unlink(entry.path().c_str())&&errno!=ENOENT)throw StorageError(errno);
            }
            sync_directory();
        }catch(...){for(auto& drive:drives_){if(drive.lock>=0)close(drive.lock);if(drive.fd>=0)close(drive.fd);drive.lock=drive.fd=-1;drive.id.clear();}throw;}
}
void configure_files(httplib::Server& server,std::shared_ptr<FileStorage> storage, std::shared_ptr<Authorization> auth){
    auto upload=[storage,auth](const httplib::Request& request,httplib::Response& response,const httplib::ContentReader& read){
        // Close after rejected uploads: their unread bodies must not become another request.
        auto bad=[&](int status,const char* code){response.set_header("Connection","close");failure(response,status,code);};
        if(request.get_header_value("Content-Type")!="application/octet-stream"){bad(415,"unsupported_media_type");return;}
        if(request.has_header("Content-Encoding") && request.get_header_value("Content-Encoding")!="identity"){bad(415,"unsupported_content_encoding");return;}
        if(!request.has_header("Content-Length") && request.get_header_value("Transfer-Encoding")!="chunked"){bad(411,"length_required");return;}
        for(const auto& [key,value]:request.params)if((key!="name" && key!="directory" && key!="drive") || request.get_param_value_count(key)!=1){bad(400,"invalid_parameters");return;}
        if(request.get_param_value_count("name")!=1){bad(400,"invalid_parameters");return;}
        const auto directory=request.has_param("directory")?request.get_param_value("directory"):"/";
        if(!valid_directory(directory)){bad(400,"invalid_directory");return;}
        const auto name=request.get_param_value("name");if(!valid_name(name)){bad(400,"invalid_filename");return;}
        std::shared_lock<std::shared_mutex> gate(storage->project_gate);
        std::string id;bool publishing=false;
        auto cleanup=[&]{if(!id.empty() && !publishing)try{storage->erase_bytes(id);storage->store->forget(id);}catch(const std::exception& e){std::cerr<<"File cleanup deferred until restart: "<<e.what()<<'\n';}};
        try {
            storage->initialize();
            std::optional<std::uint64_t> expected;
            if(request.has_header("Content-Length")){const auto value=request.get_header_value("Content-Length");std::uint64_t count=0;auto parsed=std::from_chars(value.data(),value.data()+value.size(),count);if(parsed.ec!=std::errc{}||parsed.ptr!=value.data()+value.size()){bad(400,"invalid_content_length");return;}expected=count;}
            id=storage->begin_upload(name,directory,expected,request.has_param("drive")?std::optional<std::string>(request.get_param_value("drive")):std::nullopt);
            Descriptor file(open(storage->path(id,true).c_str(),O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC|O_NOFOLLOW,0600));
            std::uint64_t bytes=0;int io_error=0;
            const bool complete=read([&](const char* data,std::size_t size){
                if(size>static_cast<std::uint64_t>(std::numeric_limits<off_t>::max())-bytes){io_error=EFBIG;return false;}
                std::size_t offset=0;
                while(offset<size){auto written=write(file.fd,data+offset,size-offset);if(written<0 && errno==EINTR)continue;if(written<=0){io_error=written<0?errno:EIO;return false;}offset+=static_cast<std::size_t>(written);}
                bytes+=size;return true;
            });
            if(io_error)throw StorageError(io_error);
            if(!complete){cleanup();bad(400,"incomplete_upload");return;}
            if(auth&&!auth->request(request,response)){cleanup();return;}
            sync_fd(file.fd);
            if(rename(storage->path(id,true).c_str(),storage->path(id).c_str()))throw StorageError(errno);
            storage->sync_directory();
            // A failed commit response may still have committed: never remove bytes here.
            publishing=true;
            auto record=storage->store->publish(id,bytes);
            response.set_header("Location","/files/"+id);reply(response,201,record);
        }catch(const DatabaseError&){cleanup();bad(503,"database_unavailable");}
        catch(const std::invalid_argument& error){cleanup();bad(std::string(error.what())=="drive_not_found"?404:409,error.what());}
        catch(const StorageError& error){cleanup();bad(error.status,error.what());}
        catch(const std::exception&){cleanup();bad(503,"storage_unavailable");}
    };
    server.Post("/files",upload);
    server.Put(R"(/files/([^/]+)/content)",[storage,auth](const httplib::Request& request,httplib::Response& response,const httplib::ContentReader& read){
        auto bad=[&](int status,const char* code){response.set_header("Connection","close");failure(response,status,code);};
        const auto id=request.matches[1].str();
        if(!valid_id(id)){bad(400,"invalid_file_id");return;}
        if(!request.params.empty()){bad(400,"invalid_parameters");return;}
        const auto type=request.get_header_value("Content-Type");
        if(type!="text/plain" && type!="text/plain; charset=utf-8"){bad(415,"unsupported_media_type");return;}
        if(request.has_header("Content-Encoding") && request.get_header_value("Content-Encoding")!="identity"){bad(415,"unsupported_content_encoding");return;}
        if(!request.has_header("Content-Length") && request.get_header_value("Transfer-Encoding")!="chunked"){bad(411,"length_required");return;}
        if(request.get_header_value_count("If-Match")!=1){bad(428,"precondition_required");return;}
        std::string body;bool large=false;
        if(!read([&](const char* data,std::size_t size){if(size>2097152-body.size()){large=true;return false;}body.append(data,size);return true;})){bad(large?413:400,large?"payload_too_large":"incomplete_upload");return;}
        if(auth&&!auth->request(request,response))return;
        try {json(body).dump();}catch(const json::exception&){bad(400,"invalid_text");return;}
        for(unsigned char c:body)if((c<32 && c!=9 && c!=10 && c!=13) || c==127){bad(400,"invalid_text");return;}
        // Serialize replacements against reads, moves, deletes and project mutations.
        std::unique_lock<std::shared_mutex> gate(storage->project_gate);
        std::filesystem::path fresh;bool committing=false;
        auto cleanup=[&]{if(!fresh.empty() && !committing)unlink(fresh.c_str());};
        try {
            storage->initialize();auto record=storage->store->get(id);
            if(!record){bad(404,"file_not_found");return;}
            const auto old_revision=(*record)["content_revision"].get<std::string>();
            const auto etag="\""+id+(old_revision.empty()?"":"-"+old_revision)+"\"";
            if(request.get_header_value("If-Match")!=etag){bad(412,"file_changed");return;}
            std::random_device random;std::string revision;const char* hex="0123456789abcdef";
            for(int i=0;i<32;++i)revision+=hex[random()&15];
            auto next=*record;next["content_revision"]=revision;const auto candidate=storage->content_path(next);
            Descriptor file(open(candidate.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC|O_NOFOLLOW,0600));
            fresh=candidate;
            std::size_t offset=0;
            while(offset<body.size()){auto count=write(file.fd,body.data()+offset,body.size()-offset);if(count<0 && errno==EINTR)continue;if(count<=0)throw StorageError(count<0?errno:EIO);offset+=static_cast<std::size_t>(count);}
            sync_fd(file.fd);storage->sync_directory();
            // An uncertain commit must retain both versions for startup recovery.
            committing=true;auto saved=storage->store->replace(id,revision,body.size());
            if(unlink(storage->content_path(*record).c_str())==0)storage->sync_directory();
            response.set_header("ETag","\""+id+"-"+revision+"\"");reply(response,200,saved);
        }catch(const DatabaseError&){cleanup();bad(503,"database_unavailable");}
        catch(const std::invalid_argument& error){failure(response,std::string(error.what())=="drive_not_found"?404:409,error.what());}
        catch(const StorageError& error){cleanup();bad(error.status,error.what());}
        catch(const std::exception&){cleanup();bad(503,"storage_unavailable");}
    });
    auto handler=[storage](const httplib::Request& request,httplib::Response& response){
        const bool collection=request.path=="/files",download=request.path.size()>8 && request.path.compare(request.path.size()-8,8,"/content")==0;
        std::string id;
        if(!collection){id=request.matches[1].str();if(!valid_id(id)){failure(response,400,"invalid_file_id");return;}}
        int limit=100,offset=0;std::optional<std::string> directory,drive;
        if(collection){
            for(const auto& [key,value]:request.params){
                if(request.get_param_value_count(key)!=1 || (key!="limit" && key!="offset" && key!="directory" && key!="drive")){failure(response,400,"invalid_parameters");return;}
                if(key=="drive"){drive=value;continue;}
                if(key=="directory"){if(!valid_directory(value)){failure(response,400,"invalid_directory");return;}directory=value;continue;}
                auto parsed=integer(value,key=="limit"?1:0,key=="limit"?500:1000000);
                if(parsed<0){failure(response,400,"invalid_pagination");return;}
                (key=="limit"?limit:offset)=parsed;
            }
        }else if(!request.params.empty()){failure(response,400,"invalid_parameters");return;}
        try {
            std::shared_lock<std::shared_mutex> gate(storage->project_gate);
            storage->initialize();
            if(collection){reply(response,200,storage->store->list(limit,offset,directory,drive?std::optional<std::string>(storage->resolve_drive(*drive)):std::nullopt));return;}
            if(request.method=="PATCH"){
                if(request.get_header_value("Content-Type").substr(0,request.get_header_value("Content-Type").find(';'))!="application/json"){failure(response,415,"unsupported_media_type");return;}
                auto packet=json::parse(request.body,nullptr,false);
                if(!packet.is_object() || packet.size()!=1 || !packet.contains("directory") || !packet["directory"].is_string() || !valid_directory(packet["directory"])){failure(response,400,"invalid_directory");return;}
                auto existing=storage->store->get(id);if(existing)storage->require_drive_online((*existing)["drive_id"]);
                auto moved=storage->store->move(id,packet["directory"]);if(!moved){failure(response,404,"file_not_found");return;}reply(response,200,*moved);return;
            }
            if(request.method=="DELETE"){
                auto existing=storage->store->get(id);if(existing)storage->content_path(*existing);
                if(!storage->store->deleting(id)){failure(response,404,"file_not_found");return;}
                storage->erase_bytes(id);storage->store->forget(id);response.status=204;return;
            }
            auto record=storage->store->get(id);if(!record){failure(response,404,"file_not_found");return;}
            if(!download){reply(response,200,*record);return;}
            auto transfer=std::make_shared<std::shared_lock<std::shared_mutex>>(std::move(gate));
            auto file=std::make_shared<Descriptor>(open(storage->content_path(*record).c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW));
            struct stat info{};
            if(fstat(file->fd,&info) || !S_ISREG(info.st_mode) || info.st_size<0 || static_cast<std::uint64_t>(info.st_size)!=(*record)["size_bytes"].get<std::uint64_t>())throw StorageError(EIO);
            response.set_header("Content-Disposition","attachment; filename=\"download\"; filename*=UTF-8''"+encoded_name((*record)["name"]));
            response.set_header("X-Content-Type-Options","nosniff");response.set_header("Accept-Ranges","bytes");
            const auto revision=(*record)["content_revision"].get<std::string>();
            response.set_header("ETag","\""+id+(revision.empty()?"":"-"+revision)+"\"");
            if(!info.st_size){response.set_content("","application/octet-stream");return;}
            response.set_content_provider(static_cast<std::size_t>(info.st_size),"application/octet-stream",[file,transfer](std::size_t offset,std::size_t length,httplib::DataSink& sink){
                std::array<char,65536> buffer{};ssize_t count;
                do{count=pread(file->fd,buffer.data(),std::min(length,buffer.size()),static_cast<off_t>(offset));}while(count<0 && errno==EINTR);
                return count>0 && sink.write(buffer.data(),static_cast<std::size_t>(count));
            });
        }catch(const DatabaseError&){failure(response,503,"database_unavailable");}
        catch(const std::invalid_argument& error){failure(response,std::string(error.what())=="drive_not_found"?404:409,error.what());}
        catch(const StorageError& error){failure(response,error.status,error.what());}
        catch(const std::exception&){failure(response,503,"storage_unavailable");}
    };
    auto drive_handler=[storage](const httplib::Request& request,httplib::Response& response){
        if(!request.params.empty()){failure(response,400,"invalid_parameters");return;}
        try{
            if(request.method=="GET"){
                std::shared_lock<std::shared_mutex> gate(storage->project_gate);storage->initialize();reply(response,200,storage->drives());return;
            }
            const auto origin=request.get_header_value("Origin");
            if(request.get_header_value("Sec-Fetch-Site")=="cross-site"||(!origin.empty()&&origin!="http://"+request.get_header_value("Host")&&origin!="https://"+request.get_header_value("Host"))){failure(response,403,"cross_site_request");return;}
            std::unique_lock<std::shared_mutex> gate(storage->project_gate);storage->initialize();
            if(request.method=="DELETE"){storage->detach(request.matches[1].str());response.status=204;return;}
            const auto type=request.get_header_value("Content-Type");if(type.substr(0,type.find(';'))!="application/json"){failure(response,415,"unsupported_media_type");return;}
            const auto packet=json::parse(request.body,nullptr,false);
            if(!packet.is_object()||packet.size()!=1||!packet.contains("path")||!packet["path"].is_string()){failure(response,400,"invalid_drive_path");return;}
            const auto path=packet["path"].get<std::string>();
            if(path.empty()||path[0]!='/'||path.size()>4096||std::any_of(path.begin(),path.end(),[](unsigned char c){return c<32||c==127;})){failure(response,400,"invalid_drive_path");return;}
            reply(response,201,storage->attach(path));
        }catch(const std::invalid_argument& error){failure(response,std::string(error.what())=="drive_not_found"?404:409,error.what());}
        catch(const DatabaseError&){failure(response,503,"database_unavailable");}
        catch(const std::exception&){failure(response,503,"storage_unavailable");}
    };
    server.Get("/files/drives",drive_handler);body_route(server,"POST","/files/drives",drive_handler,auth);
    body_route(server,"DELETE",R"(/files/drives/([A-Z]+))",drive_handler,auth);
    server.Get("/files/explorer",[](const auto&,auto& response){response.set_content(std::string(file_explorer_html),"text/html; charset=utf-8");});
    auto directories=[storage](const httplib::Request& request,httplib::Response& response){
        for(const auto& [key,value]:request.params)if(request.method!="GET"&&request.method!="HEAD"||key!="drive"||request.get_param_value_count(key)!=1){failure(response,400,"invalid_parameters");return;}
        try {
            std::shared_lock<std::shared_mutex> gate(storage->project_gate);storage->initialize();
            if(request.method=="POST"){
                const auto type=request.get_header_value("Content-Type");if(type.substr(0,type.find(';'))!="application/json"){failure(response,415,"unsupported_media_type");return;}
                auto packet=json::parse(request.body,nullptr,false);
                if(!packet.is_object()||!packet.contains("directory")||!packet["directory"].is_string()||!valid_directory(packet["directory"])||packet.size()>(packet.contains("drive")?2:1)||(packet.contains("drive")&&!packet["drive"].is_string())){failure(response,400,"invalid_directory");return;}
                const auto drive=packet.contains("drive")?storage->resolve_drive(packet["drive"]):storage->store->default_drive(packet["directory"]);
                storage->require_drive_online(drive);storage->store->create_directory(packet["directory"],drive);reply(response,200,packet);
            }else{
                const auto drive=request.has_param("drive")?storage->resolve_drive(request.get_param_value("drive")):storage->store->default_drive();
                reply(response,200,storage->store->directories(drive));
            }
        }catch(const std::invalid_argument& error){failure(response,std::string(error.what())=="drive_not_found"?404:409,error.what());}
        catch(const DatabaseError&){failure(response,503,"database_unavailable");}catch(const std::exception&){failure(response,503,"storage_unavailable");}
    };
    server.Get("/files/directories",directories);body_route(server,"POST","/files/directories",directories,auth);
    body_route(server,"PATCH",R"(/files/([^/]+))",handler,auth);
    server.Get("/files",handler);server.Get(R"(/files/([^/]+))",handler);server.Get(R"(/files/([^/]+)/content)",handler);
    body_route(server,"DELETE",R"(/files/([^/]+))",handler,auth);
}
json file_endpoints(){
    const json file={{"id","12345678-1234-1234-1234-123456789abc"},{"name","example.bin"},{"directory","/"},{"drive_id","0123456789abcdef0123456789abcdef"},{"drive_letter","A"},{"size_bytes",1024},{"content_revision",""},{"content_type","application/octet-stream"},{"download_url","/files/12345678-1234-1234-1234-123456789abc/content"},{"created_at","2026-09-19T00:00:00.000000Z"}};
    json endpoints=json::array();
    for(const std::string op:{"upload","list","get","download","delete"}){
        bool upload=op=="upload",list=op=="list",download=op=="download",remove=op=="delete";
        endpoints.push_back({{"method",upload?"POST":remove?"DELETE":"GET"},{"path",upload || list?"/files":download?"/files/{id}/content":"/files/{id}"},
            {"description",upload?"Upload raw file bytes. Any file type, no application size cap. Whole files stay on the selected drive. Without drive, use the uniquely matching project drive, otherwise persistent storage; ambiguous project paths require drive (409). Offline/full destinations never fall back to another drive. Name is metadata, not a path. Each upload gets a new UUID; duplicate names are allowed. A file becomes visible only after a complete, durable upload." :list?"List completed shared files, ordered by creation time then ID. Follow pagination." :download?"Stream current bytes as an attachment. Supports HEAD and HTTP byte ranges for resumed downloads. No authentication." :remove?"Delete shared file metadata and bytes. Missing files return 404; an interrupted deletion is retried on restart." :"Get metadata and download URL for a completed file."},
            {"input",{{"path",upload || list?json::object():json{{"id","lowercase file UUID"}}},{"headers",upload?json{{"Content-Type","application/octet-stream (required regardless of file type)"},{"Content-Length","byte length, or use Transfer-Encoding: chunked"}}:download?json{{"Range","optional bytes=start-end"}}:json::object()},
                {"query",upload?json{{"drive","optional drive letter; otherwise matching project drive or persistent storage; ambiguous paths return 409 drive_required"},{"directory","optional absolute logical path; default /; parents created automatically"},{"name","required UTF-8 filename, 1..255 bytes; no slash, backslash, control characters, '.' or '..'; URL encode"}}:list?json{{"drive","optional drive letter; includes metadata while offline"},{"directory","optional exact logical directory; omit to list all files"},{"limit","1..500, default 100"},{"offset","0..1000000, default 0"}}:json::object()},
                {"body",upload?json("Raw binary bytes, not JSON, base64 or multipart; empty files allowed"):json(nullptr)}}},
            {"output",{{"status",upload?201:remove?204:200},{"headers",download?json{{"Content-Type","application/octet-stream"},{"Content-Disposition","attachment with UTF-8 filename"},{"Accept-Ranges","bytes"}}:json{{"Content-Type",remove?"none":"application/json"}}},
                {"body",remove?json(nullptr):download?json("Current file bytes (206 for a valid range)"):list?json{{"files",json::array({file})},{"limit",100},{"offset",0},{"has_more",false}}:file}}},
            {"errors",{{"400","invalid_directory, invalid_filename, invalid_parameters, invalid_pagination, invalid_file_id, incomplete_upload"},{"404","file_not_found, drive_not_found"},{"409","drive_required"},{"411","length_required"},{"415","unsupported_media_type, unsupported_content_encoding"},{"416","unsatisfiable byte range"},{"503","database_unavailable, storage_unavailable"},{"507","insufficient_storage"}}}});
    }
    const std::string rule="Absolute logical directory: / or /folder/subfolder. Case-sensitive UTF-8, max 1024 bytes total and 255 per component; no empty components, trailing slash, dot/dot-dot, backslashes or control characters. Not a server filesystem path.";
    for(const std::string op:{"move","directories","mkdir","explorer"}){
        bool move=op=="move",mkdir=op=="mkdir",html=op=="explorer",write=move||mkdir;
        endpoints.push_back({{"method",move?"PATCH":mkdir?"POST":"GET"},{"path",move?"/files/{id}":html?"/files/explorer":"/files/directories"},
            {"description",move?"Move a completed file to a directory on its current drive; creates missing parents. Drive must be online; this does not transfer bytes between drives. ID, bytes and download URL remain unchanged. Duplicate filenames are allowed." :mkdir?"Create an empty directory and parents on the selected online drive; optional body drive letter, otherwise matching project drive or persistent storage. Idempotent per drive." :html?"Browser file explorer: view and edit UTF-8 text, preview log beginnings/endings, upload, download, delete and move shared files." :"List directory paths on one drive, including empty folders and root. Query drive selects a letter; omitted uses persistent storage. Offline folder metadata remains available."},
            {"input",{{"path",move?json{{"id","file UUID"}}:json::object()},{"query",!write&&!html?json{{"drive","optional drive letter; default persistent storage"}}:json::object()},{"headers",write?json{{"Content-Type","application/json"}}:json::object()},{"body",mkdir?json{{"directory","/documents/manuals"},{"drive","A"}}:move?json{{"directory","/documents/manuals"}}:json(nullptr)},{"validation",rule}}},
            {"output",{{"status",200},{"headers",{{"Content-Type",html?"text/html; charset=utf-8":"application/json"}}},{"body",move?file:mkdir?json{{"directory","/documents/manuals"}}:html?json("HTML file explorer"):json{{"directories",json::array({"/","/documents","/documents/manuals"})}}}}},
            {"errors",{{"400","invalid_directory, invalid_file_id, invalid_parameters"},{"404","file_not_found"},{"415","unsupported_media_type"},{"503","database_unavailable, storage_unavailable"}}}});
    }
    endpoints.push_back({{"method","PUT"},{"path","/files/{id}/content"},
        {"description","Replace UTF-8 text (up to 2 MiB) while retaining file identity, directory and download URL. Save requires the exact ETag from GET content; concurrent changes return 412. Emits file.updated after commit."},
        {"input",{{"path",{{"id","file UUID"}}},{"headers",{{"Content-Type","text/plain; charset=utf-8"},{"If-Match","exact quoted ETag from GET /files/{id}/content"},{"Content-Length","byte length, or Transfer-Encoding: chunked"}}},{"body","Raw UTF-8 text; optional BOM; controls allowed only tab, CR and LF; empty text allowed"}}},
        {"output",{{"status",200},{"headers",{{"ETag","new quoted content revision"}}},{"body",file}}},
        {"errors",{{"400","invalid_file_id, invalid_parameters, invalid_text, incomplete_upload"},{"404","file_not_found"},{"411","length_required"},{"412","file_changed; reload before saving"},{"413","payload_too_large (2 MiB editor limit; uploads remain unlimited)"},{"415","unsupported_media_type, unsupported_content_encoding"},{"428","precondition_required"},{"503","database_unavailable, storage_unavailable"},{"507","insufficient_storage"}}}});
    const json drive={{"id","0123456789abcdef0123456789abcdef"},{"letter","A"},{"path","/srv/spark/files"},{"persistent",true},{"attached",true},{"online",true},{"available_bytes",1073741824}};
    for(const auto& method:{"GET","POST","DELETE"}){
        const bool get=std::string(method)=="GET",remove=std::string(method)=="DELETE";
        endpoints.push_back({{"method",method},{"path",remove?"/files/drives/{letter}":"/files/drives"},
            {"description",get?"List registered drives, persistent letters, attachment state and live availability. Offline drives retain their file metadata.":remove?"Detach a removable drive without deleting files or releasing its letter. Waits for file transfers to finish. Persistent server storage cannot be detached. Safely eject via the OS afterward.":"Attach an existing absolute storage directory on an OS-mounted drive. New identities receive a permanent letter (A..Z, AA...). Reconnecting preserves its letter. Does not mount filesystems or import arbitrary files. Emits drives.updated."},
            {"input",{{"path",remove?json{{"letter","uppercase drive letter"}}:json::object()},{"query",json::object()},{"headers",!get&&!remove?json{{"Content-Type","application/json"}}:json::object()},{"body",!get&&!remove?json{{"path","/media/demo/SSD/spark-files"}}:json(nullptr)}}},
            {"output",{{"status",get?200:remove?204:201},{"body",get?json{{"drives",json::array({drive})}}:remove?json(nullptr):drive}}},
            {"errors",{{"400","invalid_drive_path, invalid_parameters"},{"403","cross_site_request"},{"404","drive_not_found"},{"409","persistent_drive, drive_already_attached, duplicate_drive_identity, overlapping_drive_path, drive_directory_not_empty, too_many_drives"},{"415","unsupported_media_type"},{"503","storage_unavailable, database_unavailable"}}}});
    }
    return endpoints;
}
}
