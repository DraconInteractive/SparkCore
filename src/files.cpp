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
FileStorage::FileStorage(std::shared_ptr<FileStore> metadata,std::filesystem::path root):store(std::move(metadata)),root_(std::filesystem::absolute(root)){}
FileStorage::~FileStorage(){if(lock_>=0)close(lock_);}
std::filesystem::path FileStorage::default_root(){const char* value=std::getenv("SPARK_FILE_DIR");return value && *value?value:"storage/files";}
std::filesystem::path FileStorage::path(const std::string& id,bool partial)const{return root_/(id+(partial?".part":".blob"));}
void FileStorage::sync_directory(){Descriptor directory(open(root_.c_str(),O_RDONLY|O_DIRECTORY|O_CLOEXEC));sync_fd(directory.fd);}
void FileStorage::erase_bytes(const std::string& id){
    for(bool partial:{true,false})if(unlink(path(id,partial).c_str()) && errno!=ENOENT)throw StorageError(errno);
    sync_directory();
}
void FileStorage::initialize(){
    std::call_once(initialized_,[this]{
        store->verify();
        if(std::filesystem::create_directories(root_)) {
            if(chmod(root_.c_str(),0700))throw StorageError(errno);
        }
        lock_=open((root_/".spark.lock").c_str(),O_CREAT|O_RDWR|O_CLOEXEC|O_NOFOLLOW,0600);
        if(lock_<0)throw StorageError(errno);
        try {
            if(flock(lock_,LOCK_EX|LOCK_NB))throw std::runtime_error("File storage directory is already in use");
            // Uploading/deleting rows are intentionally hidden until recovery completes.
            for(const auto& id:store->unfinished()){erase_bytes(id.get<std::string>());store->forget(id);}
            sync_directory();
        }catch(...){close(lock_);lock_=-1;throw;}
    });
}
void configure_files(httplib::Server& server,std::shared_ptr<FileStorage> storage){
    auto upload=[storage](const httplib::Request& request,httplib::Response& response,const httplib::ContentReader& read){
        // Close after rejected uploads: their unread bodies must not become another request.
        auto bad=[&](int status,const char* code){response.set_header("Connection","close");failure(response,status,code);};
        if(request.get_header_value("Content-Type")!="application/octet-stream"){bad(415,"unsupported_media_type");return;}
        if(request.has_header("Content-Encoding") && request.get_header_value("Content-Encoding")!="identity"){bad(415,"unsupported_content_encoding");return;}
        if(!request.has_header("Content-Length") && request.get_header_value("Transfer-Encoding")!="chunked"){bad(411,"length_required");return;}
        for(const auto& [key,value]:request.params)if((key!="name" && key!="directory") || request.get_param_value_count(key)!=1){bad(400,"invalid_parameters");return;}
        if(request.get_param_value_count("name")!=1){bad(400,"invalid_parameters");return;}
        const auto directory=request.has_param("directory")?request.get_param_value("directory"):"/";
        if(!valid_directory(directory)){bad(400,"invalid_directory");return;}
        const auto name=request.get_param_value("name");if(!valid_name(name)){bad(400,"invalid_filename");return;}
        std::string id;bool publishing=false;
        auto cleanup=[&]{if(!id.empty() && !publishing)try{storage->erase_bytes(id);storage->store->forget(id);}catch(const std::exception& e){std::cerr<<"File cleanup deferred until restart: "<<e.what()<<'\n';}};
        try {
            storage->initialize();id=storage->store->begin(name,directory);
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
            sync_fd(file.fd);
            if(rename(storage->path(id,true).c_str(),storage->path(id).c_str()))throw StorageError(errno);
            storage->sync_directory();
            // A failed commit response may still have committed: never remove bytes here.
            publishing=true;
            auto record=storage->store->publish(id,bytes);
            response.set_header("Location","/files/"+id);reply(response,201,record);
        }catch(const DatabaseError&){cleanup();bad(503,"database_unavailable");}
        catch(const StorageError& error){cleanup();bad(error.status,error.what());}
        catch(const std::exception&){cleanup();bad(503,"storage_unavailable");}
    };
    server.Post("/files",upload);
    auto handler=[storage](const httplib::Request& request,httplib::Response& response){
        const bool collection=request.path=="/files",download=request.path.size()>8 && request.path.compare(request.path.size()-8,8,"/content")==0;
        std::string id;
        if(!collection){id=request.matches[1].str();if(!valid_id(id)){failure(response,400,"invalid_file_id");return;}}
        int limit=100,offset=0;std::optional<std::string> directory;
        if(collection){
            for(const auto& [key,value]:request.params){
                if(request.get_param_value_count(key)!=1 || (key!="limit" && key!="offset" && key!="directory")){failure(response,400,"invalid_parameters");return;}
                if(key=="directory"){if(!valid_directory(value)){failure(response,400,"invalid_directory");return;}directory=value;continue;}
                auto parsed=integer(value,key=="limit"?1:0,key=="limit"?500:1000000);
                if(parsed<0){failure(response,400,"invalid_pagination");return;}
                (key=="limit"?limit:offset)=parsed;
            }
        }else if(!request.params.empty()){failure(response,400,"invalid_parameters");return;}
        try {
            storage->initialize();
            if(collection){reply(response,200,storage->store->list(limit,offset,directory));return;}
            if(request.method=="PATCH"){
                if(request.get_header_value("Content-Type").substr(0,request.get_header_value("Content-Type").find(';'))!="application/json"){failure(response,415,"unsupported_media_type");return;}
                auto packet=json::parse(request.body,nullptr,false);
                if(!packet.is_object() || packet.size()!=1 || !packet.contains("directory") || !packet["directory"].is_string() || !valid_directory(packet["directory"])){failure(response,400,"invalid_directory");return;}
                auto moved=storage->store->move(id,packet["directory"]);if(!moved){failure(response,404,"file_not_found");return;}reply(response,200,*moved);return;
            }
            if(request.method=="DELETE"){
                if(!storage->store->deleting(id)){failure(response,404,"file_not_found");return;}
                storage->erase_bytes(id);storage->store->forget(id);response.status=204;return;
            }
            auto record=storage->store->get(id);if(!record){failure(response,404,"file_not_found");return;}
            if(!download){reply(response,200,*record);return;}
            auto file=std::make_shared<Descriptor>(open(storage->path(id).c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW));
            struct stat info{};
            if(fstat(file->fd,&info) || !S_ISREG(info.st_mode) || info.st_size<0 || static_cast<std::uint64_t>(info.st_size)!=(*record)["size_bytes"].get<std::uint64_t>())throw StorageError(EIO);
            response.set_header("Content-Disposition","attachment; filename=\"download\"; filename*=UTF-8''"+encoded_name((*record)["name"]));
            response.set_header("X-Content-Type-Options","nosniff");response.set_header("Accept-Ranges","bytes");
            response.set_header("ETag","\""+id+"\"");
            if(!info.st_size){response.set_content("","application/octet-stream");return;}
            response.set_content_provider(static_cast<std::size_t>(info.st_size),"application/octet-stream",[file](std::size_t offset,std::size_t length,httplib::DataSink& sink){
                std::array<char,65536> buffer{};ssize_t count;
                do{count=pread(file->fd,buffer.data(),std::min(length,buffer.size()),static_cast<off_t>(offset));}while(count<0 && errno==EINTR);
                return count>0 && sink.write(buffer.data(),static_cast<std::size_t>(count));
            });
        }catch(const DatabaseError&){failure(response,503,"database_unavailable");}
        catch(const StorageError& error){failure(response,error.status,error.what());}
        catch(const std::exception&){failure(response,503,"storage_unavailable");}
    };
    server.Get("/files/explorer",[](const auto&,auto& response){response.set_content(std::string(file_explorer_html),"text/html; charset=utf-8");});
    auto directories=[storage](const httplib::Request& request,httplib::Response& response){
        if(!request.params.empty()){failure(response,400,"invalid_parameters");return;}
        try {
            storage->initialize();
            if(request.method=="POST"){
                const auto type=request.get_header_value("Content-Type");if(type.substr(0,type.find(';'))!="application/json"){failure(response,415,"unsupported_media_type");return;}
                auto packet=json::parse(request.body,nullptr,false);
                if(!packet.is_object() || packet.size()!=1 || !packet.contains("directory") || !packet["directory"].is_string() || !valid_directory(packet["directory"])){failure(response,400,"invalid_directory");return;}
                storage->store->create_directory(packet["directory"]);reply(response,200,packet);
            }else reply(response,200,storage->store->directories());
        }catch(const DatabaseError&){failure(response,503,"database_unavailable");}catch(const std::exception&){failure(response,503,"storage_unavailable");}
    };
    server.Get("/files/directories",directories);body_route(server,"POST","/files/directories",directories);
    body_route(server,"PATCH",R"(/files/([^/]+))",handler);
    server.Get("/files",handler);server.Get(R"(/files/([^/]+))",handler);server.Get(R"(/files/([^/]+)/content)",handler);
    body_route(server,"DELETE",R"(/files/([^/]+))",handler);
}
json file_endpoints(){
    const json file={{"id","12345678-1234-1234-1234-123456789abc"},{"name","example.bin"},{"directory","/"},{"size_bytes",1024},{"content_type","application/octet-stream"},{"download_url","/files/12345678-1234-1234-1234-123456789abc/content"},{"created_at","2026-09-19T00:00:00.000000Z"}};
    json endpoints=json::array();
    for(const std::string op:{"upload","list","get","download","delete"}){
        bool upload=op=="upload",list=op=="list",download=op=="download",remove=op=="delete";
        endpoints.push_back({{"method",upload?"POST":remove?"DELETE":"GET"},{"path",upload || list?"/files":download?"/files/{id}/content":"/files/{id}"},
            {"description",upload?"Upload raw file bytes. Any file type, no application size cap. Name is metadata, not a path. Each upload gets a new UUID; duplicate names are allowed. A file becomes visible only after a complete, durable upload." :list?"List completed shared files, ordered by creation time then ID. Follow pagination." :download?"Stream original bytes as an attachment. Supports HEAD and HTTP byte ranges for resumed downloads. No authentication." :remove?"Delete shared file metadata and bytes. Missing files return 404; an interrupted deletion is retried on restart." :"Get metadata and download URL for a completed file."},
            {"input",{{"path",upload || list?json::object():json{{"id","lowercase file UUID"}}},{"headers",upload?json{{"Content-Type","application/octet-stream (required regardless of file type)"},{"Content-Length","byte length, or use Transfer-Encoding: chunked"}}:download?json{{"Range","optional bytes=start-end"}}:json::object()},
                {"query",upload?json{{"directory","optional absolute logical path; default /; parents created automatically"},{"name","required UTF-8 filename, 1..255 bytes; no slash, backslash, control characters, '.' or '..'; URL encode"}}:list?json{{"directory","optional exact logical directory; omit to list all files"},{"limit","1..500, default 100"},{"offset","0..1000000, default 0"}}:json::object()},
                {"body",upload?json("Raw binary bytes, not JSON, base64 or multipart; empty files allowed"):json(nullptr)}}},
            {"output",{{"status",upload?201:remove?204:200},{"headers",download?json{{"Content-Type","application/octet-stream"},{"Content-Disposition","attachment with UTF-8 filename"},{"Accept-Ranges","bytes"}}:json{{"Content-Type",remove?"none":"application/json"}}},
                {"body",remove?json(nullptr):download?json("Original file bytes (206 for a valid range)"):list?json{{"files",json::array({file})},{"limit",100},{"offset",0},{"has_more",false}}:file}}},
            {"errors",{{"400","invalid_directory, invalid_filename, invalid_parameters, invalid_pagination, invalid_file_id, incomplete_upload"},{"404","file_not_found"},{"411","length_required"},{"415","unsupported_media_type, unsupported_content_encoding"},{"416","unsatisfiable byte range"},{"503","database_unavailable, storage_unavailable"},{"507","insufficient_storage"}}}});
    }
    const std::string rule="Absolute logical directory: / or /folder/subfolder. Case-sensitive UTF-8, max 1024 bytes total and 255 per component; no empty components, trailing slash, dot/dot-dot, backslashes or control characters. Not a server filesystem path.";
    for(const std::string op:{"move","directories","mkdir","explorer"}){
        bool move=op=="move",mkdir=op=="mkdir",html=op=="explorer",write=move||mkdir;
        endpoints.push_back({{"method",move?"PATCH":mkdir?"POST":"GET"},{"path",move?"/files/{id}":html?"/files/explorer":"/files/directories"},
            {"description",move?"Move a completed file to a logical directory; creates missing parents. ID, bytes and download URL remain unchanged. Duplicate filenames are allowed." :mkdir?"Create a persistent empty directory and missing parents; idempotent." :html?"Browser file explorer: view details, upload, download, delete and move shared files." :"List all logical directory paths, including empty directories and root."},
            {"input",{{"path",move?json{{"id","file UUID"}}:json::object()},{"query",json::object()},{"headers",write?json{{"Content-Type","application/json"}}:json::object()},{"body",write?json{{"directory","/documents/manuals"}}:json(nullptr)},{"validation",rule}}},
            {"output",{{"status",200},{"headers",{{"Content-Type",html?"text/html; charset=utf-8":"application/json"}}},{"body",move?file:mkdir?json{{"directory","/documents/manuals"}}:html?json("HTML file explorer"):json{{"directories",json::array({"/","/documents","/documents/manuals"})}}}}},
            {"errors",{{"400","invalid_directory, invalid_file_id, invalid_parameters"},{"404","file_not_found"},{"415","unsupported_media_type"},{"503","database_unavailable, storage_unavailable"}}}});
    }
    return endpoints;
}
}
