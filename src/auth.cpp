#include "auth.hpp"
#include <openssl/evp.h>
#include <openssl/rand.h>
#include <openssl/crypto.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/file.h>
#include <unistd.h>
#include <cerrno>
#include <stdexcept>
#include <vector>

namespace spark {
using nlohmann::json;
namespace {
void require(bool condition,const std::string& message) { if(!condition) throw std::runtime_error(message); }
void keys(const json& value, const std::set<std::string>& expected) {
    require(value.is_object() && value.size()==expected.size(),"Invalid credential schema");
    for(const auto& key:expected) require(value.contains(key),"Invalid credential schema");
}
bool hex(const std::string& s,std::size_t length) {
    return s.size()==length && s.find_first_not_of("0123456789abcdef")==std::string::npos;
}
bool identifier(const std::string& s) {
    return !s.empty() && s.size()<=128 && s.find_first_not_of("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789._-:@")==std::string::npos;
}
std::string encode(const unsigned char* bytes,std::size_t n) {
    std::string out; const char* digits="0123456789abcdef";
    for(std::size_t i=0;i<n;++i){out+=digits[bytes[i]>>4];out+=digits[bytes[i]&15];}return out;
}
std::string random_hex(std::size_t n) {
    unsigned char bytes[32];require(n<=sizeof(bytes)&&RAND_bytes(bytes,n)==1,"Secure random generation failed");return encode(bytes,n);
}
std::string digest(const std::string& token) {
    unsigned char bytes[EVP_MAX_MD_SIZE];unsigned int size=0;
    require(EVP_Digest(token.data(),token.size(),bytes,&size,EVP_sha256(),nullptr)==1,"Token hashing failed");return encode(bytes,size);
}
Identity identity(const json& record) {
    return {record.at("id"),record.at("subject"),record.at("kind"),record.at("permissions").get<std::set<std::string>>()};
}
struct FD { int fd; ~FD(){if(fd>=0)close(fd);} };
struct Lock {
    FD handle;
    explicit Lock(const std::filesystem::path& file):handle{open((file.string()+".lock").c_str(),O_RDWR|O_CREAT|O_CLOEXEC|O_NOFOLLOW|O_NONBLOCK,0600)} {
        require(handle.fd>=0,"Cannot lock credentials");struct stat st{};
        require(fstat(handle.fd,&st)==0&&S_ISREG(st.st_mode)&&st.st_uid==geteuid()&&(st.st_mode&077)==0,"Unsafe credential lock");
        require(flock(handle.fd,LOCK_EX)==0,"Cannot lock credentials");
    }
};
void save(const std::filesystem::path& path,const json& document,bool initial=false) {
    validate_credentials(document);
    auto temporary=path.string()+".tmp."+random_hex(16);
    FD fd{open(temporary.c_str(),O_WRONLY|O_CREAT|O_EXCL|O_CLOEXEC,0600)};
    require(fd.fd>=0,"Cannot create credential file");
    try {
        auto bytes=document.dump(2)+"\n";std::size_t offset=0;
        while(offset<bytes.size()){auto n=write(fd.fd,bytes.data()+offset,bytes.size()-offset);if(n<0&&errno==EINTR)continue;require(n>0,"Cannot write credentials");offset+=n;}
        require(fsync(fd.fd)==0,"Cannot sync credentials");
        if(initial){require(link(temporary.c_str(),path.c_str())==0,"Credentials already exist or cannot be created");unlink(temporary.c_str());}
        else require(rename(temporary.c_str(),path.c_str())==0,"Cannot replace credentials");
        auto parent=path.parent_path();if(parent.empty())parent=".";
        FD directory{open(parent.c_str(),O_RDONLY|O_DIRECTORY|O_CLOEXEC)};
        require(directory.fd>=0&&fsync(directory.fd)==0,"Cannot sync credential directory");
    } catch(...) {unlink(temporary.c_str());throw;}
}
json load(const std::filesystem::path& file) {auto value=read_json_object(file,"credentials file",true);validate_credentials(value);return value;}
}
nlohmann::json read_json_object(const std::filesystem::path& path,const std::string& label,bool private_file) {
    // Reject directories, devices and FIFOs; never block on a credentials pipe.
    int fd=open(path.c_str(),O_RDONLY|O_CLOEXEC|O_NONBLOCK|(private_file?O_NOFOLLOW:0));
    require(fd>=0,"Cannot read "+label);
    struct FD {int value;~FD(){close(value);}} descriptor{fd};
    struct stat statbuf{};
    require(fstat(fd,&statbuf)==0&&S_ISREG(statbuf.st_mode),label+" must be a regular file");
    require(!private_file || ((statbuf.st_mode & 077)==0 && statbuf.st_uid==geteuid()), "Credentials file must be owned by the current user and have no group/other permissions");
    constexpr std::size_t max_bytes=1048576;
    require(statbuf.st_size>=0&&static_cast<unsigned long long>(statbuf.st_size)<=max_bytes,label+" exceeds 1 MiB");
    std::string bytes;char buffer[4096];
    for(;;){auto count=read(fd,buffer,sizeof(buffer));if(count<0&&errno==EINTR)continue;require(count>=0,"Cannot read "+label);if(!count)break;require(bytes.size()+count<=max_bytes,label+" exceeds 1 MiB");bytes.append(buffer,count);}
    // JSON normally accepts duplicate keys with the last value winning. Refuse
    // ambiguity here, particularly duplicate auth sections or provider fields.
    std::vector<std::set<std::string>> objects;
    auto callback=[&](int,json::parse_event_t event,json& parsed){
        if(event==json::parse_event_t::object_start)objects.emplace_back();
        else if(event==json::parse_event_t::key)require(objects.back().insert(parsed.get<std::string>()).second,"Duplicate key in "+label);
        else if(event==json::parse_event_t::object_end)objects.pop_back();
        return true;
    };
    try {auto result=json::parse(bytes,callback);require(result.is_object(),label+" must contain a JSON object");return result;}
    catch(const json::exception&){throw std::runtime_error("Invalid JSON in "+label);}
}
const std::set<std::string>& permission_names() {
    static const std::set<std::string> names={
        "discovery.read","devices.read","devices.write","pulse.read","pulse.write",
        "timers.read","timers.write","events.read","events.write","tasks.read","tasks.write",
        "files.read","files.write","projects.read","projects.write","boards.read","boards.write",
        "commands.read","commands.invoke","commands.advertise","commands.execute","notifications.read",
        "devices.delegate","admin.restart","admin.backup","admin.storage","admin.credentials","admin.connections"};
    return names;
}
bool permitted(const Identity& who,const std::string& permission) {
    return permission_names().count(permission) && (who.kind=="open" || who.permissions.count(permission));
}
bool acts_as(const Identity& who,const std::string& device_id) {
    return who.kind=="open" || (who.kind=="device" && who.subject==device_id) || permitted(who,"devices.delegate");
}
std::optional<Identity> OpenIdentityProvider::authenticate(const std::string&) const {return Identity{"","anonymous","open",{}};}
bool OpenIdentityProvider::current(const Identity& who) const {return who.kind=="open";}
void validate_credentials(const json& document) {
    try {
        keys(document,{"version","credentials"});
        require(document["version"].is_number_integer()&&document["version"]==1&&document["credentials"].is_array()&&document["credentials"].size()<=1024,"Invalid credentials document");
        std::set<std::string> ids,hashes;
        for(const auto& item:document["credentials"]){
            keys(item,{"id","kind","subject","token_sha256","permissions","revoked"});
            auto id=item.at("id").get<std::string>(),kind=item.at("kind").get<std::string>(),subject=item.at("subject").get<std::string>(),hash=item.at("token_sha256").get<std::string>();
            require(hex(id,32)&&ids.insert(id).second&&hex(hash,64)&&hashes.insert(hash).second,"Invalid or duplicate credential identifier/hash");
            require((kind=="device"||kind=="administrator")&&identifier(subject)&&item["revoked"].is_boolean()&&item["permissions"].is_array(),"Invalid credential identity");
            std::set<std::string> seen;
            for(const auto& p:item["permissions"]){auto name=p.get<std::string>();require(permission_names().count(name)&&seen.insert(name).second,"Unknown or duplicate permission");require(kind=="administrator"||name.rfind("admin.",0)!=0,"Administrative permissions require administrator identity");}
        }
        require(document.dump(2).size()+1<=1048576,"Credentials file exceeds 1 MiB");
    }catch(const json::exception&){throw std::runtime_error("Invalid credential schema");}
}
TokenIdentityProvider::TokenIdentityProvider(std::filesystem::path file):file_(std::move(file)) {load(file_);}
std::optional<Identity> TokenIdentityProvider::authenticate(const std::string& token) const {
    // Reload on every resolution: atomic revocation needs no process restart.
    try {
        if(token.size()!=71||token.compare(0,7,"spark1_")!=0||!hex(token.substr(7),64))return std::nullopt;
        auto hash=digest(token);auto document=load(file_);
        for(const auto& record:document["credentials"]){auto stored=record["token_sha256"].get<std::string>();if(CRYPTO_memcmp(hash.data(),stored.data(),64)==0&&!record["revoked"].get<bool>())return identity(record);}
    }catch(const std::exception&){} // Unreadable/malformed store denies everyone.
    return std::nullopt;
}
bool TokenIdentityProvider::current(const Identity& who) const {
    try {auto document=load(file_);for(const auto& r:document["credentials"]){auto now=identity(r);if(!r["revoked"].get<bool>()&&now.credential_id==who.credential_id&&now.subject==who.subject&&now.kind==who.kind&&now.permissions==who.permissions)return true;}}catch(const std::exception&){}
    return false;
}
void initialize_credentials(const std::filesystem::path& file){Lock lock(file);save(file,{{"version",1},{"credentials",json::array()}},true);}
json issue_credential(const std::filesystem::path& file,const std::string& kind,const std::string& subject,const std::set<std::string>& permissions){
    Lock lock(file);auto document=load(file);auto token="spark1_"+random_hex(32),id=random_hex(16);
    document["credentials"].push_back({{"id",id},{"kind",kind},{"subject",subject},{"token_sha256",digest(token)},{"permissions",permissions},{"revoked",false}});
    save(file,document);return {{"id",id},{"token",token}};
}
void revoke_credential(const std::filesystem::path& file,const std::string& id){
    Lock lock(file);auto document=load(file);bool found=false;
    for(auto& record:document["credentials"])if(record["id"]==id){record["revoked"]=true;found=true;}
    require(found,"Credential not found");save(file,document);
}
json list_credentials(const std::filesystem::path& file){auto records=load(file)["credentials"];for(auto& record:records)record.erase("token_sha256");return records;}
}
