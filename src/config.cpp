#include "config.hpp"
#include "auth.hpp"
#include "browser_sessions.hpp"
#include <charconv>
#include <cstdlib>
#include <set>
#include <map>
#include <fstream>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>

namespace spark {
namespace {
using nlohmann::json;
namespace fs = std::filesystem;
void require(bool ok,const std::string& error) { if(!ok) throw ConfigError(error); }
std::string env(const char* key,const std::string& fallback={}) {
    const char* value=std::getenv(key);return value?value:fallback;
}
void text_value(const std::string& value,const std::string& field,bool empty=false) {
    require((empty||!value.empty())&&value.size()<=4096,field+" must be a nonempty string of at most 4096 bytes");
    require(value.find_first_of(std::string("\0\r\n\t",4))==std::string::npos,field+" contains an unsupported control character");
}
void keys(const json& object,const std::set<std::string>& allowed,const std::string& section) {
    require(object.is_object(),section+" must be an object");
    for(const auto& item:object.items())require(allowed.count(item.key()),"Unknown field in "+section);
}
int port_text(const std::string& value,const std::string& field) {
    int port=0;auto parsed=std::from_chars(value.data(),value.data()+value.size(),port);
    require(parsed.ec==std::errc{}&&parsed.ptr==value.data()+value.size()&&port>=1&&port<=65535,field+" must be an integer from 1 to 65535");return port;
}
int port_json(const json& value,const std::string& field) {
    require(value.is_number_integer()&&value>=1&&value<=65535,field+" must be an integer from 1 to 65535");return value.get<int>();
}
std::string string_field(const json& object,const char* key,const std::string& fallback,const std::string& field,bool empty=false) {
    if(!object.contains(key)){text_value(fallback,field,empty);return fallback;}
    require(object[key].is_string(),field+" must be a string");auto value=object[key].get<std::string>();text_value(value,field,empty);return value;
}
fs::path path_value(const std::string& value,const fs::path& base,const std::string& field) {
    text_value(value,field);fs::path path(value);
    if(path.is_relative())path=base/path;
    std::error_code ec;auto canonical=fs::weakly_canonical(path,ec);
    require(!ec,"Cannot resolve "+field);return canonical.lexically_normal();
}
bool ancestor(const fs::path& a,const fs::path& b) {
    auto ai=a.begin(),bi=b.begin();for(;ai!=a.end()&&bi!=b.end();++ai,++bi)if(*ai!=*bi)return false;return ai==a.end();
}
}
ServerConfig ServerConfig::load(int argc,char** argv) {
    ServerConfig result;
    if(argc==2&&std::string(argv[1])=="--help"){result.help=true;return result;}
    std::map<std::string,std::string> options;std::set<std::string> seen;
    for(int i=1;i<argc;++i){
        const std::string option=argv[i];
        require(seen.insert(option).second,"Duplicate command-line option");
        if(option=="--remote"){result.remote=true;continue;}
        if(option=="--check-config"){result.check_only=true;continue;}
        require(option=="--config"||option=="--host"||option=="--port"||option=="--notification-port","Unknown command-line option; use --help");
        require(i+1<argc,"Missing command-line option value");options[option]=argv[++i];
    }
    const auto cwd=fs::current_path();fs::path base=cwd;json root=json::object();
    if(options.count("--config")){
        // Relative fields use the directory of the specified config path, even
        // when the config itself is a symlink to a versioned file elsewhere.
        text_value(options.at("--config"),"config path");
        auto config=fs::absolute(options.at("--config")).lexically_normal();base=config.parent_path();
        root=read_json_object(config,"configuration file");
    }
    keys(root,{"server","database","storage","auth"},"configuration");
    const auto server=root.value("server",json::object());keys(server,{"host","port","notification_port","tls_notification_port"},"server");
    if(server.contains("tls_notification_port"))result.tls_notification_port=port_json(server["tls_notification_port"],"server.tls_notification_port");
    result.host=string_field(server,"host",result.host,"server.host");
    if(server.contains("port"))result.port=port_json(server["port"],"server.port");
    if(server.contains("notification_port"))result.notification_port=port_json(server["notification_port"],"server.notification_port");
    if(options.count("--host")){text_value(options.at("--host"),"--host");result.host=options.at("--host");}
    if(options.count("--port"))result.port=port_text(options.at("--port"),"--port");
    if(options.count("--notification-port"))result.notification_port=port_text(options.at("--notification-port"),"--notification-port");
    require(result.port!=result.notification_port,"HTTP and TCP notification ports must differ");

    const auto db=root.value("database",json::object());keys(db,{"host","port","user","name","socket"},"database");
    result.database.host=string_field(db,"host",env("SPARK_DB_HOST","localhost"),"database.host");
    result.database.user=string_field(db,"user",env("SPARK_DB_USER",env("USER","spark")),"database.user");
    result.database.database=string_field(db,"name",env("SPARK_DB_NAME","spark"),"database.name");
    result.database.port=db.contains("port")?port_json(db["port"],"database.port"):port_text(env("SPARK_DB_PORT","3306"),"SPARK_DB_PORT");
    result.database.password=env("SPARK_DB_PASSWORD");
    result.database.socket=string_field(db,"socket",env("SPARK_DB_SOCKET"),"database.socket",true);
    if(!result.database.socket.empty())result.database.socket=path_value(result.database.socket,db.contains("socket")?base:cwd,"database.socket").string();

    if(root.contains("storage")){
        const auto& storage=root["storage"];keys(storage,{"drives"},"storage");
        require(storage.contains("drives")&&storage["drives"].is_array()&&!storage["drives"].empty()&&storage["drives"].size()<=64,"storage.drives must be an array of 1..64 directory paths");
        for(const auto& drive:storage["drives"]){require(drive.is_string(),"storage.drives entries must be strings");result.drives.push_back(path_value(drive.get<std::string>(),base,"storage.drives"));}
    } else {
        auto drive=env("SPARK_FILE_DIR","storage/files");if(drive.empty())drive="storage/files";
        result.drives.push_back(path_value(drive,cwd,"SPARK_FILE_DIR"));
    }
    for(std::size_t i=0;i<result.drives.size();++i){
        std::error_code ec;auto status=fs::status(result.drives[i],ec);
        require(!ec||ec==std::errc::no_such_file_or_directory,"Cannot inspect storage drive");
        require(!fs::exists(status)||fs::is_directory(status),"Storage drive must be a directory");
        for(std::size_t j=0;j<i;++j)require(!ancestor(result.drives[i],result.drives[j])&&!ancestor(result.drives[j],result.drives[i]),"Storage drives must not duplicate or contain one another");
    }
    if(root.contains("auth")){
        const auto& auth=root["auth"];keys(auth,{"provider","credentials_file","browser_origin"},"auth");
        require(auth.contains("provider")&&auth.contains("credentials_file"),"auth requires provider and credentials_file");
        auto provider=string_field(auth,"provider",{},"auth.provider");require(provider=="tokens","Unsupported auth provider");
        auto path=path_value(string_field(auth,"credentials_file",{},"auth.credentials_file"),base,"auth.credentials_file");
        validate_credentials(read_json_object(path,"credentials file",true));
        std::string origin;
        if(auth.contains("browser_origin")){origin=string_field(auth,"browser_origin",{},"auth.browser_origin");require(valid_browser_origin(origin),"auth.browser_origin must be a canonical HTTPS origin without path or default port");}
        result.auth=AuthConfig{provider,path,origin};
    }
    if(result.remote || result.tls_notification_port){
        require(result.auth.has_value(),"Remote deployment requires token authentication");
        require(!result.auth->browser_origin.empty(),"Remote deployment requires auth.browser_origin");
        require(result.host=="127.0.0.1","Remote deployment requires server.host=127.0.0.1");
        require(result.tls_notification_port.has_value(),"Remote deployment requires server.tls_notification_port");
        auto authority=result.auth->browser_origin.substr(8);
        auto colon=authority.rfind(':');
        int https_port=443;
        if(colon!=std::string::npos && authority.back()!=']')https_port=port_text(authority.substr(colon+1),"HTTPS port");
        std::set<int> ports{result.port,result.notification_port,*result.tls_notification_port,https_port};
        require(ports.size()==4,"Remote HTTPS, TLS notifications and backend ports must all differ");
        result.remote=true;
    }
    return result;
}
nlohmann::json ServerConfig::public_notifications() const {
    if(!tls_notification_port)return nullptr;
    auto authority=auth->browser_origin.substr(8);
    std::string host;
    if(authority.front()=='[')host=authority.substr(1,authority.find(']')-1);
    else host=authority.substr(0,authority.find(':'));
    return {{"transport","tcp+tls"},{"host",host},{"port",*tls_notification_port},
            {"tls",{{"required",true},{"minimum_version","1.2"},{"verify_certificate",true}}}};
}
void ServerConfig::require_supported_runtime() const {
    // All configured providers have REST and TCP enforcement.
}
void ServerConfig::apply_environment() const {
    require_supported_runtime();
    auto set=[](const char* key,const std::string& value){if(setenv(key,value.c_str(),1))throw ConfigError("Cannot apply resolved runtime environment");};
    set("SPARK_DB_HOST",database.host);set("SPARK_DB_PORT",std::to_string(database.port));
    set("SPARK_DB_USER",database.user);set("SPARK_DB_NAME",database.database);set("SPARK_DB_SOCKET",database.socket);
    set("SPARK_DB_PASSWORD",database.password);set("SPARK_FILE_DIR",drives.front().string());
    json roots=json::array();for(const auto& drive:drives)roots.push_back(drive.string());
    set("SPARK_FILE_DRIVES",roots.dump());
}
json ServerConfig::summary() const {
    json paths=json::array();for(const auto& drive:drives)paths.push_back(drive.string());
    json blockers=json::array();
    // Never print the credential document, database password or environment.
    json output={{"configuration_valid",true},{"runtime_supported",blockers.empty()},{"startup_blockers",blockers},
        {"server",{{"host",host},{"port",port},{"notification_port",notification_port}}},
        {"storage",{{"drives",paths}}},{"auth_mode",auth?"required":"open"},{"remote",remote}};
    if(remote){output["browser_origin"]=auth->browser_origin;output["public_notifications"]=public_notifications();}
    return output;
}
}
