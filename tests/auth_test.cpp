#include "auth.hpp"
#include <fstream>
#include <iostream>
#include <sys/stat.h>
#include <unistd.h>
using namespace spark;
void check(bool ok){if(!ok)throw std::runtime_error("Authentication assertion failed");}
template<class F> void rejects(F f){bool rejected=false;try{f();}catch(const std::exception&){rejected=true;}check(rejected);}
int main(){
    char pattern[]="/tmp/spark-auth-XXXXXX";auto root=mkdtemp(pattern);if(!root)return 1;
    auto file=std::filesystem::path(root)/"credentials.json";
    try {
        OpenIdentityProvider open;auto anon=*open.authenticate("");check(permitted(anon,"files.write")&&acts_as(anon,"other")&&!permitted(anon,"bogus"));
        initialize_credentials(file);rejects([&]{initialize_credentials(file);});
        auto issued=issue_credential(file,"device","watch-1",{"events.read","notifications.read"});
        auto token=issued["token"].get<std::string>();TokenIdentityProvider provider(file);
        auto who=provider.authenticate(token);check(who.has_value());
        check(permitted(*who,"events.read")&&!permitted(*who,"events.write")&&acts_as(*who,"watch-1")&&!acts_as(*who,"phone-1"));
        check(provider.current(*who));check(!provider.authenticate("")&&!provider.authenticate(token+"x"));
        auto altered=token;altered.back()=altered.back()=='a'?'b':'a';check(!provider.authenticate(altered));
        auto document=read_json_object(file,"credentials",true);check(document.dump().find(token)==std::string::npos);
        check(!list_credentials(file)[0].contains("token_sha256"));
        for(auto permission:permission_names())check(permitted(*who,permission)==(permission=="events.read"||permission=="notifications.read"));
        rejects([&]{issue_credential(file,"device","bad",{"admin.restart"});});
        rejects([&]{issue_credential(file,"device","bad",{"files.*"});});
        auto admin=issue_credential(file,"administrator","owner",{"admin.restart"});auto administrator=provider.authenticate(admin["token"]);check(administrator&&permitted(*administrator,"admin.restart")&&!permitted(*administrator,"files.read")&&!acts_as(*administrator,"watch-1"));
        auto delegated=issue_credential(file,"administrator","delegate",{"devices.delegate","devices.write"});check(acts_as(*provider.authenticate(delegated["token"]),"watch-1"));
        revoke_credential(file,issued["id"]);check(!provider.authenticate(token)&&!provider.current(*who));check(provider.authenticate(admin["token"]).has_value());
        revoke_credential(file,issued["id"]);rejects([&]{revoke_credential(file,"absent");});
        auto invalid=document;invalid["credentials"][0]["extra"]="secret";rejects([&]{validate_credentials(invalid);});
        invalid=document;invalid["credentials"].push_back(invalid["credentials"][0]);rejects([&]{validate_credentials(invalid);});
        invalid=document;invalid["credentials"][0]["revoked"]=0;rejects([&]{validate_credentials(invalid);});
        chmod(file.c_str(),0644);rejects([&]{TokenIdentityProvider other(file);});check(!provider.authenticate(admin["token"])&&!provider.current(*administrator));chmod(file.c_str(),0600);
        std::ofstream(file)<<"{\"version\":1,\"version\":1,\"credentials\":[]}";rejects([&]{TokenIdentityProvider other(file);});check(!provider.authenticate(admin["token"]));
        std::filesystem::remove(file);check(!provider.current(*administrator));
        auto target=std::filesystem::path(root)/"target";std::ofstream(target)<<"{}";std::filesystem::create_symlink(target,file);rejects([&]{TokenIdentityProvider other(file);});
        std::filesystem::remove_all(root);std::cout<<"Open mode, permissions, ownership, issue, revocation and fail-closed credential validation passed.\n";return 0;
    }catch(const std::exception& error){std::cerr<<error.what()<<'\n';std::filesystem::remove_all(root);return 1;}
}
