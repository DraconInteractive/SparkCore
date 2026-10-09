#include "browser_sessions.hpp"
#include <openssl/rand.h>
#include <openssl/crypto.h>
#include <regex>
#include <charconv>
#include <arpa/inet.h>
namespace spark {
namespace {
using nlohmann::json;
void reply(httplib::Response& res,int status,const json& body){res.status=status;res.set_content(body.dump(),"application/json");res.set_header("Cache-Control","no-store");}
void clear(httplib::Response& res){res.set_header("Set-Cookie",std::string(session_cookie)+"=; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age=0");}
std::string random_secret(){unsigned char b[32];if(RAND_bytes(b,sizeof(b))!=1)throw std::runtime_error("Secure randomness unavailable");std::string out;for(auto c:b){out+="0123456789abcdef"[c>>4];out+="0123456789abcdef"[c&15];}return out;}
bool equal(const std::string& a,const std::string& b){return a.size()==b.size()&&CRYPTO_memcmp(a.data(),b.data(),a.size())==0;}
std::string cookie(const httplib::Request& req){
    std::string result;unsigned count=0;
    for(const auto& h:req.headers)if(std::regex_match(h.first,std::regex("[Cc][Oo][Oo][Kk][Ii][Ee]"))){
        std::size_t start=0;while(start<h.second.size()){
            auto end=h.second.find(';',start);auto part=h.second.substr(start,end==std::string::npos?end:end-start);
            auto first=part.find_first_not_of(" \t");if(first!=std::string::npos)part.erase(0,first);
            auto eq=part.find('=');if(eq!=std::string::npos&&part.substr(0,eq)==session_cookie){++count;result=part.substr(eq+1);}
            if(end==std::string::npos)break;start=end+1;
        }
    }
    if(count!=1||result.size()!=64||result.find_first_not_of("0123456789abcdef")!=std::string::npos)return {};return result;
}
}
bool valid_browser_origin(const std::string& origin){
    std::smatch match;
    if(!std::regex_match(origin,match,std::regex(R"(https://([a-z0-9]([a-z0-9.-]*[a-z0-9])?|\[[0-9a-f:]+\])(:([0-9]+))?)")))return false;
    auto host=match[1].str();
    if(host.front()=='['){
        unsigned char address[16];char canonical[INET6_ADDRSTRLEN];auto literal=host.substr(1,host.size()-2);
        if(inet_pton(AF_INET6,literal.c_str(),address)!=1||!inet_ntop(AF_INET6,address,canonical,sizeof(canonical))||literal!=canonical)return false;
    }else{
        if(host.size()>253)return false;
        for(std::size_t pos=0;pos<host.size();){auto end=host.find('.',pos);auto label=host.substr(pos,end==std::string::npos?end:end-pos);if(label.empty()||label.size()>63||label.front()=='-'||label.back()=='-')return false;if(end==std::string::npos)break;pos=end+1;}
        if(host.find_first_not_of("0123456789.")==std::string::npos){unsigned char address[4];if(inet_pton(AF_INET,host.c_str(),address)!=1)return false;}
    }
    if(match[4].matched){auto p=match[4].str();int port=0;auto r=std::from_chars(p.data(),p.data()+p.size(),port);if(r.ec!=std::errc{}||port<1||port>65535||p=="443"||p[0]=='0')return false;}
    return origin.size()<=300;
}
BrowserSessions::BrowserSessions(std::shared_ptr<IdentityProvider> provider,std::string origin,std::chrono::seconds idle,std::chrono::seconds lifetime):provider_(std::move(provider)),origin_(std::move(origin)),idle_(idle),lifetime_(lifetime){if(!origin_.empty()&&!valid_browser_origin(origin_))throw std::runtime_error("Invalid browser origin");}
bool BrowserSessions::origin_ok(const httplib::Request& req,bool mutation)const{
    if(!enabled()||req.get_header_value_count("Host")!=1||req.get_header_value("Host")!=origin_.substr(8))return false;
    if(req.has_header("Sec-Fetch-Site")&&req.get_header_value("Sec-Fetch-Site")!="same-origin"&&req.get_header_value("Sec-Fetch-Site")!="none")return false;
    if(req.has_header("Origin"))return req.get_header_value_count("Origin")==1&&req.get_header_value("Origin")==origin_;
    return !mutation;
}
void BrowserSessions::prune(){auto now=Clock::now();for(auto it=sessions_.begin();it!=sessions_.end();)if(now-it->second.created>=lifetime_||now-it->second.seen>=idle_)it=sessions_.erase(it);else ++it;}
std::optional<Identity> BrowserSessions::resolve(const httplib::Request& req,httplib::Response& res,bool touch){
    const bool mutation=req.method!="GET"&&req.method!="HEAD";
    if(!origin_ok(req,mutation)){reply(res,403,{{"error","browser_origin_mismatch"}});return {};}
    auto id=cookie(req);std::lock_guard<std::mutex> lock(mutex_);prune();auto it=sessions_.find(id);
    if(it==sessions_.end()||!provider_->current(it->second.identity)){
        if(it!=sessions_.end())sessions_.erase(it);clear(res);reply(res,401,{{"error","unauthorized"}});return {};
    }
    if(mutation&&(req.get_header_value_count("X-SPARK-CSRF")!=1||!equal(req.get_header_value("X-SPARK-CSRF"),it->second.csrf))){reply(res,403,{{"error","csrf_failed"}});return {};}
    if(touch)it->second.seen=Clock::now();return it->second.identity;
}
void BrowserSessions::login(const httplib::Request& req,httplib::Response& res){
    if(!enabled()){reply(res,503,{{"error","browser_login_not_configured"}});return;}
    if(!origin_ok(req,true)||req.get_header_value_count("X-SPARK-Login")!=1||req.get_header_value("X-SPARK-Login")!="1"){reply(res,403,{{"error","browser_origin_mismatch"}});return;}
    if(req.has_header("Authorization")){reply(res,400,{{"error","ambiguous_credentials"}});return;}
    if(req.get_header_value("Content-Type").substr(0,req.get_header_value("Content-Type").find(';'))!="application/json"){reply(res,415,{{"error","unsupported_media_type"}});return;}
    auto packet=json::parse(req.body,nullptr,false);
    if(!packet.is_object()||packet.size()!=1||!packet.contains("token")||!packet["token"].is_string()){reply(res,400,{{"error","invalid_packet"}});return;}
    auto who=provider_->authenticate(packet["token"].get<std::string>());
    if(!who||who->kind!="administrator"){reply(res,401,{{"error","invalid_login"}});return;}
    auto id=random_secret(),csrf=random_secret();std::lock_guard<std::mutex> lock(mutex_);prune();
    auto old=cookie(req);if(!old.empty())sessions_.erase(old);
    if(sessions_.size()>=256){reply(res,429,{{"error","session_limit"}});return;}
    auto now=Clock::now();sessions_.emplace(id,Session{*who,csrf,now,now});
    res.set_header("Set-Cookie",std::string(session_cookie)+"="+id+"; Path=/; Secure; HttpOnly; SameSite=Strict; Max-Age="+std::to_string(lifetime_.count()));
    reply(res,200,{{"mode","session"},{"subject",who->subject},{"permissions",who->permissions},{"csrf_token",csrf},{"expires_in_seconds",lifetime_.count()}});
}
void BrowserSessions::session(const httplib::Request& req,httplib::Response& res){
    auto who=resolve(req,res,false);if(!who)return;
    std::lock_guard<std::mutex> lock(mutex_);auto it=sessions_.find(cookie(req));if(it==sessions_.end()){reply(res,401,{{"error","unauthorized"}});return;}
    reply(res,200,{{"mode","session"},{"subject",who->subject},{"permissions",who->permissions},{"csrf_token",it->second.csrf}});
}
void BrowserSessions::logout(const httplib::Request& req,httplib::Response& res){
    if(!resolve(req,res))return;
    std::lock_guard<std::mutex> lock(mutex_);sessions_.erase(cookie(req));clear(res);res.status=204;
}
}
