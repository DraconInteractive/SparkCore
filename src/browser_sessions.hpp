#pragma once
#include "auth.hpp"
#include <httplib.h>
#include <chrono>
#include <memory>
#include <mutex>
#include <unordered_map>
namespace spark {
bool valid_browser_origin(const std::string&);
class BrowserSessions {
public:
    using Clock=std::chrono::steady_clock;
    BrowserSessions(std::shared_ptr<IdentityProvider> provider,std::string origin,
                    std::chrono::seconds idle=std::chrono::minutes(30),std::chrono::seconds lifetime=std::chrono::hours(8));
    std::optional<Identity> resolve(const httplib::Request&,httplib::Response&,bool touch=true);
    void login(const httplib::Request&,httplib::Response&);
    void session(const httplib::Request&,httplib::Response&);
    void logout(const httplib::Request&,httplib::Response&);
    bool enabled()const{return !origin_.empty();}
private:
    struct Session {Identity identity;std::string csrf;Clock::time_point created,seen;};
    std::shared_ptr<IdentityProvider> provider_;
    std::string origin_;
    std::chrono::seconds idle_,lifetime_;
    std::mutex mutex_;
    std::unordered_map<std::string,Session> sessions_;
    bool origin_ok(const httplib::Request&,bool mutation)const;
    void prune();
};
inline constexpr const char* session_cookie="__Host-spark_session";
}
