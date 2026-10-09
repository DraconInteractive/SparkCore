#pragma once
#include "auth.hpp"
#include "browser_sessions.hpp"
#include <httplib.h>
#include <memory>
#include <regex>
#include <vector>
namespace spark {
// Explicit allowlist shared by routing, JSON bodies, and notification resync.
struct RoutePolicy {
    std::string method, path;
    std::vector<std::string> permissions;
    std::regex pattern;
    bool own_device = false;
};
const std::vector<RoutePolicy>& route_policies();
bool notification_permitted(const Identity&, const nlohmann::json& event);
class Authorization {
public:
    explicit Authorization(std::shared_ptr<IdentityProvider> provider,std::string browser_origin={}) : provider_(std::move(provider)), sessions_(std::make_shared<BrowserSessions>(provider_,std::move(browser_origin))) {}
    std::shared_ptr<BrowserSessions> sessions()const{return sessions_;}
    std::optional<Identity> identity(const httplib::Request& req,httplib::Response& res)const{return resolve(req,res);}
    bool request(const httplib::Request&, httplib::Response&) const;
    bool body(const httplib::Request&, httplib::Response&) const;
    std::shared_ptr<IdentityProvider> provider() const {return provider_;}
private:
    std::optional<Identity> resolve(const httplib::Request&, httplib::Response&) const;
    std::shared_ptr<IdentityProvider> provider_;
    std::shared_ptr<BrowserSessions> sessions_;
};
}
