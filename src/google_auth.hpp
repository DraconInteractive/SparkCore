#pragma once
#include "browser_sessions.hpp"
#include <filesystem>
#include <functional>
#include <memory>
namespace spark {
struct GoogleConfig {std::filesystem::path client_credentials_file,owner_file;};
void validate_google_config(const GoogleConfig&,const std::string& origin);
class Authorization;
class GoogleAuth:public std::enable_shared_from_this<GoogleAuth>{
public:
 using Fetch=std::function<nlohmann::json(const std::string&,const std::string&)>;
 GoogleAuth(GoogleConfig,std::string,std::shared_ptr<BrowserSessions>,Fetch fetch={});
 ~GoogleAuth();
 void start(const httplib::Request&,httplib::Response&,const std::optional<Identity>& enrol=std::nullopt);
 void callback(const httplib::Request&,httplib::Response&);
 nlohmann::json status()const;
 void reset(bool unlink);
 bool current(const Identity&)const;
private: struct Impl;std::unique_ptr<Impl> impl_;
};
void configure_google_auth(httplib::Server&,std::shared_ptr<Authorization>);
nlohmann::json google_endpoints();
}
