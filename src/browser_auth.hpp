#pragma once
#include "authorization.hpp"
namespace spark {
bool browser_public_route(const httplib::Request&);
void configure_browser_auth(httplib::Server&,std::shared_ptr<Authorization>);
nlohmann::json browser_endpoints();
}
