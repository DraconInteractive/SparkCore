#pragma once
#include "authorization.hpp"
namespace spark {
void configure_credentials(httplib::Server&,std::shared_ptr<Authorization>);
nlohmann::json credential_endpoints();
}
