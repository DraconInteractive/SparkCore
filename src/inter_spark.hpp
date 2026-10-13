#pragma once
#include "dashboard.hpp"
#include "files.hpp"
namespace spark {
void configure_inter_spark(httplib::Server&,std::shared_ptr<FileStorage>,std::shared_ptr<Maintenance>,std::shared_ptr<Authorization>);
nlohmann::json inter_spark_endpoints();
}
