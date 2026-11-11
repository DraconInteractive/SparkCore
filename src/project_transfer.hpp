#pragma once
#include "projects.hpp"
namespace spark {
// Used only under FileStorage::project_gate. Applying a reviewed plan is one SQL transaction.
class ProjectTransferStore {
public:
 explicit ProjectTransferStore(DatabaseConfig c):config_(std::move(c)){}
 nlohmann::json snapshot(const std::string& drive) const;
 void apply(const std::string& drive,const nlohmann::json& expected,const nlohmann::json& operations,const nlohmann::json& directories) const;
private: DatabaseConfig config_;
};
void configure_project_transfers(httplib::Server&,std::shared_ptr<FileStorage>,std::shared_ptr<Authorization>);
nlohmann::json project_transfer_endpoints();
}
