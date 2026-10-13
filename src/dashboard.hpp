#pragma once
#include "authorization.hpp"
#include <httplib.h>
#include <nlohmann/json.hpp>
#include <atomic>
#include <mutex>
#include <string>
namespace spark {
struct Maintenance {
    std::mutex mutex;
    std::atomic<int> requested{0}; // 1 restart, 2 backup
    std::string mount, destination;
    std::atomic<bool> transfer_active{false};
    nlohmann::json result = {{"state","idle"}};
    std::string control_directory;
    bool initialized=false;
    nlohmann::json backup_status={{"state","idle"}};
    void initialize();
    nlohmann::json snapshot();
    nlohmann::json estimate(const std::string& mount,const std::string& destination);
    void begin_backup(const nlohmann::json& estimate); // caller holds mutex
    void progress(const nlohmann::json& patch);
    void record_success();
    void execute();
};
void configure_dashboard(httplib::Server&, std::shared_ptr<Maintenance>, std::shared_ptr<Authorization> auth = nullptr);
void configure_backup_visibility(httplib::Server&,std::shared_ptr<Maintenance>,std::shared_ptr<Authorization>);
void run_maintenance_listener(const std::string& host,int port,std::shared_ptr<Maintenance>,std::shared_ptr<Authorization>);
nlohmann::json backup_visibility_endpoints();
nlohmann::json dashboard_endpoints();
}
