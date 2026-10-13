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
    void execute();
};
void configure_dashboard(httplib::Server&, std::shared_ptr<Maintenance>, std::shared_ptr<Authorization> auth = nullptr);
nlohmann::json dashboard_endpoints();
}
