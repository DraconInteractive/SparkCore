#pragma once
#include "authorization.hpp"
#include "database.hpp"
#include <httplib.h>
#include <memory>
#include <mutex>
#include <condition_variable>
#include <thread>
namespace spark {
class DailyTaskStore {
public:
    explicit DailyTaskStore(DatabaseConfig config) : config_(std::move(config)) {}
    void verify() const;
    void tick() const;
    nlohmann::json list() const;
    nlohmann::json create(const nlohmann::json& packet) const;
    std::optional<nlohmann::json> get(const std::string& id) const;
    std::optional<nlohmann::json> update(const std::string& id, const nlohmann::json& packet, bool check_state) const;
    bool remove(const std::string& id) const;
private:
    DatabaseConfig config_;
};
class DailyTaskScheduler {
public:
    explicit DailyTaskScheduler(std::shared_ptr<DailyTaskStore> store);
    ~DailyTaskScheduler();
    DailyTaskScheduler(const DailyTaskScheduler&) = delete;
    DailyTaskScheduler& operator=(const DailyTaskScheduler&) = delete;
private:
    std::mutex mutex_;
    std::condition_variable wake_;
    bool stopping_ = false;
    std::thread worker_;
};
void configure_daily_tasks(httplib::Server& server, std::shared_ptr<DailyTaskStore> store, std::shared_ptr<Authorization> auth = nullptr);
nlohmann::json daily_task_endpoints();
}
