#pragma once
#include "authorization.hpp"
#include "database.hpp"
#include <httplib.h>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

namespace spark {
class TimerConflict : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};
class TimerStore {
public:
    explicit TimerStore(DatabaseConfig config) : config_(std::move(config)) {}
    void verify() const;
    nlohmann::json create(const nlohmann::json& packet) const;
    nlohmann::json active() const;
    std::optional<nlohmann::json> get(const std::string& id) const;
    std::optional<nlohmann::json> transition(const std::string& id, const std::string& action,
                                          const nlohmann::json& source_device) const;
    void expire() const;
private:
    DatabaseConfig config_;
};
class TimerScheduler {
public:
    explicit TimerScheduler(std::shared_ptr<TimerStore> store);
    ~TimerScheduler();
    TimerScheduler(const TimerScheduler&) = delete;
    TimerScheduler& operator=(const TimerScheduler&) = delete;
private:
    std::mutex mutex_;
    std::condition_variable wake_;
    bool stopping_ = false;
    std::thread worker_;
};
void configure_timers(httplib::Server& server, std::shared_ptr<TimerStore> store, std::shared_ptr<Authorization> auth = nullptr);
nlohmann::json timer_endpoints();
}
