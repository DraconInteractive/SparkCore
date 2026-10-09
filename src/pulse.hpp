#pragma once
#include "authorization.hpp"
#include "database.hpp"
#include <httplib.h>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>
namespace spark {
class PulseStore {
public:
    explicit PulseStore(DatabaseConfig config) : config_(std::move(config)) {}
    void verify() const;
    nlohmann::json get() const;
    nlohmann::json patch(const nlohmann::json& packet) const;
    void tick() const;
    void subscribe(const std::string& device_id) const;
    void unsubscribe(const std::string& device_id) const;
private:
    DatabaseConfig config_;
};
class PulseScheduler {
public:
    explicit PulseScheduler(std::shared_ptr<PulseStore> store);
    ~PulseScheduler();
    PulseScheduler(const PulseScheduler&) = delete;
    PulseScheduler& operator=(const PulseScheduler&) = delete;
private:
    std::mutex mutex_;
    std::condition_variable wake_;
    bool stopping_ = false;
    std::thread worker_;
};
void configure_pulse(httplib::Server& server, std::shared_ptr<PulseStore> store, std::shared_ptr<Authorization> auth = nullptr);
nlohmann::json pulse_endpoints();
}
