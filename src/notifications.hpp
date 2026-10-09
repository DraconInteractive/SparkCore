#pragma once
#include "database.hpp"
#include "authorization.hpp"
#include <memory>
#include <chrono>
#include <unordered_set>
#include <limits>

namespace spark {
class NotificationStore {
public:
    explicit NotificationStore(DatabaseConfig config) : config_(std::move(config)) {}
    void verify() const;
    std::unordered_set<std::string> pulse_subscribers() const;
    void sequence_events() const;
    std::unordered_set<std::string> device_actions(const std::string& device) const;
    unsigned long long latest() const;
    nlohmann::json after(unsigned long long sequence, unsigned long long through = std::numeric_limits<unsigned long long>::max()) const;
private:
    DatabaseConfig config_;
};
struct NotificationOptions {
    std::shared_ptr<IdentityProvider> identity_provider = std::make_shared<OpenIdentityProvider>();
    std::string host = "0.0.0.0";
    int port = 47684;
    std::chrono::milliseconds heartbeat{30000};
    std::chrono::milliseconds idle_timeout{90000};
    std::chrono::milliseconds hello_timeout{10000};
};
class NotificationServer {
public:
    NotificationServer(std::shared_ptr<NotificationStore> store, NotificationOptions options = {});
    ~NotificationServer();
    NotificationServer(const NotificationServer&) = delete;
    NotificationServer& operator=(const NotificationServer&) = delete;
    int port() const;
private:
    struct Impl;
    std::unique_ptr<Impl> impl_;
};
nlohmann::json notification_discovery(int port);
}
