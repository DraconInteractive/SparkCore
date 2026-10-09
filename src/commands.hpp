#pragma once
#include "database.hpp"
#include <httplib.h>
#include <condition_variable>
#include <mutex>
#include <thread>
#include <memory>
namespace spark {
class CommandError : public std::runtime_error {
public:
    int status;
    CommandError(int code,const char* message):std::runtime_error(message),status(code){}
};
class CommandStore {
    DatabaseConfig config_;
public:
    explicit CommandStore(DatabaseConfig config):config_(std::move(config)){}
    void verify() const;
    nlohmann::json providers(int limit,int offset) const;
    nlohmann::json actions(const std::string& device, const nlohmann::json& replacement=nullptr) const;
    nlohmann::json create(const nlohmann::json&) const;
    nlohmann::json get(const std::string&) const;
    nlohmann::json list(const nlohmann::json&) const;
    nlohmann::json claim(const std::string&,int lease_seconds,const std::optional<std::string>& command_id = std::nullopt) const;
    nlohmann::json transition(const std::string&,const std::string&,const nlohmann::json&) const;
    void expire() const;
};
class CommandScheduler {
    std::mutex mutex_;
    std::condition_variable wake_;
    bool stopping_=false;
    std::thread worker_;
public:
    explicit CommandScheduler(std::shared_ptr<CommandStore>);
    ~CommandScheduler();
};
void configure_commands(httplib::Server&,std::shared_ptr<CommandStore>);
nlohmann::json command_endpoints();
}
