#pragma once
#include <chrono>
#include <exception>
#include <string>
#include <nlohmann/json.hpp>
namespace spark {
// Pass fixed messages and explicitly selected metadata, never request packets/secrets.
void log_event(const char* level,const std::string& component,const std::string& message,
               nlohmann::json fields=nlohmann::json::object()) noexcept;
void log_exception(const char* level,const std::string& component,const std::string& message,
                   const std::exception&,nlohmann::json fields=nlohmann::json::object()) noexcept;
// Single-worker retry state: first failure, at most one reminder/minute, recovery.
class RetryLog {
    std::string component_;
    uint64_t failures_=0;
    std::chrono::steady_clock::time_point since_,last_;
public:
    explicit RetryLog(std::string component):component_(std::move(component)){}
    void failure(const std::exception&) noexcept;
    void success() noexcept;
};
}
