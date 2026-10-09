#pragma once
#include "database.hpp"
#include <filesystem>
#include <optional>
#include <vector>

namespace spark {
class ConfigError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};
struct AuthConfig {
    std::string provider;
    std::filesystem::path credentials_file;
};
struct ServerConfig {
    std::string host = "0.0.0.0";
    int port = 47683;
    int notification_port = 47684;
    DatabaseConfig database;
    std::vector<std::filesystem::path> drives;
    std::optional<AuthConfig> auth;
    bool check_only = false;
    bool help = false;

    static ServerConfig load(int argc, char** argv);
    // A structurally valid config may request functionality not implemented yet.
    void require_supported_runtime() const;
    // Called once, before any worker thread. Existing stores and backup subprocesses
    // consume these resolved values through their existing environment interfaces.
    void apply_environment() const;
    nlohmann::json summary() const;
};
}
