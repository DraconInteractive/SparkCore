#pragma once
#include <nlohmann/json.hpp>
#include <optional>
#include <stdexcept>
#include <string>

namespace spark {
struct DatabaseConfig {
    std::string host = "localhost";
    std::string user;
    std::string password;
    std::string database = "spark";
    std::string socket;
    unsigned int port = 3306;
    static DatabaseConfig from_environment();
};
class DatabaseError : public std::runtime_error {
public:
    using std::runtime_error::runtime_error;
};
class DeviceStore {
public:
    explicit DeviceStore(DatabaseConfig config);
    void verify() const;
    std::optional<nlohmann::json> get(const std::string& id) const;
    bool create(const nlohmann::json& device) const;
    std::optional<nlohmann::json> patch(const std::string& id, const nlohmann::json& fields) const;
    bool remove(const std::string& id) const;
private:
    DatabaseConfig config_;
};
}
