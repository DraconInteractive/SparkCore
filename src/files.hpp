#pragma once
#include "database.hpp"
#include <httplib.h>
#include <filesystem>
#include <mutex>
namespace spark {
class FileStore {
public:
    explicit FileStore(DatabaseConfig config): config_(std::move(config)) {}
    void verify() const;
    std::string begin(const std::string& name, const std::string& directory = "/") const;
    nlohmann::json publish(const std::string& id, std::uint64_t size) const;
    std::optional<nlohmann::json> get(const std::string& id) const;
    nlohmann::json list(int limit, int offset, const std::optional<std::string>& directory = std::nullopt) const;
    std::optional<nlohmann::json> move(const std::string& id, const std::string& directory) const;
    nlohmann::json directories() const;
    void create_directory(const std::string& directory) const;
    bool deleting(const std::string& id) const;
    void forget(const std::string& id) const;
    nlohmann::json unfinished() const;
private:
    DatabaseConfig config_;
};
class FileStorage {
public:
    FileStorage(std::shared_ptr<FileStore> store, std::filesystem::path root);
    ~FileStorage();
    void initialize();
    void erase_bytes(const std::string& id);
    void sync_directory();
    std::filesystem::path path(const std::string& id, bool partial=false) const;
    std::shared_ptr<FileStore> store;
    static std::filesystem::path default_root();
private:
    std::filesystem::path root_;
    std::once_flag initialized_;
    int lock_=-1;
};
void configure_files(httplib::Server&,std::shared_ptr<FileStorage>);
nlohmann::json file_endpoints();
}
