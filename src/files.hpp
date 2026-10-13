#pragma once
#include "authorization.hpp"
#include "database.hpp"
#include <httplib.h>
#include <filesystem>
#include <mutex>
#include <shared_mutex>
#include <vector>
namespace spark {
class FileStore {
public:
    explicit FileStore(DatabaseConfig config): config_(std::move(config)) {}
    void verify() const;
    std::uint64_t database_bytes() const;
    nlohmann::json layout() const;
    void register_drives(const std::vector<std::string>& drives,const nlohmann::json& assignments, const nlohmann::json& roots, bool anchor) const;
    nlohmann::json drives() const;
    void detach_drive(const std::string& id) const;
    std::optional<std::string> location(const std::string& id) const;
    nlohmann::json replace(const std::string& id, const std::string& revision, std::uint64_t size) const;
    std::string begin(const std::string& name, const std::string& directory = "/", const std::optional<std::string>& drive = std::nullopt) const;
    nlohmann::json publish(const std::string& id, std::uint64_t size) const;
    std::optional<nlohmann::json> get(const std::string& id) const;
    nlohmann::json list(int limit, int offset, const std::optional<std::string>& directory = std::nullopt, const std::optional<std::string>& drive = std::nullopt) const;
    std::optional<nlohmann::json> move(const std::string& id, const std::string& directory) const;
    nlohmann::json directories(const std::optional<std::string>& drive = std::nullopt) const;
    std::string default_drive(const std::string& directory = "/") const;
    void create_directory(const std::string& directory, const std::optional<std::string>& drive = std::nullopt) const;
    nlohmann::json change_directory(const std::string& drive, const std::string& directory, const std::optional<std::string>& destination, bool recursive) const;
    bool deleting(const std::string& id) const;
    void forget(const std::string& id) const;
    nlohmann::json unfinished() const;
private:
    DatabaseConfig config_;
};
class FileStorage {
public:
    FileStorage(std::shared_ptr<FileStore> store, std::filesystem::path root);
    FileStorage(std::shared_ptr<FileStore> store, std::vector<std::filesystem::path> roots);
    ~FileStorage();
    std::string begin_upload(const std::string&,const std::string&,std::optional<std::uint64_t>, const std::optional<std::string>& letter = std::nullopt);
    nlohmann::json drives() const;
    nlohmann::json attach(const std::filesystem::path& root);
    void detach(const std::string& letter);
    std::string resolve_drive(const std::string& letter) const;
    void require_drive_online(const std::string& id) const;
    bool drive_online(const std::string& id) const;
    void initialize();
    std::filesystem::path content_path(const nlohmann::json& record) const;
    void erase_bytes(const std::string& id);
    void sync_directory();
    std::filesystem::path path(const std::string& id, bool partial=false) const;
    std::shared_ptr<FileStore> store;
    // File requests share this gate; project mutations take exclusive ownership.
    std::shared_mutex project_gate;
    static std::filesystem::path default_root();
    static std::vector<std::filesystem::path> default_roots();
private:
    struct Drive {std::filesystem::path root;std::string id;int lock=-1;int fd=-1;};
    std::vector<Drive> drives_;
    void mount_roots(bool startup);
    bool online(std::size_t index) const;
    std::filesystem::path drive_path(std::size_t index) const;
    std::size_t drive_index(const nlohmann::json& id) const;
    std::mutex allocation_;
    std::size_t next_drive_=0;
    std::once_flag initialized_;

};
void configure_files(httplib::Server&,std::shared_ptr<FileStorage>, std::shared_ptr<Authorization> auth = nullptr);
nlohmann::json file_endpoints();
}
