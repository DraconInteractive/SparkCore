#pragma once
#include "files.hpp"
namespace spark {
class ProjectStore {
public:
    explicit ProjectStore(DatabaseConfig config):config_(std::move(config)){}
    void verify() const;
    nlohmann::json create(const nlohmann::json&) const;
    std::optional<nlohmann::json> get(const std::string&,bool deep = false) const;
    nlohmann::json list(int limit,int offset,const std::optional<std::string>& parent = std::nullopt) const;
    std::optional<nlohmann::json> patch(const std::string&,const nlohmann::json&) const;
    std::optional<nlohmann::json> remove(const std::string&,bool destroy) const;
private:
    DatabaseConfig config_;
};
void configure_projects(httplib::Server&,std::shared_ptr<ProjectStore>,std::shared_ptr<FileStorage>);
nlohmann::json project_endpoints();
}
