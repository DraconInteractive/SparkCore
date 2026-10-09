#pragma once
#include "files.hpp"
namespace spark {
class BoardStore {
public:
    explicit BoardStore(DatabaseConfig config):config_(std::move(config)){}
    void verify() const;
    nlohmann::json execute(const std::string& project,const std::string& board,
        const std::string& kind,const std::string& id,const std::string& method,
        const nlohmann::json& packet) const;
private:
    DatabaseConfig config_;
};
void configure_boards(httplib::Server&,std::shared_ptr<BoardStore>,std::shared_ptr<FileStorage>);
nlohmann::json board_endpoints();
}
