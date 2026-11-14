#pragma once
#include "authorization.hpp"
#include "actions.hpp"
#include <functional>
#include <mutex>
namespace spark {
struct AssistantConfig {
    std::filesystem::path credentials_file, state_directory;
    bool allow_open_access=false;
    std::string model="gpt-6-luna", reasoning="none";
};
void validate_assistant_config(const AssistantConfig&);
class Assistant {
public:
    using Transport=std::function<nlohmann::json(const nlohmann::json&)>;
    explicit Assistant(AssistantConfig,Transport={},std::shared_ptr<ActionStore> = nullptr);
    bool has_tools() const { return bool(actions_); }
    nlohmann::json action(const Identity&,const std::string&,bool undo);
    nlohmann::json confirmation(const Identity&,const std::string&,const std::string& decision="");
    bool allows_open_access() const { return config_.allow_open_access; }
    nlohmann::json query(const Identity&,const nlohmann::json&,std::function<bool()> current = {});
    nlohmann::json get(const Identity&,const std::string&);
private:
    std::shared_ptr<ActionStore> actions_;
    AssistantConfig config_;
    Transport transport_;
    std::mutex mutex_;
};
void configure_assistant(httplib::Server&,std::shared_ptr<Assistant>,std::shared_ptr<Authorization>);
nlohmann::json assistant_endpoints();
}
