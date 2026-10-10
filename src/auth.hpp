#pragma once
#include <filesystem>
#include <optional>
#include <set>
#include <string>
#include <nlohmann/json.hpp>

namespace spark {
struct Identity {
    std::string credential_id;
    std::string subject;
    std::string kind; // open, device, administrator
    std::set<std::string> permissions;
};
// Resource-family permissions are global within an instance. Ownership checks
// are additional constraints, never substitutes for the permission check.
const std::set<std::string>& permission_names();
bool permitted(const Identity&, const std::string& permission);
bool acts_as(const Identity&, const std::string& device_id);
class IdentityProvider {
public:
    virtual ~IdentityProvider() = default;
    virtual std::optional<Identity> authenticate(const std::string& token) const = 0;
    virtual bool current(const Identity&) const = 0;
};
class OpenIdentityProvider final : public IdentityProvider {
public:
    std::optional<Identity> authenticate(const std::string&) const override;
    bool current(const Identity&) const override;
};
class TokenIdentityProvider final : public IdentityProvider {
public:
    explicit TokenIdentityProvider(std::filesystem::path file);
    const std::filesystem::path& credentials_file()const{return file_;}
    std::optional<Identity> authenticate(const std::string&) const override;
    bool current(const Identity&) const override;
private:
    std::filesystem::path file_;
};
// Shared strict reader for configuration and credential files.
nlohmann::json read_json_object(const std::filesystem::path&, const std::string& label, bool private_file=false);
void validate_credentials(const nlohmann::json&);
void initialize_credentials(const std::filesystem::path&);
// Returns plaintext once; only its SHA-256 digest is persisted.
nlohmann::json issue_credential(const std::filesystem::path&, const std::string& kind,
                               const std::string& subject, const std::set<std::string>& permissions);
void revoke_credential(const std::filesystem::path&, const std::string& id);
nlohmann::json list_credentials(const std::filesystem::path&);
}
