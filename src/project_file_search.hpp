#pragma once
#include "files.hpp"
namespace spark {
nlohmann::json list_project_file_names(const nlohmann::json& manifest);
// Caller holds project_gate while obtaining the manifest and reading bytes.
nlohmann::json search_project_text(FileStorage&,const nlohmann::json& manifest,const std::string& query);
}
