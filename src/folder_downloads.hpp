#pragma once
#include "files.hpp"
namespace spark {
void configure_folder_downloads(httplib::Server&,std::shared_ptr<FileStorage>,std::shared_ptr<Authorization>);
nlohmann::json folder_download_endpoints();
}
