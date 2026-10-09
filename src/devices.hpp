#pragma once
#include "authorization.hpp"
#include <httplib.h>
#include <memory>
#include <nlohmann/json.hpp>
namespace spark {
class DeviceStore;
void configure_devices(httplib::Server& server, std::shared_ptr<DeviceStore> registry, std::shared_ptr<Authorization> auth = nullptr);
nlohmann::json device_endpoints();
}
