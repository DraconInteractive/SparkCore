#pragma once
#include <httplib.h>
#include <memory>
#include <nlohmann/json.hpp>
namespace spark {
class DeviceStore;
void configure_devices(httplib::Server& server, std::shared_ptr<DeviceStore> registry);
nlohmann::json device_endpoints();
}
