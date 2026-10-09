#include "authorization.hpp"
#include <iostream>
#include <stdexcept>
using namespace spark;
void check(bool ok,const std::string& message){if(!ok)throw std::runtime_error(message);}
struct Provider : IdentityProvider {
    Identity who{"id","watch","device",{}};
    std::optional<Identity> authenticate(const std::string& token)const override{return token=="test"?std::optional<Identity>(who):std::nullopt;}
    bool current(const Identity&)const override{return true;}
};
int main(){try{
    auto provider=std::make_shared<Provider>();Authorization auth(provider);
    httplib::Request req;req.headers.emplace("Authorization","Bearer test");
    // Exercise every permission independently; removing any required grant denies.
    for(const auto& rule:route_policies()){
        for(const auto& permission:rule.permissions)check(permission_names().count(permission),"unknown policy permission");
        req.method=rule.method;req.path=std::regex_replace(rule.path,std::regex(R"(\{[^}]+\})"),"watch");
        provider->who.permissions={rule.permissions.begin(),rule.permissions.end()};
        httplib::Response allowed;check(auth.request(req,allowed),"policy unexpectedly denied: "+req.path);
        for(const auto& permission:rule.permissions){provider->who.permissions.erase(permission);httplib::Response denied;check(!auth.request(req,denied)&&denied.status==403,"missing grant accepted");provider->who.permissions.insert(permission);}
    }
    req.method="POST";req.path="/timers";provider->who.permissions={"timers.write"};
    req.body=R"({"duration_seconds":10,"source_device":"other"})";httplib::Response denied;check(!auth.body(req,denied),"impersonation");
    provider->who.permissions.insert("devices.delegate");httplib::Response delegated;check(auth.body(req,delegated),"explicit delegation");
    req.path="/unknown";provider->who.permissions=permission_names();httplib::Response unknown;check(!auth.request(req,unknown),"unknown endpoint");
    Identity reader{"id","watch","device",{"notifications.read","events.read"}};
    check(notification_permitted(reader,{{"type","calendar.reminder"}}),"reminder permission");
    for(const auto& type:{"timer.expired","file.created","project.updated","command.available","command.updated","device.updated","unknown"})check(!notification_permitted(reader,{{"type",type}}),"notification disclosure");
    reader.permissions={"commands.read","notifications.read"};check(!notification_permitted(reader,{{"type","command.available"}}),"nonexecutor availability");
    reader.permissions.insert("commands.execute");check(notification_permitted(reader,{{"type","command.available"}}),"executor availability");
    std::cout<<"Authorization route matrix, missing permissions, ownership and notification policy passed.\n";return 0;
}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
