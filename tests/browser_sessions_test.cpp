#include "browser_sessions.hpp"
#include <iostream>
#include <stdexcept>
using namespace spark;
using nlohmann::json;
void check(bool ok,const char* message){if(!ok)throw std::runtime_error(message);}
struct Provider:IdentityProvider{
    bool valid=true;
    std::optional<Identity> authenticate(const std::string& token)const override{if(token=="admin")return Identity{"id","owner","administrator",{"files.read"}};if(token=="device")return Identity{"dev","watch","device",{}};return {};}
    bool current(const Identity&)const override{return valid;}
};
httplib::Request request(const std::string& method="GET") {httplib::Request req;req.method=method;req.headers.emplace("Host","spark.example");req.headers.emplace("Origin","https://spark.example");return req;}
int main(){try{
    check(valid_browser_origin("https://spark.example")&&valid_browser_origin("https://localhost:8443"),"valid origins");
    for(auto bad:{"http://spark.example","https://spark.example/","https://user@spark.example","https://spark.example:443","https://spark.example:0","https://spark.example:65536","https://spark.example?x=1"})check(!valid_browser_origin(bad),"invalid origin accepted");
    auto provider=std::make_shared<Provider>();BrowserSessions sessions(provider,"https://spark.example");
    auto login=request("POST");login.headers.emplace("X-SPARK-Login","1");login.headers.emplace("Content-Type","application/json");login.body=R"({"token":"device"})";
    httplib::Response no_device;sessions.login(login,no_device);check(no_device.status==401,"device login");
    login.body=R"({"token":"admin"})";httplib::Response logged;sessions.login(login,logged);check(logged.status==200,"admin login");
    check(json::parse(logged.body)["expires_in_seconds"]==43200,"12-hour lifetime");
    check(logged.get_header_value("Set-Cookie").find("Max-Age=43200")!=std::string::npos,"12-hour cookie");
    auto cookie=logged.get_header_value("Set-Cookie");check(cookie.find("Secure; HttpOnly; SameSite=Strict")!=std::string::npos,"cookie flags");cookie=cookie.substr(0,cookie.find(';'));
    auto csrf=json::parse(logged.body)["csrf_token"].get<std::string>();auto req=request();req.headers.emplace("Cookie",cookie);
    httplib::Response ok;check(sessions.resolve(req,ok).has_value(),"valid session");
    req.method="POST";httplib::Response no_csrf;check(!sessions.resolve(req,no_csrf)&&no_csrf.status==403,"missing CSRF");
    req.headers.emplace("X-SPARK-CSRF",csrf);httplib::Response mutation;check(sessions.resolve(req,mutation).has_value(),"valid CSRF");
    auto cross=req;cross.headers.erase("Origin");cross.headers.emplace("Origin","https://evil.example");httplib::Response rejected;check(!sessions.resolve(cross,rejected),"cross origin");
    auto duplicate=req;duplicate.headers.emplace("Cookie",cookie);httplib::Response duplicated;check(!sessions.resolve(duplicate,duplicated),"duplicate cookies");
    auto changed=req;changed.headers.erase("Host");changed.headers.emplace("Host","evil.example");httplib::Response badhost;check(!sessions.resolve(changed,badhost),"host mismatch");
    provider->valid=false;httplib::Response revoked;check(!sessions.resolve(req,revoked)&&revoked.status==401,"revocation");provider->valid=true;
    httplib::Response again;check(!sessions.resolve(req,again),"revoked session resurrected");
    httplib::Response fresh;sessions.login(login,fresh);req.headers.erase("Cookie");auto next=fresh.get_header_value("Set-Cookie");req.headers.emplace("Cookie",next.substr(0,next.find(';')));req.headers.erase("X-SPARK-CSRF");req.headers.emplace("X-SPARK-CSRF",json::parse(fresh.body)["csrf_token"]);
    httplib::Response logout;sessions.logout(req,logout);check(logout.status==204,"logout");httplib::Response after;check(!sessions.resolve(req,after),"logout invalidates");
    for(bool idle:{false,true}){BrowserSessions expired(provider,"https://spark.example",idle?std::chrono::seconds(0):std::chrono::hours(1),idle?std::chrono::hours(1):std::chrono::seconds(0));httplib::Response start;expired.login(login,start);auto value=start.get_header_value("Set-Cookie");auto probe=request();probe.headers.emplace("Cookie",value.substr(0,value.find(';')));httplib::Response end;check(!expired.resolve(probe,end),"expiry");}
    for(bool remember:{false,true})for(bool absolute:{false,true}){
        BrowserSessions timed(provider,"https://spark.example",std::chrono::seconds(0),absolute?std::chrono::seconds(0):std::chrono::hours(12));
        login.body=json{{"token","admin"},{"remember_device",remember}}.dump();httplib::Response start;timed.login(login,start);check(start.status==200,"remember login");
        auto value=start.get_header_value("Set-Cookie");auto probe=request();probe.headers.emplace("Cookie",value.substr(0,value.find(';')));httplib::Response end;
        check(timed.resolve(probe,end).has_value()==(remember&&!absolute),"remember bypasses idle only");
        if(remember&&!absolute){provider->valid=false;httplib::Response revoked;check(!timed.resolve(probe,revoked),"remember revocation");provider->valid=true;}
    }
    for(auto body:{R"({"token":"admin","remember_device":"true"})",R"({"token":"admin","remember_device":null})",R"({"token":"admin","other":true})",R"({"token":"admin","remember_device":true,"other":true})"}){login.body=body;httplib::Response invalid;sessions.login(login,invalid);check(invalid.status==400,"strict login fields");}
    std::cout<<"Browser cookie, CSRF, origin, revocation, logout and expiry checks passed.\n";return 0;
}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
