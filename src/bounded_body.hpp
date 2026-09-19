#pragma once
#include <httplib.h>
namespace spark {
// JSON routes remain bounded even though the file reader accepts arbitrary sizes.
inline void body_route(httplib::Server& server, const std::string& method,
                       const std::string& path, httplib::Server::Handler handler) {
    httplib::Server::HandlerWithContentReader reader=[handler](const auto& request,auto& response,const httplib::ContentReader& read) {
        auto reject=[&](int status,const char* message){response.status=status;response.set_header("Connection","close");response.set_content(message,"application/json");};
        if(request.is_multipart_form_data()) {reject(415,R"({"error":"unsupported_media_type"})");return;}
        std::string body; bool large=false;
        if(!read([&](const char* data,std::size_t size){if(size>16384-body.size()){large=true;return false;}body.append(data,size);return true;})) {
            reject(large?413:400,large?R"({"error":"payload_too_large"})":R"({"error":"invalid_body"})");return;
        }
        auto copied=request;copied.body=std::move(body);handler(copied,response);
    };
    if(method=="POST"){server.Post(path,handler);server.Post(path,reader);}
    else if(method=="PATCH"){server.Patch(path,handler);server.Patch(path,reader);}
    else if(method=="PUT"){server.Put(path,handler);server.Put(path,reader);}
    else if(method=="DELETE"){server.Delete(path,handler);server.Delete(path,reader);}
}
}
