#include "api.hpp"
#include "files.hpp"
#include "timers.hpp"
#include <mysql.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <unistd.h>
#include <fstream>
#include <thread>
#include <iostream>
using nlohmann::json;
void check(bool value,const char* message){if(!value)throw std::runtime_error(message);}
json expect(const httplib::Result& result,int status,const char* error=nullptr){
    if(!result || result->status!=status)throw std::runtime_error("File HTTP status expected "+std::to_string(status)+" got "+(result?std::to_string(result->status)+" "+result->body:httplib::to_string(result.error())));
    if(status==204)return nullptr;auto body=json::parse(result->body);if(error)check(body["error"]==error,"file error code");return body;
}
struct DB {
    MYSQL* db;
    explicit DB(const spark::DatabaseConfig& c):db(mysql_init(nullptr)){check(db && mysql_real_connect(db,c.host.c_str(),c.user.c_str(),c.password.c_str(),c.database.c_str(),c.port,c.socket.empty()?nullptr:c.socket.c_str(),0),"SQL connect");}
    ~DB(){mysql_close(db);}
    void run(const std::string& sql){if(mysql_query(db,sql.c_str()))throw std::runtime_error(mysql_error(db));}
};
std::string content_url(const json& file){return file["download_url"];}
int main(int argc,char**){
    auto config=spark::DatabaseConfig::from_environment();if(config.database.rfind("spark_test_",0)!=0 || !std::getenv("SPARK_FILE_DIR"))return 77;
    auto store=std::make_shared<spark::FileStore>(config);store->verify();
    const auto root=spark::FileStorage::default_root();const bool verify=argc>1;
    auto storage=std::make_shared<spark::FileStorage>(store,root);
    std::string pending;
    if(!verify){
        std::filesystem::create_directories(root);
        pending=store->begin("Interrupted");std::ofstream(storage->path(pending,true))<<"partial";
        auto deleting=store->begin("Deleting");std::ofstream(storage->path(deleting))<<"x";store->publish(deleting,1);store->deleting(deleting);
        auto publishing=store->begin("Publishing");std::ofstream(storage->path(publishing))<<"uncommitted";
    }
    storage->initialize();
    if(!verify){check(store->unfinished().empty() && !std::filesystem::exists(storage->path(pending,true)),"startup recovery");bool locked=false;try{spark::FileStorage second(store,root);second.initialize();}catch(const std::exception&){locked=true;}check(locked,"exclusive storage lock");}
    int result=0;
    {
        httplib::Server server;spark::configure_api(server,std::make_shared<spark::DeviceStore>(config),std::make_shared<spark::TimerStore>(config),47684,nullptr,nullptr,nullptr,storage);
        const int port=server.bind_to_any_port("127.0.0.1");if(port<0)return 1;
        std::thread worker([&]{server.listen_after_bind();});server.wait_until_ready();
        try {
            httplib::Client client("127.0.0.1",port);client.set_read_timeout(10,0);
            if(verify){
                auto listing=expect(client.Get("/files"),200);bool found=false;
                for(auto& file:listing["files"])if(file["name"]=="Persistent.bin"){
                    auto data=client.Get(content_url(file));check(data && data->status==200 && data->body==std::string("\0persist\xff",9),"file bytes survive restart");found=true;
                }
                check(found,"metadata survives restart");
                check(expect(client.Get("/files/directories"),200)["directories"].dump().find("/empty/nested")!=std::string::npos,"empty directories survive restart");
            }else{
                DB db(config);
                expect(client.Post("/files?name=wrong.txt","text","text/plain"),415,"unsupported_media_type");
                for(const std::string name:{"..","../x","%2Fetc%2Fpasswd","a%0Ab","a%5Cb"})expect(client.Post("/files?name="+name,"x","application/octet-stream"),400,"invalid_filename");
                expect(client.Post("/files?name=a&name=b","x","application/octet-stream"),400,"invalid_parameters");
                auto upload=[&](const std::string& name,const std::string& bytes){return expect(client.Post("/files?name="+name,bytes,"application/octet-stream"),201);};
                auto empty=upload("empty","");auto empty_response=client.Get(content_url(empty));check(empty_response && empty_response->status==200 && empty_response->body.empty(),"empty file");
                auto html=upload("a%22%F0%9F%8C%B3.html","<script>alert(1)</script>");auto html_response=client.Get(content_url(html));
                check(html_response && html_response->get_header_value("Content-Type")=="application/octet-stream" && html_response->get_header_value("X-Content-Type-Options")=="nosniff" && html_response->get_header_value("Content-Disposition").find("a\"🌳.html")!=std::string::npos,"arbitrary type and encoded attachment filename");
                // httplib decodes percent escapes in received headers; inspect the wire too.
                int header_fd=socket(AF_INET,SOCK_STREAM,0);sockaddr_in header_addr{};header_addr.sin_family=AF_INET;header_addr.sin_port=htons(port);inet_pton(AF_INET,"127.0.0.1",&header_addr.sin_addr);
                check(connect(header_fd,reinterpret_cast<sockaddr*>(&header_addr),sizeof(header_addr))==0,"header connect");
                auto header_request="HEAD "+content_url(html)+" HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n";
                check(send(header_fd,header_request.data(),header_request.size(),MSG_NOSIGNAL)==static_cast<ssize_t>(header_request.size()),"header send");
                std::string wire;char header_buffer[4096];ssize_t header_count;
                while((header_count=recv(header_fd,header_buffer,sizeof(header_buffer),0))>0)wire.append(header_buffer,header_count);close(header_fd);
                check(wire.find("filename*=UTF-8''a%22%F0%9F%8C%B3.html")!=std::string::npos,"UTF-8 filename encoded on wire");
                std::string block(65536,'\0');for(std::size_t i=0;i<block.size();++i)block[i]=static_cast<char>(i%256);
                const std::size_t total=3*1024*1024+17;
                bool polled=false;
                auto large=expect(client.Post("/files?name=large.bin",total,[&](std::size_t offset,std::size_t length,httplib::DataSink& sink){
                    if(!polled){httplib::Client other("127.0.0.1",port);auto health=other.Get("/health");check(health && health->status==200,"REST during upload");polled=true;}
                    return sink.write(block.data(),std::min(length,block.size()));
                },"application/octet-stream"),201);
                check(large["size_bytes"]==total,"large upload size");
                std::size_t received=0;auto download=client.Get(content_url(large),[&](const char* bytes,std::size_t length){for(std::size_t i=0;i<length;++i)check(static_cast<unsigned char>(bytes[i])==(received+i)%256,"binary streaming integrity");received+=length;return true;});
                check(download && download->status==200 && received==total,"streaming download");
                auto head=client.Head(content_url(large));check(head && head->status==200 && head->body.empty() && head->get_header_value("Content-Length")==std::to_string(total),"HEAD size");
                auto range=client.Get(content_url(large),{{"Range","bytes=250-269"}});check(range && range->status==206 && range->body==block.substr(250,20),"byte-range resume");
                auto bad_range=client.Get(content_url(large),{{"Range","bytes=999999999-"}});check(bad_range && bad_range->status==416,"invalid range");
                auto chunked=expect(client.Post("/files?name=chunked.bin",[&](std::size_t offset,httplib::DataSink& sink){if(offset>=block.size()){sink.done();return true;}return sink.write(block.data()+offset,std::min<std::size_t>(8192,block.size()-offset));},"application/octet-stream"),201);
                check(client.Get(content_url(chunked))->body==block,"chunked upload");
                auto duplicate=upload("large.bin","different");check(duplicate["id"]!=large["id"],"duplicate names do not overwrite");
                auto list=expect(client.Get("/files?limit=2"),200);check(list["files"].size()==2 && list["has_more"]==true,"file pagination");
                expect(client.Get("/files?limit=0"),400,"invalid_pagination");expect(client.Get("/files?extra=x"),400,"invalid_parameters");
                expect(client.Get("/files/not-a-uuid"),400,"invalid_file_id");
                expect(client.Get("/files/00000000-0000-0000-0000-000000000000"),404,"file_not_found");
                auto options=client.Options("/files");check(options && options->status==204 && options->get_header_value("Allow")=="GET, HEAD, POST, OPTIONS","file OPTIONS");
                expect(client.Put("/files","","application/json"),405,"method_not_allowed");
                expect(client.Post("/timers",std::string(20000,'x'),"application/json"),413,"payload_too_large");
                expect(client.Post("/timers",[&](std::size_t offset,httplib::DataSink& sink){if(offset>=20000){sink.done();return true;}return sink.write(block.data(),std::min<std::size_t>(10000,20000-offset));},"application/json"),413,"payload_too_large");
                expect(client.Post("/timers",[](std::size_t offset,httplib::DataSink& sink){if(offset){sink.done();return true;}return sink.write("{}",2);},"application/json"),400,"invalid_duration");
                // Abort a framed upload; no partial file is published.
                int fd=socket(AF_INET,SOCK_STREAM,0);sockaddr_in address{};address.sin_family=AF_INET;address.sin_port=htons(port);inet_pton(AF_INET,"127.0.0.1",&address.sin_addr);check(connect(fd,reinterpret_cast<sockaddr*>(&address),sizeof(address))==0,"abort connect");
                std::string request="POST /files?name=aborted HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/octet-stream\r\nContent-Length: 100000\r\n\r\npartial";check(send(fd,request.data(),request.size(),MSG_NOSIGNAL)>0,"abort send");shutdown(fd,SHUT_WR);char buffer[1024];while(recv(fd,buffer,sizeof(buffer),0)>0){}close(fd);
                check(store->unfinished().empty(),"aborted upload cleanup");
                // A download holding an open descriptor survives concurrent deletion.
                bool removed=false;received=0;
                auto ongoing=client.Get(content_url(large),[&](const char*,std::size_t size){received+=size;if(!removed){httplib::Client other("127.0.0.1",port);expect(other.Delete("/files/"+large["id"].get<std::string>()),204);removed=true;}return true;});
                check(ongoing && ongoing->status==200 && received==total,"in-flight download survives delete");
                expect(client.Get(content_url(large)),404,"file_not_found");expect(client.Delete("/files/"+large["id"].get<std::string>()),404,"file_not_found");
                // Sparse large file verifies 64-bit sizes and offsets without using disk space.
                auto sparse=store->begin("Sparse");int sparse_fd=open(storage->path(sparse).c_str(),O_CREAT|O_RDWR,0600);check(sparse_fd>=0,"sparse open");
                const off_t huge=static_cast<off_t>(5)*1024*1024*1024;check(ftruncate(sparse_fd,huge)==0 && pwrite(sparse_fd,"END",3,huge-3)==3,"sparse write");close(sparse_fd);store->publish(sparse,huge);
                auto tail=client.Get("/files/"+sparse+"/content",{{"Range","bytes=5368709117-5368709119"}});check(tail && tail->status==206 && tail->body=="END","download offsets beyond 4 GiB");expect(client.Delete("/files/"+sparse),204);
                db.run("CREATE TRIGGER fail_file_publish BEFORE UPDATE ON files FOR EACH ROW BEGIN IF NEW.state='ready' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test'; END IF; END");
                expect(client.Post("/files?name=unpublished","bytes","application/octet-stream"),503,"database_unavailable");
                check(store->unfinished().size()==1,"failed publication remains recoverable");db.run("DROP TRIGGER fail_file_publish");
                auto persistent=upload("Persistent.bin",std::string("\0persist\xff",9));check(spark::FileStore(config).get(persistent["id"])==persistent,"fresh metadata store");
                auto explorer=client.Get("/files/explorer");check(explorer && explorer->status==200 && explorer->get_header_value("Content-Type")=="text/html; charset=utf-8","explorer route precedes UUID route");
                check(client.Head("/files/explorer")->body.empty(),"explorer HEAD");expect(client.Delete("/files/explorer"),405,"method_not_allowed");
                check(persistent["directory"]=="/","default root directory");
                auto folder=expect(client.Post("/files/directories",R"({"directory":"/empty/nested"})","application/json"),200);
                check(expect(client.Post("/files/directories",folder.dump(),"application/json"),200)==folder,"idempotent directory creation");
                auto nested=expect(client.Post("/files?name=notes.txt&directory=%2FDocuments%2FManuals","notes","application/octet-stream"),201);
                auto dirlist=expect(client.Get("/files/directories"),200);check(dirlist["directories"].dump().find("/Documents")!=std::string::npos,"upload creates parents");
                check(expect(client.Get("/files?directory=%2FDocuments%2FManuals"),200)["files"].size()==1,"directory filter");
                check(expect(client.Get("/files?directory=%2Fdocuments%2FManuals"),200)["files"].empty(),"case sensitive directories");
                auto moved=expect(client.Patch("/files/"+nested["id"].get<std::string>(),R"({"directory":"/Archive/2026"})","application/json"),200);
                check(moved["id"]==nested["id"] && moved["download_url"]==nested["download_url"] && moved["directory"]=="/Archive/2026" && client.Get(content_url(moved))->body=="notes","move preserves identity and content");
                check(expect(client.Get("/files?directory=%2FDocuments%2FManuals"),200)["files"].empty(),"move removes old membership");
                check(expect(client.Patch("/files/"+nested["id"].get<std::string>(),R"({"directory":"/Archive/2026"})","application/json"),200)==moved,"idempotent move");
                for(const std::string bad:{"relative","/../etc","//etc","/a/","/a/./b","/a\\b"}){
                    auto packet=json{{"directory",bad}};
                    expect(client.Post("/files/directories",packet.dump(),"application/json"),400,"invalid_directory");
                    expect(client.Patch("/files/"+nested["id"].get<std::string>(),packet.dump(),"application/json"),400,"invalid_directory");
                }
                expect(client.Post("/files?name=x&directory=%2F..","x","application/octet-stream"),400,"invalid_directory");
                expect(client.Get("/files?directory=%2F.."),400,"invalid_directory");
                expect(client.Patch("/files/00000000-0000-0000-0000-000000000000",R"({"directory":"/"})","application/json"),404,"file_not_found");
                db.run("CREATE TRIGGER fail_file_move BEFORE UPDATE ON files FOR EACH ROW BEGIN IF NEW.directory='/rollback/new' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test move'; END IF; END");
                expect(client.Patch("/files/"+nested["id"].get<std::string>(),R"({"directory":"/rollback/new"})","application/json"),503,"database_unavailable");
                check(store->get(nested["id"])->at("directory")=="/Archive/2026" && store->directories()["directories"].dump().find("/rollback")==std::string::npos,"atomic move and parent creation rollback");
                db.run("DROP TRIGGER fail_file_move");
                expect(client.Get("/files/"+empty["id"].get<std::string>()),200);
                std::cout<<"File streaming, ranges, 64-bit sizes, JSON limits, concurrent deletion, validation and recovery passed\n";
            }
        }catch(const std::exception& error){std::cerr<<error.what()<<'\n';result=1;}
        server.stop();worker.join();
    }
    storage.reset();
    if(!result){try{spark::FileStorage restarted(store,root);restarted.initialize();check(store->unfinished().empty(),"recovery after failed publication");}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
    return result;
}
