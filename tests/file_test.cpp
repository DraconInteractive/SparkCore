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
                bool text_found=false;
                for(const auto& file:listing["files"])if(file["name"]=="Persistent.txt"){
                    check(client.Get(content_url(file))->body=="after 🌳\r\n","edited bytes survive restart");
                    check(!std::filesystem::exists(storage->path(file["id"])),"obsolete original recovered");
                    auto orphan=file;orphan["content_revision"]=std::string(32,'a');check(!std::filesystem::exists(storage->content_path(orphan)),"uncommitted revision recovered");text_found=true;
                }
                check(text_found,"edited metadata survives restart");
                check(found,"metadata survives restart");
                auto projects=expect(client.Get("/projects"),200);check(projects["projects"].size()==3,"project hierarchy survives database restart");
                auto roots=expect(client.Get("/projects?parent_id=null"),200)["projects"];check(roots.size()==1 && roots[0]["name"]=="Persistent project","persistent root");
                auto tree=expect(client.Get("/projects/"+roots[0]["id"].get<std::string>()+"?depth=deep"),200);check(tree["subprojects"][0]["subprojects"][0]["name"]=="Persistent grandchild","recursive hierarchy survives restart");
                auto bp="/projects/"+roots[0]["id"].get<std::string>()+"/boards";auto boards=expect(client.Get(bp),200)["boards"];check(boards.size()==1 && boards[0]["name"]=="Persistent board","board survives restart");
                auto board=expect(client.Get(bp+"/"+boards[0]["id"].get<std::string>()),200);check(board["columns"][0]["tasks"][0]["name"]=="Persistent task","task column and data survive restart");
                check(expect(client.Get("/files/directories"),200)["directories"].dump().find("/empty/nested")!=std::string::npos,"empty directories survive restart");
            }else{
                DB db(config);
                expect(client.Post("/files?name=wrong.txt","text","text/plain"),415,"unsupported_media_type");
                for(const std::string name:{"..","../x","%2Fetc%2Fpasswd","a%0Ab","a%5Cb"})expect(client.Post("/files?name="+name,"x","application/octet-stream"),400,"invalid_filename");
                expect(client.Post("/files?name=a&name=b","x","application/octet-stream"),400,"invalid_parameters");
                auto upload=[&](const std::string& name,const std::string& bytes){return expect(client.Post("/files?name="+name,bytes,"application/octet-stream"),201);};
                auto textfile=upload("editable.txt","original");const auto texturl=content_url(textfile);
                auto original=client.Get(texturl);const auto original_tag=original->get_header_value("ETag");
                httplib::Headers match={{"If-Match",original_tag}};
                expect(client.Put(texturl,"new","text/plain"),428,"precondition_required");
                expect(client.Put(texturl,match,"new","application/json"),415,"unsupported_media_type");
                expect(client.Put(texturl,match,std::string("a\0b",3),"text/plain"),400,"invalid_text");
                expect(client.Put(texturl,match,std::string("\xff",1),"text/plain"),400,"invalid_text");
                // Header-only oversized request: the server closes without consuming its body.
                int size_fd=socket(AF_INET,SOCK_STREAM,0);sockaddr_in size_addr{};size_addr.sin_family=AF_INET;size_addr.sin_port=htons(port);inet_pton(AF_INET,"127.0.0.1",&size_addr.sin_addr);
                check(connect(size_fd,reinterpret_cast<sockaddr*>(&size_addr),sizeof(size_addr))==0,"size check connect");
                const auto oversized="PUT "+texturl+" HTTP/1.1\r\nHost: localhost\r\nContent-Type: text/plain\r\nContent-Length: 2097153\r\nIf-Match: "+original_tag+"\r\nConnection: close\r\n\r\n";
                check(send(size_fd,oversized.data(),oversized.size(),MSG_NOSIGNAL)==static_cast<ssize_t>(oversized.size()),"size check send");
                std::string size_reply;char size_buffer[1024];ssize_t size_count;
                while((size_count=recv(size_fd,size_buffer,sizeof(size_buffer),0))>0)size_reply.append(size_buffer,size_count);close(size_fd);
                check(size_reply.find("413 Payload Too Large")!=std::string::npos && size_reply.find("payload_too_large")!=std::string::npos,"oversized content length rejected");
                expect(client.Put(texturl,match,[](std::size_t offset,httplib::DataSink& sink){if(offset>=2097153){sink.done();return true;}std::string block(65536,'x');return sink.write(block.data(),std::min<std::size_t>(block.size(),2097153-offset));},"text/plain"),413,"payload_too_large");
                check(client.Get(texturl)->body=="original","rejected edits leave bytes intact");
                const std::string edited="\xef\xbb\xbfUnicode 🌳\r\n"+std::string(20000,'x');
                auto saved_response=client.Put(texturl,match,edited,"text/plain; charset=utf-8");auto saved=expect(saved_response,200);
                check(saved["id"]==textfile["id"] && saved["directory"]==textfile["directory"] && saved["created_at"]==textfile["created_at"] && saved["size_bytes"]==edited.size(),"edit preserves identity and updates size");
                auto edited_download=client.Get(texturl);check(edited_download->body==edited && edited_download->get_header_value("ETag")!=original_tag && edited_download->get_header_value("ETag")==saved_response->get_header_value("ETag"),"edit byte integrity and revision");
                expect(client.Put(texturl,match,"stale","text/plain"),412,"file_changed");
                auto slice=client.Get(texturl,httplib::Headers{{"Range","bytes=0-2"}});check(slice->status==206 && slice->body==edited.substr(0,3),"range after edit");
                match={{"If-Match",edited_download->get_header_value("ETag")}};
                db.run("CREATE TRIGGER fail_text_edit BEFORE UPDATE ON files FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='edit test'");
                expect(client.Put(texturl,match,"failed","text/plain"),503,"database_unavailable");
                db.run("DROP TRIGGER fail_text_edit");check(client.Get(texturl)->body==edited,"failed edit preserves committed bytes");
                expect(client.Put(texturl,match,"","text/plain"),200);check(client.Get(texturl)->body.empty(),"empty edit");
                match={{"If-Match",client.Get(texturl)->get_header_value("ETag")}};
                // Simultaneous writers share a revision: exactly one may commit.
                int first=0,second=0;
                std::thread writer([&]{httplib::Client peer("127.0.0.1",port);auto r=peer.Put(texturl,match,"first","text/plain");first=r?r->status:0;});
                auto competing=client.Put(texturl,match,"second","text/plain");second=competing?competing->status:0;writer.join();
                check((first==200 && second==412)||(first==412 && second==200),"concurrent edits compare revision under lock");
                expect(client.Delete("/files/"+textfile["id"].get<std::string>()),204);
                for(const auto& entry:std::filesystem::directory_iterator(root))check(entry.path().filename().string().find(textfile["id"].get<std::string>())!=0,"delete removes every content revision");
                auto durable=upload("Persistent.txt","before");auto durable_url=content_url(durable);
                auto durable_saved=expect(client.Put(durable_url,{{"If-Match",client.Get(durable_url)->get_header_value("ETag")}},"after 🌳\r\n","text/plain"),200);
                // Simulate obsolete and uncommitted versions left by a crash.
                std::ofstream(storage->path(durable["id"]))<<"obsolete";
                auto orphan=durable_saved;orphan["content_revision"]=std::string(32,'a');std::ofstream(storage->content_path(orphan))<<"uncommitted";
                auto empty=upload("empty","");auto empty_response=client.Get(content_url(empty));check(empty_response && empty_response->status==200 && empty_response->body.empty(),"empty file");
                auto html=upload("a%22%F0%9F%8C%B3.html","<script>alert(1)</script>");auto html_response=client.Get(content_url(html));
                check(html_response && html_response->get_header_value("Content-Type")=="application/octet-stream" && html_response->get_header_value("X-Content-Type-Options")=="nosniff" && html_response->get_header_value("Content-Disposition").find("filename*=UTF-8''a%22%F0%9F%8C%B3.html")!=std::string::npos,"arbitrary type and encoded attachment filename");
                // Verify the encoded attachment filename on the wire as well.
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
                auto range=client.Get(content_url(large),httplib::Headers{{"Range","bytes=250-269"}});check(range && range->status==206 && range->body==block.substr(250,20),"byte-range resume");
                auto bad_range=client.Get(content_url(large),httplib::Headers{{"Range","bytes=999999999-"}});check(bad_range && bad_range->status==416,"invalid range");
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
                auto tail=client.Get("/files/"+sparse+"/content",httplib::Headers{{"Range","bytes=5368709117-5368709119"}});check(tail && tail->status==206 && tail->body=="END","download offsets beyond 4 GiB");expect(client.Delete("/files/"+sparse),204);
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
                auto project_ui=client.Get("/projects/explorer");check(project_ui && project_ui->status==200 && project_ui->get_header_value("Content-Type")=="text/html; charset=utf-8","project explorer route");
                check(client.Head("/projects/explorer")->body.empty(),"project explorer HEAD");
                check(client.Options("/projects/explorer")->get_header_value("Allow")=="GET, HEAD, OPTIONS","project explorer OPTIONS");
                expect(client.Delete("/projects/explorer"),405,"method_not_allowed");
                auto project_create=[&](const std::string& name){return expect(client.Post("/projects",json{{"name",name},{"description","Project details"},{"github_url","https://github.com/example/repo"}}.dump(),"application/json"),201);};
                auto project=project_create("Project%_🌳");const auto project_path="/projects/"+project["id"].get<std::string>();
                check(project["directory"]=="/projects/Project%_🌳","project logical directory");
                check(expect(client.Get(project_path),200)==project,"project get");
                check(client.Head(project_path)->status==200 && client.Options(project_path)->status==204,"project HEAD and OPTIONS");
                expect(client.Post("/projects",R"({"name":"../bad","description":""})","application/json"),400,"invalid_name");
                expect(client.Post("/projects",R"({"name":"Bad","description":"","github_url":"http://github.com/a/b"})","application/json"),400,"invalid_github_url");
                expect(client.Post("/projects",json{{"name",project["name"]},{"description",""}}.dump(),"application/json"),409,"project_conflict");
                expect(client.Patch(project_path,R"({"directory":"/"})","application/json"),400,"unknown_field");
                auto renamed=expect(client.Patch(project_path,R"({"name":"Renamed","github_url":null})","application/json"),200);
                check(renamed["directory"]==project["directory"] && renamed["github_url"].is_null(),"rename preserves directory; clear optional URL");
                auto project_upload=[&](const std::string& directory){std::string encoded;const char* hex="0123456789ABCDEF";for(unsigned char c:directory){encoded+='%';encoded+=hex[c>>4];encoded+=hex[c&15];}return expect(client.Post("/files?name=project.bin&directory="+encoded,"data","application/octet-stream"),201);};
                auto project_file=project_upload(project["directory"]);
                auto child=project_upload(project["directory"].get<std::string>()+"/nested");
                const auto shorthand="/project/"+project["id"].get<std::string>()+"/files";
                auto direct=expect(client.Get(shorthand),200);
                check(direct["files"]==json::array({project_file}) && direct["has_more"]==false,"project shorthand uses stable renamed directory and excludes nested files");
                check(expect(client.Get(shorthand+"?limit=1&offset=1"),200)["files"].empty(),"project file pagination");
                expect(client.Get(shorthand+"?limit=0"),400,"invalid_pagination");
                expect(client.Get(shorthand+"?directory=/"),400,"invalid_parameters");
                expect(client.Get(shorthand+"?limit=1&limit=2"),400,"invalid_parameters");
                expect(client.Get("/project/bad/files"),400,"invalid_project_id");
                expect(client.Get("/project/00000000-0000-0000-0000-000000000000/files"),404,"project_not_found");
                check(client.Head(shorthand)->status==200 && client.Head(shorthand)->body.empty(),"project file HEAD");
                check(client.Options(shorthand)->get_header_value("Allow")=="GET, HEAD, OPTIONS","project file OPTIONS");
                expect(client.Delete(shorthand),405,"method_not_allowed");
                auto neighbour=project_upload(project["directory"].get<std::string>()+"-other");
                auto wildcard_neighbour=project_upload("/projects/ProjectXX🌳");
                expect(client.Delete(project_path,"{}","application/json"),400,"invalid_destroy_directory");
                expect(client.Delete(project_path,R"({"destroy_directory":"true"})","application/json"),400,"invalid_destroy_directory");
                db.run("CREATE TRIGGER fail_project_delete BEFORE DELETE ON projects FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test project rollback'");
                expect(client.Delete(project_path,R"({"destroy_directory":true})","application/json"),503,"database_unavailable");
                check(client.Get(content_url(child))->status==200 && expect(client.Get(project_path),200)==renamed,"delete rollback restores project and files");
                db.run("DROP TRIGGER fail_project_delete");
                expect(client.Delete(project_path,R"({"destroy_directory":false})","application/json"),204);
                check(client.Get(content_url(child))->body=="data","retain project files");
                expect(client.Get(project_path),404,"project_not_found");
                auto adopted=project_create("Project%_🌳");
                expect(client.Delete("/projects/"+adopted["id"].get<std::string>(),R"({"destroy_directory":true})","application/json"),204);
                check(!std::filesystem::exists(storage->path(child["id"])) && !std::filesystem::exists(storage->path(project_file["id"])),"recursive physical deletion");
                check(client.Get(content_url(child))->status==404 && client.Get(content_url(neighbour))->status==200 && client.Get(content_url(wildcard_neighbour))->status==200,"literal subtree deletion isolates neighbours");
                auto remaining_directories=store->directories();
                for(const auto& dir:remaining_directories["directories"])check(dir!=project["directory"] && dir!=project["directory"].get<std::string>()+"/nested","recursive directory deletion");
                db.run("CREATE TRIGGER fail_project_create BEFORE INSERT ON projects FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test project create rollback'");
                expect(client.Post("/projects",R"({"name":"RollbackProject","description":""})","application/json"),503,"database_unavailable");
                check(store->directories()["directories"].dump().find("RollbackProject")==std::string::npos,"project creation rolls directory back");
                db.run("DROP TRIGGER fail_project_create");
                auto interrupted_project=project_create("Interrupted project");
                auto interrupted_file=project_upload(interrupted_project["directory"]);
                db.run("CREATE TRIGGER fail_project_cleanup BEFORE DELETE ON files FOR EACH ROW BEGIN IF OLD.directory='/projects/Interrupted project' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test cleanup'; END IF; END");
                expect(client.Delete("/projects/"+interrupted_project["id"].get<std::string>(),R"({"destroy_directory":true})","application/json"),503,"database_unavailable");
                expect(client.Get("/projects/"+interrupted_project["id"].get<std::string>()),404,"project_not_found");
                check(!store->get(interrupted_file["id"]),"failed byte cleanup remains hidden for recovery");
                db.run("DROP TRIGGER fail_project_cleanup");
                auto child_create=[&](const json& parent,const std::string& name){return expect(client.Post("/projects",json{{"name",name},{"description","Nested project"},{"parent_id",parent["id"]}}.dump(),"application/json"),201);};
                auto project_url=[](const json& p){return "/projects/"+p["id"].get<std::string>();};
                auto tree=project_create("Tree");auto other_tree=project_create("Other tree");
                auto mobile=child_create(tree,"Mobile");auto server_project=child_create(tree,"Server");auto watch=child_create(mobile,"Watch");auto other_mobile=child_create(other_tree,"Mobile");
                check(mobile["directory"]=="/projects/Tree/Mobile" && watch["directory"]=="/projects/Tree/Mobile/Watch" && watch["parent_id"]==mobile["id"],"nested parent directory assignment");
                auto shallow=expect(client.Get(project_url(tree)),200);
                check(shallow["parent_id"].is_null() && shallow["subprojects"]==json::array({mobile["id"],server_project["id"]}),"shallow sorted immediate child IDs");
                check(expect(client.Get(project_url(tree)+"?depth=shallow"),200)==shallow,"explicit shallow read");
                auto deep=expect(client.Get(project_url(tree)+"?depth=deep"),200);
                check(deep["subprojects"][0]["id"]==mobile["id"] && deep["subprojects"][0]["subprojects"][0]==watch && deep["subprojects"][1]["subprojects"].empty(),"deep complete recursive child records and empty leaves");
                check(expect(client.Get("/projects?parent_id="+tree["id"].get<std::string>()+"&limit=1"),200)["has_more"]==true,"child list pagination");
                check(expect(client.Get("/projects?parent_id=null"),200)["projects"].size()==2,"root-only filter");
                expect(client.Get(project_url(tree)+"?depth=invalid"),400,"invalid_depth");
                expect(client.Get(project_url(tree)+"?depth=deep&depth=shallow"),400,"invalid_parameters");
                expect(client.Get("/projects?parent_id=bad"),400,"invalid_parent_id");
                expect(client.Post("/projects",json{{"name","Mobile"},{"description",""},{"parent_id",tree["id"]}}.dump(),"application/json"),409,"project_conflict");
                expect(client.Post("/projects",R"({"name":"Missing parent","description":"","parent_id":"00000000-0000-0000-0000-000000000000"})","application/json"),404,"parent_not_found");
                expect(client.Post("/projects",R"({"name":"Bad parent","description":"","parent_id":true})","application/json"),400,"invalid_parent_id");
                expect(client.Patch(project_url(tree),json{{"parent_id",watch["id"]}}.dump(),"application/json"),400,"unknown_field");
                expect(client.Patch(project_url(server_project),R"({"name":"Mobile"})","application/json"),409,"project_conflict");
                expect(client.Patch(project_url(tree),R"({"name":"Renamed tree"})","application/json"),200);
                auto new_child=child_create(tree,"New child");check(new_child["directory"]=="/projects/Tree/New child","child uses stable parent directory after rename");
                auto tree_file=project_upload(watch["directory"]);auto other_file=project_upload(other_mobile["directory"]);
                db.run("CREATE TRIGGER fail_subtree_delete BEFORE DELETE ON projects FOR EACH ROW BEGIN IF OLD.name='Renamed tree' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='subtree rollback'; END IF; END");
                expect(client.Delete(project_url(tree),R"({"destroy_directory":true})","application/json"),503,"database_unavailable");
                check(expect(client.Get(project_url(tree)+"?depth=deep"),200)["subprojects"].size()==3 && client.Get(project_url(watch))->status==200 && client.Get(content_url(tree_file))->status==200,"whole subtree rollback");
                db.run("DROP TRIGGER fail_subtree_delete");
                expect(client.Delete(project_url(mobile),R"({"destroy_directory":false})","application/json"),204);
                check(client.Get(project_url(watch))->status==404 && client.Get(content_url(tree_file))->status==200,"subtree metadata removed but files retained");
                check(expect(client.Get(project_url(tree)),200)["subprojects"].size()==2,"parent child IDs updated after removal");
                expect(client.Delete(project_url(tree),R"({"destroy_directory":true})","application/json"),204);
                check(client.Get(project_url(server_project))->status==404 && client.Get(content_url(tree_file))->status==404 && client.Get(content_url(other_file))->status==200,"recursive destroy preserves other tree");
                expect(client.Delete(project_url(other_tree),R"({"destroy_directory":true})","application/json"),204);
                auto wide=project_create("Wide tree");
                for(int i=0;i<105;++i)child_create(wide,"Child "+std::to_string(i));
                check(expect(client.Get(project_url(wide)+"?depth=deep"),200)["subprojects"].size()==105,"deep reads do not truncate at list page size");
                expect(client.Delete(project_url(wide),R"({"destroy_directory":true})","application/json"),204);
                auto bounded=project_create("Bounded tree");auto cursor=bounded;
                while(cursor["directory"].get<std::string>().size()+201<=1024)cursor=child_create(cursor,std::string(200,'x'));
                expect(client.Post("/projects",json{{"name",std::string(200,'x')},{"description",""},{"parent_id",cursor["id"]}}.dump(),"application/json"),400,"directory_too_long");
                expect(client.Delete(project_url(bounded),R"({"destroy_directory":true})","application/json"),204);
                auto persistent_project=project_create("Persistent project");
                check(expect(client.Get("/projects?limit=1"),200)["projects"][0]==persistent_project,"project listing");
                check(expect(client.Get("/projects?offset=1"),200)["projects"].empty(),"project pagination");
                auto persistent_child=child_create(persistent_project,"Persistent child");child_create(persistent_child,"Persistent grandchild");
                std::cout<<"File streaming, ranges, 64-bit sizes, JSON limits, concurrent deletion, validation and recovery passed\n";
            }
        }catch(const std::exception& error){std::cerr<<error.what()<<'\n';result=1;}
        server.stop();worker.join();
    }
    storage.reset();
    if(!result){try{spark::FileStorage restarted(store,root);restarted.initialize();check(store->unfinished().empty(),"recovery after failed publication");}catch(const std::exception& e){std::cerr<<e.what()<<'\n';return 1;}}
    return result;
}
