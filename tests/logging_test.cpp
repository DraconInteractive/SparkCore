#include "logging.hpp"
#include "database.hpp"
#include <cstdio>
#include <unistd.h>
#include <thread>
#include <vector>
#include <regex>
#include <sstream>
#include <iostream>
void check(bool ok,const char* message){if(!ok)throw std::runtime_error(message);}
int main(){
    FILE* file=std::tmpfile();if(!file)return 1;
    int saved=dup(STDERR_FILENO);if(saved<0||dup2(fileno(file),STDERR_FILENO)<0)return 1;
    int result=0;
    try{
        std::vector<std::thread> threads;
        for(int i=0;i<6;++i)threads.emplace_back([i]{for(int j=0;j<30;++j)spark::log_event("info","test","Concurrent record",{{"worker",i},{"index",j}});});
        for(auto& thread:threads)thread.join();
        spark::RetryLog retry("test.retry");
        spark::DatabaseError error("Cannot connect to MariaDB",1045,"28000","connect");
        retry.success(); // Healthy polls stay silent.
        retry.failure(error);retry.failure(error);retry.failure(error);retry.success();retry.success();
        retry.failure(error);retry.success();
        spark::log_exception("error","test","Opaque exception",std::runtime_error("SECRET_TOKEN_DO_NOT_LOG"));
        std::fflush(stderr);std::rewind(file);
        std::string output;char chunk[4096];size_t n;while((n=std::fread(chunk,1,sizeof(chunk),file)))output.append(chunk,n);
        check(output.find("SECRET_TOKEN_DO_NOT_LOG")==std::string::npos,"exception secret leaked");
        std::istringstream lines(output);std::string line;size_t count=0,recoveries=0,failures=0;
        while(std::getline(lines,line)){
            auto record=nlohmann::json::parse(line);++count;
            check(std::regex_match(record["timestamp"].get<std::string>(),std::regex(R"([0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z)")),"timestamp format");
            check(record["pid"]==getpid(),"process identity");
            if(record["component"]=="test.retry"){
                if(record["level"]=="warning"){
                    ++failures;check(record["details"]["database_code"]==1045&&record["details"]["sqlstate"]=="28000"&&record["details"]["database_reason"]=="authentication_denied","database diagnostics");
                }else{
                    ++recoveries;check(record["details"]["failed_attempts"]==(recoveries==1?3:1),"retry count");check(record["details"]["outage_ms"].get<long long>()>=0,"recovery duration");
                }
            }
        }
        check(count==185&&failures==2&&recoveries==2,"retry suppression or concurrent output");
    }catch(const std::exception& e){dup2(saved,STDERR_FILENO);std::cerr<<e.what()<<'\n';result=1;}
    dup2(saved,STDERR_FILENO);close(saved);std::fclose(file);
    if(!result)std::cout<<"Structured logging, concurrent records, safe diagnostics and retry recovery passed\n";
    return result;
}
