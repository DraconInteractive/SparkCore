#include "logging.hpp"
#include "database.hpp"
#include <cstdio>
#include <ctime>
#include <mutex>
#include <string_view>
#include <system_error>
#include <unistd.h>
namespace spark {
namespace {
const char* database_reason(unsigned code) {
    switch(code){
    case 1040:return "too_many_connections";
    case 1044:return "database_access_denied";
    case 1045:case 1698:return "authentication_denied";
    case 1049:return "unknown_database";
    case 1054:return "missing_column";
    case 1062:return "duplicate_key";
    case 1142:case 1143:return "permission_denied";
    case 1146:return "missing_table";
    case 1205:return "lock_timeout";
    case 1213:return "deadlock";
    case 2002:return "local_socket_connection_failed";
    case 2003:return "tcp_connection_failed";
    case 2005:return "host_lookup_failed";
    case 2006:return "server_gone";
    case 2013:return "connection_lost";
    default:return "database_error";
    }
}
}
void log_event(const char* level,const std::string& component,const std::string& message,nlohmann::json fields) noexcept {
    try {
        const auto now=std::chrono::system_clock::now();
        const auto ms=std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
        const std::time_t seconds=ms.count()/1000;
        std::tm utc{};gmtime_r(&seconds,&utc);char date[32],timestamp[40];
        std::strftime(date,sizeof(date),"%Y-%m-%dT%H:%M:%S",&utc);
        std::snprintf(timestamp,sizeof(timestamp),"%s.%03lldZ",date,static_cast<long long>(ms.count()%1000));
        nlohmann::json record={{"timestamp",timestamp},{"level",level},{"component",component},{"message",message},{"pid",getpid()}};
        if(!fields.empty())record["details"]=std::move(fields);
        auto line=record.dump(-1,' ',false,nlohmann::json::error_handler_t::replace)+"\n";
        static std::mutex mutex;std::lock_guard<std::mutex> lock(mutex);
        std::fwrite(line.data(),1,line.size(),stderr);std::fflush(stderr);
    }catch(...){/* Logging must never interrupt the operation being diagnosed. */}
}
void log_exception(const char* level,const std::string& component,const std::string& message,const std::exception& error,nlohmann::json fields) noexcept {
    try {
        if(const auto* db=dynamic_cast<const DatabaseError*>(&error)){
            fields["error_kind"]="database";fields["database_message"]=db->what();
            if(db->code){fields["database_code"]=db->code;fields["database_reason"]=database_reason(db->code);}
            if(!db->sqlstate.empty())fields["sqlstate"]=db->sqlstate;
            if(!db->operation.empty())fields["operation"]=db->operation;
        }else if(const auto* system=dynamic_cast<const std::system_error*>(&error)){
            fields["error_kind"]="system";
            fields["system_code"]=system->code().value();
            if(system->code().category()==std::generic_category()||system->code().category()==std::system_category())
                fields["system_reason"]=system->code().message();
        }else {
            // Arbitrary exception strings (including JSON and filesystem errors) may
            // contain submitted values. Do not forward them into runtime logs.
            fields["error_kind"]="exception";
            // These exact application-owned diagnostics contain no submitted values.
            for(const auto* safe:{"Cannot read credentials file","Invalid JSON in credentials file",
                "Invalid credential schema","Invalid credentials document","Invalid credential identity",
                "Unknown or duplicate permission","Invalid or duplicate credential identifier/hash",
                "Credentials file must be owned by the current user and have no group/other permissions",
                "Cannot resolve notification bind address","Cannot bind TCP notification listener",
                "Cannot inspect notification listener"})
                if(std::string_view(error.what())==safe){fields["reason"]=safe;break;}
        }
        log_event(level,component,message,std::move(fields));
    }catch(...){}
}
void RetryLog::failure(const std::exception& error) noexcept {
    const auto now=std::chrono::steady_clock::now();
    if(failures_++==0){since_=last_=now;log_exception("warning",component_,"Background operation failed; retrying",error,{{"failed_attempts",failures_}});}
    else if(now-last_>=std::chrono::seconds(60)){
        last_=now;log_exception("warning",component_,"Background operation still failing; retrying",error,{{"failed_attempts",failures_},{"outage_ms",std::chrono::duration_cast<std::chrono::milliseconds>(now-since_).count()}});
    }
}
void RetryLog::success() noexcept {
    if(!failures_)return;
    log_event("info",component_,"Background operation recovered",{{"failed_attempts",failures_},{"outage_ms",std::chrono::duration_cast<std::chrono::milliseconds>(std::chrono::steady_clock::now()-since_).count()}});
    failures_=0;
}
}
