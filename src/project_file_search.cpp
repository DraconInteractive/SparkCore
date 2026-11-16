#include "project_file_search.hpp"
#include <algorithm>
#include <set>
#include <fcntl.h>
#include <sys/stat.h>
#include <unistd.h>
#include <cerrno>
namespace spark {
namespace {
using nlohmann::json;
std::string fold(std::string s){for(auto& c:s)if(c>='A'&&c<='Z')c+=32;return s;}
struct File {int fd;~File(){if(fd>=0)close(fd);}};
std::string snippet(const std::string& text,size_t start,size_t end,size_t match){
 if(match>start+100)start=match-100;
 while(start<end&&(static_cast<unsigned char>(text[start])&0xc0)==0x80)++start;
 end=std::min(end,start+320);
 while(end<text.size()&&end>start&&(static_cast<unsigned char>(text[end])&0xc0)==0x80)--end;
 return text.substr(start,end-start);
}
}
nlohmann::json list_project_file_names(const json& manifest){
 json rows=json::array(),cursor=nullptr;bool more=manifest.at("has_more");
 for(const auto& f:manifest.at("files")){
  if(rows.size()==20){more=true;break;}
  const auto name=f.at("name").get<std::string>();
  rows.push_back({{"file_id",f.at("id")},{"name",name},{"path",f.at("directory").get<std::string>()+"/"+name},{"size_bytes",f.at("size_bytes")},{"extension",fold(std::filesystem::path(name).extension().string())},{"drive_id",f.at("drive_id")},{"download_url",f.at("download_url")}});
  cursor=json::array({f.at("page_rank"),f.at("directory"),name,f.at("id")});
 }
 return {{"mode","names"},{"query",""},{"matches",rows},{"has_more",more},{"next_cursor",more?cursor:json(nullptr)},{"candidates_examined",rows.size()},{"files_scanned",0},{"bytes_read",0},{"skipped",json::object()}};
}
nlohmann::json search_project_text(FileStorage& storage,const json& manifest,const std::string& query){
 const std::set<std::string> extensions={".txt",".json",".md",".markdown",".log",".csv",".tsv",".yaml",".yml",".toml",".ini",".xml"};
 storage.require_drive_online(manifest.at("drive_id"));
 json results=json::array(),skipped={{"unsupported_type",0},{"too_large",0},{"binary_or_invalid_utf8",0},{"unavailable",0}};
 const bool overview=query.empty();const auto needle=fold(query);size_t bytes=0,scanned=0;bool limited=manifest.at("has_more");
 auto candidates=manifest.at("files");
 json cursor=nullptr;size_t examined=0;
 auto advance=[&](const json& f){cursor=json::array({f.at("page_rank"),f.at("directory"),f.at("name"),f.at("id")});++examined;};
 for(const auto& record:candidates){
  if(results.size()==(overview?12:30)||bytes>=8*1024*1024){limited=true;break;}
  auto name=record.at("name").get<std::string>();auto extension=fold(std::filesystem::path(name).extension().string());
  if(!extensions.count(extension)){skipped["unsupported_type"]=skipped["unsupported_type"].get<int>()+1;advance(record);continue;}
  ++scanned;bool name_match=!overview&&fold(name).find(needle)!=std::string::npos;std::string text,reason;
  if(record.at("size_bytes").get<uint64_t>()>1024*1024)reason="too_large";
  else{
   File file{open(storage.content_path(record).c_str(),O_RDONLY|O_CLOEXEC|O_NOFOLLOW|O_NONBLOCK)};struct stat info{};
   if(file.fd<0||fstat(file.fd,&info)!=0||!S_ISREG(info.st_mode))reason="unavailable";
   else if(info.st_size>1024*1024)reason="too_large";
   else if(bytes+static_cast<size_t>(info.st_size)>8*1024*1024){limited=true;break;}
   else{
    char buffer[8192];for(;;){auto n=read(file.fd,buffer,sizeof buffer);if(n<0&&errno==EINTR)continue;if(n<0){reason="unavailable";break;}if(!n)break;bytes+=n;if(text.size()+n>1024*1024||bytes>8*1024*1024){reason="too_large";limited=true;break;}text.append(buffer,n);}
    if(reason.empty()){
     try{json(text).dump();}catch(const json::exception&){reason="binary_or_invalid_utf8";}
     for(unsigned char c:text)if((c<32&&c!=9&&c!=10&&c!=13)||c==127){reason="binary_or_invalid_utf8";break;}
    }
   }
  }
  advance(record);
  json lines=json::array();bool more_lines=false;
  if(reason.empty()&&!overview){
   auto lowered=fold(text);size_t start=0,line=1;
   while(start<text.size()){auto end=text.find('\n',start);if(end==std::string::npos)end=text.size();auto match=lowered.find(needle,start);
    if(match!=std::string::npos&&match<end&&match+needle.size()<=end){if(lines.size()==3){more_lines=true;break;}lines.push_back({{"line",line},{"snippet",snippet(text,start,end,match)}});}
    start=end+1;++line;
   }
  }else if(!reason.empty()) skipped[reason]=skipped[reason].get<int>()+1;
  if(overview||name_match||!lines.empty()){
   auto directory=record.at("directory").get<std::string>();results.push_back({{"file_id",record.at("id")},{"name",name},{"path",directory+"/"+name},{"drive_id",record.at("drive_id")},{"download_url",record.at("download_url")},{"content_revision",record.at("content_revision")},{"name_match",name_match},{"matches",lines},{"more_matching_lines",more_lines},{"content_skipped",reason.empty()?json(nullptr):json(reason)}});
   if(overview){size_t end=std::min<size_t>(1600,text.size());while(end<text.size()&&end>0&&(static_cast<unsigned char>(text[end])&0xc0)==0x80)--end;auto& file=results.back();file["excerpt"]=reason.empty()?text.substr(0,end):std::string();file["excerpt_start_line"]=1;file["content_truncated"]=reason.empty()&&end<text.size();}
  }
 }
 return {{"mode",overview?"overview":"search"},{"query",query},{"matches",results},{"has_more",limited},{"next_cursor",limited?cursor:json(nullptr)},{"candidates_examined",examined},{"files_scanned",scanned},{"bytes_read",bytes},{"skipped",skipped}};
}
}
