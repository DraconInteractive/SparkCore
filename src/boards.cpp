#include "boards.hpp"
#include "bounded_body.hpp"
#include <regex>
namespace spark {
namespace {
using nlohmann::json;
bool uuid(const std::string& s){return std::regex_match(s,std::regex("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"));}
bool text(const json& value,std::size_t max,bool nonblank){return value.is_string() && value.get_ref<const std::string&>().size()<=max && (!nonblank || value.get_ref<const std::string&>().find_first_not_of(" \t\r\n")!=std::string::npos);}
}
void configure_boards(httplib::Server& server,std::shared_ptr<BoardStore> store,std::shared_ptr<FileStorage> storage){
    auto route=[&](const std::string& pattern,const std::vector<std::string>& methods,const std::string& kind,int board_match,int item_match,bool column_tasks=false){
        auto handler=[=](const httplib::Request& req,httplib::Response& res){
            auto reply=[&](int status,const json& body){res.status=status;res.set_content(body.dump(2)+"\n","application/json");};
            auto bad=[&](int status,const std::string& code){reply(status,{{"error",code}});};
            const std::string project=req.matches[1].str(),board=board_match?req.matches[board_match].str():"",id=item_match?req.matches[item_match].str():"";
            if(!uuid(project) || (!board.empty() && !uuid(board)) || (!id.empty() && !uuid(id))){bad(400,"invalid_id");return;}
            if(!req.params.empty()){bad(400,"invalid_parameters");return;}
            json packet=json::object();
            if(req.method=="POST" || req.method=="PATCH"){
                const auto type=req.get_header_value("Content-Type");if(type.substr(0,type.find(';'))!="application/json"){bad(415,"unsupported_media_type");return;}
                packet=json::parse(req.body,nullptr,false);if(!packet.is_object()){bad(400,"invalid_packet");return;}
                for(const auto& field:packet.items())if(field.key()!="name" && !(kind=="task" && field.key()=="description") && !(kind=="task" && req.method=="PATCH" && field.key()=="column_id")){bad(400,"unknown_field");return;}
                if(packet.empty() || (req.method=="POST" && (!packet.contains("name") || (kind=="task" && !packet.contains("description"))))){bad(400,"missing_fields");return;}
                if(packet.contains("name") && !text(packet["name"],200,true)){bad(400,"invalid_name");return;}
                if(packet.contains("description") && !text(packet["description"],4000,false)){bad(400,"invalid_description");return;}
                if(packet.contains("column_id") && (!packet["column_id"].is_string() || !uuid(packet["column_id"].get<std::string>()))){bad(400,"invalid_column_id");return;}
                if(column_tasks)packet["column_id"]=id;
            }else if(!req.body.empty()){bad(400,"unexpected_body");return;}
            try{
                // Shares the gate with ordinary file requests; excludes recursive project deletion.
                std::shared_lock<std::shared_mutex> gate(storage->project_gate);
                auto result=store->execute(project,board,kind,column_tasks?"":id,req.method,packet);
                if(req.method=="DELETE"){res.status=204;return;}
                if(req.method=="POST"){
                    auto location="/projects/"+project+"/boards/"+(board.empty()?result["id"].get<std::string>():board+"/"+(kind=="task"?"tasks/":"columns/")+result["id"].get<std::string>());
                    res.set_header("Location",location);
                }
                reply(req.method=="POST"?201:200,result);
            }catch(const std::invalid_argument& e){const std::string code=e.what();bad(code=="name_conflict" || code=="column_not_empty"?409:404,code);}
            catch(const DatabaseError&){bad(503,"database_unavailable");}
        };
        for(const auto& method:methods)if(method=="GET")server.Get(pattern,handler);else body_route(server,method,pattern,handler);
    };
    route(R"(/projects/([^/]+)/boards)",{"GET","POST"},"board",0,0);
    route(R"(/projects/([^/]+)/boards/([^/]+))",{"GET","PATCH","DELETE"},"board",2,0);
    route(R"(/projects/([^/]+)/boards/([^/]+)/columns)",{"POST"},"column",2,0);
    route(R"(/projects/([^/]+)/boards/([^/]+)/columns/([^/]+))",{"PATCH","DELETE"},"column",2,3);
    route(R"(/projects/([^/]+)/boards/([^/]+)/columns/([^/]+)/tasks)",{"POST"},"task",2,3,true);
    route(R"(/projects/([^/]+)/boards/([^/]+)/tasks/([^/]+))",{"GET","PATCH","DELETE"},"task",2,3);
}
json board_endpoints(){
    const std::string base="/projects/{project_id}/boards",detail=base+"/{board_id}";
    const json task={{"id","task-uuid"},{"board_id","board-uuid"},{"column_id","column-uuid"},{"name","Build mobile client"},{"description","Implement timer view"},{"created_at","2026-09-22T00:00:00.000000Z"},{"updated_at","2026-09-22T00:00:00.000000Z"}};
    const json column={{"id","column-uuid"},{"board_id","board-uuid"},{"name","To do"},{"created_at","2026-09-22T00:00:00.000000Z"},{"updated_at","2026-09-22T00:00:00.000000Z"}};
    const json board={{"id","board-uuid"},{"project_id","project-uuid"},{"name","Development"},{"created_at","2026-09-22T00:00:00.000000Z"},{"updated_at","2026-09-22T00:00:00.000000Z"}};
    auto full=board;auto col=column;col["tasks"]=json::array({task});full["columns"]=json::array({col});
    json result=json::array();
    auto add=[&](const std::string& method,const std::string& path,const std::string& description,const json& input,const json& output){
        result.push_back({{"method",method},{"path",path},{"description",description},{"input",{{"path",{{"project_id","owning project UUID; subprojects have independent boards"},{"board_id","board UUID where present"},{"column_id","column UUID where present"},{"task_id","task UUID where present"}}},{"query",json::object()},{"headers",input.is_null()?json::object():json{{"Content-Type","application/json"}}},{"body",input},{"validation","Names: nonblank, max 200 UTF-8 bytes. Task descriptions: string, max 4000 UTF-8 bytes, required on creation (empty allowed). UUIDs are lowercase. Unknown fields/query parameters rejected. PATCH requires at least one editable field. Board names unique per project; column names unique per board (case-sensitive)."}}},{"output",{{"status",method=="POST"?201:method=="DELETE"?204:200},{"body",output}}},{"errors",{{"400","invalid_id, invalid_parameters, invalid_packet, missing_fields, unknown_field, invalid_name, invalid_description, invalid_column_id, unexpected_body"},{"404","project_not_found, board_not_found, column_not_found, task_not_found; IDs must belong to the enclosing project/board"},{"409","name_conflict, column_not_empty"},{"415","unsupported_media_type"},{"503","database_unavailable"}}}});
    };
    add("GET",base,"List all board summaries belonging directly to this project, ordered by creation time then ID. No pagination.",nullptr,{{"boards",json::array({board})}});
    auto empty=board;empty["columns"]=json::array();
    add("POST",base,"Create an empty named board. Configure its columns separately. Emits board.created.",{{"name","Development"}},empty);
    add("GET",detail,"Read the complete board with columns and tasks, each ordered by creation time then ID. No truncation or pagination.",nullptr,full);
    add("PATCH",detail,"Rename board. Emits board.updated only if changed.",{{"name","Release planning"}},full);
    add("DELETE",detail,"Delete board and ALL its columns and tasks. Emits board.deleted.",nullptr,nullptr);
    add("POST",detail+"/columns","Append a named column; emits board.updated.",{{"name","To do"}},column);
    add("PATCH",detail+"/columns/{column_id}","Rename column; emits board.updated if changed.",{{"name","In progress"}},column);
    add("DELETE",detail+"/columns/{column_id}","Delete an EMPTY column; 409 column_not_empty if tasks remain. Move or delete them first. Emits board.updated.",nullptr,nullptr);
    add("POST",detail+"/columns/{column_id}/tasks","Create a task in the column; emits board.updated.",{{"name","Build mobile client"},{"description","Implement timer view"}},task);
    add("GET",detail+"/tasks/{task_id}","Read a single task.",nullptr,task);
    add("PATCH",detail+"/tasks/{task_id}","Edit name/description and/or move by setting column_id. Destination must be on this board. Omitted fields retained; identity preserved. Emits board.updated if changed.",{{"column_id","destination-column-uuid"}},task);
    add("DELETE",detail+"/tasks/{task_id}","Delete task; emits board.updated.",nullptr,nullptr);
    return result;
}
}
