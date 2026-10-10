#include "credential_management.hpp"
#include "bounded_body.hpp"
namespace spark {
namespace {
using nlohmann::json;
void error(httplib::Response& res,int status,const char* message){res.status=status;res.set_content(json{{"error",message}}.dump(),"application/json");}
constexpr auto page=R"HTML(<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>SPARK device access</title>
<style>body{background:#101820;color:#e5edf3;font:16px system-ui;margin:0}main{max-width:950px;margin:auto;padding:32px}a{color:#78dfcc}section{background:#1a2935;padding:24px;border-radius:12px;margin:24px 0}input,button,textarea{font:inherit;padding:10px}input[type=text],textarea{box-sizing:border-box;width:100%;background:#101820;color:white;border:1px solid #56717f}button{cursor:pointer;margin:8px 8px 8px 0}label{display:block;margin:8px 0}#permissions{display:grid;grid-template-columns:repeat(auto-fit,minmax(210px,1fr))}article{border-top:1px solid #56717f;padding:16px 0;overflow-wrap:anywhere}textarea{height:100px}#status{white-space:pre-wrap}</style>
<main><a href="/">← Dashboard</a><h1>Device access</h1><p>Create a separate token for each device. The device uses it for REST requests and TCP notifications.</p><p id="status" role="status"></p>
<section><h2>Provision a device</h2><form id="create"><label>Device ID<input id="subject" type="text" required maxlength="64" pattern="[A-Za-z0-9_\-]+" placeholder="watch-1"></label><p>Use the same ID when registering the device and opening its notification connection. This does not register the device automatically.</p><h3>Permissions</h3><p>Discovery reads the API guide; devices.write lets the device register itself; notifications.read enables notifications for the resource permissions you choose. Resource access applies across this instance.</p><div id="permissions"></div><button id="issue" disabled>Create token</button></form></section>
<section id="secret" hidden><h2>Save this token now</h2><p>It will not be shown again. Transfer it securely to your device. No token is saved in browser storage.</p><textarea id="token" readonly aria-label="New device token"></textarea><button id="copy">Copy token</button><button id="dismiss">I have saved it</button></section>
<section><h2>Device credentials</h2><p>To replace a token, create a new one for the same device, update the device, then revoke the old credential.</p><div id="records"></div></section></main>
<script src="/auth/client.js"></script><script>
const $=id=>document.getElementById(id);let pending=false;
const descriptions={'discovery.read':'Read API documentation and server status','devices.write':'Register and update this device','devices.read':'Read device details','devices.delegate':'Act on behalf of other devices (advanced)','notifications.read':'Receive notifications for permitted resources','events.read':'Read calendar events','events.write':'Create, change and delete calendar events','timers.read':'Read timers','timers.write':'Create, stop and confirm timers','tasks.read':'Read daily tasks','tasks.write':'Manage and check daily tasks','files.read':'Browse and download files','files.write':'Upload, edit and delete files','projects.read':'Read projects','projects.write':'Manage projects','boards.read':'Read project boards','boards.write':'Manage boards and their tasks','pulse.read':'Read pulse state','pulse.write':'Change pulse settings and subscriptions','commands.read':'Read commands','commands.invoke':'Request commands','commands.advertise':'Advertise this device’s commands','commands.execute':'Claim and execute commands'};
async function api(path,options){const r=await fetch(path,options);const p=r.status===204?null:await r.json();if(!r.ok)throw Error(p.error);return p;}
async function refresh(){const p=await api('/admin/credentials');$('permissions').replaceChildren();for(const name of p.permissions){const label=document.createElement('label'),input=document.createElement('input');input.type='checkbox';input.value=name;input.checked=['discovery.read','devices.write','notifications.read'].includes(name);label.title=name;label.append(input,document.createTextNode(' '+(descriptions[name]||name)));$('permissions').append(label);}$('records').replaceChildren();for(const record of p.credentials){const item=document.createElement('article'),title=document.createElement('strong'),details=document.createElement('p');title.textContent=record.subject;details.textContent=record.id+' · '+(record.revoked?'Revoked':'Active')+' · '+record.permissions.join(', ');item.append(title,details);if(!record.revoked){const revoke=document.createElement('button');revoke.textContent='Revoke';revoke.onclick=async()=>{if(!confirm('Revoke this credential for '+record.subject+'? Its token will stop working.'))return;revoke.disabled=true;try{await api('/admin/credentials/'+record.id,{method:'DELETE'});await refresh();}catch(e){$('status').textContent=e.message;revoke.disabled=false;}};item.append(revoke);}$('records').append(item);}$('issue').disabled=false;}
$('create').onsubmit=async e=>{e.preventDefault();if(pending||!$('secret').hidden)return;pending=true;$('issue').disabled=true;$('status').textContent='';try{const permissions=Array.from($('permissions').querySelectorAll('input:checked'),i=>i.value);const p=await api('/admin/credentials',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({subject:$('subject').value,permissions})});$('token').value=p.token;$('secret').hidden=false;$('secret').scrollIntoView();await refresh();}catch(e){$('status').textContent=e.message;}finally{pending=false;$('issue').disabled=!$('secret').hidden;}};
$('copy').onclick=async()=>{try{await navigator.clipboard.writeText($('token').value);$('status').textContent='Token copied.';}catch(e){$('status').textContent='Select the token and copy it manually.';}};$('dismiss').onclick=()=>{$('token').value='';$('secret').hidden=true;$('issue').disabled=false;};window.addEventListener('pagehide',()=>{$('token').value='';});refresh().catch(e=>{$('status').textContent=e.message;});
</script></html>)HTML";
}
void configure_credentials(httplib::Server& server,std::shared_ptr<Authorization> auth){
    auto handle=[auth](const httplib::Request& req,httplib::Response& res){
        if(!auth){error(res,409,"authentication_disabled");return;}
        auto who=auth->identity(req,res);if(!who)return;
        if(who->kind!="administrator"||!permitted(*who,"admin.credentials")){error(res,403,"forbidden");return;}
        auto provider=std::dynamic_pointer_cast<TokenIdentityProvider>(auth->provider());
        if(!provider){error(res,409,"credential_management_unavailable");return;}
        res.set_header("Cache-Control","no-store");
        if(req.path=="/admin/credentials/explorer"){res.set_content(page,"text/html; charset=utf-8");return;}
        try{
            const auto& file=provider->credentials_file();
            if(req.method=="GET"||req.method=="HEAD"){
                auto records=json::array();for(const auto& record:list_credentials(file))if(record["kind"]=="device")records.push_back(record);
                auto available=json::array();for(const auto& permission:who->permissions)if(permission.rfind("admin.",0)!=0)available.push_back(permission);
                res.set_content(json{{"credentials",records},{"permissions",available}}.dump(),"application/json");return;
            }
            if(req.method=="POST"){
                auto body=json::parse(req.body,nullptr,false);
                if(!body.is_object()||body.size()!=2||!body.contains("subject")||!body["subject"].is_string()||!body.contains("permissions")||!body["permissions"].is_array()||body["permissions"].empty()){error(res,400,"invalid_credential");return;}
                auto subject=body["subject"].get<std::string>();
                if(!std::regex_match(subject,std::regex("[A-Za-z0-9_-]{1,64}"))){error(res,400,"invalid_device_id");return;}
                std::set<std::string> permissions;
                for(const auto& p:body["permissions"]){
                    if(!p.is_string()||!permission_names().count(p.get<std::string>())||!permissions.insert(p.get<std::string>()).second){error(res,400,"invalid_permissions");return;}
                    if(p.get<std::string>().rfind("admin.",0)==0||!permitted(*who,p.get<std::string>())){error(res,403,"permission_not_grantable");return;}
                }
                auto result=issue_credential(file,"device",subject,permissions);res.status=201;res.set_content(result.dump(),"application/json");return;
            }
            auto id=req.matches[1].str();bool found=false;
            for(const auto& record:list_credentials(file))if(record["id"]==id&&record["kind"]=="device")found=true;
            if(!found){error(res,404,"credential_not_found");return;}
            revoke_credential(file,id);res.status=204;
        }catch(const std::exception&){error(res,503,"credential_store_unavailable");}
    };
    server.Get("/admin/credentials/explorer",handle);server.Get("/admin/credentials",handle);
    body_route(server,"POST","/admin/credentials",handle,auth);
    body_route(server,"DELETE",R"(/admin/credentials/([^/]+))",handle,auth);
}
nlohmann::json credential_endpoints(){
    auto out=json::array();
    auto add=[&](const char* method,const char* path,const char* description,json input,int status,json output){
        out.push_back({{"method",method},{"path",path},{"description",description},{"input",{{"body",input}}},{"output",{{"status",status},{"body",output}}}});
    };
    add("GET","/admin/credentials/explorer","Administrator device-token management page. Token mode and admin.credentials required.",nullptr,200,"HTML device credential manager");
    add("GET","/admin/credentials","List device credentials without secrets or hashes, and device permissions this administrator may grant. Token mode and administrator identity required.",nullptr,200,
        json::parse(R"({"credentials":[{"id":"<credential-id>","kind":"device","subject":"watch-1","permissions":["discovery.read"],"revoked":false}],"permissions":["discovery.read","devices.write","notifications.read"]})"));
    add("POST","/admin/credentials","Issue a device token; plaintext returned once. subject is device ID (1..64 ASCII letters, digits, _ and -). Nonempty unique permissions must be held by administrator; admin.* cannot be granted. Does not register device. Browser sessions require Origin and X-SPARK-CSRF.",
        json::parse(R"({"subject":"watch-1","permissions":["discovery.read","devices.write","events.read","notifications.read"]})"),201,{{"id","<credential-id>"},{"token","<one-time-device-token>"}});
    add("DELETE","/admin/credentials/{id}","Revoke device credential by credential ID, not device ID; repeated revocation succeeds. Unknown/non-device ID returns 404. Browser sessions require Origin and X-SPARK-CSRF.",nullptr,204,nullptr);
    return out;
}
}
