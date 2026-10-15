#include "browser_auth.hpp"
#include "bounded_body.hpp"
namespace spark {
namespace {
constexpr const char* login_html=R"HTML(<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>SPARK · Sign in</title>
<style>body{background:#101820;color:#e5edf3;font:16px system-ui;margin:0}main{max-width:420px;margin:12vh auto;padding:28px}input,button{box-sizing:border-box;width:100%;padding:12px;margin:12px 0;font:inherit;border-radius:6px;border:1px solid #56717f}input{background:#1a2935;color:white}button{background:#78dfcc;cursor:pointer}input[type=checkbox]{width:auto;margin:0 8px 0 0}a{color:#78dfcc}#error{color:#ffb5ad;min-height:24px}</style>
<main><h1>Sign in to SPARK</h1><p>Use your administrator token. Your token is exchanged for a browser session and is not saved in browser storage.</p><form id="login"><label for="token">Administrator token</label><input id="token" type="password" autocomplete="off" required spellcheck="false"><label><input id="remember-device" type="checkbox">Remember this device</label><p>Stay signed in for up to 12 hours. Remembering this device disables the 30-minute inactivity timeout. Server restarts still require sign-in.</p><button id="submit">Sign in</button><p id="error" role="alert"></p></form></main>
<script>
const form=document.getElementById('login'),token=document.getElementById('token'),error=document.getElementById('error');
if(location.protocol!=='https:'){error.textContent='Use the HTTPS address for SPARK to sign in.';document.getElementById('submit').disabled=true;}
form.onsubmit=async e=>{e.preventDefault();const button=document.getElementById('submit');button.disabled=true;error.textContent='';let secret=token.value;token.value='';try{const r=await fetch('/auth/login',{method:'POST',credentials:'same-origin',headers:{'Content-Type':'application/json','X-SPARK-Login':'1'},body:JSON.stringify({token:secret,remember_device:document.getElementById("remember-device").checked})});secret='';const p=await r.json();if(!r.ok)throw Error(p.error==='invalid_login'?'The token is invalid or is not an administrator token.':p.error.replaceAll('_',' '));let next=new URLSearchParams(location.search).get('next')||'/';const url=new URL(next,location.origin);if(url.origin!==location.origin||url.pathname==='/login')next='/';location.replace(next);}catch(e){secret='';error.textContent=e.message;button.disabled=false;}};
</script></html>)HTML";
constexpr const char* client_js=R"JS((()=>{
'use strict';
const nativeFetch=window.fetch.bind(window);
const login=()=>{location.assign('/login?next='+encodeURIComponent(location.pathname+location.search+location.hash));};
let state;
const ready=nativeFetch('/auth/session',{credentials:'same-origin',cache:'no-store'}).then(async r=>{if(r.status===401){login();throw Error('Sign in required');}if(!r.ok)throw Error('Cannot verify browser session');state=await r.json();return state;});
window.sparkSession={ready,headers:async()=>{const s=await ready;return s.mode==='session'?{'X-SPARK-CSRF':s.csrf_token}:{};},expired:login};
window.fetch=async(input,options={})=>{const url=new URL(input instanceof Request?input.url:input,location.href);if(url.origin!==location.origin)return nativeFetch(input,options);const s=await ready;const method=(options.method||(input instanceof Request?input.method:'GET')).toUpperCase();const headers=new Headers(input instanceof Request?input.headers:undefined);new Headers(options.headers).forEach((v,k)=>headers.set(k,v));if(s.mode==='session'&&!['GET','HEAD','OPTIONS'].includes(method))headers.set('X-SPARK-CSRF',s.csrf_token);const response=await nativeFetch(input,{...options,headers,credentials:'same-origin'});if(response.status===401&&s.mode==='session')login();return response;};
ready.then(s=>{if(s.mode!=='session')return;const bar=document.createElement('div');bar.style.cssText='padding:10px 20px;background:#1a2935;color:#e5edf3;display:flex;gap:16px;align-items:center;flex-wrap:wrap';const name=document.createElement('span');name.textContent='Signed in as '+s.subject;const out=document.createElement('button');out.textContent='Sign out';out.onclick=async()=>{out.disabled=true;try{const r=await window.fetch('/auth/logout',{method:'POST'});if(r.ok||r.status===401)location.replace('/login');else throw Error('Sign out failed');}catch(e){out.disabled=false;name.textContent=e.message;}};bar.append(name,out);document.body.prepend(bar);const apply=()=>document.querySelectorAll('[data-spark-permission]').forEach(e=>{if(!e.dataset.sparkPermission.split(' ').every(p=>s.permissions.includes(p))&&!e.hidden)e.hidden=true;});apply();new MutationObserver(apply).observe(document.body,{childList:true,subtree:true,attributes:true,attributeFilter:['hidden','data-spark-permission']});}).catch(()=>{});
})();)JS";
}
bool browser_public_route(const httplib::Request& req){
    const bool read=req.method=="GET"||req.method=="HEAD";
    return (read&&(req.path=="/login"||req.path=="/auth/client.js"||req.path=="/auth/session")) || (req.method=="POST"&&(req.path=="/auth/login"||req.path=="/auth/logout"));
}
void configure_browser_auth(httplib::Server& server,std::shared_ptr<Authorization> auth){
    server.Get("/login",[auth](const auto&,auto& res){if(!auth){res.set_redirect("/");return;}res.set_content(login_html,"text/html; charset=utf-8");});
    server.Get("/auth/client.js",[](const auto&,auto& res){res.set_content(client_js,"application/javascript; charset=utf-8");});
    server.Get("/auth/session",[auth](const auto& req,auto& res){if(auth)auth->sessions()->session(req,res);else res.set_content("{\"mode\":\"open\"}","application/json");});
    body_route(server,"POST","/auth/login",[auth](const auto& req,auto& res){if(auth)auth->sessions()->login(req,res);else {res.status=409;res.set_content("{\"error\":\"authentication_disabled\"}","application/json");}});
    body_route(server,"POST","/auth/logout",[auth](const auto& req,auto& res){if(auth)auth->sessions()->logout(req,res);else res.status=204;});
}
nlohmann::json browser_endpoints(){
    using nlohmann::json;json out=json::array();
    for(const auto& entry:std::vector<std::pair<std::string,std::string>>{{"GET","/login"},{"GET","/auth/client.js"},{"GET","/auth/session"},{"POST","/auth/login"},{"POST","/auth/logout"}}){
        const auto& path=entry.second;
        out.push_back({{"method",entry.first},{"path",path},{"description",path=="/auth/login"?"Exchange an administrator token for a Secure HttpOnly SameSite=Strict session. Requires configured HTTPS browser_origin, exact Host/Origin and X-SPARK-Login: 1. Device tokens cannot sign in. Sessions last at most 12 hours. Optional remember_device (boolean, default false) disables the 30-minute idle timeout.":path=="/auth/logout"?"Revoke this browser session; exact Origin and X-SPARK-CSRF required.":path=="/auth/session"?"Read browser identity, explicit permissions and session CSRF token; requires valid cookie in token mode. Returns mode=open without authentication in open mode.":path=="/login"?"Public administrator sign-in page; HTTPS required.":"Shared same-origin browser session and CSRF helper."},
            {"access",path=="/auth/session"||path=="/auth/logout"?"session":"public"},{"input",{{"body",path=="/auth/login"?json{{"token","<administrator-token>"},{"remember_device",false}}:json(nullptr)},{"headers",path=="/auth/login"?json{{"Content-Type","application/json"},{"Origin","<configured HTTPS origin>"},{"X-SPARK-Login","1"}}:path=="/auth/logout"?json{{"Origin","<configured HTTPS origin>"},{"X-SPARK-CSRF","<session csrf_token>"}}:json::object()}}},
            {"output",{{"status",path=="/auth/logout"?204:200},{"body",path=="/auth/login"||path=="/auth/session"?json{{"mode","session"},{"subject","owner"},{"permissions",json::array()},{"csrf_token","<session-specific token>"}}:json("HTML or JavaScript")}}}});
    }return out;
}
}
