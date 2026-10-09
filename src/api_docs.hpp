#pragma once
#include <string_view>
namespace spark {
inline constexpr std::string_view api_docs_html = R"HTML(<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>SPARK · API reference</title>
<style>
:root{color-scheme:dark;--bg:#10151d;--panel:#171e29;--line:#2b3544;--text:#e5ecf5;--muted:#a2b1c4;--accent:#89dbbd}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--text);font:15px/1.6 system-ui,sans-serif}a{color:var(--accent)}button,input{font:inherit}button,a,input{touch-action:manipulation}:focus-visible{outline:2px solid var(--accent);outline-offset:3px}
header{height:84px;border-bottom:1px solid var(--line);display:flex;align-items:center;justify-content:space-between;padding:0 28px;gap:16px}header strong{font-size:21px;letter-spacing:.16em}header span{color:var(--muted);margin-left:18px;font-size:14px}header a{font-size:13px}
.layout{display:grid;grid-template-columns:320px minmax(0,1fr);height:calc(100vh - 84px);height:calc(100dvh - 84px)}aside{border-right:1px solid var(--line);display:flex;flex-direction:column;min-height:0;background:var(--panel)}.search{padding:20px 18px 12px}.search label{display:block;font-size:12px;color:var(--muted);margin-bottom:6px}input{width:100%;background:var(--bg);color:var(--text);border:1px solid var(--line);border-radius:7px;padding:10px}#count{margin:8px 0 0;color:var(--muted);font-size:12px}nav{overflow-y:auto;padding:0 10px 20px;flex:1}nav a{display:flex;align-items:center;gap:10px;text-decoration:none;padding:11px 10px;margin:3px 0;border:1px solid transparent;border-radius:6px;color:var(--text);font-size:13px;overflow-wrap:anywhere}nav a:hover{background:#222e3d}nav a[aria-current=page]{background:#233b39;border-color:#496b61}.method{font:700 11px/1.7 ui-monospace,monospace;color:var(--accent);min-width:49px}.method[data-method=POST]{color:#aebdff}.method[data-method=PATCH],.method[data-method=PUT]{color:#f1c984}.method[data-method=DELETE]{color:#f6a3a8}
main{overflow-y:auto;padding:38px 44px;min-width:0;scroll-behavior:smooth}article{max-width:1000px;margin:0 auto}h1{font:600 clamp(20px,3vw,30px)/1.4 ui-monospace,monospace;margin:10px 0 16px;overflow-wrap:anywhere}h2{font-size:18px;margin:32px 0 12px}h3{font-size:14px;color:var(--muted);font-weight:500;margin:20px 0 8px}.eyebrow{font-size:11px;letter-spacing:.15em;text-transform:uppercase;color:var(--accent)}.description{font-size:16px;color:#c4d0df}.meta{color:var(--muted);font-size:13px}pre{margin:0;background:#0c1119;border:1px solid var(--line);padding:18px;border-radius:8px;overflow:auto;max-height:420px;font:13px/1.7 ui-monospace,monospace;tab-size:2}dl{border:1px solid var(--line);border-radius:8px;padding:0 18px}dt{padding-top:13px;font:600 13px/1.7 ui-monospace,monospace;overflow-wrap:anywhere}dd{margin:3px 0 0;padding-bottom:13px;border-bottom:1px solid var(--line);color:#bdcadb;white-space:pre-wrap;overflow-wrap:anywhere}dd:last-child{border:0}.notice{border-left:3px solid var(--accent);padding:12px 18px;background:var(--panel)}button{background:var(--accent);border:0;border-radius:6px;padding:8px 14px;color:#10151d;cursor:pointer}details{margin-top:30px;border-top:1px solid var(--line);padding-top:16px}summary{cursor:pointer;color:var(--muted)}.skip{position:fixed;top:-60px;left:12px;background:var(--panel);padding:8px;z-index:2}.skip:focus{top:8px}
@media(max-width:700px){header{padding:0 16px;height:76px}header span{display:block;margin:0;font-size:12px}.layout{grid-template-columns:minmax(0,1fr);height:auto}aside{border-right:0;border-bottom:1px solid var(--line);height:270px}main{overflow:visible;padding:24px 18px}.search{padding:12px 18px 6px}nav a{padding:7px 10px}}
</style>
</head>
<body>
<a class="skip" href="#detail">Skip to endpoint details</a>
<header><div><strong>SPARK</strong><span>API reference</span></div><a href="/api">Raw JSON ↗</a></header>
<div class="layout">
<aside><div class="search"><label for="search">Find an endpoint</label><input id="search" type="search" placeholder="Search method, path or description" autocomplete="off"><p id="count" role="status">Loading endpoints…</p></div><nav id="endpoints" aria-label="API endpoints"></nav></aside>
<main id="detail" tabindex="-1"><article id="content" aria-live="polite"><h1>API reference</h1><p>Loading the current server contract…</p><noscript>JavaScript is required for this view. Read the <a href="/api">JSON API reference</a> instead.</noscript></article></main>
</div>
<script src="/auth/client.js"></script>
<script>
'use strict';
const content=document.getElementById('content'), nav=document.getElementById('endpoints'), search=document.getElementById('search'), count=document.getElementById('count');
let api, selected;
function node(tag,text,parent,cls){const element=document.createElement(tag);if(text!==undefined)element.textContent=text;if(cls)element.className=cls;if(parent)parent.appendChild(element);return element;}
function value(data){return typeof data==='string'?data:JSON.stringify(data,null,2);}
function packet(title,data,parent){node('h3',title,parent);node('pre',data===null?'No body':value(data),parent);}
function fields(title,data,parent){if(!data || !Object.keys(data).length)return;node('h3',title,parent);const list=node('dl',undefined,parent);for(const [key,description] of Object.entries(data)){node('dt',key,list);node('dd',value(description),list);}}
function key(endpoint){return endpoint.method+' '+endpoint.path;}
function renderList(){const scroll=nav.scrollTop;nav.replaceChildren();const query=search.value.toLowerCase().trim();const matches=api.endpoints.filter(e=>(key(e)+' '+e.description).toLowerCase().includes(query));count.textContent=matches.length+' of '+api.endpoints.length+' endpoints';for(const e of matches){const link=node('a',undefined,nav);link.href='#'+encodeURIComponent(key(e));if(selected===e)link.setAttribute('aria-current','page');const method=node('span',e.method,link,'method');method.dataset.method=e.method;node('span',e.path,link);link.addEventListener('click',()=>{show(e);document.getElementById('detail').focus({preventScroll:true});});}if(!matches.length)node('p','No matching endpoints.',nav,'meta');nav.scrollTop=scroll;}
function section(title,data){node('h2',title,content);for(const [field,detail] of Object.entries(data||{})){const label=field.replaceAll('_',' ');if(field==='body'||field==='example')packet(field==='body'?'Example body':'Example request',detail,content);else if(detail!==null && typeof detail==='object' && !Array.isArray(detail))fields(label,detail,content);else {node('h3',label,content);node('p',value(detail),content);}}}
function show(endpoint){selected=endpoint;content.replaceChildren();node('div','Endpoint reference',content,'eyebrow');node('h1',key(endpoint),content);node('p',endpoint.description,content,'description');node('p','Server '+api.server_version+' · API '+api.api_version+' · Authentication: '+api.authentication,content,'meta');section('Request',endpoint.input);section('Response',endpoint.output);if(endpoint.errors){node('h2','Endpoint errors',content);fields('HTTP status and error codes',endpoint.errors,content);}const shared=node('details',undefined,content);node('summary','Shared errors, polling and TCP notifications',shared);packet('Shared HTTP errors',api.errors,shared);fields('Polling',api.polling,shared);packet('TCP notification contract',api.notifications,shared);renderList();document.getElementById('detail').scrollTop=0;document.title=key(endpoint)+' · SPARK API';}
function selectHash(){let wanted;try{wanted=decodeURIComponent(location.hash.slice(1));}catch{wanted='';}show(api.endpoints.find(e=>key(e)===wanted)||api.endpoints[0]);}
async function load(){try{const response=await fetch('/api',{cache:'no-store'});if(!response.ok)throw new Error('HTTP '+response.status);api=await response.json();if(!Array.isArray(api.endpoints)||!api.endpoints.length)throw new Error('Empty endpoint contract');selectHash();}catch(error){content.replaceChildren();node('h1','Unable to load API reference',content);node('p','Could not fetch /api: '+error.message,content,'notice');const retry=node('button','Retry',content);retry.addEventListener('click',load);count.textContent='Reference unavailable';}}
search.addEventListener('input',()=>{if(api)renderList();});window.addEventListener('hashchange',()=>{if(api)selectHash();});load();
</script>
</body>
</html>)HTML";
}
