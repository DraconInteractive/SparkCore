#pragma once
namespace spark {
inline constexpr const char* config_explorer_html=R"HTML(<!doctype html>
<html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>SPARK configs</title>
<style>
body{margin:0;background:#101820;color:#e5edf3;font:16px system-ui}a{color:#78dfcc}main{max-width:1300px;margin:auto;padding:24px}h1{margin-bottom:8px}.layout{display:grid;grid-template-columns:310px minmax(0,1fr);gap:24px}aside,section{background:#1a2935;border:1px solid #456;border-radius:10px;padding:18px}label{display:block;margin:12px 0 5px}input,textarea,button{font:inherit;box-sizing:border-box;border-radius:5px;padding:9px}input,textarea{width:100%;background:#101820;color:#e5edf3;border:1px solid #56717f}textarea{min-height:360px;resize:vertical;font:14px monospace;tab-size:2}button{cursor:pointer;margin:5px 5px 5px 0;border:1px solid #56717f;background:#263f4b;color:white}button:disabled{opacity:.5;cursor:default}#list{max-height:65vh;overflow:auto;margin:12px 0}#list button{display:block;width:100%;text-align:left;overflow-wrap:anywhere}#list small{display:block;color:#adc1ce}#status{white-space:pre-wrap;color:#ffd49a;min-height:1.5em}#description{min-height:70px;font:inherit}.actions{display:flex;align-items:center;gap:8px;flex-wrap:wrap}.expanded{position:fixed;inset:10px;z-index:10;overflow:auto}.expanded #data{height:60vh}[hidden]{display:none!important}@media(max-width:800px){.layout{grid-template-columns:1fr}#list{max-height:30vh}}
</style>
<main><a href="/">SPARK dashboard</a> · <a href="/api/docs">API reference</a><h1>Configs</h1>
<p>Shared JSON settings for devices. Select a config to inspect its current revision, or create one with default values.</p>
<p id="status" role="status"></p><div class="layout"><aside>
<button id="new" data-spark-permission="configs.write" disabled>Create config</button>
<form id="filters"><label for="filter-name">Exact name</label><input id="filter-name"><label for="filter-tags">Match all tags (comma separated)</label><input id="filter-tags" placeholder="watch, production"><button>Search</button></form>
<div id="list"></div><button id="more" hidden>Load more</button>
</aside><section id="editor"><p id="empty">Select a config or create a new one.</p>
<form id="form" hidden><h2 id="heading">New config</h2><p id="identity"></p>
<label for="name">Name</label><input id="name" required maxlength="200">
<label for="description">Description</label><textarea id="description" maxlength="4000"></textarea>
<label for="tags">Tags (comma separated; case-sensitive)</label><input id="tags" placeholder="watch, production">
<div class="actions"><label for="data">JSON object</label><button type="button" id="expand">Expand editor</button></div>
<textarea id="data" spellcheck="false" required aria-label="JSON configuration"></textarea>
<p>Maximum 64 KiB of compact JSON. Saving replaces the entire object.</p>
<div class="actions"><button id="save" data-spark-permission="configs.write">Save</button><button id="reload" type="button" hidden>Reload saved</button>
<button id="delete" type="button" data-spark-permission="configs.write" hidden>Delete</button>
<a id="export" hidden>Export saved JSON</a></div>
<label id="import-label" data-spark-permission="configs.write">Import JSON into draft<input id="import" type="file" accept=".json,application/json"></label>
</form></section></div></main><script src="/auth/client.js"></script><script>
const $=id=>document.getElementById(id);let selected=null,dirty=false,busy=false,canWrite=false,offset=0;
function tagList(value){return value.split(',').map(t=>t.trim()).filter(Boolean);}
function status(message){$('status').textContent=message;}
function lock(value){busy=value;for(const id of ['new','save','delete','import'])$(id).disabled=value||!canWrite;}
async function api(path,method='GET',packet,rawData){
 const headers={},options={method,headers};
 if(packet!==undefined){headers['Content-Type']='application/json';options.body=rawData===undefined?JSON.stringify(packet):JSON.stringify(packet).slice(0,-1)+',"data":'+rawData+'}';}
 const response=await fetch(path,options);const body=response.status===204?null:await response.json();
 if(!response.ok){const error=Error(body.error||'Request failed');error.body=body;throw error;}return body;
}
function showError(error){
 if(error.body?.error==='revision_conflict')status('The saved config changed to revision '+error.body.current_revision+'. Your draft is kept. Reload the saved version before editing again.');
 else status(error.message);
}
function mayDiscard(){return !dirty||confirm('Discard the unsaved draft?');}
function populate(record){
 selected=record;dirty=false;$('empty').hidden=true;$('form').hidden=false;
 $('heading').textContent=record?'Edit config':'New config';$('identity').textContent=record?record.id+' · Revision '+record.revision:'A new ID will be assigned when saved.';
 $('name').value=record?.name||'';$('description').value=record?.description||'';$('tags').value=(record?.tags||[]).join(', ');$('data').value=JSON.stringify(record?.data||{},null,2);
 $('reload').hidden=$('delete').hidden=$('export').hidden=!record;
 if(record){$('export').href='/configs/'+record.id+'/content';$('export').download=record.name.replace(/[^a-zA-Z0-9_.-]/g,'_')+'.json';}
 for(const id of ['name','description','tags','data'])$(id).readOnly=!canWrite;lock(false);
}
async function openConfig(id){
 if(busy||!mayDiscard())return;lock(true);try{populate(await api('/configs/'+id));status('');}catch(e){showError(e);}finally{lock(false);}
}
async function list(reset=true){
 if(reset){offset=0;$('list').replaceChildren();}
 const params=new URLSearchParams({limit:'50',offset:String(offset)});if($('filter-name').value)params.set('name',$('filter-name').value);for(const tag of tagList($('filter-tags').value))params.append('tag',tag);
 const page=await api('/configs?'+params);
 for(const config of page.configs){const button=document.createElement('button');button.type='button';button.textContent=config.name;const small=document.createElement('small');small.textContent=(config.tags.join(', ')||'No tags')+' · revision '+config.revision;button.append(small);button.onclick=()=>openConfig(config.id);$('list').append(button);}
 offset+=page.configs.length;$('more').hidden=!page.has_more;
 if(!offset){const empty=document.createElement('p');empty.textContent='No matching configs.';$('list').append(empty);}
}
$('filters').onsubmit=async event=>{event.preventDefault();if(busy)return;try{await list();}catch(e){showError(e);}};
$('more').onclick=async()=>{if(busy)return;$('more').disabled=true;try{await list(false);}catch(e){showError(e);}finally{$('more').disabled=false;}};
$('new').onclick=()=>{if(!busy&&canWrite&&mayDiscard()){populate(null);status('');}};
$('reload').onclick=()=>{if(selected)openConfig(selected.id);};
$('form').oninput=()=>{dirty=true;};
$('form').onsubmit=async event=>{
 event.preventDefault();if(busy||!canWrite)return;
 try{
  const raw=$('data').value;const data=JSON.parse(raw);if(!data||Array.isArray(data)||typeof data!=='object')throw Error('JSON data must be an object.');
  const packet={name:$('name').value,description:$('description').value,tags:tagList($('tags').value)};if(selected)packet.revision=selected.revision;
  lock(true);const saved=await api('/configs'+(selected?'/'+selected.id:''),selected?'PATCH':'POST',packet,raw);
  populate(saved);status('Saved revision '+saved.revision+'.');await list();
 }catch(e){showError(e);}finally{lock(false);}
};
$('delete').onclick=async()=>{
 if(busy||!selected||!canWrite||!confirm('Delete this config permanently? Unsaved changes will also be discarded.'))return;
 lock(true);try{await api('/configs/'+selected.id+'?revision='+selected.revision,'DELETE');selected=null;dirty=false;$('form').hidden=true;$('empty').hidden=false;status('Config deleted.');await list();}catch(e){showError(e);}finally{lock(false);}
};
$('import').onchange=async()=>{
 const file=$('import').files[0];if(!file||busy||!canWrite)return;
 try{if(file.size>131072)throw Error('Import file exceeds 128 KiB.');const raw=await file.text();const parsed=JSON.parse(raw);if(!parsed||Array.isArray(parsed)||typeof parsed!=='object')throw Error('Import must contain a JSON object.');$('data').value=raw;dirty=true;status('Imported into the draft. Save to apply it.');}catch(e){showError(e);}finally{$('import').value='';}
};
$('expand').onclick=()=>{const expanded=$('editor').classList.toggle('expanded');$('expand').textContent=expanded?'Shrink editor':'Expand editor';};
window.addEventListener('beforeunload',event=>{if(dirty){event.preventDefault();event.returnValue='';}});
sparkSession.ready.then(async session=>{canWrite=session.mode==='open'||session.permissions.includes('configs.write');lock(false);await list();}).catch(showError);
</script></html>)HTML";
}
