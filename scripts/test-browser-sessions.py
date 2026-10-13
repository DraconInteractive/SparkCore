#!/usr/bin/env python3
"""Real token-mode REST/TCP coverage in a disposable database and file root."""
from http.client import HTTPConnection
import json
import os
from pathlib import Path
import re
import socket
import subprocess
import tempfile
import time
import uuid

assert os.environ.get('SPARK_DB_NAME', '').startswith('spark_test_')
assert os.environ.get('SPARK_DB_SOCKET')
database = 'spark_test_auth_' + uuid.uuid4().hex
sql_args = ['mariadb', '--no-defaults', '--socket='+os.environ['SPARK_DB_SOCKET'], '--user=root', '--batch', '--skip-column-names']
def sql(statement, db=True):
    return subprocess.run(sql_args + ([database] if db else []), input=statement, text=True, capture_output=True, check=True).stdout.strip()
def port():
    with socket.socket() as s:
        s.bind(('127.0.0.1', 0))
        return s.getsockname()[1]
http, tcp = port(), port()
def call(method, path, token=None, body=None, status=200, headers=None, raw=None):
    h = {'Content-Type':'application/json', **(headers or {})}
    if token is not None: h['Authorization']='Bearer '+token
    payload = raw if raw is not None else json.dumps(body).encode() if body is not None else None
    c=HTTPConnection('127.0.0.1', http, timeout=5)
    try:
        c.request(method, path, payload, h)
        r=c.getresponse(); data=r.read()
        assert r.status==status,(method,path,r.status,data[:300])
        if status==401: assert r.getheader('WWW-Authenticate')
        if data and r.getheader('Content-Type','').startswith('application/json'): return json.loads(data)
        return data
    finally:c.close()
def wait_start(token):
    for _ in range(100):
        try:call('GET','/health',token);return
        except OSError:time.sleep(.1)
    raise AssertionError('server did not start')
def boundary():
    for _ in range(80):
        if sql('SELECT COUNT(*) FROM events e LEFT JOIN notification_log n ON n.event_id=e.event_id WHERE n.event_id IS NULL')=='0':
            return int(sql('SELECT COALESCE(MAX(sequence),0) FROM notification_log'))
        time.sleep(.1)
    raise AssertionError('sequencer stalled')
class Peer:
    def __init__(self, device, token=None, cursor=None, error=None):
        self.sock=socket.create_connection(('127.0.0.1',tcp),timeout=5)
        self.file=self.sock.makefile('rb');p={'type':'hello','device_id':device}
        if token is not None:p['token']=token
        if cursor is not None:p['after_sequence']=cursor
        self.sock.sendall((json.dumps(p)+'\n').encode());self.welcome=self.read()
        assert self.welcome.get('error')==error if error else self.welcome['type']=='welcome',self.welcome
    def read(self):
        p=json.loads(self.file.readline())
        if p['type']=='ping':self.sock.sendall(b'{"type":"pong"}\n')
        return p
    def through(self, cursor):
        result=[]
        while True:
            p=self.read()
            if 'event_id' in p:result.append(p)
            if p.get('sequence',-1)>=cursor:return result
    def close(self):self.file.close();self.sock.close()


# Browser session integration. Isolated TLS proxy is a test fixture, not deployment.
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import ssl
import threading
import asyncio

def browser_call(method,path,cookie=None,csrf=None,body=None,status=200,origin=True,extra=None):
    headers={'Host':browser_host,'Content-Type':'application/json'}
    if origin:headers['Origin']=browser_origin
    if cookie:headers['Cookie']=cookie
    if csrf:headers['X-SPARK-CSRF']=csrf
    headers.update(extra or {})
    c=HTTPConnection('127.0.0.1',http,timeout=5)
    c.request(method,path,None if body is None else json.dumps(body).encode(),headers)
    r=c.getresponse();data=r.read();response_headers=dict(r.getheaders());code=r.status;c.close()
    assert code==status,(method,path,code,data[:300])
    return (json.loads(data) if data and response_headers.get('Content-Type','').startswith('application/json') else data),response_headers

class Proxy(BaseHTTPRequestHandler):
    def log_message(self,*args):pass
    def do_GET(self):self.forward()
    do_HEAD=do_POST=do_PUT=do_PATCH=do_DELETE=do_GET
    def forward(self):
        body=self.rfile.read(int(self.headers.get('Content-Length','0')))
        headers={k:v for k,v in self.headers.items() if k.lower() not in ('connection','transfer-encoding')}
        c=HTTPConnection('127.0.0.1',http,timeout=10)
        try:
            c.request(self.command,self.path,body,headers);r=c.getresponse();data=r.read();self.send_response(r.status)
            for k,v in r.getheaders():
                if k.lower() not in ('connection','transfer-encoding','content-length'):self.send_header(k,v)
            self.send_header('Content-Length',str(len(data)));self.end_headers()
            if self.command!='HEAD':self.wfile.write(data)
        except OSError:self.send_error(503)
        finally:c.close()

async def exercise_firefox(profile,admin,root):
    import websockets
    debug=port()
    with open(root/'firefox.log','wb') as log:
        proc=subprocess.Popen(['firefox','--headless','--no-remote','--profile',str(profile),'--remote-debugging-port',str(debug)],stdout=log,stderr=log)
        try:
            ws=None
            for _ in range(100):
                try:ws=await websockets.connect(f'ws://127.0.0.1:{debug}/session',open_timeout=2);break
                except OSError:await asyncio.sleep(.1)
            assert ws is not None,'Firefox BiDi unavailable'
            sequence=0
            async def command(method,params):
                nonlocal sequence
                sequence+=1;identity=sequence;await ws.send(json.dumps({'id':identity,'method':method,'params':params}))
                while True:
                    response=json.loads(await asyncio.wait_for(ws.recv(),20))
                    if response.get('id')==identity:
                        assert response.get('type')!='error',response
                        return response['result']
            await command('session.new',{'capabilities':{'alwaysMatch':{'acceptInsecureCerts':True}}})
            context=(await command('browsingContext.create',{'type':'tab'}))['context']
            async def navigate(path):await command('browsingContext.navigate',{'context':context,'url':browser_origin+path,'wait':'complete'})
            async def evaluate(expression):
                result=await command('script.evaluate',{'expression':expression,'target':{'context':context},'awaitPromise':True})
                assert result['type']=='success',result
                return result.get('result',{}).get('value')
            async def wait(expression,timeout=10):
                for _ in range(int(timeout*10)):
                    if await evaluate(expression):return
                    await asyncio.sleep(.1)
                raise AssertionError('Browser condition timed out: '+expression)
            await navigate('/files/explorer')
            await wait("location.pathname==='/login'")
            await evaluate("document.getElementById('token').value="+json.dumps(admin)+";document.getElementById('login').requestSubmit();true")
            await wait("location.pathname==='/files/explorer' && !!window.sparkSession")
            await wait("document.body.textContent.includes('Signed in as owner')")
            assert not await evaluate("document.cookie.includes('__Host-spark_session')")
            assert not await evaluate("Object.values(localStorage).some(v=>v.includes('spark1_'))")
            await navigate('/admin/credentials/explorer')
            await wait("!document.getElementById('issue').disabled")
            await evaluate("document.getElementById('subject').value='firefox-watch';document.getElementById('create').requestSubmit();true")
            await wait("!document.getElementById('secret').hidden && document.getElementById('token').value.startsWith('spark1_') && document.getElementById('records').textContent.includes('firefox-watch')")
            assert not await evaluate("Object.values(localStorage).concat(Object.values(sessionStorage)).some(v=>v.includes('spark1_'))")
            await evaluate("document.getElementById('dismiss').click();true")
            assert await evaluate("document.getElementById('token').value===''")
            await evaluate("window.confirm=()=>true;Array.from(document.querySelectorAll('#records article')).find(a=>a.querySelector('strong').textContent==='firefox-watch').querySelector('button').click();true")
            await wait("Array.from(document.querySelectorAll('#records article')).some(a=>a.querySelector('strong').textContent==='firefox-watch' && a.textContent.includes('Revoked'))")
            await navigate('/files/explorer')
            # Actual modal creates a file, XHR uploads another, and text editing saves.
            await wait("!document.getElementById('file-create').disabled")
            await evaluate("document.getElementById('file-create').click();document.getElementById('file-name').value='browser-created';document.getElementById('file-name').form.requestSubmit();true")
            await wait("document.getElementById('rows').textContent.includes('browser-created.txt')")
            await evaluate("upload([new File(['browser text'],'browser-upload.txt',{type:'text/plain'})]).then(()=>true)")
            await wait("document.getElementById('rows').textContent.includes('browser-upload.txt')")
            await evaluate("Array.from(document.querySelectorAll('#rows button.name')).find(b=>b.textContent.includes('browser-upload.txt')).click();true")
            await wait("!document.getElementById('text-edit').hidden")
            await evaluate("document.getElementById('text-edit').click();document.getElementById('text-editor').value='edited in browser';document.getElementById('text-editor').dispatchEvent(new Event('input'));document.getElementById('text-save').click();true")
            await wait("document.getElementById('text-status').textContent==='Saved to shared storage.'")
            data=await evaluate("fetch('/files').then(r=>r.json()).then(async p=>{const f=p.files.find(x=>x.name==='browser-upload.txt');return await (await fetch(f.download_url)).text();})")
            assert data=='edited in browser',data
            await evaluate("document.getElementById('detail-close').click();document.getElementById('mkdir').click();document.getElementById('destination').value='/browser-folder';document.getElementById('edit-form').requestSubmit();true")
            await wait("!busy && directories.includes('/browser-folder')")
            await evaluate("Array.from(document.querySelectorAll('#rows tr')).find(r=>r.textContent.includes('browser-folder')).querySelector('button').click();document.getElementById('destination').value='/browser-renamed';document.getElementById('edit-form').requestSubmit();true")
            await wait("!busy && directories.includes('/browser-renamed') && !directories.includes('/browser-folder')")
            await evaluate("Array.from(document.querySelectorAll('#rows tr')).find(r=>r.textContent.includes('browser-renamed')).querySelectorAll('button')[1].click();document.getElementById('edit-form').requestSubmit();true")
            await wait("!busy && !directories.includes('/browser-renamed')")
            await navigate('/connections/explorer')
            await wait("document.getElementById('title').textContent==='SPARK connections'")
            await evaluate("document.getElementById('new').click();true")
            await wait("document.getElementById('dialog').open")
            await evaluate("const f=document.getElementById('form');f.elements.name.value='Invalid HTTP';f.elements.url.value='http://localhost';f.elements.device_id.value='spark-a';f.elements.token.value='spark1_'+'a'.repeat(64);f.requestSubmit();true")
            await wait("document.getElementById('error').textContent==='https_origin_required'")
            await evaluate("document.getElementById('cancel').click();true")
            assert await evaluate("document.querySelector('input[type=password]').value===''")
            await navigate('/backups/explorer')
            await wait("document.getElementById('title').textContent==='Local backups'")
            await evaluate("document.getElementById('new').click();true")
            await wait("document.getElementById('dialog').open")
            await evaluate("document.getElementById('form').requestSubmit();true")
            await wait("document.querySelectorAll('#rows article').length===1",timeout=60)
            await evaluate("window.confirm=()=>true;Array.from(document.querySelectorAll('#rows button')).find(b=>b.textContent==='Delete').click();true")
            await wait("document.querySelectorAll('#rows article').length===0")
            await navigate('/projects/explorer');await wait("!!window.sparkSession")
            assert await evaluate("sparkSession.ready.then(s=>s.subject)")=='owner'
            await navigate('/commands/explorer');await wait("document.getElementById('devices').textContent.includes('browser-executor')")
            await evaluate("document.querySelector('#devices button').click();document.getElementById('form').requestSubmit();true")
            await wait("document.getElementById('runs').textContent.includes('queued')")
            await navigate('/api/docs');await wait("document.getElementById('endpoints').textContent.includes('/files')")
            await navigate('/');await wait("document.getElementById('status').textContent.includes('Online')")
            await evaluate("Array.from(document.querySelectorAll('button')).find(b=>b.textContent==='Sign out').click();true")
            await wait("location.pathname==='/login'")
            await ws.close()
        finally:proc.terminate();proc.wait(timeout=15)
    print('Firefox HTTPS login, device-token provisioning/revocation, HttpOnly cookie, file modal/upload/edit, project/docs/command explorers, dashboard and logout passed.')

sql('CREATE DATABASE '+database,False)
try:
    for migration in sorted(Path('sql').glob('*.sql')):sql(migration.read_text())
    with tempfile.TemporaryDirectory(prefix='spark-browser-') as directory, tempfile.TemporaryFile() as log:
        root=Path(directory);credentials=root/'credentials.json'
        def cli(*args):return subprocess.run(['./build/spark_credentials',*map(str,args)],check=True,capture_output=True,text=True).stdout
        cli('init',credentials)
        def issue(kind,subject,*permissions):return json.loads(cli('issue',credentials,kind,subject,*permissions))
        admin_record=issue('administrator','owner',*json.loads(cli('permissions')));admin=admin_record['token']
        other=issue('administrator','other','commands.invoke')['token']
        viewer=issue('administrator','viewer','discovery.read','files.read')['token']
        device=issue('device','browser-executor','devices.write','commands.advertise','commands.execute')['token']
        tls=port();browser_host='localhost:'+str(tls);browser_origin='https://'+browser_host
        config=root/'instance.json';config.write_text(json.dumps({'server':{'host':'127.0.0.1','port':http,'notification_port':tcp},'storage':{'drives':[str(root/'files')]},'auth':{'provider':'tokens','credentials_file':str(credentials),'browser_origin':browser_origin}}))
        env=dict(os.environ,SPARK_DB_NAME=database)
        proc=subprocess.Popen(['./build/spark_server','--config',str(config)],env=env,stdout=log,stderr=log)
        proxy=None
        try:
            wait_start(admin)
            call('POST','/devices/browser-executor',device,{'local_ip':'127.0.0.1'},201)
            call('PUT','/devices/browser-executor/actions',device,{'actions':['browser.status']})
            def login(token,status=200,extra=None):return browser_call('POST','/auth/login',body={'token':token},status=status,extra={'X-SPARK-Login':'1',**(extra or {})})
            login(device,401);login('wrong',401);login(admin,403,{'Origin':'https://evil.example'})
            login(admin,403,{'Host':'evil.example'})
            packet,headers=login(admin);cookie=headers['Set-Cookie'].split(';')[0];csrf=packet['csrf_token']
            assert 'Secure; HttpOnly; SameSite=Strict' in headers['Set-Cookie']
            browser_call('GET','/auth/session',cookie)
            browser_call('POST','/files/directories',cookie,body={'directory':'/denied'},status=403)
            browser_call('POST','/files/directories',cookie,csrf,{'directory':'/allowed'})
            browser_call('POST','/files/directories',cookie,csrf,{'directory':'/denied'},403,extra={'Origin':'https://evil.example'})
            browser_call('POST','/files/directories',cookie,csrf,{'directory':'/denied'},403,origin=False)
            browser_call('POST','/files/directories',cookie,csrf,{'directory':'/denied'},403,extra={'Sec-Fetch-Site':'cross-site'})
            vp,vh=login(viewer);vc=vh['Set-Cookie'].split(';')[0]
            browser_call('GET','/files',vc)
            browser_call('POST','/files/directories',vc,vp['csrf_token'],{'directory':'/denied'},403)
            browser_call('POST','/admin/restart',vc,vp['csrf_token'],{},403,extra={'X-SPARK-Admin':'dashboard'})
            # Provisioning uses the same cookie/CSRF gate; tokens never return in lists.
            browser_call('GET','/admin/credentials/explorer',cookie)
            browser_call('GET','/admin/credentials',vc,status=403)
            payload={'subject':'managed-watch','permissions':['discovery.read','events.read','notifications.read']}
            browser_call('POST','/admin/credentials',cookie,body=payload,status=403)
            browser_call('POST','/admin/credentials',cookie,csrf,payload,403,extra={'Origin':'https://evil.example'})
            minted=browser_call('POST','/admin/credentials',cookie,csrf,payload,201)[0]
            call('GET','/health',minted['token'])
            call('POST','/timers',minted['token'],{'duration_seconds':60},403)
            records=browser_call('GET','/admin/credentials',cookie)[0]
            assert minted['token'] not in json.dumps(records) and 'token_sha256' not in json.dumps(records)
            assert all(r['kind']=='device' for r in records['credentials'])
            for invalid in ({**payload,'kind':'administrator'},{**payload,'subject':'bad/name'},{**payload,'permissions':[]},{**payload,'permissions':['events.read','events.read']}):
                browser_call('POST','/admin/credentials',cookie,csrf,invalid,400)
            browser_call('POST','/admin/credentials',cookie,csrf,{**payload,'permissions':['admin.backup']},403)
            limited=issue('administrator','provisioner','admin.credentials','discovery.read')['token']
            call('POST','/admin/credentials',limited,payload,403)
            call('POST','/admin/credentials',limited,{'subject':'limited-device','permissions':['discovery.read']},201)
            browser_call('DELETE','/admin/credentials/'+admin_record['id'],cookie,csrf,status=404)
            browser_call('DELETE','/admin/credentials/'+minted['id'],cookie,status=403)
            browser_call('DELETE','/admin/credentials/'+minted['id'],cookie,csrf,status=204)
            browser_call('DELETE','/admin/credentials/'+minted['id'],cookie,csrf,status=204)
            call('GET','/health',minted['token'],status=401)
            # A browser never creates a fake device: command requester comes from identity.
            command=browser_call('POST','/commands',cookie,csrf,{'action':'browser.status','idempotency_key':'browser'},202)[0]
            assert command['source_device'] is None and command['source_admin']=='owner'
            assert sql("SELECT COUNT(*) FROM devices")=='1'
            assert browser_call('POST','/commands',cookie,csrf,{'action':'browser.status','idempotency_key':'browser'},202)[0]['id']==command['id']
            call('POST','/commands/'+command['id']+'/cancel',other,{},403)
            browser_call('POST','/commands/'+command['id']+'/cancel',cookie,csrf,{})
            browser_call('POST','/commands',cookie,csrf,{'source_admin':'other','action':'browser.status','idempotency_key':'forged'},400)
            # Rotation invalidates the previous cookie and CSRF token.
            replacement,rh=browser_call('POST','/auth/login',cookie,body={'token':admin},extra={'X-SPARK-Login':'1'})
            browser_call('GET','/auth/session',cookie,status=401)
            cookie=rh['Set-Cookie'].split(';')[0];csrf=replacement['csrf_token']
            browser_call('POST','/admin/restart',cookie,csrf,{},202,extra={'X-SPARK-Admin':'dashboard'})
            time.sleep(1.2);wait_start(admin);browser_call('GET','/auth/session',cookie)
            browser_call('POST','/auth/logout',cookie,csrf,status=204);browser_call('GET','/files',cookie,status=401)
            # Real Firefox through a disposable local HTTPS proxy.
            subprocess.run(['openssl','req','-x509','-newkey','rsa:2048','-nodes','-keyout',str(root/'key.pem'),'-out',str(root/'cert.pem'),'-days','1','-subj','/CN=localhost'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
            proxy=ThreadingHTTPServer(('127.0.0.1',tls),Proxy);context=ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER);context.load_cert_chain(root/'cert.pem',root/'key.pem');proxy.socket=context.wrap_socket(proxy.socket,server_side=True);threading.Thread(target=proxy.serve_forever,daemon=True).start()
            if os.environ.get('SPARK_TEST_FIREFOX')=='1':
                profile=root/'profile';profile.mkdir();(profile/'user.js').write_text('user_pref("remote.active-protocols", 1);\nuser_pref("browser.shell.checkDefaultBrowser", false);\nuser_pref("datareporting.policy.dataSubmissionEnabled", false);\n')
                asyncio.run(exercise_firefox(profile,admin,root))
            packet,headers=login(admin);cookie=headers['Set-Cookie'].split(';')[0]
            cli('revoke',credentials,admin_record['id']);browser_call('GET','/auth/session',cookie,status=401)
            print('Browser login, device-token provisioning/revocation, CSRF, origin, permissions, administrator command identity, rotation, logout, restart and revocation passed.')
        except BaseException:
            log.seek(0);print(log.read().decode())
            if (root/'firefox.log').exists():print((root/'firefox.log').read_text()[-3000:])
            raise
        finally:
            if proxy:proxy.shutdown();proxy.server_close()
            proc.terminate();proc.wait(timeout=10)
finally:sql('DROP DATABASE '+database,False)
