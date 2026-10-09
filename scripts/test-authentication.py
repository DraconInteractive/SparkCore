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
proxy = None
def connection():
    return proxy.connection() if proxy else HTTPConnection('127.0.0.1',http,timeout=5)
def call(method, path, token=None, body=None, status=200, headers=None, raw=None):
    h = {'Content-Type':'application/json', **(headers or {})}
    if token is not None: h['Authorization']='Bearer '+token
    payload = raw if raw is not None else json.dumps(body).encode() if body is not None else None
    c=connection()
    try:
        c.request(method, path, payload, h)
        r=c.getresponse(); data=r.read()
        assert r.status==status,(method,path,r.status,data[:300])
        if status==401 and not (proxy and 'Cookie' in h and token is None): assert r.getheader('WWW-Authenticate')
        if data and r.getheader('Content-Type','').startswith('application/json'): return json.loads(data)
        return data
    finally:c.close()
def wait_start(token):
    for _ in range(100):
        try:call('GET','/health',token);return
        except OSError:time.sleep(.1)
        except AssertionError as error:
            if proxy and error.args[0][2]==502:time.sleep(.1)
            else:raise
    raise AssertionError('server did not start')
def boundary():
    for _ in range(80):
        if sql('SELECT COUNT(*) FROM events e LEFT JOIN notification_log n ON n.event_id=e.event_id WHERE n.event_id IS NULL')=='0':
            return int(sql('SELECT COALESCE(MAX(sequence),0) FROM notification_log'))
        time.sleep(.1)
    raise AssertionError('sequencer stalled')
class Peer:
    def __init__(self, device, token=None, cursor=None, error=None):
        self.sock=proxy.peer() if proxy else socket.create_connection(('127.0.0.1',tcp),timeout=5)
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

sql('CREATE DATABASE '+database,False)
try:
    for migration in sorted(Path('sql').glob('*.sql')):sql(migration.read_text())
    with tempfile.TemporaryDirectory(prefix='spark-auth-integration-') as directory, tempfile.TemporaryFile() as log:
        root=Path(directory);credentials=root/'credentials.json'
        def cli(*args):return subprocess.run(['./build/spark_credentials',*map(str,args)],check=True,capture_output=True,text=True).stdout
        cli('init',credentials)
        def issue(kind,subject,*permissions):return json.loads(cli('issue',credentials,kind,subject,*permissions))
        all_permissions=json.loads(cli('permissions'))
        admin=issue('administrator','owner',*all_permissions)['token']
        reader_record=issue('device','auth-reader','discovery.read','events.read','notifications.read')
        reader=reader_record['token']
        nobody=issue('device','auth-nobody')['token']
        writer=issue('device','auth-writer','devices.write','timers.write','events.write','tasks.write','pulse.write','commands.invoke')['token']
        executor=issue('device','auth-executor','devices.write','commands.advertise','commands.execute','commands.read','notifications.read')['token']
        observer=issue('device','auth-executor','commands.read','notifications.read')['token']
        project_editor=issue('device','project-editor','projects.write','boards.write')['token']
        config=root/'instance.json'
        config.write_text(json.dumps({'server':{'host':'127.0.0.1','port':http,'notification_port':tcp},'storage':{'drives':[str(root/'files')]},'auth':{'provider':'tokens','credentials_file':str(credentials)}}))
        if os.environ.get('SPARK_TEST_TLS') == '1':
            from remote_proxy import RemoteProxy
            proxy=RemoteProxy(root,config)
        env=dict(os.environ,SPARK_DB_NAME=database)
        proc=subprocess.Popen(['./build/spark_server','--config',str(config)]+(['--remote'] if proxy else []),env=env,stdout=log,stderr=log)
        peers=[]
        try:
            if proxy:proxy.start()
            wait_start(admin)
            if proxy:
                proxy.check_transport()
                browser_request,browser_headers=proxy.browser_session(admin)
            contract=call('GET','/api',admin)
            if proxy:
                assert contract['notifications']['transport']=='tcp+tls'
                assert contract['notifications']['port']==proxy.tcp
                assert contract['notifications']['host']=='localhost'
            assert contract['authentication']=='bearer'
            assert contract['notifications']['authentication']=='hello.token'
            # Every documented route, plus HEAD aliases, denies missing/invalid and
            # insufficient credentials before any domain mutation or DB validation.
            for endpoint in contract['endpoints']:
                if 'access' in endpoint:continue
                path=re.sub(r'\{[^}]+\}','00000000-0000-0000-0000-000000000000',endpoint['path'])
                methods=[endpoint['method']]+(['HEAD'] if endpoint['method']=='GET' else [])
                assert endpoint['permissions'],endpoint
                for method in methods:
                    for token,code in [(None,401),('invalid',401),(nobody,403)]:call(method,path,token,{},code)
            call('GET','/missing',admin,status=403)
            call('OPTIONS','/files',admin,status=403)
            call('GET','/health?token='+admin,status=401)
            call('GET','/health',headers={'Cookie':'token='+admin},status=401)
            # Reject duplicate Authorization and unsupported schemes.
            for values in [('Bearer '+admin,'Bearer '+admin),('Basic '+admin,)]:
                c=connection();c.putrequest('GET','/health')
                for value in values:c.putheader('Authorization',value)
                c.endheaders();r=c.getresponse();assert r.status in ((400,401) if proxy and len(values)>1 else (401,));r.read();c.close()
            call('GET','/events',reader)
            call('POST','/devices/auth-writer',writer,{'local_ip':'127.0.0.1'},201)
            call('POST','/devices/auth-executor',executor,{'local_ip':'127.0.0.1'},201)
            call('POST','/devices/not-me',writer,{'local_ip':'127.0.0.1'},403)
            call('PATCH','/devices/auth-executor',writer,{'local_ip':'127.0.0.2'},403)
            call('DELETE','/devices/auth-executor',writer,status=403)
            call('POST','/pulse/subscriptions/auth-executor',writer,{},403)
            call('POST','/pulse/subscriptions/auth-writer',writer)
            call('POST','/timers',writer,{'duration_seconds':3600,'source_device':'auth-executor'},403)
            timer=call('POST','/timers',writer,{'duration_seconds':3600,'source_device':'auth-writer'},201)
            call('POST','/timers/'+timer['id']+'/stop',writer,{'source_device':'auth-executor'},403)
            for method,path,payload in [
                ('POST','/events',{'name':'Spoof','description':'','occurs_at':'2030-01-01T00:00:00Z'}),
                ('POST','/daily-tasks',{'name':'Spoof','description':''}),
                ('PATCH','/pulse',{'enabled':False}),
                ('POST','/commands',{'action':'auth.status','idempotency_key':'spoof'})]:
                call(method,path,writer,{**payload,'source_device':'auth-executor'},403)
            # Chunked JSON must pass attribution validation after bounded reading.
            c=connection()
            c.request('POST','/timers',iter([b'{"duration_seconds":3600,"source_device":"auth-executor"}']),{'Authorization':'Bearer '+writer,'Content-Type':'application/json'},encode_chunked=True)
            r=c.getresponse();assert r.status==403;r.read();c.close()
            call('PUT','/devices/auth-executor/actions',executor,{'actions':['auth.status']})
            call('PUT','/devices/auth-writer/actions',executor,{'actions':['auth.status']},403)
            command=call('POST','/commands',writer,{'source_device':'auth-writer','action':'auth.status','idempotency_key':'auth-1'},202)
            call('POST','/devices/auth-executor/commands/claim',writer,{},403)
            claim=call('POST','/devices/auth-executor/commands/claim',executor,{'command_id':command['id']})['command']
            assert claim['id']==command['id']
            completion={'device_id':'auth-executor','claim_token':claim['claim_token'],'status':'succeeded'}
            call('POST','/commands/'+command['id']+'/complete',admin,{**completion,'claim_token':'0'*64},403)
            other=issue('device','auth-other','commands.execute')['token']
            call('POST','/commands/'+command['id']+'/complete',other,completion,403)
            call('POST','/commands/'+command['id']+'/renew',executor,{'device_id':'auth-executor','claim_token':claim['claim_token']})
            call('POST','/commands/'+command['id']+'/complete',executor,completion)
            # Project file and cascade permissions cover indirect operations.
            call('POST','/projects',project_editor,{'name':'Denied','description':''},403)
            project=call('POST','/projects',admin,{'name':'Auth project','description':''},201)
            file_reader=issue('device','file-reader','files.read')['token']
            call('GET','/project/'+project['id']+'/files',file_reader,status=403)
            call('DELETE','/projects/'+project['id'],project_editor,{'destroy_directory':True},403)
            call('DELETE','/projects/'+project['id'],project_editor,{'destroy_directory':False},204)
            # Stream upload/download/edit routes cannot evade the gate.
            uploaded=call('POST','/files?name=auth.txt',admin,status=201,raw=b'hello',headers={'Content-Type':'application/octet-stream'})
            content='/files/'+uploaded['id']+'/content'
            call('GET',content,reader,status=403)
            assert call('GET',content,file_reader)==b'hello'
            call('PUT',content,reader,status=403,raw=b'changed',headers={'Content-Type':'text/plain'})
            uploader=issue('device','auth-uploader','files.write')
            c=connection()
            c.putrequest('POST','/files?name=revoked-upload.txt')
            c.putheader('Authorization','Bearer '+uploader['token'])
            c.putheader('Content-Type','application/octet-stream');c.putheader('Transfer-Encoding','chunked');c.endheaders()
            c.send(b'5\r\nhello\r\n')
            for _ in range(30):
                if sql("SELECT COUNT(*) FROM files WHERE name='revoked-upload.txt' AND state='uploading'")=='1':break
                time.sleep(.05)
            else:raise AssertionError('upload did not begin')
            cli('revoke',credentials,uploader['id']);c.send(b'0\r\n\r\n')
            r=c.getresponse();assert r.status==401;r.read();c.close()
            assert sql("SELECT COUNT(*) FROM files WHERE name='revoked-upload.txt'")=='0'
            # TCP requires identity, ownership, and notifications.read.
            for token,device,error in [(None,'auth-reader','unauthorized'),('invalid','auth-reader','unauthorized'),(reader,'auth-executor','forbidden'),(nobody,'auth-nobody','forbidden')]:
                p=Peer(device,token,error=error);p.close()
            call('POST','/pulse/subscriptions/auth-reader',admin)
            cursor=boundary()
            read_peer=Peer('auth-reader',reader,cursor);peers.append(read_peer)
            command_peer=Peer('auth-executor',executor,cursor);peers.append(command_peer)
            observer_peer=Peer('auth-executor',observer,cursor);peers.append(observer_peer)
            assert read_peer.welcome['resources']==['/events'],read_peer.welcome
            for p in peers:p.through(cursor)
            event=call('POST','/events',admin,{'name':'Auth event','description':'','occurs_at':'2030-01-01T00:00:00Z'},201)
            call('POST','/timers',admin,{'duration_seconds':3600},201)
            call('PATCH','/pulse',admin,{'interval_seconds':604800})
            queued=call('POST','/commands',writer,{'source_device':'auth-writer','action':'auth.status','idempotency_key':'auth-2'},202)
            mark=boundary()
            received=read_peer.through(mark)
            assert any(e['type']=='calendar.created' for e in received),received
            assert all(e['type'].startswith('calendar.') for e in received),received
            available=command_peer.through(mark)
            assert any(e['type']=='command.available' and e['resource']=='/commands/'+queued['id'] for e in available),available
            assert not any(e['type']=='command.available' for e in observer_peer.through(mark))
            replay=Peer('auth-reader',reader,cursor);peers.append(replay)
            received=replay.through(mark)
            assert any(e['type']=='calendar.created' for e in received)
            assert all(e['type'].startswith('calendar.') for e in received)
            cli('revoke',credentials,reader_record['id'])
            call('GET','/events',reader,status=401)
            for p in (read_peer,replay):
                while True:
                    packet=p.read()
                    if packet['type']=='error':assert packet['error']=='unauthorized';break
            p=Peer('auth-reader',reader,error='unauthorized');p.close()
            # Dashboard restart preserves auth, and does not switch to open mode.
            call('POST','/admin/restart',admin,{},202,headers={'X-SPARK-Admin':'dashboard'})
            time.sleep(1.5);wait_start(admin)
            call('GET','/health',status=401)
            if proxy:
                proxy.check_transport()
                browser_request('GET','/auth/session',headers=browser_headers)
                browser_request('POST','/auth/logout',headers=browser_headers,status=204)
                browser_request('GET','/auth/session',headers=browser_headers,status=401)
            call('GET','/events',reader,status=401)
            p=Peer('auth-executor',executor);peers.append(p)
            saved=credentials.read_bytes()
            altered=json.loads(saved)
            for record in altered['credentials']:
                if record['subject']=='auth-executor':record['permissions']=[]
            candidate=root/'credentials.new';candidate.write_text(json.dumps(altered));candidate.chmod(0o600);candidate.replace(credentials)
            while True:
                packet=p.read()
                if packet['type']=='error':assert packet['error']=='unauthorized';break
            call('GET','/commands',executor,status=403)
            credentials.write_bytes(saved)
            p=Peer('auth-executor',executor);peers.append(p)
            credentials.write_text('{bad')
            call('GET','/health',admin,status=401)
            while True:
                packet=p.read()
                if packet['type']=='error':assert packet['error']=='unauthorized';break
            credentials.write_bytes(saved)
            call('GET','/health',admin)
        except BaseException:
            log.seek(0);print(log.read().decode());raise
        finally:
            for peer in peers:peer.close()
            proc.terminate();proc.wait(timeout=10)
            if proxy:proxy.close()
        if proxy:print('Real nginx HTTPS/TCP TLS, certificate verification, private backends, browser CSRF and restart passed.')
        print('Authenticated REST allowlist, ownership, streams, claims, TCP live/replay/revocation and maintenance restart passed.')
finally:
    sql('DROP DATABASE '+database,False)
