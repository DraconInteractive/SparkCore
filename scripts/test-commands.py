#!/usr/bin/env python3
"""Command lifecycle against test-local-db's private MariaDB and real REST/TCP."""
import concurrent.futures
import json
import os
import socket
import subprocess
import tempfile
import time
import urllib.request
import urllib.error

assert os.environ.get('SPARK_DB_NAME', '').startswith('spark_test_')
assert os.environ.get('SPARK_DB_SOCKET')

def free_port():
    with socket.socket() as s:
        s.bind(('127.0.0.1',0))
        return s.getsockname()[1]

http,tcp=free_port(),free_port()
def call(method,path,body=None,status=200):
    req=urllib.request.Request(f'http://127.0.0.1:{http}'+path,method=method,data=None if body is None else json.dumps(body).encode(),headers={'Content-Type':'application/json'})
    try:
        with urllib.request.urlopen(req,timeout=10) as r: code,data=r.status,r.read()
    except urllib.error.HTTPError as e: code,data=e.code,e.read()
    assert code==status,(method,path,code,data)
    return json.loads(data) if data else None

def sql(statement):
    if statement.startswith("CREATE TRIGGER"):
        statement="DELIMITER //\n"+statement+"//\nDELIMITER ;\n"
    result=subprocess.run(['mariadb','--no-defaults','--socket='+os.environ['SPARK_DB_SOCKET'],'--user=root',os.environ['SPARK_DB_NAME'],'--batch','--skip-column-names','-e',statement],check=True,capture_output=True,text=True)
    return result.stdout.strip()

class Peer:
    def __init__(self,device,cursor=None):
        self.sock=socket.create_connection(('127.0.0.1',tcp),timeout=5)
        self.sock.settimeout(5)
        self.file=self.sock.makefile('rb')
        packet={'type':'hello','device_id':device}
        packet['after_sequence']=boundary() if cursor is None else cursor
        self.sock.sendall((json.dumps(packet)+'\n').encode())
        self.welcome=self.read()
        assert self.welcome['type']=='welcome',self.welcome
    def read(self):
        packet=json.loads(self.file.readline())
        if packet['type']=='ping': self.sock.sendall(b'{"type":"pong"}\n')
        return packet
    def through(self,cursor):
        events=[]
        while True:
            p=self.read()
            if p.get('event_id'): events.append(p)
            if p.get('sequence',-1)>=cursor: return events
    def close(self): self.file.close();self.sock.close()

def boundary():
    # Wait for the server dispatcher to sequence every committed event.
    end=time.monotonic()+8
    while time.monotonic()<end:
        if sql('SELECT COUNT(*) FROM events e LEFT JOIN notification_log n ON n.event_id=e.event_id WHERE n.event_id IS NULL')=='0':
            return int(sql('SELECT COALESCE(MAX(sequence),0) FROM notification_log'))
        time.sleep(.1)
    raise AssertionError('dispatcher stalled')

with tempfile.TemporaryFile() as log:
    process=subprocess.Popen(['./build/spark_server','--host','127.0.0.1','--port',str(http),'--notification-port',str(tcp)],stdout=log,stderr=log)
    peers=[]
    try:
        for attempt in range(100):
            try: call('GET','/health');break
            except OSError: time.sleep(.1)
        else: raise AssertionError('server did not start')
        for device in ['cmd-source','cmd-a','cmd-b','cmd-outsider']:
            call('POST','/devices/'+device,{'local_ip':'127.0.0.1'},201)
        for device in ['cmd-a','cmd-b']:
            call('PUT',f'/devices/{device}/actions',{'actions':['myservice.status','myservice.start']})
        call('PUT','/devices/missing/actions',{'actions':[]},404)
        call('PUT','/devices/cmd-a/actions',{'actions':['x','x']},400)
        call('PUT','/devices/cmd-a/actions',{'actions':['bad action']},400)
        assert call('GET','/devices/cmd-a/actions')['actions']==['myservice.start','myservice.status']
        with urllib.request.urlopen(f'http://127.0.0.1:{http}/commands/explorer') as response:
            html=response.read().decode()
            assert response.status==200 and response.headers.get_content_type()=='text/html'
            assert 'Run once' in html and '/commands/devices' in html and 'idempotency_key' in html
        assert call('GET','/commands/devices')['devices']==[
            {'id':'cmd-a','local_ip':'127.0.0.1','external_ip':None,'actions':['myservice.start','myservice.status']},
            {'id':'cmd-b','local_ip':'127.0.0.1','external_ip':None,'actions':['myservice.start','myservice.status']}]
        assert call('GET','/commands/devices?limit=1&offset=1')['devices'][0]['id']=='cmd-b'
        assert call('GET','/commands/devices?offset=100')['devices']==[]
        call('GET','/commands/devices?limit=0',status=400)
        call('GET','/commands/devices?limit=1&limit=2',status=400)
        call('GET','/commands/devices?unknown=1',status=400)
        call('POST','/commands/explorer',{},405)
        call('POST','/commands/devices',{},405)
        for device in ['cmd-source','cmd-a','cmd-b','cmd-outsider']: peers.append(Peer(device))
        source,a,b,outsider=peers
        cursor=boundary()
        def create(key,**extra):
            packet={'source_device':'cmd-source','action':'myservice.status','idempotency_key':key,**extra}
            return call('POST','/commands',packet,202)
        target=create('targeted',target_device='cmd-a')
        target_id=target['id'];path='/commands/'+target_id
        mark=boundary()
        frames=[p.through(mark) for p in peers]
        available=lambda frames:[e for e in frames if e['type']=='command.available' and e['resource']==path]
        assert not available(frames[0]) and len(available(frames[1]))==1 and not available(frames[2]) and not available(frames[3]),frames
        assert not any(e['type'].startswith('command.') for e in frames[3])
        assert call('POST','/devices/cmd-b/commands/claim',{})['command'] is None
        assert call('POST','/devices/cmd-outsider/commands/claim',{})['command'] is None
        claim=call('POST','/devices/cmd-a/commands/claim',{})['command']
        assert claim['id']==target_id and len(claim['claim_token'])==64
        assert 'claim_token' not in call('GET',path)
        assert 'claim_token' not in json.dumps(call('GET','/commands?source_device=cmd-source'))
        credentials={'device_id':'cmd-a','claim_token':claim['claim_token']}
        call('POST',path+'/renew',{**credentials,'claim_token':'0'*64},403)
        call('POST',path+'/renew',{**credentials,'device_id':'cmd-b'},403)
        call('POST',path+'/renew',{**credentials,'lease_seconds':120})
        call('POST',path+'/cancel',{'source_device':'cmd-source'},409)
        completion={**credentials,'status':'succeeded','result':{'state':'running'}}
        # Failing notification insert must roll back the completion too.
        sql("CREATE TRIGGER reject_command BEFORE INSERT ON events FOR EACH ROW BEGIN IF NEW.type='command.updated' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='intentional command rollback'; END IF; END")
        call('POST',path+'/complete',completion,503)
        sql('DROP TRIGGER reject_command')
        assert call('GET',path)['status']=='running'
        done=call('POST',path+'/complete',completion)
        assert done['result']=={'state':'running'}
        before=sql('SELECT COUNT(*) FROM events')
        assert call('POST',path+'/complete',completion)==done
        assert sql('SELECT COUNT(*) FROM events')==before
        call('POST',path+'/complete',{**completion,'result':{}},409)
        assert create('targeted',target_device='cmd-a')['id']==target_id
        call('POST','/commands',{'source_device':'cmd-source','action':'different','idempotency_key':'targeted','target_device':'cmd-a'},409)
        # Exact-ID claims honor client ordering and never fall back to queued work.
        older=create('priority-older')
        newer=create('priority-newer')
        claim_path='/devices/cmd-a/commands/claim'
        for invalid in [None,123,'','not-a-uuid',[],{}]:
            call('POST',claim_path,{'command_id':invalid},400)
        assert call('POST',claim_path,{'command_id':'00000000-0000-0000-0000-000000000000'})['command'] is None
        assert call('POST',claim_path,{'command_id':target_id})['command'] is None
        only_b=create('priority-target-b',target_device='cmd-b')
        unsupported=create('priority-unsupported',action='future.action')
        for blocked in [only_b,unsupported]:
            assert call('POST',claim_path,{'command_id':blocked['id']})['command'] is None
        selected=call('POST',claim_path,{'command_id':newer['id'],'lease_seconds':120})['command']
        assert selected['id']==newer['id']
        assert call('GET','/commands/'+older['id'])['status']=='queued'
        assert call('POST',claim_path,{'command_id':newer['id']})['command'] is None
        assert call('POST','/devices/cmd-b/commands/claim',{'command_id':newer['id']})['command'] is None
        call('POST','/commands/'+newer['id']+'/complete',{'device_id':'cmd-a','claim_token':selected['claim_token'],'status':'succeeded'})
        # Omitting command_id retains the original oldest-eligible queue behavior.
        selected=call('POST',claim_path,{})['command']
        assert selected['id']==older['id']
        call('POST','/commands/'+older['id']+'/complete',{'device_id':'cmd-a','claim_token':selected['claim_token'],'status':'succeeded'})
        for blocked in [only_b,unsupported]:
            call('POST','/commands/'+blocked['id']+'/cancel',{'source_device':'cmd-source'})
            assert call('POST',claim_path,{'command_id':blocked['id']})['command'] is None
        # Concurrent retries create one durable command and concurrent claims one winner.
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            records=list(pool.map(lambda _:create('race'),range(2)))
        assert records[0]['id']==records[1]['id']
        race=records[0]['id'];race_path='/commands/'+race
        fallback=create('race-fallback')
        with concurrent.futures.ThreadPoolExecutor(max_workers=2) as pool:
            claims=list(pool.map(lambda d:call('POST',f'/devices/{d}/commands/claim',{'command_id':race})['command'],['cmd-a','cmd-b']))
        winners=[c for c in claims if c]
        assert len(winners)==1 and winners[0]['id']==race,claims
        winner=winners[0]
        assert call('GET','/commands/'+fallback['id'])['status']=='queued'
        call('POST','/commands/'+fallback['id']+'/cancel',{'source_device':'cmd-source'})
        # Lease expiry never reassigns work, even if another eligible worker is available.
        sql("UPDATE commands SET lease_expires_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE id='"+race+"'")
        assert call('GET',race_path)['status']=='outcome_unknown'
        call('POST',race_path+'/renew',{'device_id':winner['executing_device'],'claim_token':winner['claim_token']},409)
        call('POST',race_path+'/complete',{'device_id':winner['executing_device'],'claim_token':winner['claim_token'],'status':'succeeded'},409)
        for device in ['cmd-a','cmd-b']: assert call('POST',f'/devices/{device}/commands/claim',{})['command'] is None
        # Cancellation, queued expiry, and arrival of a previously unavailable action.
        queued=create('cancel',action='future.action')
        call('POST','/commands/'+queued['id']+'/cancel',{'source_device':'cmd-outsider'},403)
        cancelled=call('POST','/commands/'+queued['id']+'/cancel',{'source_device':'cmd-source'})
        assert cancelled['status']=='cancelled'
        assert call('POST','/commands/'+queued['id']+'/cancel',{'source_device':'cmd-source'})==cancelled
        expired=create('expire',action='future.action')
        sql("UPDATE commands SET expires_at=UTC_TIMESTAMP(6)-INTERVAL 1 SECOND WHERE id='"+expired['id']+"'")
        # Scheduler expiry works without a GET/claim to trigger it.
        deadline=time.monotonic()+5
        while time.monotonic()<deadline and sql("SELECT status FROM commands WHERE id='"+expired['id']+"'")!='expired': time.sleep(.1)
        assert call('GET','/commands/'+expired['id'])['status']=='expired'
        assert call('POST','/devices/cmd-a/commands/claim',{'command_id':expired['id']})['command'] is None
        late=create('late',action='future.action')
        assert call('POST','/devices/cmd-a/commands/claim',{})['command'] is None
        call('PUT','/devices/cmd-a/actions',{'actions':['future.action','myservice.status']})
        accepted=call('POST','/devices/cmd-a/commands/claim',{})['command']
        assert accepted['id']==late['id']
        call('POST','/commands/'+late['id']+'/complete',{'device_id':'cmd-a','claim_token':accepted['claim_token'],'status':'failed','error':{'code':'unsupported_action','message':'Handler was unavailable'}})
        call('PUT','/devices/cmd-a/actions',{'actions':[]})
        assert call('GET','/devices/cmd-a/actions')['actions']==[]
        assert not any(d['id']=='cmd-a' for d in call('GET','/commands/devices')['devices'])
        # Restore advertisement for replay-time eligibility checks.
        call('PUT','/devices/cmd-a/actions',{'actions':['myservice.status']})
        end=boundary()
        replay=Peer('cmd-a',cursor);peers.append(replay)
        history=[]
        while True:
            event=replay.read()
            if event.get('event_id'): history.append(event)
            if event['type']=='catchup_complete': break
        assert any(e['type']=='command.available' and e['resource']==path for e in history)
        assert 'claim_token' not in json.dumps(history)
        unrelated=Peer('cmd-outsider',cursor);peers.append(unrelated)
        while True:
            event=unrelated.read()
            assert not event['type'].startswith('command.'),event
            if event['type']=='catchup_complete': break
        assert len(call('GET','/commands?source_device=cmd-source&status=succeeded&limit=1&offset=0')['commands'])==1
        call('GET','/commands?limit=101',status=400)
        call('GET','/commands?source_device=cmd-source&source_device=cmd-a',status=400)
        # Device deletion removes advertisements, retains command result history.
        call('DELETE','/devices/cmd-b',status=204)
        assert sql("SELECT COUNT(*) FROM device_actions WHERE device_id='cmd-b'")=='0'
        assert call('GET',path)['result']=={'state':'running'}
        # Restart the actual process: advertisements, pending work and live tokens survive.
        running=create('restart-running')
        saved=call('POST','/devices/cmd-a/commands/claim',{})['command']
        assert saved['id']==running['id']
        pending=create('restart-pending')
        for peer in peers: peer.close()
        peers=[]
        process.terminate();process.wait(timeout=10)
        process=subprocess.Popen(['./build/spark_server','--host','127.0.0.1','--port',str(http),'--notification-port',str(tcp)],stdout=log,stderr=log)
        for attempt in range(100):
            try: call('GET','/health');break
            except OSError: time.sleep(.1)
        else: raise AssertionError('server did not restart')
        call('POST','/commands/'+saved['id']+'/complete',{'device_id':'cmd-a','claim_token':saved['claim_token'],'status':'succeeded','result':'restored claim'})
        recovered=call('POST','/devices/cmd-a/commands/claim',{})['command']
        assert recovered['id']==pending['id']
        call('POST','/commands/'+pending['id']+'/complete',{'device_id':'cmd-a','claim_token':recovered['claim_token'],'status':'succeeded'})
        print('Command routing, atomic claims, idempotency, leases, expiry, rollback, action registration and TCP replay/filtering passed.')
    except BaseException:
        log.seek(0);print(log.read().decode());raise
    finally:
        for peer in peers: peer.close()
        process.terminate();process.wait(timeout=10)
