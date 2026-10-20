"""Exercise shared-calendar synchronization over the existing real HTTPS fixture."""
import json, subprocess, time, uuid

def check_calendar_sync(a,b,local,call,wait,proxy,mysql,restart):
    def peer(path,method='GET',body=None,status=200,token=None):
        return call(b['port'],path,method,body,token or b['admin'],status)
    def issue(perms):
        return json.loads(subprocess.check_output(['./build/spark_credentials','issue',str(b['credentials']),'device','calendar-coordinator',*perms]))['token']
    for saved in local('/connections')['connections']:
        if 'calendar.sync.write' not in saved['permissions']:
            local('/connections/'+saved['id']+'/calendar-sync','PUT',{'enabled':True},403)
    read_token=issue(['calendar.sync.read'])
    peer('/calendar-sync',token=read_token)
    peer('/calendar-sync/apply','POST',{},403,token=read_token)
    token=issue(['calendar.sync.read','calendar.sync.write'])
    c=local('/connections','POST',{'name':'Calendar peer','url':proxy.origin,'device_id':'calendar-coordinator','token':token},201)
    cid=c['id'];path='/connections/'+cid+'/calendar-sync'
    local(path,token=a['reader'],status=403)
    peer('/calendar-sync',token=b['reader'],status=403)
    peer('/calendar-sync/apply','POST',{},403,token=b['reader'])
    local(path,'PUT',{'enabled':True})
    # A second outgoing relationship cannot be enabled.
    second=local('/connections','POST',{'name':'Second','url':proxy.origin,'device_id':'calendar-coordinator','token':token},201)
    local('/connections/'+second['id']+'/calendar-sync','PUT',{'enabled':True},409)
    local('/connections/'+second['id'],'DELETE',status=204)
    local('/connections/'+cid,'DELETE',status=409)

    def run(success=True):
        job=local(path+'/run','POST',{},202)['job_id']
        def completed():
            found=next(j for j in local('/backups/jobs')['jobs'] if j['id']==job)
            return found if found['state'] in ('succeeded','failed') else None
        result=wait(completed)
        assert result['state']==('succeeded' if success else 'failed'),(result,local(path))
        return result

    def create(client,name,calendar='shared'):
        packet={'calendar':calendar,'name':name,'description':'Sync test','occurs_at':'2035-01-01T00:00:00Z','reminder_minutes':5}
        if name=='Coordinator event':
            packet['recurrence']={'weekdays':[1,2,3,4,5,6,7],'interval_weeks':2,'start_date':'2035-01-01','end_date':'2035-02-01'}
        return client('/events','POST',packet,201)
    def events(client):
        return client('/events?calendar=shared&limit=500')['events']
    private=create(local,'Private coordinator','local');private_peer=create(peer,'Private peer','local')
    first=create(local,'Coordinator event');second_event=create(peer,'Peer event')
    # Force multiple change-feed pages.
    for i in range(52):create(local,'Page '+str(i))
    run()
    assert len(events(local))==54 and len(events(peer))==54
    assert local('/events')['events']==[private] and peer('/events')['events']==[private_peer]
    peer_first=next(e for e in events(peer) if e['name']=='Coordinator event')
    assert peer_first['id']!=first['id'],'sync identity must not depend on local IDs'
    assert peer_first['recurrence']==first['recurrence'] and peer_first['reminder_minutes']==5
    peer_db=json.loads(b['config'].read_text())['database']['name']
    subprocess.run(mysql+[peer_db,'-e',"UPDATE calendar_events SET last_reminder_occurs_at=occurs_at WHERE id='"+peer_first['id']+"'"],check=True)
    peer('/events/'+peer_first['id'],'PATCH',{'name':'Peer edit'})
    run();assert local('/events/'+first['id'])['name']=='Peer edit'
    assert local('/events/'+first['id'])['last_reminder_occurs_at'] is None
    assert peer('/events/'+peer_first['id'])['last_reminder_occurs_at']==peer_first['occurs_at']
    # Coordinator wins simultaneous edits, with both versions retained.
    local('/events/'+first['id'],'PATCH',{'name':'Coordinator wins'})
    peer('/events/'+peer_first['id'],'PATCH',{'name':'Losing edit'})
    run();assert peer('/events/'+peer_first['id'])['name']=='Coordinator wins'
    conflicts=local(path+'/conflicts')['conflicts']
    assert conflicts[-1]['conflict']['peer']['data']['name']=='Losing edit'
    # No notification storm on repeated runs.
    def seq(client):return client('/calendar-sync')['sequence']
    sequence=(seq(local),seq(peer));run();assert sequence==(seq(local),seq(peer))
    # Moving out withdraws the shared copy but preserves the local event.
    local('/events/'+first['id']+'/move','POST',{'calendar':'local'})
    run();peer('/events/'+peer_first['id'],status=404)
    assert local('/events/'+first['id'])['calendar']=='local'
    local('/events/'+first['id']+'/move','POST',{'calendar':'shared'});run()
    assert len([e for e in events(peer) if e['name']=='Coordinator wins'])==1
    # Deletions propagate in either direction.
    peer('/events/'+second_event['id'],'DELETE',status=204);run()
    assert not [e for e in events(local) if e['name']=='Peer event']
    selected=next(e for e in events(local) if e['name']=='Page 0')
    local('/events/'+selected['id'],'DELETE',status=204);run()
    assert not [e for e in events(peer) if e['name']=='Page 0']

    # Compare-and-swap and receipt replay, including operation ID misuse.
    info=peer('/calendar-sync');ours=local('/calendar-sync')
    item=next(e for e in peer('/calendar-sync/changes')['items'] if e['data'])
    request={'instance_id':info['instance_id'],'coordinator_id':ours['instance_id'],
             'operation_id':str(uuid.uuid4()),'sync_id':item['sync_id'],
             'expected_revision':item['revision'],'data':item['data']}
    result=peer('/calendar-sync/apply','POST',request)
    assert result['applied'] and peer('/calendar-sync/apply','POST',request)==result
    peer('/calendar-sync/apply','POST',{**request,'data':{'name':'invalid'}},400)
    peer('/calendar-sync/apply','POST',{**request,'instance_id':str(uuid.uuid4())},400)
    stale={**request,'operation_id':str(uuid.uuid4()),'expected_revision':str(uuid.uuid4())}
    assert not peer('/calendar-sync/apply','POST',stale)['applied']
    reused={**request,'expected_revision':str(uuid.uuid4())}
    assert peer('/calendar-sync/apply','POST',reused,400)['error']=='operation_id_reused'
    # A failed notification insert must roll back the imported event and revision.
    changed={**request,'operation_id':str(uuid.uuid4()),'data':{**item['data'],'description':'Atomic import'}}
    subprocess.run(mysql+[peer_db,'-e',"CREATE TRIGGER sync_outbox_failure BEFORE INSERT ON events FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='fixture'"],check=True)
    try:
        peer('/calendar-sync/apply','POST',changed,503)
        assert peer('/calendar-sync/items/'+item['sync_id'])==item
    finally:subprocess.run(mysql+[peer_db,'-e','DROP TRIGGER sync_outbox_failure'],check=True)
    accepted=peer('/calendar-sync/apply','POST',changed)
    assert accepted['applied'] and peer('/calendar-sync/apply','POST',changed)==accepted

    # Simulate a coordinator restart after the peer commits but before the response
    # is recorded locally: its durable pending request must replay, not duplicate.
    selected=next(e for e in events(local) if e['name']=='Page 1')
    local('/events/'+selected['id'],'PATCH',{'name':'Recovered write'})
    # Locate the portable record independently of its local ID.
    cursor=0;record=None
    while True:
        page=local('/calendar-sync/changes?after='+str(cursor))
        for entry in page['items']:
            if entry['data'] and entry['data']['name']=='Recovered write':record=entry
        if not page['has_more']:break
        cursor=page['next']
    assert record
    other=peer('/calendar-sync/items/'+record['sync_id'])
    op=str(uuid.uuid4())
    remote_request={'instance_id':info['instance_id'],'coordinator_id':ours['instance_id'],'operation_id':op,
                    'sync_id':record['sync_id'],'expected_revision':other['revision'],'data':record['data']}
    local_request={**remote_request,'instance_id':ours['instance_id'],'expected_revision':record['revision']}
    peer('/calendar-sync/apply','POST',remote_request)
    db=json.loads(a['config'].read_text())['database']['name']
    raw=subprocess.check_output(mysql+[db,'--batch','--skip-column-names','-e',"SELECT state FROM calendar_sync_connections WHERE connection_id='"+cid+"'"],text=True)
    state=json.loads(raw);state['pending']={'remote_request':remote_request,'local_request':local_request,'conflict':None};state['status']='running'
    encoded=json.dumps(state).encode().hex()
    subprocess.run(mysql+[db,'-e',"UPDATE calendar_sync_connections SET state=CONVERT(0x"+encoded+" USING utf8mb4) WHERE connection_id='"+cid+"'"],check=True)
    restart()
    assert local('/calendar-sync')['instance_id']==ours['instance_id']
    assert local(path)['status']=='interrupted' and local(path)['pending_operation']
    run();assert len([e for e in events(peer) if e['name']=='Recovered write'])==1
    assert not local(path)['pending_operation']
    # Sync-only permission cannot mutate local events through the general API.
    peer('/events/'+private_peer['id'],'PATCH',{'name':'No'},403,token=token)
    local(path,'PUT',{'enabled':False})
    local(path+'/run','POST',{},409)
    local('/connections/'+cid,'DELETE',status=204)
    local('/backups/jobs/completed','DELETE')
    print('Calendar sync: bidirectional edits/deletes, recurrence, reminder isolation, paging, moves, conflicts, rollback, CAS, restart recovery and permissions passed.')
