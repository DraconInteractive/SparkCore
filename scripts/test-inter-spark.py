#!/usr/bin/env python3
"""Two isolated SPARK instances; real HTTPS, backup creation and verified transfers."""
import gzip,json,os,socket,subprocess,tempfile,time,urllib.request,urllib.error,tarfile,io
from pathlib import Path
from remote_proxy import RemoteProxy,free_port
assert os.environ.get('SPARK_DB_NAME','').startswith('spark_test_')
assert os.environ.get('SPARK_TEST_NGINX'), 'Select real nginx fixture'
mysql=['mariadb','--no-defaults','--socket='+os.environ['SPARK_DB_SOCKET'],'--user=root']
repo=Path.cwd(); processes=[]; proxy=None; databases=[]
def sql(query,db=None):subprocess.run(mysql+([db] if db else []),input=query,text=True,check=True,stdout=subprocess.DEVNULL)
def call(port,path,method='GET',body=None,token=None,status=200,raw=None):
    headers={'Authorization':'Bearer '+token} if token else {}
    if body is not None:raw=json.dumps(body).encode();headers['Content-Type']='application/json';headers['X-SPARK-Admin']='dashboard'
    req=urllib.request.Request(f'http://127.0.0.1:{port}'+path,data=raw,headers=headers,method=method)
    try:
        with urllib.request.urlopen(req,timeout=40) as r:code,data=r.status,r.read()
    except urllib.error.HTTPError as e:code,data=e.code,e.read()
    assert code==status,(path,code,data)
    return json.loads(data) if data else None

def wait(fn,timeout=90):
    end=time.monotonic()+timeout
    while time.monotonic()<end:
        try:
            value=fn()
            if value:return value
        except (OSError,ValueError):pass
        time.sleep(.1)
    raise AssertionError('Timed out')
try:
    with tempfile.TemporaryDirectory(prefix='spark-inter-') as workspace,tempfile.TemporaryFile() as log:
        root=Path(workspace);all_permissions=json.loads(subprocess.check_output(['./build/spark_credentials','permissions']))
        instances=[]
        for label in ('a','b'):
            folder=root/label;folder.mkdir();db='spark_test_inter_'+label+'_'+str(os.getpid());databases.append(db);sql('CREATE DATABASE '+db);sql('\n'.join(p.read_text() for p in sorted((repo/'sql').glob('*.sql'))),db)
            credentials=folder/'credentials.json';subprocess.run(['./build/spark_credentials','init',str(credentials)],check=True,stdout=subprocess.DEVNULL)
            def issue(kind,subject,permissions):return json.loads(subprocess.check_output(['./build/spark_credentials','issue',str(credentials),kind,subject,*permissions]))
            admin=issue('administrator','owner',all_permissions)['token'];device=issue('device','spark-a',['files.read','files.write']);reader=issue('device','reader',['files.read'])['token'];minimal=issue('device','minimal',[])['token']
            port=free_port();config=folder/'config.json';config.write_text(json.dumps({'server':{'host':'127.0.0.1','port':port,'notification_port':free_port()},'storage':{'drives':[str(folder/'files')]},'database':{'name':db},'auth':{'provider':'tokens','credentials_file':str(credentials)}}))
            instances.append(dict(folder=folder,port=port,config=config,admin=admin,device=device,reader=reader,minimal=minimal,credentials=credentials))
        a,b=instances;proxy=RemoteProxy(b['folder'],b['config']);proxy.start()
        env=dict(os.environ,CURL_CA_BUNDLE=str(proxy.cert),SPARK_BACKUP_MOUNT='',SPARK_BACKUP_DIR='')
        for item in instances:
            proc=subprocess.Popen(['./build/spark_server','--config',str(item['config'])],env=env,stdout=log,stderr=log);processes.append(proc)
            wait(lambda:call(item['port'],'/health',token=item['admin']))
        def local(path,method='GET',body=None,status=200,token=None):return call(a['port'],path,method,body,token or a['admin'],status)
        assert call(b['port'],'/auth/verify','POST',{'device_id':'minimal'},b['minimal'])['permissions']==[]
        call(b['port'],'/auth/verify','POST',{'device_id':'wrong'},b['device']['token'],403)
        call(b['port'],'/auth/verify','POST',{'device_id':'spark-a'},None,401)
        call(b['port'],'/auth/verify','POST',{'device_id':'owner'},b['admin'],403)
        local('/connections',token=a['reader'],status=403)
        connection={'name':'SPARK B','url':proxy.origin,'device_id':'spark-a','token':b['device']['token'],'drive':'A','directory':'/backups'}
        local('/connections','POST',{**connection,'url':'http://localhost'},400)
        local('/connections','POST',{**connection,'url':proxy.origin.replace('localhost','127.0.0.1')},502)
        created=local('/connections','POST',connection,201);cid=created['id'];assert 'token' not in created and created['permissions']==['files.read','files.write']
        assert b['device']['token'] not in json.dumps(local('/connections'))
        assert (a['folder']/'files/.spark-control/connections.json').stat().st_mode&0o777==0o600
        local('/admin/backup','POST',{},202)
        wait(lambda: local('/admin/status')['state']=='succeeded')
        backups=local('/backups')['backups'];assert len(backups)==1,backups
        first=backups[0]['id'];source=a['folder']/'files/.spark-backups'/first
        subprocess.run(['./scripts/verify-backup.sh',str(source)],check=True,stdout=subprocess.DEVNULL)
        archive=b''.join(p.read_bytes() for p in sorted(source.glob('files.tar.gz.part-*')))
        with tarfile.open(fileobj=io.BytesIO(gzip.decompress(archive))) as tar:
            assert not any('.spark-control' in n or '.spark-backups' in n for n in tar.getnames())
        def job(request):return local('/backups/'+first+'/transfers','POST',request,202)['job_id']
        def completed(jid):
            found=next(j for j in local('/backups/jobs')['jobs'] if j['id']==jid)
            return found if found['state'] in ('failed','succeeded','interrupted') else None
        jid=job({'action':'send','connection_id':cid});result=wait(lambda:completed(jid));assert result['state']=='succeeded',result
        directory=result['remote_directory'];from urllib.parse import quote
        remote=call(b['port'],'/files?directory='+quote(directory,safe=''),token=b['admin'])['files'];assert any(f['name']=='SPARK-BACKUP-COMPLETE.json' for f in remote)
        assert source.exists()
        # Re-sending the same backup reuses verified remote file IDs.
        jid=job({'action':'send','connection_id':cid});assert wait(lambda:completed(jid))['state']=='succeeded'
        assert call(b['port'],'/files?directory='+quote(directory,safe=''),token=b['admin'])['files']==remote
        # Loss of destination permission is discovered before uploading.
        local('/connections/'+cid,'PATCH',{'device_id':'reader','token':b['reader']})
        jid=job({'action':'send','connection_id':cid});assert wait(lambda:completed(jid))['state']=='failed'
        local('/connections/'+cid,'PATCH',{'device_id':'spark-a','token':b['device']['token']})
        retry=local('/backups/jobs/'+jid+'/retry','POST',{},202)['job_id'];assert wait(lambda:completed(retry))['state']=='succeeded'
        # Attach another storage location, copy, then verified move.
        other=root/'other';other.mkdir();drive=local('/files/drives','POST',{'path':str(other)},201)['letter']
        jid=job({'action':'copy','drive':drive});result=wait(lambda:completed(jid));assert result['state']=='succeeded',result
        assert source.exists() and (other/'.spark-backups'/first).exists()
        jid=job({'action':'move','drive':drive});assert wait(lambda:completed(jid))['state']=='succeeded';assert not source.exists()
        local('/backups/'+first,'DELETE',status=404)
        # A new local backup excludes previous backup archives on every drive.
        local('/admin/backup','POST',{},202);wait(lambda:local('/admin/status')['state']=='succeeded')
        second=local('/backups')['backups'][0]['id'];local('/backups/'+second,'DELETE',status=204);assert not local('/backups')['backups']
        local('/connections/'+cid,'DELETE',status=204);assert not local('/connections')['connections']
        print('Local backup exclusion, two-instance HTTPS verification, permissions, idempotent send, retry, drive copy/move and deletion passed.')
except BaseException:
    # Server output intentionally excludes credentials.
    raise
finally:
    for proc in processes:proc.terminate()
    for proc in processes:proc.wait(timeout=70)
    if proxy:proxy.close()
    for db in databases:sql('DROP DATABASE IF EXISTS '+db)
