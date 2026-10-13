#!/usr/bin/env python3
"""Real multi-drive storage, failure handling, dashboard backup and restore."""
from concurrent.futures import ThreadPoolExecutor
import gzip
import json
import os
from pathlib import Path
import shutil
import socket
import subprocess
import tempfile
import time
import urllib.parse
import urllib.request
import urllib.error

assert os.environ.get('SPARK_DB_NAME','').startswith('spark_test_')
assert os.environ.get('SPARK_DB_SOCKET')
repo=Path.cwd()
db_name='spark_test_drives_'+str(os.getpid())
restore_db=db_name+'_restore'
mysql=['mariadb','--no-defaults','--socket='+os.environ['SPARK_DB_SOCKET'],'--user=root']
def sql(query,db=db_name):
    result=subprocess.run(mysql+['--batch','--skip-column-names',db],input=query,text=True,capture_output=True)
    if result.returncode: raise AssertionError(result.stderr)
    return result.stdout.strip()
def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1',0));return sock.getsockname()[1]
http,tcp=port(),port()
def request(method,path,data=None,status=200,headers=None):
    headers=dict(headers or {})
    if isinstance(data,dict): headers['Content-Type']='application/json';data=json.dumps(data).encode()
    req=urllib.request.Request(f'http://127.0.0.1:{http}'+path,data=data,method=method,headers=headers)
    try:
        with urllib.request.urlopen(req,timeout=5) as response: code,body,h=response.status,response.read(),response.headers
    except urllib.error.HTTPError as error: code,body,h=error.code,error.read(),error.headers
    assert code==status,(method,path,code,body)
    if h.get_content_type()=='application/json': body=json.loads(body)
    return body,h

def wait_ready():
    for _ in range(100):
        try: request('GET','/health');return
        except OSError: time.sleep(.1)
    raise AssertionError('server unavailable')

subprocess.run(mysql,input=f'CREATE DATABASE {db_name}; CREATE DATABASE {restore_db};',text=True,check=True)
process=None
try:
    schema='\n'.join(p.read_text() for p in sorted((repo/'sql').glob('*.sql')))
    sql(schema)
    with tempfile.TemporaryDirectory(prefix='spark-drives-') as workspace, tempfile.TemporaryDirectory(prefix='spark-drives-backup-',dir='/dev/shm') as backups, tempfile.TemporaryFile() as log:
        root=Path(workspace);a=root/'A';b=root/'B';a.mkdir();b.mkdir()
        config=root/'instance.json'
        def configure(roots,db=db_name):
            config.write_text(json.dumps({'server':{'host':'127.0.0.1','port':http,'notification_port':tcp},'database':{'name':db},'storage':{'drives':[str(p) for p in roots]}}))
        env=dict(os.environ,SPARK_BACKUP_HISTORY=str(root/'history.tsv'))
        def start():
            global process
            process=subprocess.Popen([str(repo/'build/spark_server'),'--config',str(config)],env=env,stdout=log,stderr=log)
            try: wait_ready()
            except BaseException:
                log.flush();log.seek(0);print(log.read().decode());raise
        def stop():
            global process
            if process is not None: process.terminate();process.wait(timeout=10);process=None
        def rejected():
            result=subprocess.run([str(repo/'build/spark_server'),'--config',str(config)],env=env,capture_output=True,text=True,timeout=10)
            assert result.returncode!=0,(result.stdout,result.stderr)
        legacy='aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa'
        (b/(legacy+'.blob')).write_bytes(b'legacy')
        sql(f"INSERT INTO files(id,name,directory,state,size_bytes) VALUES('{legacy}','legacy.txt','/','ready',6)")
        sql("INSERT IGNORE INTO file_directories(path) VALUES('/legacy-empty'),('/projects/Legacy'); INSERT INTO projects(id,name,description,directory) VALUES('cccccccc-cccc-cccc-cccc-cccccccccccc','Legacy','migrate','/projects/Legacy')")

        configure([a,b]);start()
        aid=(a/'.spark-drive').read_text().strip();bid=(b/'.spark-drive').read_text().strip()
        assert aid!=bid
        assert request('GET','/files/'+legacy)[0]['drive_id']==bid
        def upload(name,data,directory='/',drive=None):
            return request('POST','/files?name='+name+'&directory='+urllib.parse.quote(directory,safe='')+('&drive='+drive if drive else ''),data,201,{'Content-Type':'application/octet-stream'})[0]
        letters={d['id']:d['letter'] for d in request('GET','/files/drives')[0]['drives']};al,bl=letters[aid],letters[bid]
        first=upload('first.txt',b'first');second=upload('second.txt',b'second',drive=bl)
        assert first['drive_id']==aid and second['drive_id']==bid
        assert request('GET','/projects/cccccccc-cccc-cccc-cccc-cccccccccccc')[0]['drive_id']==aid
        assert '/legacy-empty' in request('GET','/files/directories?drive='+al)[0]['directories']
        assert '/legacy-empty' not in request('GET','/files/directories?drive='+bl)[0]['directories']
        assert (a/(first['id']+'.blob')).read_bytes()==b'first'
        assert (b/(second['id']+'.blob')).read_bytes()==b'second'
        content='/files/'+second['id']+'/content'
        _,headers=request('GET',content)
        revised=request('PUT',content,b'edited',headers={'Content-Type':'text/plain','If-Match':headers['ETag']})[0]
        assert revised['drive_id']==bid
        assert not (b/(second['id']+'.blob')).exists()
        assert (b/(second['id']+'.'+revised['content_revision']+'.blob')).read_bytes()==b'edited'
        moved=request('PATCH','/files/'+first['id'],{'directory':'/logical/move'})[0]
        assert moved['drive_id']==aid and (a/(first['id']+'.blob')).exists()
        project=request('POST','/projects',{'name':'Multi','description':'drive deletion'},201)[0]
        peer=request('POST','/projects',{'name':'Multi','description':'other drive','drive':bl},201)[0]
        assert peer['directory']==project['directory'] and peer['drive_id']==bid
        sql(schema) # Reapplying older migrations must not restore global uniqueness.
        one=upload('p1',b'one',project['directory'],al);two=upload('p2',b'two',peer['directory'],bl)
        assert request('GET','/project/'+project['id']+'/files')[0]['files']==[one]
        assert request('GET','/project/'+peer['id']+'/files')[0]['files']==[two]
        request('POST','/files?name=ambiguous&directory='+urllib.parse.quote(peer['directory'],safe=''),b'bad',409,{'Content-Type':'application/octet-stream'})

        request('DELETE','/projects/'+project['id'],{'destroy_directory':True},204)
        assert request('GET','/files/'+two['id']+'/content')[0]==b'two'
        assert peer['directory'] in request('GET','/files/directories?drive='+bl)[0]['directories']
        request('DELETE','/projects/'+peer['id'],{'destroy_directory':True},204)
        for file in [one,two]:
            assert not (a/(file['id']+'.blob')).exists() and not (b/(file['id']+'.blob')).exists()
        # A request too large for any single drive is rejected before reading bytes.
        with socket.create_connection(('127.0.0.1',http),timeout=5) as sock:
            sock.sendall(b'POST /files?name=too-big HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/octet-stream\r\nContent-Length: 999999999999999999\r\nConnection: close\r\n\r\n')
            assert b'507' in sock.recv(1024).split(b'\r\n',1)[0]
        # Replacing a live path must not redirect file writes onto the replacement.
        relocated=root/'relocated-A';a.rename(relocated);a.mkdir()
        request('GET','/files/'+first['id']+'/content',status=503)
        request('POST','/files?name=offline',b'bad',503,{'Content-Type':'application/octet-stream'})
        assert list(a.iterdir())==[]
        stop();a.rmdir();a=relocated
        interrupted='bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb'
        sql(f"INSERT INTO files(id,name,directory,drive_id,state) VALUES('{interrupted}','partial','/','{bid}','uploading')")
        (b/(interrupted+'.part')).write_bytes(b'incomplete')
        orphan=b/(second['id']+'.'+'f'*32+'.blob');orphan.write_bytes(b'orphan')
        configure([b,a]);start()
        assert not orphan.exists() and not (b/(interrupted+'.part')).exists()
        assert sql(f"SELECT COUNT(*) FROM files WHERE id='{interrupted}'")=='0'
        assert request('GET','/files/'+first['id']+'/content')[0]==b'first'
        assert request('GET',content)[0]==b'edited'
        stop()
        # Runtime letters survive detachment, reconnection and restart.
        configure([a,b]);start()
        rows=request('GET','/files/drives')[0]['drives']
        letters={d['id']:d['letter'] for d in rows};al,bl=letters[aid],letters[bid]
        assert next(d for d in rows if d['id']==aid)['persistent']
        request('DELETE','/files/drives/'+al,status=409)
        assert request('GET','/files?drive='+bl)[0]['files'][0]['drive_letter']==bl
        offline_project=request('POST','/projects',{'name':'Offline project','description':'retain metadata','drive':bl},201)[0]
        project_file=request('POST','/files?name=project&drive='+bl+'&directory='+urllib.parse.quote(offline_project['directory'],safe=''),b'project',201,{'Content-Type':'application/octet-stream'})[0]
        request('POST','/files/directories',{'directory':'/A-only/empty','drive':al})
        request('POST','/files/directories',{'directory':'/B-only/empty','drive':bl})
        for letter in (al,bl): request('POST','/files/directories',{'directory':'/same/child','drive':letter})
        request('PATCH','/files/directories',{'directory':'/same','destination':'/moved','drive':bl})
        assert '/same/child' in request('GET','/files/directories?drive='+al)[0]['directories']
        assert '/moved/child' in request('GET','/files/directories?drive='+bl)[0]['directories']
        request('DELETE','/files/directories',{'directory':'/moved','drive':bl,'recursive':True},204)
        request('DELETE','/files/directories',{'directory':'/same','drive':al,'recursive':True},204)
        adirs=request('GET','/files/directories?drive='+al)[0]['directories'];bdirs=request('GET','/files/directories?drive='+bl)[0]['directories']
        assert '/A-only/empty' in adirs and '/B-only/empty' not in adirs
        assert '/B-only/empty' in bdirs and '/A-only/empty' not in bdirs
        child=request('POST','/projects',{'name':'Child','description':'inherit','parent_id':offline_project['id']},201)[0]
        assert child['drive_id']==bid
        request('POST','/projects',{'name':'Wrong','description':'wrong','parent_id':offline_project['id'],'drive':al},409)
        auto=upload('automatic',b'project routing',child['directory']);assert auto['drive_id']==bid
        # Detach waits for a streaming upload to publish, then releases the root.
        with socket.create_connection(('127.0.0.1',http),timeout=5) as slow, ThreadPoolExecutor(max_workers=1) as workers:
            slow.sendall(('POST /files?name=slow&drive='+bl+' HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/octet-stream\r\nContent-Length: 1\r\nConnection: close\r\n\r\n').encode())
            time.sleep(.15)
            detached=workers.submit(request,'DELETE','/files/drives/'+bl,None,204)
            time.sleep(.15);assert not detached.done()
            slow.sendall(b'x');reply=b''
            while True:
                part=slow.recv(4096)
                if not part:break
                reply+=part
            assert b'201' in reply.split(b'\r\n',1)[0],reply
            detached.result(timeout=5)

        request('DELETE','/projects/'+offline_project['id'],{'destroy_directory':True},503)
        offline=request('GET','/projects/'+offline_project['id']+'?depth=deep')[0]
        assert offline['name']=='Offline project' and not offline['drive_online'] and not offline['subprojects'][0]['drive_online']
        assert not request('GET','/project/'+offline_project['id']+'/files')[0]['drive_online']
        assert request('PATCH','/projects/'+offline_project['id'],{'description':'offline edit'})[0]['description']=='offline edit'
        request('POST','/files/directories',{'directory':'/offline-new','drive':bl},503)
        request('PATCH','/files/directories',{'directory':'/B-only','destination':'/new','drive':bl},503)
        request('DELETE','/files/directories',{'directory':'/B-only','recursive':True,'drive':bl},503)

        request('POST','/projects',{'name':'Offline child','description':'fail','parent_id':offline_project['id']},503)
        request('POST','/files?name=offline&directory='+urllib.parse.quote(child['directory'],safe=''),b'bad',503,{'Content-Type':'application/octet-stream'})
        assert '/B-only/empty' in request('GET','/files/directories?drive='+bl)[0]['directories']


        assert request('GET','/files/'+second['id'])[0]['drive_letter']==bl
        request('GET',content,status=503)
        request('DELETE','/files/'+second['id'],status=503)
        assert request('GET','/files/'+second['id'])[0]['id']==second['id']
        request('POST','/files?name=offline&drive='+bl,b'bad',503,{'Content-Type':'application/octet-stream'})
        local=upload('local.txt',b'local');assert local['drive_id']==aid
        stop();start()
        assert not next(d for d in request('GET','/files/drives')[0]['drives'] if d['id']==bid)['online']
        # Swap disks at the same mount location: the new identity gets a new letter.
        parked=root/'parked-B';b.rename(parked);b.mkdir()
        third=request('POST','/files/drives',{'path':str(b)},201)[0]
        assert third['letter'] not in [al,bl] and third['online']
        third_file=request('POST','/files?name=third&drive='+third['letter'],b'third',201,{'Content-Type':'application/octet-stream'})[0]
        assert third_file['drive_id']==third['id']
        request('DELETE','/files/drives/'+third['letter'],status=204)
        third_root=root/'third';b.rename(third_root);parked.rename(b)
        restored=request('POST','/files/drives',{'path':str(b)},201)[0];assert restored['letter']==bl
        assert request('GET',content)[0]==b'edited'
        # Restore the third drive under a different path; runtime paths persist.
        assert request('POST','/files/drives',{'path':str(third_root)},201)[0]['letter']==third['letter']
        stop();start();assert request('GET','/files/'+third_file['id']+'/content')[0]==b'third'
        # Unexpected external removal leaves server storage working across restart.
        absent=root/'absent-B';b.rename(absent);b.mkdir();stop();start();assert list(b.iterdir())==[]
        assert not next(d for d in request('GET','/files/drives')[0]['drives'] if d['id']==bid)['online']
        assert request('GET','/files/'+local['id']+'/content')[0]==b'local'
        b.rmdir();absent.rename(b);assert request('POST','/files/drives',{'path':str(b)},201)[0]['letter']==bl
        # Reject overlapping roots and cloned identities before recovery can touch data.
        foreign=root/'foreign';foreign.mkdir();(foreign/'keep.txt').write_text('keep')
        request('POST','/files/drives',{'path':str(foreign)},409);assert (foreign/'keep.txt').read_text()=='keep'
        nested=a/'nested';nested.mkdir();request('POST','/files/drives',{'path':str(nested)},409)
        c=root/'clone';c.mkdir();shutil.copy(a/'.spark-drive',c/'.spark-drive')
        request('POST','/files/drives',{'path':str(c)},409)
        # Dashboard takes both sources offline, backs them up and restarts them.
        request('POST','/admin/backup',{'mount':'/dev/shm','destination':backups},202,{'X-SPARK-Admin':'dashboard'})
        deadline=time.monotonic()+60
        while time.monotonic()<deadline:
            try:
                state=request('GET','/admin/status')[0]
                if state['state'] in ('succeeded','failed'):
                    assert state['state']=='succeeded',state;break
            except OSError: pass
            time.sleep(.2)
        else: raise AssertionError('backup timeout')
        stop()
        archives=list(Path(backups).glob('spark-*'));assert len(archives)==1
        archive=archives[0]
        assert (archive/'manifest.txt').read_text().startswith('format=spark-backup-v2\n')
        subprocess.run([str(repo/'scripts/verify-backup.sh'),str(archive)],check=True,stdout=subprocess.DEVNULL)
        # A partial source list must never publish a completed backup.
        backup_env=dict(env,SPARK_DB_NAME=db_name)
        omitted=subprocess.run([str(repo/'scripts/backup.sh'),'--mount','/dev/shm','--destination',backups+'/partial','--file-dir',str(a)],env=backup_env,capture_output=True,text=True)
        assert omitted.returncode!=0 and not list((Path(backups)/'partial').glob('spark-*'))
        # Restore the SQL and every drive archive to new locations, reverse order.
        dump=gzip.decompress(b''.join(p.read_bytes() for p in sorted(archive.glob('database.sql.gz.part-*'))))
        subprocess.run(mysql+[restore_db],input=dump,check=True)
        roots=[]
        for line in (archive/'drives.tsv').read_text().splitlines():
            identity,_=line.split('\t');destination=root/('restored-'+identity);destination.mkdir();roots.append(destination)
            data=gzip.decompress(b''.join(p.read_bytes() for p in sorted(archive.glob('drive-'+identity+'.tar.gz.part-*'))))
            subprocess.run(['tar','--extract','--file=-','--directory',str(destination),'--no-same-owner'],input=data,check=True)
            assert not (destination/'.spark.lock').exists()
        configure(list(reversed(roots)),restore_db);start()
        for file,body in [(first,b'first'),(second,b'edited'),({'id':legacy},b'legacy')]:
            assert request('GET','/files/'+file['id']+'/content')[0]==body
        stop()
        part=next(archive.glob('drive-*.tar.gz.part-*'));part.unlink()
        invalid=subprocess.run([str(repo/'scripts/verify-backup.sh'),str(archive)],capture_output=True)
        assert invalid.returncode!=0
        print('Drive-scoped directory/project migration, isolation, inheritance, offline guards, SSD swaps, backup and restore passed.')
except BaseException:
    if 'log' in globals() and not log.closed: log.seek(0);print(log.read().decode())
    raise
finally:
    if process is not None: process.terminate();process.wait(timeout=10)
    subprocess.run(mysql,input=f'DROP DATABASE IF EXISTS {db_name}; DROP DATABASE IF EXISTS {restore_db};',text=True,check=True)
