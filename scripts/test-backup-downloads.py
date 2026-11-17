#!/usr/bin/env python3
"""Backup ZIP downloads against a disposable SPARK database/server only."""
import gzip,hashlib,io,json,os,socket,subprocess,tempfile,time,urllib.request,urllib.error,zipfile
from pathlib import Path
assert os.environ.get('SPARK_DB_NAME','').startswith('spark_test_')
def port():
    with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
with tempfile.TemporaryDirectory(prefix='spark-backup-download-') as workspace,tempfile.TemporaryFile() as log:
    root=Path(workspace);credentials=root/'credentials.json'
    subprocess.run(['./build/spark_credentials','init',str(credentials)],check=True,stdout=subprocess.DEVNULL)
    def token(kind,subject,*permissions):
        return json.loads(subprocess.check_output(['./build/spark_credentials','issue',str(credentials),kind,subject,*permissions]))['token']
    admin=token('administrator','download-owner','admin.backup','files.read','files.write','discovery.read')
    other=token('administrator','other','admin.backup','files.read')
    reader=token('device','reader','files.read');device=token('device','device-backup','files.write')
    address=port();config=root/'config.json';files=root/'files'
    config.write_text(json.dumps({'server':{'host':'127.0.0.1','port':address,'notification_port':port()},'storage':{'drives':[str(files)]},'auth':{'provider':'tokens','credentials_file':str(credentials)}}))
    proc=None
    def request(path,method='GET',body=None,headers=None,status=200,credential=admin):
        h=dict(headers or {})
        if credential:h['Authorization']='Bearer '+credential
        if isinstance(body,dict):body=json.dumps(body).encode();h['Content-Type']='application/json'
        req=urllib.request.Request(f'http://127.0.0.1:{address}'+path,data=body,headers=h,method=method)
        try:
            with urllib.request.urlopen(req,timeout=30) as r:code,data,hs=r.status,r.read(),r.headers
        except urllib.error.HTTPError as e:code,data,hs=e.code,e.read(),e.headers
        assert code==status,(path,code,data[:400]);return data,hs
    def api(path,method='GET',body=None,**kw):
        data,_=request(path,method,body,**kw);return json.loads(data) if data else None
    def wait(fn):
        until=time.monotonic()+30
        while time.monotonic()<until:
            result=fn()
            if result:return result
            time.sleep(.05)
        raise AssertionError('timed out')
    def start():
        global proc
        env=dict(os.environ,SPARK_FILE_DIR=str(files))
        proc=subprocess.Popen(['./build/spark_server','--config',str(config)],env=env,stdout=log,stderr=log)
        def healthy():
            assert proc.poll() is None,'server exited'
            try:return api('/health')
            except OSError:return False
        wait(healthy)
    def stop():
        global proc
        if proc:proc.terminate();proc.wait(timeout=20);proc=None
    try:
        start();api('/backups');backup=files/'.spark-backups'/'spark-20261117T000000Z-download';backup.mkdir(mode=0o700)
        members={'manifest.txt':b'format=spark-backup-v1\n','database.sql.gz.part-000000':gzip.compress(b'CREATE TABLE fixture (id INT);\n'),'files.tar.gz.part-000000':bytes(range(256))*1024,'files.tar.gz.part-000001':b'next part\n'*1000}
        members['SHA256SUMS']=''.join(hashlib.sha256(data).hexdigest()+'  '+name+'\n' for name,data in members.items()).encode()
        for name,data in members.items():(backup/name).write_bytes(data)
        create='/backups/'+backup.name+'/downloads'
        for credential,status in [(None,401),(reader,403),(device,403)]:api(create,'POST',{},credential=credential,status=status)
        api(create,'POST',{'path':'/etc'},status=400)
        job=api(create,'POST',{},status=202);path='/backups/downloads/'+job['id'];url=job['download_url']
        def ready():
            value=api(path);assert value['state'] not in ('failed','interrupted'),value
            return value if value['state']=='ready' else None
        job=wait(ready);assert job['expires_at']-job['created_at']==604800
        assert job['prepared_bytes']==sum(map(len,members.values())) and 'entries' not in job
        for credential,status in [(None,401),(reader,403),(other,404),(device,403)]:request(url,credential=credential,status=status)
        api('/files/downloads/'+job['id'],status=404)
        request('/files/downloads/'+job['id']+'/content',status=404)
        raw,hs=request(url);etag=hs['ETag'];assert hs['Accept-Ranges']=='bytes' and len(raw)==job['archive_bytes']
        with zipfile.ZipFile(io.BytesIO(raw)) as archive:
            assert archive.testzip() is None and set(archive.namelist())=={backup.name+'/'+n for n in members}
            for n,data in members.items():assert archive.read(backup.name+'/'+n)==data
        data,head=request(url,'HEAD');assert not data and int(head['Content-Length'])==len(raw) and head['ETag']==etag
        first,_=request(url,headers={'Range':'bytes=0-4095','If-Range':etag},status=206)
        stop();start()
        rest,rh=request(url,headers={'Range':'bytes=4096-','If-Range':etag},status=206)
        assert first+rest==raw and rh['ETag']==etag
        suffix,_=request(url,headers={'Range':'bytes=-100'},status=206);assert suffix==raw[-100:]
        full,_=request(url,headers={'Range':'bytes=999999999-','If-Range':'"old"'});assert full==raw
        request(url,headers={'Range':f'bytes={len(raw)}-'},status=416)
        assert not list(files.rglob('*.zip')),'must not stage a full ZIP'
        # Simulate a PC restart: committed range plus uncommitted tail. The helper
        # discards only that tail and resumes, then verifies ZIP CRCs.
        import importlib.util
        spec=importlib.util.spec_from_file_location('backup_client',Path('scripts/download-backup.py'));helper=importlib.util.module_from_spec(spec);spec.loader.exec_module(helper)
        output=root/'received.zip';part=Path(str(output)+'.part');state=Path(str(output)+'.spark-download.json')
        full_url=f'http://127.0.0.1:{address}'+url
        part.write_bytes(raw[:5000]);state.write_text(json.dumps({'url':full_url,'etag':etag,'size':len(raw),'completed_bytes':4096}))
        helper.download(full_url,output,admin,True,chunk_size=65536)
        assert output.read_bytes()==raw and not part.exists() and not state.exists()

        # Changed, missing and symlinked sources cannot be resumed under the old ETag.
        (backup/'manifest.txt').write_bytes(b'changed')
        request(url,headers={'Range':'bytes=-100','If-Range':etag},status=409)
        api(path,'DELETE',status=204);assert backup.exists();api(path,status=404)
        (backup/'manifest.txt').unlink();(backup/'manifest.txt').symlink_to(root/'config.json')
        broken=api(create,'POST',{},status=202)
        failure=wait(lambda:(lambda j:j if j['state']=='failed' else None)(api('/backups/downloads/'+broken['id'])))
        assert failure['error']=='source_unavailable'
        # Existing ordinary folder downloads still work after the shared implementation change.
        from folder_download_checks import check_downloads
        minimal=token('device','minimal')
        resume_folder=check_downloads(address,admin,reader,minimal,wait)
        stop();start();resume_folder()
        print('Backup ZIP contents, auth, ownership, streamed ranges, restart resume, source changes, symlinks and ordinary folder regressions passed')
    except Exception:
        log.seek(0);print(log.read().decode(errors='replace')[-4000:]);raise
    finally:stop()
