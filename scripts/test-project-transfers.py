#!/usr/bin/env python3
"""Portable project ZIPs, reviewed merges, isolation, integrity and atomic failure."""
import io,json,os,socket,subprocess,tempfile,time,urllib.request,urllib.error,urllib.parse,zipfile
from pathlib import Path
assert os.environ.get('SPARK_DB_NAME','').startswith('spark_test_')
mysql=['mariadb','--no-defaults','--socket='+os.environ['SPARK_DB_SOCKET'],'--user=root']
processes=[];databases=[]
def sql(query,db=None):
    return subprocess.check_output(mysql+['--batch','--skip-column-names']+([db] if db else []),input=query,text=True)
def port():
    with socket.socket() as s:s.bind(('127.0.0.1',0));return s.getsockname()[1]
def call(instance,path,method='GET',body=None,status=200,raw=None,token=None):
    headers={'Authorization':'Bearer '+(token or instance['token'])}
    if body is not None:raw=json.dumps(body).encode();headers['Content-Type']='application/json'
    elif raw is not None:
        headers['Content-Type']='application/octet-stream'
        if method=='PUT':
            record=call(instance,path.removesuffix('/content'))
            headers['Content-Type']='text/plain; charset=utf-8'
            headers['If-Match']='"'+record['id']+('-'+record['content_revision'] if record['content_revision'] else '')+'"'
    try:
        with urllib.request.urlopen(urllib.request.Request(instance['url']+path,data=raw,headers=headers,method=method),timeout=60) as r:code,data=r.status,r.read();kind=r.headers.get('Content-Type','')
    except urllib.error.HTTPError as e:code,data,kind=e.code,e.read(),e.headers.get('Content-Type','')
    assert code==status,(path,code,data[:1000])
    return json.loads(data) if data and 'json' in kind else data

def poll(i,job,expected):
    for _ in range(300):
        job=call(i,'/projects/transfers/'+job['id'])
        if job['state'] not in ('preparing','applying'):
            assert job['state']==expected,job
            return job
        time.sleep(.05)
    raise AssertionError('Transfer timeout')
def upload(i,name,data,directory='/'):
    return call(i,'/files?'+urllib.parse.urlencode(dict(name=name,directory=directory,drive='A')),'POST',raw=data,status=201)
def review(i,data,expected='review'):
    f=upload(i,'transfer.zip',data,'/incoming')
    job=call(i,'/projects/imports','POST',{'file_id':f['id'],'drive':'A'},202)
    return poll(i,job,expected)
def apply(i,j,resolutions=None,status=200):return call(i,'/projects/transfers/'+j['id']+'/apply','POST',{'resolutions':resolutions or {}},status)
def export(i,id):
    j=poll(i,call(i,'/projects/'+id+'/export','POST',{},202),'complete')
    return call(i,j['result']['file']['download_url'])
try:
 with tempfile.TemporaryDirectory(prefix='spark-project-test-') as directory,tempfile.TemporaryFile() as log:
    instances=[]
    for label in ('source','target'):
        root=Path(directory)/label;root.mkdir();db='spark_test_transfer_'+label+'_'+str(os.getpid());databases.append(db);sql('CREATE DATABASE '+db);sql('\n'.join(p.read_text() for p in sorted(Path('sql').glob('*.sql'))),db)
        credentials=root/'credentials.json';subprocess.check_call(['./build/spark_credentials','init',str(credentials)],stdout=subprocess.DEVNULL)
        permissions=json.loads(subprocess.check_output(['./build/spark_credentials','permissions']))
        def issue(subject,permissions):return json.loads(subprocess.check_output(['./build/spark_credentials','issue',str(credentials),'administrator',subject,*permissions]))['token']
        token=issue('owner',permissions);other=issue('other',permissions);reader=issue('reader',['projects.read','boards.read','files.read'])
        p=port();config=root/'config.json';config.write_text(json.dumps({'server':{'host':'127.0.0.1','port':p,'notification_port':port()},'storage':{'drives':[str(root/'files')]},'database':{'name':db},'auth':{'provider':'tokens','credentials_file':str(credentials)}}))
        proc=subprocess.Popen(['./build/spark_server','--config',str(config)],env=os.environ,stdout=log,stderr=log);processes.append(proc);instance={'url':'http://127.0.0.1:'+str(p),'token':token,'other':other,'reader':reader,'db':db};instances.append(instance)
        for _ in range(100):
            try:call(instance,'/health');break
            except OSError:time.sleep(.1)
        else:raise AssertionError('Server did not start')
    a,b=instances
    root=call(a,'/projects','POST',{'name':'Portable','description':'Original','github_url':'https://github.com/example/project'},201)
    child=call(a,'/projects','POST',{'name':'Child','description':'Child details','parent_id':root['id']},201)
    board=call(a,'/projects/'+child['id']+'/boards','POST',{'name':'Development / planning'},201);base='/projects/'+child['id']+'/boards/'+board['id']
    column=call(a,base+'/columns','POST',{'name':'Todo'},201)
    task=call(a,base+'/columns/'+column['id']+'/tasks','POST',{'name':'Ship / verify 🌳','description':'Retain severity','severity':'Urgent'},201)
    text=upload(a,'readme.txt',b'Original\n',root['directory']);binary=upload(a,'asset.bin',b'\0\x01\xff',child['directory'])
    call(a,'/files/directories','POST',{'directory':child['directory']+'/empty','drive':'A'},200)
    call(a,'/projects/'+root['id']+'/export','POST',{},403,token=a['reader'])
    data=export(a,root['id']);z=zipfile.ZipFile(io.BytesIO(data));manifest=json.loads(z.read('manifest.json'));assert len(manifest['projects'])==2 and manifest['tasks'][0]['severity']=='Urgent';assert 'Child/empty' in manifest['directories']
    j=review(b,data);call(b,'/projects/transfers/'+j['id'],status=404,token=b['other']);assert call(b,'/projects')['projects']==[]
    apply(b,j);assert call(b,'/projects/'+root['id'])['description']=='Original';assert call(b,base+'/tasks/'+task['id'])['severity']=='Urgent';assert call(b,text['download_url'])==b'Original\n';assert call(b,binary['download_url'])==b'\0\x01\xff'
    # Reimport is an explicit no-op by default, with no duplicated identities.
    j=review(b,data);assert all(e['exists'] for e in j['entries']);assert apply(b,j)['result']['applied_items']==0
    call(b,'/projects/'+root['id'],'PATCH',{'description':'Local description'})
    call(b,'/files/'+text['id']+'/content','PUT',raw=b'Local\n')
    # Change the source and export a conflict, including a binary replacement.
    call(a,'/files/'+text['id']+'/content','PUT',raw=b'Incoming\n')
    # PUT is a text editor route, so binary content changes use an archive fixture below.
    data=export(a,root['id']);j=review(b,data);entry=next(e for e in j['entries'] if e['source_id']==text['id']);assert entry['mergeable']
    preview=call(b,'/projects/transfers/'+j['id']+'?text='+urllib.parse.quote(entry['key']));assert preview=={'local':'Local\n','incoming':'Incoming\n'}
    merged='Local\nIncoming\n'+'x'*20000
    apply(b,j,{entry['key']:{'action':'merge','content':merged},'tasks:'+task['id']:{'action':'import'}})
    assert call(b,text['download_url'])==merged.encode();assert call(b,'/projects/'+root['id'])['description']=='Local description'
    # Review must fail if its target changes, before any write is made.
    j=review(b,data);call(b,base+'/tasks/'+task['id'],'PATCH',{'severity':'Low'})
    assert apply(b,j,status=409)['error']=='import_target_changed'
    # Notification failure rolls back metadata and file replacement together.
    j=review(b,data);sql("CREATE TRIGGER reject_import BEFORE INSERT ON events FOR EACH ROW SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='test rollback'",b['db'])
    apply(b,j,{'projects:'+root['id']:{'action':'import'},'files:'+text['id']:{'action':'import'}},503);sql('DROP TRIGGER reject_import',b['db'])
    assert call(b,text['download_url'])==merged.encode();assert call(b,'/projects/'+root['id'])['description']=='Local description'
    # A binary file may never be merged, even if the caller bypasses the UI.
    j=review(b,data);apply(b,j,{'files:'+binary['id']:{'action':'merge','content':'unsafe'}},400)
    # Archives are parsed without extracting attacker-chosen paths.
    for name in ('../escape','/tmp/escape','data/../../escape'):
        buf=io.BytesIO()
        with zipfile.ZipFile(buf,'w') as out:out.writestr(name,b'bad')
        assert review(b,buf.getvalue(),'failed')['error']=='unsafe_zip_entry'
    buf=io.BytesIO()
    with zipfile.ZipFile(io.BytesIO(data)) as original,zipfile.ZipFile(buf,'w') as out:
        for name in original.namelist():out.writestr(name,b'tampered' if name=='data/'+text['id'] else original.read(name))
    assert review(b,buf.getvalue(),'failed')['error']=='file_integrity_mismatch'
    # Name matching preserves an existing destination identity.
    source_alias=call(a,'/projects','POST',{'name':'By name','description':'Incoming name match'},201)
    target_alias=call(b,'/projects','POST',{'name':'By name','description':'Existing'},201)
    j=review(b,export(a,source_alias['id']));assert j['result']['project_id']==target_alias['id']
    apply(b,j,{'projects:'+source_alias['id']:{'action':'import'}})
    assert call(b,'/projects/'+target_alias['id'])['description']=='Incoming name match'
    skipped=call(a,'/projects','POST',{'name':'Discard branch','description':''},201)
    upload(a,' ',b'Whitespace filename',skipped['directory'])
    j=review(b,export(a,skipped['id']))
    assert apply(b,j,{'projects:'+skipped['id']:{'action':'discard'}})['result']['project_id'] is None
    call(b,'/projects/'+skipped['id'],status=404)
    buf=io.BytesIO()
    with zipfile.ZipFile(buf,'w') as out:
        info=zipfile.ZipInfo('manifest.json');info.create_system=3;info.external_attr=(0o120777 << 16);out.writestr(info,'/etc/passwd')
    assert review(b,buf.getvalue(),'failed')['error']=='unsafe_zip_entry'
    print('Project export/import: hierarchy, boards, severity, empty directories, files, identity, permissions, text merges, stale review, rollback and malicious ZIP checks passed.')
finally:
 for p in processes:
    p.terminate()
 for p in processes:
    try:p.wait(timeout=20)
    except subprocess.TimeoutExpired:p.kill();p.wait()
 for db in databases:sql('DROP DATABASE IF EXISTS '+db)
