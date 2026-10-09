#!/usr/bin/env python3
"""Configuration/startup validation; no database or listeners required."""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

binary=str(Path(sys.argv[1]).resolve())
env={k:v for k,v in os.environ.items() if not k.startswith('SPARK_DB_') and k not in ('SPARK_FILE_DIR','SPARK_FILE_DRIVES')}
env['USER']='config-test'
with tempfile.TemporaryDirectory(prefix='spark-config-') as temporary:
    root=Path(temporary)
    config_dir=root/'config'
    config_dir.mkdir()
    config=config_dir/'spark.json'
    secret='SENSITIVE_TEST_VALUE_DO_NOT_LOG'
    def run(args,code=0,overrides=None):
        process=subprocess.run([binary,*args],cwd=root,env=dict(env,**(overrides or {})),capture_output=True,text=True,timeout=5)
        assert process.returncode==code,(args,process.returncode,process.stdout,process.stderr)
        assert secret not in process.stdout+process.stderr
        return process
    def check(packet,code=0,extra=(),overrides=None):
        config.write_text(json.dumps(packet))
        result=run(['--config',str(config),'--check-config',*extra],code,overrides)
        return json.loads(result.stdout) if code==0 else result.stderr
    def raw(text):
        config.write_text(text)
        return run(['--config',str(config)],2)

    default=json.loads(run(['--check-config']).stdout)
    assert default['auth_mode']=='open' and default['runtime_supported']
    assert default['server']=={'host':'0.0.0.0','port':47683,'notification_port':47684}
    assert default['storage']['drives']==[str(root/'storage/files')]
    assert check({})==default
    assert not (root/'storage').exists()
    assert check({'storage':{'drives':['../data']}})['storage']['drives']==[str(root/'data')]
    assert check({'storage':{'drives':['first','second']}})['runtime_supported']
    for packet in [None,[],{'auth':None},{'auth':{}},{'auth':False},{'auth':'disabled'},
                   {'Auth':{}},{'server':None},{'server':{'unknown':1}},{'server':{'port':0}},
                   {'server':{'port':True}},{'server':{'port':3.5}},{'server':{'port':'47683'}},
                   {'server':{'port':65536}},{'server':{'host':''}},
                   {'server':{'host':'bad\nname'}},{'server':{'port':47684}},
                   {'database':{'port':None}},{'database':{'password':secret}},
                   {'storage':None},{'storage':{}},{'storage':{'directory':'legacy'}},
                   {'storage':{'drives':[]}},{'storage':{'drives':'dir'}},
                   {'storage':{'drives':[None]}},{'storage':{'drives':['']}},
                   {'storage':{'drives':['bad\x00path']}},
                   {'storage':{'drives':['a','./a']}},{'storage':{'drives':['a','a/b']}},
                   {'storage':{'drives':['a/b','a']}},
                   {'auth':{'provider':'other','credentials_file':'creds.json'}},
                   {'auth':{'provider':'tokens','credentials_file':'missing.json'}},
                   {'auth':{'provider':'tokens','credentials_file':None}},
                   {'auth':{'provider':'tokens','credentials_file':'creds.json','enabled':False}}]:
        check(packet,2)
        run(['--config',str(config)],2)
    for document in ['',secret,'{"auth":{},"auth":null}',
                     '{"server":{"host":"a","host":"b"}}','{"auth":',
                     '{"auth":null,"auth":{}}']:
        raw(document)
    raw(' '*1048577)
    run(['--config',str(root/'absent.json')],2)
    run(['--config',str(config_dir)],2)
    run(['--config'],2)
    run(['--unknown'],2)
    run(['--port','1','--port','2'],2)
    run(['--check-config','--check-config'],2)
    run(['--config',str(config),'--config',str(config)],2)
    assert 'Usage:' in run(['--help']).stdout

    (config_dir/'a').mkdir()
    (config_dir/'alias').symlink_to(config_dir/'a',target_is_directory=True)
    check({'storage':{'drives':['a','alias']}},2)
    (config_dir/'not-directory').write_text('data')
    check({'storage':{'drives':['not-directory']}},2)
    creds=config_dir/'credentials.json'
    packet={'auth':{'provider':'tokens','credentials_file':'credentials.json'}}
    for contents in ['',secret,'[]','{}','{"key":1,"key":2}']:
        creds.write_text(contents)
        check(packet,2)
    creds.write_text(json.dumps({'version':1,'credentials':[]}))
    creds.chmod(0o600)
    for origin in ['http://spark.example','https://spark.example/','https://user@spark.example','https://spark.example:443','https://spark.example:65536',False,None]:
        check({'auth':{**packet['auth'],'browser_origin':origin}},2)
    check({'auth':{**packet['auth'],'browser_origin':'https://spark.example:8443'}})
    remote={'server':{'host':'127.0.0.1','tls_notification_port':47685},
            'auth':{**packet['auth'],'browser_origin':'https://spark.example'}}
    result=check(remote,extra=['--remote'])
    assert result['remote'] and result['public_notifications']['transport']=='tcp+tls'
    assert result['public_notifications']['host']=='spark.example'
    check(remote,2,extra=['--remote','--host','0.0.0.0'])
    check({},2,extra=['--remote'])
    check(packet,2,extra=['--remote'])
    for host in ['0.0.0.0','::','localhost','192.168.4.20']:
        check({**remote,'server':{**remote['server'],'host':host}},2)
    for port in [443,47683,47684,0,True]:
        check({**remote,'server':{**remote['server'],'tls_notification_port':port}},2)
    check({'server':remote['server']},2)
    check({**remote,'auth':packet['auth']},2)
    summary=check(packet)
    assert summary['auth_mode']=='required' and summary['runtime_supported']
    assert summary['startup_blockers']==[]
    creds.chmod(0)
    if os.geteuid()!=0: check(packet,2)
    creds.chmod(0o600)
    fifo=config_dir/'pipe'
    os.mkfifo(fifo)
    check({'auth':{'provider':'tokens','credentials_file':'pipe'}},2)
    check({'auth':{'provider':'tokens','credentials_file':'.'}},2)
    config.chmod(0)
    if os.geteuid()!=0: run(['--config',str(config)],2)
    config.chmod(0o600)

    override={'SPARK_FILE_DIR':'from-env','SPARK_DB_PASSWORD':secret}
    assert check({},overrides=override)['storage']['drives']==[str(root/'from-env')]
    assert check({'storage':{'drives':['from-config']}},overrides=override)['storage']['drives']==[str(config_dir/'from-config')]
    assert check({'server':{'host':'127.0.0.2','port':1234,'notification_port':1235}},extra=['--host','127.0.0.1','--port','2234','--notification-port','2235'])['server']=={'host':'127.0.0.1','port':2234,'notification_port':2235}
    check({'database':{'port':3306}},overrides={'SPARK_DB_PORT':'invalid'})
    check({},2,overrides={'SPARK_DB_PORT':'invalid'})
    for port in ['0','65536','1.0','abc','-1']:
        run(['--check-config','--port',port],2)
    assert not (root/'data').exists() and not (root/'storage').exists()
print('Configuration defaults, precedence, paths, strict JSON, secret redaction and fail-closed startup passed.')
