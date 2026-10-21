#!/usr/bin/env python3
"""Verify setup-db grants as the application OS user in the PRIVATE test server."""
import json, os, pwd, socket, subprocess, tempfile, time, urllib.request, uuid
from pathlib import Path

assert os.environ.get('SPARK_DB_NAME', '').startswith('spark_test_')
db_socket=Path(os.environ['SPARK_DB_SOCKET'])
assert db_socket.parent.name.startswith('spark-db-test.') and str(db_socket).startswith('/tmp/')
user=pwd.getpwuid(os.getuid()).pw_name
assert user!='root'
database='spark_test_provision'
admin=['mariadb','--no-defaults','--socket='+str(db_socket),'--user=root']
def sql(statement,who=None):
    command=admin if who is None else ['mariadb','--no-defaults','--socket='+str(db_socket),'--user='+who]
    return subprocess.check_output(command+['--batch','--skip-column-names'],input=statement,text=True)

process=None
with tempfile.TemporaryDirectory(prefix='spark-provision-') as directory:
    root=Path(directory)
    try:
        # Capture the exact production provisioning SQL without invoking real sudo.
        shim=root/'sudo'
        shim.write_text('#!/bin/sh\n[ "$#" -eq 1 ] && [ "$1" = mariadb ] || exit 1\ncat > "$SPARK_PROVISION_SQL"\n'.replace('\\n','\n'))
        shim.chmod(0o700)
        captured=root/'setup.sql'
        env=dict(os.environ,PATH=str(root)+':'+os.environ['PATH'],SPARK_PROVISION_SQL=str(captured))
        subprocess.run(['sh','scripts/setup-db.sh'],env=env,check=True,stdout=subprocess.DEVNULL)
        provision=captured.read_text().replace('CREATE DATABASE IF NOT EXISTS spark;', 'CREATE DATABASE IF NOT EXISTS '+database+';').replace('USE spark;', 'USE '+database+';').replace(' ON spark.',' ON '+database+'.')
        # mariadb-install-db can create a privileged account for its launching OS user.
        # Replace it only on this disposable server, to exercise least-privilege grants.
        sql("DROP USER IF EXISTS '"+user+"'@'localhost';")
        sql(provision)
        assert sql('SELECT CURRENT_USER();',user).strip()==user+'@localhost'
        for table in ('calendar_sync_identity','calendar_sync_items','calendar_sync_receipts','calendar_sync_connections','calendar_sync_baselines','calendar_sync_conflicts'):
            sql('SELECT * FROM '+database+'.'+table+' LIMIT 0;',user)
        ports=[]
        for _ in range(2):
            sock=socket.socket();sock.bind(('127.0.0.1',0));ports.append(sock)
        http_port,tcp_port=[s.getsockname()[1] for s in ports]
        for s in ports:s.close()
        config=root/'config.json'
        config.write_text(json.dumps({'server':{'host':'127.0.0.1','port':http_port,'notification_port':tcp_port},'storage':{'drives':[str(root/'files')]},'database':{'name':database,'user':user,'socket':str(db_socket)}}))
        def call(path,body=None):
            req=urllib.request.Request('http://127.0.0.1:'+str(http_port)+path,data=None if body is None else json.dumps(body).encode(),headers={'Content-Type':'application/json'})
            with urllib.request.urlopen(req,timeout=5) as response:return json.load(response)
        with (root/'server.log').open('w+') as log:
            process=subprocess.Popen(['./build/spark_server','--config',str(config)],env=dict(os.environ,SPARK_DB_USER=user,SPARK_DB_NAME=database),stdout=log,stderr=log)
            deadline=time.monotonic()+15
            while True:
                try:call('/health');break
                except OSError:
                    if process.poll() is not None or time.monotonic()>deadline:
                        log.seek(0);raise AssertionError(log.read())
                    time.sleep(.1)
            event=call('/events',{'calendar':'shared','name':'Provisioning','description':'Restricted user','occurs_at':'2035-01-01T00:00:00Z'})
            info=call('/calendar-sync')
            item=call('/calendar-sync/changes')['items'][0]
            request={'instance_id':info['instance_id'],'coordinator_id':str(uuid.uuid4()),'operation_id':str(uuid.uuid4()),'sync_id':item['sync_id'],'expected_revision':item['revision'],'data':{**item['data'],'description':'Imported'}}
            result=call('/calendar-sync/apply',request)
            assert result['applied'] and call('/calendar-sync/apply',request)==result
            assert call('/events/'+event['id'])['description']=='Imported'
        # Coordinator bookkeeping also needs INSERT and UPDATE, not just startup SELECT.
        for table in ('calendar_sync_connections','calendar_sync_baselines','calendar_sync_conflicts'):
            if table.endswith('connections'):
                columns="connection_id,enabled,state";values="'test',1,'{}'";change="state='{}'"
            elif table.endswith('baselines'):
                columns="connection_id,sync_id,data";values="'test','test','{}'";change="data='{}'"
            else:
                columns="connection_id,operation_id,data";values="'test','test','{}'";change="data='{}'"
            sql('START TRANSACTION; INSERT INTO '+database+'.'+table+'('+columns+') VALUES ('+values+'); UPDATE '+database+'.'+table+' SET '+change+" WHERE connection_id='test'; ROLLBACK;",user)
        print('Production setup grants: restricted OS user startup, shared mutations, sync imports, receipts and coordinator bookkeeping passed.')
    finally:
        if process:
            process.terminate();process.wait(timeout=15)
        sql('DROP DATABASE IF EXISTS '+database+"; DROP USER IF EXISTS '"+user+"'@'localhost';")
