#!/usr/bin/env python3
"""Startup connection diagnostics must distinguish failures without logging secrets."""
import json, os, subprocess, sys, tempfile
from pathlib import Path
binary=str(Path(sys.argv[1]).resolve())
with tempfile.TemporaryDirectory(prefix='spark-log-test-') as folder:
    root=Path(folder);secret='SECRET_VALUE_DO_NOT_LOG'
    config=root/'config.json'
    config.write_text(json.dumps({'server':{'host':'127.0.0.1'},'database':{'host':'localhost','user':secret,'name':'spark_test_missing','socket':str(root/(secret+'.sock'))},'storage':{'drives':[str(root/'files')]}}))
    env=dict(os.environ,SPARK_DB_PASSWORD=secret)
    p=subprocess.run([binary,'--config',str(config)],env=env,text=True,capture_output=True,timeout=15)
    assert p.returncode==1,(p.returncode,p.stderr)
    assert not p.stdout and secret not in p.stderr
    records=[json.loads(line) for line in p.stderr.splitlines()]
    failure=next(r for r in records if r['message']=='Startup check failed')
    assert failure['details']['check']=='devices'
    assert failure['details']['operation']=='connect' and failure['details']['database_code']==2002,failure
    assert failure['details']['database_reason']=='local_socket_connection_failed'
    assert failure['timestamp'].endswith('Z') and failure['level']=='error'
    p=subprocess.run([binary,'--config',str(config),'--check-config'],env=env,text=True,capture_output=True,timeout=15)
    assert p.returncode==0 and not p.stderr
    assert json.loads(p.stdout)['configuration_valid']
print('Startup logs: connection code, SQLSTATE, failing component, redaction and clean check-config output passed.')
