#!/usr/bin/env python3
"""Exercise real service teardown and recovery against test-local-db's private DB."""
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time
import urllib.request
import urllib.error

assert os.environ.get('SPARK_DB_NAME', '').startswith('spark_test_')

def port():
    with socket.socket() as sock:
        sock.bind(('127.0.0.1', 0))
        return sock.getsockname()[1]

http, tcp = port(), port()
base = f'http://127.0.0.1:{http}'
def request(path, body=None, headers=None):
    req = urllib.request.Request(base+path, data=None if body is None else json.dumps(body).encode(), headers=headers or {})
    try:
        with urllib.request.urlopen(req, timeout=2) as response:
            return response.status, response.read().decode()
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()

def wait(predicate):
    deadline = time.monotonic()+60
    while time.monotonic()<deadline:
        try:
            value = predicate()
            if value:
                return value
        except (OSError, ValueError):
            pass
        time.sleep(.15)
    raise AssertionError('Timed out waiting for dashboard')

with tempfile.TemporaryDirectory(prefix='spark-dashboard-', dir='/dev/shm') as backup:
    # File values must override conflicting inherited DB/storage settings and
    # reach both domain stores and the dashboard backup subprocess.
    config_path=Path(backup)/'instance.json'
    config_path.write_text(json.dumps({
        'server':{'host':'127.0.0.1','port':47683,'notification_port':tcp},
        'database':{'name':os.environ['SPARK_DB_NAME'],'host':'localhost','user':'root','port':3306,
                    'socket':os.path.relpath(os.environ['SPARK_DB_SOCKET'],backup)},
        'storage':{'drives':[os.path.relpath(os.environ['SPARK_FILE_DIR'],backup)]}
    }))
    env = dict(os.environ, SPARK_BACKUP_HISTORY=backup+'/history.tsv',
               SPARK_DB_NAME='spark_test_nonexistent_config_probe',SPARK_DB_SOCKET='/missing/mysql.sock',
               SPARK_DB_PORT='invalid',SPARK_FILE_DIR=backup+'/wrong-files')
    with tempfile.TemporaryFile() as log:
        process = subprocess.Popen(['./build/spark_server', '--config', str(config_path), '--port', str(http)], env=env, stdout=log, stderr=log)
        try:
            wait(lambda: request('/health')[0]==200)
            code, html = request('/')
            assert code==200 and '/files/explorer' in html and '/projects/explorer' in html
            assert request('/admin/restart')[0]==405
            assert request('/', {})[0]==405
            assert request('/admin/restart', {}, {'Content-Type':'application/json'})[0]==403
            headers = {'Content-Type':'application/json','X-SPARK-Admin':'dashboard'}
            assert request('/admin/restart', {}, dict(headers, **{'Sec-Fetch-Site':'cross-site'}))[0]==403
            assert request('/admin/restart', {}, {'X-SPARK-Admin':'dashboard','Content-Type':'text/plain'})[0]==415
            assert request('/admin/backup', {'mount':'relative'}, headers)[0]==400
            # Configuration is a process-lifetime snapshot, including restart and backup.
            config_path.write_text('{"storage":{"drives":["changed-after-startup"]}}')
            assert request('/admin/restart', {}, headers)[0]==202
            assert request('/admin/restart', {}, headers)[0]==409
            wait(lambda: json.loads(request('/admin/status')[1]).get('state')=='succeeded')
            with socket.create_connection(('127.0.0.1',tcp),timeout=2):
                pass
            assert request('/admin/backup', {'mount':'/dev/shm','destination':backup+'/archives'}, headers)[0]==202
            wait(lambda: json.loads(request('/admin/status')[1]).get('state') in ('succeeded','failed'))
            state=json.loads(request('/admin/status')[1])
            assert state['state']=='succeeded', state
            archives=list(Path(backup+'/archives').glob('spark-*'))
            assert len(archives)==1
            subprocess.run(['./scripts/verify-backup.sh',str(archives[0])],check=True,stdout=subprocess.DEVNULL)
            assert json.loads(request('/admin/backup-locations')[1])['locations'][0]['mount']=='/dev/shm'
            assert request('/admin/backup', {'mount':'/no-such-spark-drive'}, headers)[0]==202
            wait(lambda: json.loads(request('/admin/status')[1]).get('state')=='failed')
            assert request('/health')[0]==200
            with socket.create_connection(('127.0.0.1',tcp),timeout=2):
                pass
            print('Config-file precedence, restart snapshot, backup propagation, history, request guards and failure recovery passed.')
        except BaseException:
            log.seek(0)
            print(log.read().decode())
            raise
        finally:
            process.terminate()
            process.wait(timeout=10)
