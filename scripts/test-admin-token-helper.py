#!/usr/bin/env python3
"""Exercise the helper without changing the user's real credential store."""
import json, os, subprocess, tempfile
from pathlib import Path
repo=Path(__file__).resolve().parent.parent
tool=repo/'build/spark_credentials'
helper=repo/'scripts/issue-admin-token.sh'
assert os.geteuid()!=0, 'Run as a normal user; the helper deliberately rejects direct root.'
permissions=json.loads(subprocess.check_output([tool,'permissions']))
with tempfile.TemporaryDirectory(prefix='spark-admin-helper-') as folder:
    credentials=Path(folder)/'credentials with spaces.json'
    subprocess.run([tool,'init',credentials],check=True)
    old=json.loads(subprocess.check_output([tool,'issue',credentials,'administrator','existing','discovery.read']))
    # Default subject comes from the OS account, not $USER/$HOME (which sudo may change).
    import pwd
    account=pwd.getpwuid(os.getuid()).pw_name
    for args,subject in [([str(credentials)],account),([str(credentials),'custom-admin'],'custom-admin')]:
        issued=json.loads(subprocess.check_output([helper,*args],cwd='/tmp',env=dict(os.environ,HOME='/not-the-user-home',USER='wrong')))
        assert issued['token'].startswith('spark1_')
        document=json.loads(credentials.read_text())
        record=next(r for r in document['credentials'] if r['id']==issued['id'])
        assert record['subject']==subject and record['kind']=='administrator'
        assert sorted(record['permissions'])==sorted(permissions)
        assert issued['token'] not in credentials.read_text()
        assert not next(r for r in document['credentials'] if r['id']==old['id'])['revoked']
        assert credentials.stat().st_uid==os.getuid() and credentials.stat().st_mode&0o777==0o600
    failed=subprocess.run([helper,str(Path(folder)/'missing.json')],capture_output=True)
    assert failed.returncode!=0 and not failed.stdout
    assert b'Usage:' in subprocess.check_output([helper,'--help'])
print('Admin-token helper: full permissions, default/custom subject, private ownership, secret hashing and existing-token preservation passed.')
