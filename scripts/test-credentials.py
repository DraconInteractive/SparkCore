#!/usr/bin/env python3
"""Offline credential CLI tests; no database or listeners."""
import concurrent.futures
import json
import pathlib
import subprocess
import sys
import tempfile
binary = str(pathlib.Path(sys.argv[1]).resolve())
with tempfile.TemporaryDirectory(prefix='spark-credentials-') as directory:
    path = pathlib.Path(directory) / 'credentials.json'
    def run(*args, code=0):
        result = subprocess.run([binary, *map(str, args)], capture_output=True, text=True, timeout=10)
        assert result.returncode == code, result.stderr
        return result.stdout
    run('init', path)
    run('init', path, code=2)
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
        issued = list(pool.map(lambda i: json.loads(run('issue', path, 'device', f'device-{i}', 'files.read')), range(12)))
    records = json.loads(run('list', path))
    assert len(records) == 12
    assert len({entry['id'] for entry in records}) == 12
    assert path.stat().st_mode & 0o777 == 0o600
    for entry in issued:
        assert entry['token'] not in path.read_text()
        assert entry['token'] not in json.dumps(records)
    run('revoke', path, issued[0]['id'])
    assert next(r for r in json.loads(run('list', path)) if r['id'] == issued[0]['id'])['revoked']
    before = path.read_bytes()
    run('issue', path, 'device', 'bad', 'admin.restart', code=2)
    run('issue', path, 'administrator', 'bad', 'unknown', code=2)
    run('issue', path, 'device', 'bad', 'files.read', 'files.read', code=2)
    assert path.read_bytes() == before
print('Credential CLI concurrent issuance, revocation, redaction, permissions and failed-write preservation passed.')
