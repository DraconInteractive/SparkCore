"""Config API checks, run against the disposable authenticated server."""
import json
from concurrent.futures import ThreadPoolExecutor


def check_configs(call, issue, admin, sql, Peer, boundary):
    reader = issue('device', 'config-reader', 'configs.read', 'notifications.read')['token']
    writer = issue('device', 'config-writer', 'configs.read', 'configs.write')['token']
    cursor = boundary()
    base = {'name': 'display', 'tags': ['watch', 'production'], 'data': {'brightness': 70, 'nested': {'enabled': True}}}
    call('POST', '/configs', reader, base, 403)
    record = call('POST', '/configs', writer, base, 201)
    path = '/configs/' + record['id']
    assert record['revision'] == 1 and record['tags'] == ['production', 'watch']
    assert call('GET', path, reader)['data'] == base['data']
    assert call('GET', path + '/content', reader) == base['data']
    call('GET', path, reader, status=304, headers={'If-None-Match': '"config-' + record['id'] + '-1-record"'})
    duplicate = call('POST', '/configs', writer, base, 201)
    listing = call('GET', '/configs?name=display&tag=watch&tag=production&limit=1', reader)
    assert listing['has_more'] and len(listing['configs']) == 1 and 'data' not in listing['configs'][0]
    assert len(call('GET', '/configs?name=display&tag=watch&offset=1', reader)['configs']) == 1
    assert not call('GET', '/configs?name=Display', reader)['configs']
    assert not call('GET', '/configs?tag=watch&tag=missing', reader)['configs']
    for data in [[], None, 'text', 4]:
        call('POST', '/configs', writer, dict(base, data=data), 400)
    for packet in [dict(base, tags=['watch', 'watch']), dict(base, name=' '), dict(base, unknown=1)]:
        call('POST', '/configs', writer, packet, 400)
    call('POST', '/configs', writer, raw=b'{"name":"x","data":{"x":1,"x":2}}', status=400)
    call('POST', '/configs', writer, dict(base, data={'large': 'x' * 65536}), 413)
    call('POST', '/configs', writer, raw=b'{"name":"x","data":'+b'{"x":'*66+b'{}'+b'}'*66+b'}', status=400)
    call('POST', '/configs', writer, dict(base, source_device='someone-else'), 403)
    call('PATCH', path, writer, {'data': {}}, 400)
    before = sql("SELECT COUNT(*) FROM events WHERE type LIKE 'config.%'")
    assert call('PATCH', path, writer, dict(base, revision=1))['revision'] == 1
    assert sql("SELECT COUNT(*) FROM events WHERE type LIKE 'config.%'") == before
    # Two clients editing the same revision cannot silently overwrite each other.
    def update(value):
        try:
            return call('PATCH', path, writer, {'revision': 1, 'data': {'brightness': value}})['revision']
        except AssertionError as error:
            assert error.args[0][2] == 409, error
            return 'conflict'
    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(update, [80, 90]))
    assert sorted(map(str, results)) == ['2', 'conflict']
    assert call('PATCH', path, writer, {'revision': 1, 'data': {}}, 409)['current_revision'] == 2
    call('DELETE', path + '?revision=1', writer, status=409)
    # Event and config mutation must roll back together.
    sql("DELIMITER //\nCREATE TRIGGER config_event_failure BEFORE INSERT ON events FOR EACH ROW BEGIN IF NEW.type='config.updated' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='fixture failure'; END IF; END//\nDELIMITER ;\n")
    try:
        call('PATCH', path, writer, {'revision': 2, 'data': {'failed': True}}, 503)
        assert call('GET', path, reader)['revision'] == 2
    finally:
        sql('DROP TRIGGER config_event_failure')
    call('DELETE', path + '?revision=2', reader, status=403)
    call('DELETE', path + '?revision=2', writer, status=204)
    call('GET', path, reader, status=404)
    call('DELETE', path + '?revision=2', writer, status=404)
    call('DELETE', '/configs/' + duplicate['id'] + '?revision=1', writer, status=204)
    rows = sql("SELECT payload FROM events WHERE type LIKE 'config.%'").splitlines()
    assert rows
    for row in rows:
        assert set(json.loads(row)) == {'config_id', 'revision'}
    end = boundary()
    peer = Peer('config-reader', reader, cursor)
    try:
        assert peer.welcome['resources'] == ['/configs'], peer.welcome
        messages = peer.through(end)
        changes = [m for m in messages if m.get('type', '').startswith('config.')]
        assert len(changes) == 5, changes
        for message in changes:
            assert 'config_id' in message and 'revision' in message
            assert not any(key in message for key in ('data', 'name', 'tags', 'description'))
            assert message['resource'].startswith('/configs/')
    finally:
        peer.close()
    print('Config CRUD, filtering, permissions, conflicts and transactional events passed')
