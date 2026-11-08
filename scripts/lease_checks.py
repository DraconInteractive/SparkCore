"""Public download boundary and lease lifecycle checks on a private instance."""
import json
from datetime import datetime, timedelta, timezone


def check_leases(call, issue, admin, sql, connection):
    def expiry(days=1):
        return (datetime.now(timezone.utc)+timedelta(days=days)).strftime('%Y-%m-%dT%H:%M:%SZ')
    reader=issue('device','lease-reader','files.read')['token']
    share_only=issue('device','lease-share-only','files.share')['token']
    sharer=issue('device','lease-sharer','files.read','files.share')['token']
    secret=b'private archive contents\n'
    file=call('POST','/files?name=leased.txt',admin,status=201,headers={'Content-Type':'application/octet-stream'},raw=secret)
    path='/files/'+file['id']; collection=path+'/leases'
    call('GET',file['download_url'],status=401)
    call('POST',collection,reader,{'expires_at':expiry()},403)
    call('GET',collection,reader,status=403)
    call('POST',collection,share_only,{'expires_at':expiry()},403)
    for value in [expiry(-1),'2030-02-30T12:00:00Z','tomorrow','2030-01-01T12:00:00+08:00',None]:
        call('POST',collection,sharer,{'expires_at':value},400)
    lease=call('POST',collection,sharer,{'expires_at':expiry()},201)
    public=lease['download_url']
    c=connection()
    try:
        c.request('GET',public);response=c.getresponse()
        assert response.status==200 and response.read()==secret
        assert 'no-store' in response.getheader('Cache-Control')
        assert response.getheader('Referrer-Policy') and all(value.strip()=='no-referrer' for value in response.getheader('Referrer-Policy').split(','))
        assert response.getheader('X-Content-Type-Options') and all(value.strip()=='nosniff' for value in response.getheader('X-Content-Type-Options').split(','))
        assert response.getheader('Content-Disposition').startswith('attachment;')
    finally:
        c.close()
    call('PATCH',path,reader,{'name':'forbidden.txt'},403)
    for value in ['', '.', '..', 'a/b', 'a\\b', 'bad\nname', 'x'*256, None, 123]:
        call('PATCH',path,admin,{'name':value},400)
    call('PATCH',path,admin,{},400)
    call('PATCH',path,admin,{'name':'ok.txt','unknown':True},400)
    renamed=call('PATCH',path,admin,{'name':'renamed résumé.txt'})
    assert renamed['name']=='renamed résumé.txt' and renamed['id']==file['id']
    assert renamed['directory']==file['directory'] and renamed['content_revision']==file['content_revision'] and renamed['download_url']==file['download_url']
    count=sql("SELECT COUNT(*) FROM events WHERE type='file.updated' AND resource='"+path+"'")
    assert count=='1'
    assert call('PATCH',path,admin,{'name':'renamed résumé.txt'})==renamed
    assert sql("SELECT COUNT(*) FROM events WHERE type='file.updated' AND resource='"+path+"'")==count
    c=connection()
    try:
        c.request('GET',public);r=c.getresponse();assert r.read()==secret
        assert 'renamed%20r%C3%A9sum%C3%A9.txt' in r.getheader('Content-Disposition')
    finally:c.close()
    sql("DELIMITER //\nCREATE TRIGGER rename_event_failure BEFORE INSERT ON events FOR EACH ROW BEGIN IF NEW.type='file.updated' THEN SIGNAL SQLSTATE '45000' SET MESSAGE_TEXT='fixture failure'; END IF; END//\nDELIMITER ;\n")
    try:
        call('PATCH',path,admin,{'name':'rolled-back.txt','directory':'/rollback-rename'},503)
        assert call('GET',path,admin)==renamed
        assert '/rollback-rename' not in call('GET','/files/directories',admin)['directories']
    finally:sql('DROP TRIGGER rename_event_failure')
    moved=call('PATCH',path,admin,{'name':'moved.txt','directory':'/combined-rename'})
    assert moved['name']=='moved.txt' and moved['directory']=='/combined-rename'
    assert call('GET',public)==secret
    assert call('GET',public,'invalid-token')==secret
    assert call('HEAD',public)==b''
    assert call('GET',public,headers={'Range':'bytes=0-6'},status=206)==secret[:7]
    call('GET',public,headers={'Range':'bytes=9999-'},status=416)
    call('GET','/downloads/'+'0'*64,status=404)
    call('POST',public,body={},status=401)
    call('GET',public+'?file_id='+file['id'],status=400)
    listing=call('GET',collection,sharer)
    assert listing['leases'][0]['id']==lease['id']
    assert 'download_url' not in listing['leases'][0] and public.split('/')[-1] not in json.dumps(listing)
    stored=sql("SELECT token_hash FROM file_leases WHERE id='"+lease['id']+"'")
    assert len(stored)==64 and stored!=public.split('/')[-1]
    call('DELETE',collection+'/'+lease['id'],reader,status=403)
    call('DELETE',collection+'/'+lease['id'],sharer,status=204)
    call('GET',public,status=404)
    lease=call('POST',collection,sharer,{'expires_at':expiry()},201)
    sql("UPDATE file_leases SET expires_at=UTC_TIMESTAMP()-INTERVAL 1 SECOND WHERE id='"+lease['id']+"'")
    call('GET',lease['download_url'],status=404)
    call('HEAD',lease['download_url'],status=404)
    assert not call('GET',collection,sharer)['leases'][0]['active']
    # Content replacement invalidates the lease; directory moves do not.
    lease=call('POST',collection,sharer,{'expires_at':expiry()},201)
    call('PATCH',path,admin,{'directory':'/lease-test'})
    assert call('GET',lease['download_url'])==secret
    call('PUT',file['download_url'],admin,raw=b'new private content',headers={'Content-Type':'text/plain; charset=utf-8','If-Match':'"'+file['id']+'"'})
    call('GET',lease['download_url'],status=404)
    replacement=call('POST',collection,sharer,{'expires_at':expiry()},201)
    assert call('GET',replacement['download_url'])==b'new private content'
    call('DELETE',path,admin,status=204)
    call('GET',replacement['download_url'],status=404)
    assert sql("SELECT COUNT(*) FROM file_leases WHERE file_id='"+file['id']+"'")=='0'
    print('File leases: public GET/HEAD/ranges, authorization, secret hashing, expiry, revocation, replacement and deletion passed.')
