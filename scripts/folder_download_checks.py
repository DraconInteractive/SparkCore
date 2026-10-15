"""Checks used by the isolated inter-SPARK integration harness."""
import io,json,urllib.request,urllib.error,zipfile

def check_downloads(port,token,other,minimal,wait):
    def request(path,method='GET',body=None,headers=None,status=200,credential=token):
        h={'Authorization':'Bearer '+credential,**(headers or {})}
        if isinstance(body,dict):body=json.dumps(body).encode();h['Content-Type']='application/json'
        req=urllib.request.Request(f'http://127.0.0.1:{port}'+path,data=body,headers=h,method=method)
        try:
            with urllib.request.urlopen(req,timeout=30) as r:code,data,hs=r.status,r.read(),r.headers
        except urllib.error.HTTPError as e:code,data,hs=e.code,e.read(),e.headers
        assert code==status,(path,code,data[:300])
        return data,hs
    def api(path,method='GET',body=None,**kw):
        data,_=request(path,method,body,**kw);return json.loads(data) if data else None
    def upload(name,folder,data):
        from urllib.parse import urlencode
        return api('/files?'+urlencode({'name':name,'directory':folder,'drive':'A'}),'POST',data,headers={'Content-Type':'application/octet-stream'},status=201)
    base='/zip-fixture'
    api('/files/directories','POST',{'directory':base+'/empty','drive':'A'})
    first=upload('hello.txt',base,b'hello world\n');upload('caf\u00e9.bin',base+'/nested',bytes(range(256))*1024);upload('zero',base,b'')
    api('/files/downloads','POST',{'drive':'A','directory':base},credential=minimal,status=403)
    job=api('/files/downloads','POST',{'drive':'A','directory':base},status=202);path='/files/downloads/'+job['id'];url=job['download_url']
    def ready():
        j=api(path);assert j['state'] not in ('failed','interrupted'),j;return j if j['state']=='ready' else None
    job=wait(ready)
    api(path,credential=other,status=404);request(url,credential=other,status=404)
    raw,h=request(url);etag=h['ETag'];assert len(raw)==job['archive_bytes']
    with zipfile.ZipFile(io.BytesIO(raw)) as z:
        assert set(z.namelist())=={'empty/','nested/','nested/caf\u00e9.bin','hello.txt','zero'}
        assert z.testzip() is None and z.read('hello.txt')==b'hello world\n'
    head,hh=request(url,'HEAD');assert not head and int(hh['Content-Length'])==len(raw)
    partial,_=request(url,headers={'Range':'bytes=17-700','If-Range':etag},status=206);assert partial==raw[17:701]
    rest,rh=request(url,headers={'Range':'bytes=701-','If-Range':etag},status=206);assert rh['Content-Range']==f'bytes 701-{len(raw)-1}/{len(raw)}' and raw[:17]+partial+rest==raw
    suffix,_=request(url,headers={'Range':'bytes=-98'},status=206);assert suffix==raw[-98:]
    full,_=request(url,headers={'Range':'bytes=999999999-','If-Range':'"different"'});assert full==raw
    request(url,headers={'Range':f'bytes={len(raw)}-'},status=416)
    # Revision changes must not resume a stale archive, even at an offset in its central directory.
    _,fh=request('/files/'+first['id']+'/content','HEAD')
    request('/files/'+first['id']+'/content','PUT',b'changed',{'Content-Type':'text/plain','If-Match':fh['ETag']})
    request(url,headers={'Range':'bytes=-98','If-Range':etag},status=409)
    api(path,'DELETE',status=204);api(path,status=404)
    second=api('/files/downloads','POST',{'drive':'A','directory':base},status=202);path='/files/downloads/'+second['id'];url=second['download_url'];wait(ready)
    data,h=request(url);stable_etag=h['ETag']
    upload('same', '/zip-duplicate', b'a');upload('same', '/zip-duplicate', b'b')
    duplicate=api('/files/downloads','POST',{'drive':'A','directory':'/zip-duplicate'},status=202)
    failure=wait(lambda:(lambda j:j if j['state']=='failed' else None)(api('/files/downloads/'+duplicate['id'])))
    assert failure['error']=='duplicate_archive_name'
    api('/files/downloads/'+duplicate['id'],'DELETE',status=204)
    def after_restart():
        resumed,rh=request(url,headers={'Range':'bytes=100-','If-Range':stable_etag},status=206);assert resumed==data[100:] and rh['ETag']==stable_etag
        api(path,'DELETE',status=204)
    return after_restart
