#!/usr/bin/env python3
"""Download a prepared SPARK backup ZIP in resumable ranges; no third-party modules."""
import argparse,getpass,json,os,re,sys,urllib.request,urllib.parse,urllib.error,zipfile
from pathlib import Path

class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self,*args,**kwargs):
        raise RuntimeError('Redirect refused; use the original SPARK download URL')

def download(url,output,token,allow_http=False,chunk_size=16*1024*1024):
    address=urllib.parse.urlsplit(url)
    if address.scheme not in ('https','http') or (address.scheme=='http' and not allow_http) or not address.hostname or address.username or address.password or address.query or address.fragment or not re.fullmatch(r'/backups/downloads/[0-9a-f]{32}/content',address.path):
        raise ValueError('Use the prepared HTTPS backup download URL (or --allow-http for a trusted local connection)')
    output=Path(output);part=Path(str(output)+'.part');state_path=Path(str(output)+'.spark-download.json')
    if output.exists():raise ValueError('Output already exists; choose another name')
    opener=urllib.request.build_opener(NoRedirect())
    def request(method='GET',headers=None):
        h=dict(headers or {})
        if token:h['Authorization']='Bearer '+token
        return opener.open(urllib.request.Request(url,method=method,headers=h),timeout=60)
    with request('HEAD') as response:
        size=int(response.headers['Content-Length']);etag=response.headers.get('ETag','')
        if response.status!=200 or not etag.startswith('"') or not etag.endswith('"') or response.headers.get('Accept-Ranges')!='bytes':raise ValueError('Server does not advertise a stable resumable download')
    identity={'url':url,'etag':etag,'size':size}
    if state_path.exists():
        state=json.loads(state_path.read_text())
        if any(state.get(k)!=v for k,v in identity.items()):raise ValueError('Download identity changed; keep the old partial file and use a new output name')
        offset=state.get('completed_bytes')
        if type(offset)!=int or not 0<=offset<=size or not part.is_file() or part.stat().st_size<offset:raise ValueError('Partial download or saved progress is inconsistent')
    else:
        if part.exists():raise ValueError('Partial file exists without saved progress; choose a new output name')
        offset=0
        with part.open('xb'):pass
    def save():
        temporary=Path(str(state_path)+'.tmp')
        with temporary.open('w') as stream:
            json.dump({**identity,'completed_bytes':offset},stream);stream.flush();os.fsync(stream.fileno())
        os.replace(temporary,state_path)
    save()
    with part.open('r+b') as stream:
        stream.truncate(offset);stream.seek(offset)
        while offset<size:
            end=min(size,offset+chunk_size)-1
            with request(headers={'Range':f'bytes={offset}-{end}','If-Range':etag}) as response:
                if response.status!=206 or response.headers.get('ETag')!=etag or response.headers.get('Content-Range')!=f'bytes {offset}-{end}/{size}':raise ValueError('Resume response changed; refusing to append incompatible data')
                remaining=end-offset+1
                while remaining:
                    data=response.read(min(65536,remaining))
                    if not data:raise IOError('Download interrupted; run the same command to resume')
                    stream.write(data);remaining-=len(data)
                if response.read(1):raise ValueError('Server returned excess data')
            stream.flush();os.fsync(stream.fileno());offset=end+1;save()
            print(f'{offset:,} / {size:,} bytes ({offset*100/size:.1f}%)',flush=True)
    print('Checking ZIP checksums...',flush=True)
    with zipfile.ZipFile(part) as archive:
        bad=archive.testzip()
        if bad:raise ValueError('ZIP checksum failed for '+bad+'; keep partial data for diagnosis')
    if output.exists():raise ValueError('Output appeared during download; refusing to overwrite it')
    part.rename(output);state_path.unlink();print('Saved '+str(output),flush=True)

def main():
    parser=argparse.ArgumentParser(description=__doc__)
    parser.add_argument('url',help='Prepared /backups/downloads/{id}/content URL')
    parser.add_argument('output',help='Destination ZIP; .part and progress sidecar are retained on interruption')
    parser.add_argument('--allow-http',action='store_true',help='Allow unencrypted HTTP on a trusted local/Tailscale connection')
    parser.add_argument('--open',action='store_true',help='No token; only for an open SPARK instance')
    args=parser.parse_args()
    try:
        token='' if args.open else os.environ.get('SPARK_TOKEN') or getpass.getpass('SPARK admin token (not saved): ')
        if not args.open and not token:raise ValueError('A SPARK administrator token is required')
        download(args.url,args.output,token,args.allow_http)
    except KeyboardInterrupt:print('\nStopped. Run the same command to resume.',file=sys.stderr);return 130
    except Exception as error:print(str(error)+'\nPartial data retained. Re-run the same command to resume after fixing the issue.',file=sys.stderr);return 1
    return 0
if __name__=='__main__':sys.exit(main())
