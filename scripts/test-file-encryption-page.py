#!/usr/bin/env python3
"""Firefox integration for encryption controls and temporary plaintext lifecycle."""
import http.server,json,subprocess,tempfile,threading,time
from pathlib import Path
from urllib.parse import urlsplit
page=Path('src/file_explorer.hpp').read_text().split('R"HTML(',1)[1].split(')HTML";',1)[0]
check=r'''<script>(async()=>{
 const wait=async fn=>{for(let i=0;i<300;i++){if(fn())return;await new Promise(r=>setTimeout(r,50));}throw Error('Timed out: '+document.getElementById('text-status').textContent+' / '+document.getElementById('crypto-error').textContent);};
 const node=id=>document.getElementById(id),click=id=>node(id).click();
 async function unlock(mode,password='test-password'){click('decrypt-'+mode);node('crypto-password').value=password;node('crypto-form').requestSubmit();await wait(()=>!cryptoPending);}
 try{
  await wait(()=>document.querySelector('#rows .name'));document.querySelector('#rows .name').click();await wait(()=>!node('encrypt-file').hidden&&node('preview').textContent.includes('Secret'));
  click('encrypt-file');node('crypto-password').value='test-password';node('crypto-confirm').value='test-password';node('crypto-form').requestSubmit();await wait(()=>!node('crypto-dialog').open&&!node('decrypt-view').hidden&&!cryptoPending);
  if(node('preview').textContent||node('text-editor').value||node('crypto-password').value)throw Error('Plaintext/password retained after encrypt');
  await unlock('view','wrong-password');if(!node('crypto-error').textContent.includes('Incorrect password')||node('preview').textContent)throw Error('Wrong password UI');click('crypto-cancel');
  await unlock('view');if(!node('preview').textContent.includes('Secret')||document.querySelector('#preview script'))throw Error('Safe temporary view');if(cryptoPassword)throw Error('View retained password');click('detail-close');await wait(()=>!node('details').open);if(node('preview').textContent||node('text-editor').value)throw Error('Close retained plaintext');
  document.querySelector('#rows .name').click();await wait(()=>!node('decrypt-edit').hidden);await unlock('edit');if(node('text-editor').hidden||cryptoPassword!=='test-password')throw Error('Encrypted edit setup');node('text-editor').value='Edited secret\n';node('text-editor').dispatchEvent(new Event('input'));click('text-save');await wait(()=>!textSaving&&!node('decrypt-view').hidden);
  if(cryptoPassword||node('text-editor').value||node('preview').textContent)throw Error('Save retained decrypted state');
  await unlock('view');if(node('preview').textContent!=='Edited secret\n')throw Error('Saved edit not visible');click('text-reload');await wait(()=>!node('decrypt-full').hidden);
  await unlock('full');await wait(()=>!node('encrypt-file').hidden&&node('preview').textContent==='Edited secret\n');
  await fetch('/result',{method:'POST',body:JSON.stringify({ok:true})});
 }catch(e){await fetch('/result',{method:'POST',body:JSON.stringify({ok:false,error:e.message})});}
})();</script>'''
page=page.replace('</body>',check+'</body>');result=None;encrypted=False;revision=0;text='Secret <script>throw Error("executed")</script>\n';actions=[]
id='12345678-1234-1234-1234-123456789abc';base='/files/'+id
class Handler(http.server.BaseHTTPRequestHandler):
 def log_message(self,*args):pass
 def send(self,data,kind='application/json',status=200):
  raw=data.encode() if isinstance(data,str) else json.dumps(data).encode();self.send_response(status);self.send_header('Content-Type',kind);self.send_header('Content-Length',str(len(raw)));self.send_header('ETag','"rev-'+str(revision)+'"');self.end_headers();self.wfile.write(raw)
 def metadata(self):return {'id':id,'name':'secret.txt','directory':'/','drive_letter':'A','drive_id':'drive','size_bytes':100 if encrypted else len(text.encode()),'created_at':'2026-11-20T00:00:00Z','content_revision':str(revision),'download_url':base+'/content'}
 def do_GET(self):
  path=urlsplit(self.path).path
  if path=='/files/explorer':self.send(page,'text/html')
  elif path=='/auth/client.js':self.send('window.sparkSession={headers:async()=>({})};','application/javascript')
  elif path=='/files/drives':self.send({'drives':[{'id':'drive','letter':'A','persistent':True,'online':True}]})
  elif path=='/files/directories':self.send({'directories':['/']})
  elif path=='/files':self.send({'files':[self.metadata()],'has_more':False})
  elif path==base:self.send(self.metadata())
  elif path==base+'/encryption':self.send({'encrypted':encrypted,'format':'SPARKENC-1' if encrypted else None,'plaintext_bytes':len(text.encode()) if encrypted else None})
  elif path==base+'/content':self.send('SPARKENC' if encrypted else text,'application/octet-stream')
  else:self.send({},status=404)
 def mutate(self):
  global result,encrypted,revision,text
  p=json.loads(self.rfile.read(int(self.headers.get('Content-Length',0))) or b'{}')
  if self.path=='/result':result=p;self.send({});return
  assert self.headers.get('If-Match')=='"rev-'+str(revision)+'"'
  if p.get('password')!='test-password':self.send({'error':'incorrect_password_or_damaged_file'},status=422);return
  if self.path==base+'/encrypt':encrypted=True;revision+=1;actions.append('encrypt');self.send(self.metadata())
  elif self.path==base+'/encrypted-content':assert encrypted;text=p['text'];revision+=1;actions.append('save');self.send(self.metadata())
  elif self.path==base+'/decrypt':
   assert encrypted;actions.append(p['mode'])
   if p['mode']=='full':encrypted=False;revision+=1;self.send(self.metadata())
   else:self.send(text,'application/octet-stream')
  else:self.send({},status=404)
 do_POST=mutate
 do_PUT=mutate
server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler);threading.Thread(target=server.serve_forever,daemon=True).start()
with tempfile.TemporaryDirectory(prefix='spark-encryption-ui-') as profile:
 process=subprocess.Popen(['firefox','--headless','--no-remote','--profile',profile,f'http://127.0.0.1:{server.server_port}/files/explorer'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 try:
  for _ in range(450):
   if result is not None:break
   time.sleep(.1)
  assert result and result.get('ok'),(result,actions)
  assert actions==['encrypt','view','edit','save','view','full'],actions
  assert not encrypted and text=='Edited secret\n'
  print('PASS: encryption UI, wrong password, safe text preview, temporary-state cleanup, encrypted editing and full decryption')
 finally:process.terminate();process.wait(timeout=15);server.shutdown()
