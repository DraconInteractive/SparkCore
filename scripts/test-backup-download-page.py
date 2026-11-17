#!/usr/bin/env python3
"""Headless browser check for backup prepare/download and persisted resume URL."""
import http.server,json,subprocess,tempfile,threading,time
from pathlib import Path
source=Path('src/inter_spark.cpp').read_text();page=source.split('constexpr const char* page=R"HTML(',1)[1].split(')HTML";',1)[0]
check=r'''<script>(async()=>{
const wait=async fn=>{for(let i=0;i<200;i++){if(fn())return;await new Promise(r=>setTimeout(r,100));}throw Error('Timed out');};
try{
 if(!sessionStorage.getItem('reloaded')){
  await wait(()=>[...document.querySelectorAll('button')].some(b=>b.textContent==='Prepare download'));
  [...document.querySelectorAll('button')].find(b=>b.textContent==='Prepare download').click();
  await wait(()=>document.querySelector('#rows a[download$=".zip"]'));
  sessionStorage.setItem('download-link',document.querySelector('#rows a[download$=".zip"]').href);
  sessionStorage.setItem('reloaded','yes');location.reload();return;
 }
 await wait(()=>document.querySelector('#rows a[download$=".zip"]'));
 if(document.querySelector('#rows a[download$=".zip"]').href!==sessionStorage.getItem('download-link'))throw Error('resume URL changed');
 await fetch('/result',{method:'POST',body:JSON.stringify({ok:true})});
}catch(e){await fetch('/result',{method:'POST',body:JSON.stringify({ok:false,error:e.message})});}
})();</script>'''
page=page.replace('</html>',check+'</html>');result=None;posts=0;polls=0
backup='spark-20261117T000000Z-ui';job='a'*32
class Handler(http.server.BaseHTTPRequestHandler):
 def log_message(self,*args):pass
 def send(self,value,kind='application/json',status=200):
  data=value.encode() if isinstance(value,str) else json.dumps(value).encode();self.send_response(status);self.send_header('Content-Type',kind);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
 def do_GET(self):
  global polls
  if self.path=='/backups/explorer':self.send(page,'text/html')
  elif self.path=='/auth/client.js':self.send('','application/javascript')
  elif self.path=='/backups':self.send({'backups':[{'id':backup,'state':'complete','size_bytes':100,'verification':'test','manifest':'test','files':[]}]})
  elif self.path=='/backups/jobs':self.send({'jobs':[]})
  elif self.path=='/backups/downloads/'+job:
   polls+=1;self.send({'id':job,'state':'preparing' if polls==1 else 'ready','prepared_bytes':100,'total_bytes':100,'archive_bytes':500,'expires_at':2000000000,'download_url':'/backups/downloads/'+job+'/content'})
  else:self.send({},status=404)
 def do_POST(self):
  global result,posts
  body=json.loads(self.rfile.read(int(self.headers.get('Content-Length',0))) or b'{}')
  if self.path=='/result':result=body;self.send({})
  elif self.path=='/backups/'+backup+'/downloads':
   assert body=={};posts+=1;self.send({'id':job,'state':'preparing'},status=202)
  else:self.send({},status=404)
server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler);threading.Thread(target=server.serve_forever,daemon=True).start()
with tempfile.TemporaryDirectory(prefix='spark-backup-ui-') as profile:
 process=subprocess.Popen(['firefox','--headless','--no-remote','--profile',profile,f'http://127.0.0.1:{server.server_port}/backups/explorer'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 try:
  for _ in range(400):
   if result is not None:break
   time.sleep(.1)
  assert result and result.get('ok'),result
  assert posts==1 and polls>=3,(posts,polls)
  print('PASS: backup preparation, progress polling, download link and same URL after reload')
 finally:process.terminate();process.wait(timeout=15);server.shutdown()
