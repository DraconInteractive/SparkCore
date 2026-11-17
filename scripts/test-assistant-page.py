import http.server,threading,subprocess,tempfile,time,json,shutil,sys,os
from pathlib import Path
os.chdir(Path(__file__).resolve().parent.parent)
if not shutil.which('firefox'):
 print('Firefox is required for the browser test');sys.exit(77)
page=Path('src/assistant_page.hpp').read_text().split('R"HTML(',1)[1].split(')HTML"',1)[0]
check=r'''<script>
(async()=>{const $=id=>document.getElementById(id);const wait=async f=>{for(let i=0;i<100;i++){if(f())return;await new Promise(r=>setTimeout(r,100));}throw Error('UI timeout');};const assert=(b,m)=>{if(!b)throw Error(m);};
try{await wait(()=>!$('send').disabled);$('secure').checked=true;$('query').value='First question';$('form').requestSubmit();await wait(()=>!$('send').disabled);assert(document.querySelectorAll('#messages img').length===0,'unsafe HTML rendering');assert($('secure').disabled,'privacy locked during conversation');
$('query').value='Follow up';$('form').requestSubmit();await wait(()=>[...document.querySelectorAll('button')].some(b=>b.textContent==='Confirm'&&!b.disabled));[...document.querySelectorAll('button')].find(b=>b.textContent==='Confirm').click();await wait(()=>[...document.querySelectorAll('button')].some(b=>b.textContent==='Undo'&&!b.disabled));[...document.querySelectorAll('button')].find(b=>b.textContent==='Undo').click();await wait(()=>$('messages').textContent.includes('Action undone.'));
$('query').value='Recover me';$('form').requestSubmit();await wait(()=>!$('retry').hidden&&!$('retry').disabled);assert($('send').disabled&&$('new').disabled,'uncertain request not blocked');$('retry').click();await wait(()=>!$('send').disabled);const next=[...document.querySelectorAll('button')].find(b=>b.textContent==='Explore next files');assert(next,'next page button');next.click();await wait(()=>$('messages').textContent.includes('Second page'));$('new').click();assert($('messages').children.length===0,'new conversation');assert(!$('secure').disabled,'privacy selectable for new conversation');await fetch('/result',{method:'POST',body:JSON.stringify({ok:true})});
}catch(e){await fetch('/result',{method:'POST',body:JSON.stringify({ok:false,error:e.message})});}})();</script>'''
page=page.replace('</body>',check+'</body>')
result=None;requests=[];retries=[]
class H(http.server.BaseHTTPRequestHandler):
 def log_message(self,*a):pass
 def send(self,data,kind='application/json',status=200):
  b=data.encode() if isinstance(data,str) else json.dumps(data).encode();self.send_response(status);self.send_header('Content-Type',kind);self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
 def do_GET(self):
  if self.path=='/':self.send(page,'text/html')
  elif self.path=='/auth/client.js':self.send('','application/javascript')
  elif self.path=='/assistant/setup':self.send({'enabled':True})
  else:self.send({})
 def do_POST(self):
  global result
  body=json.loads(self.rfile.read(int(self.headers['Content-Length'])))
  if self.path=='/result':result=body;self.send({});return
  if self.path=='/assistant/requests':
   requests.append(body)
   common={'conversation_id':'test-conversation','error':None}
   if body['text']=='First question':self.send(dict(common,status='needs_clarification',message='<img src=x onerror=alert(1)> Clarify?'));return
   if body.get('file_page_from'):
    assert body['file_page_from']=='saved-page' and 'conversation_id' not in body
    self.send(dict(common,status='completed',message='Second page',file_search={'has_more':False,'next_page_from':None}));return
   if body['text']=='Follow up':self.send(dict(common,status='needs_confirmation',message='Confirm this change?',confirmation_id='c'*64,confirmation_expires_at='2035-01-01T00:00:00Z'));return
   retries.append(body)
   if len(retries)==1:self.send('unavailable','text/plain',503)
   else:self.send(dict(common,status='completed',message='Recovered',file_search={'has_more':True,'next_page_from':'saved-page'}))
  elif self.path.endswith('/confirm'):self.send({'status':'completed','message':'Confirmed','action_id':'a'*64,'undo_available':True})
  elif self.path.endswith('/undo'):self.send({'status':'undone'})
  else:self.send({},status=404)
server=http.server.ThreadingHTTPServer(('127.0.0.1',0),H);threading.Thread(target=server.serve_forever,daemon=True).start()
with tempfile.TemporaryDirectory(prefix='spark-ui-firefox-') as profile:
 p=subprocess.Popen(['firefox','--headless','--no-remote','--profile',profile,'http://127.0.0.1:'+str(server.server_port)],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 try:
  for _ in range(300):
   if result is not None:break
   time.sleep(.1)
  assert result and result.get('ok'),result
  assert requests[0]['secure'] is True
  assert requests[1]['conversation_id']=='test-conversation' and 'secure' not in requests[1]
  assert len(retries)==2 and retries[0]==retries[1]
  print('PASS: Firefox query/follow-up, safe text rendering, confirmation, undo, identical-ID recovery pagination and new conversation')
 finally:p.terminate();p.wait(timeout=15);server.shutdown()
