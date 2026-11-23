#!/usr/bin/env python3
"""Exercise the budget explorer in Firefox against a small deterministic mock API."""
import http.server,json,subprocess,tempfile,threading,time
from pathlib import Path
from urllib.parse import urlsplit
page=Path('src/budget_explorer.hpp').read_text().split('R"HTML(',1)[1].split(')HTML";',1)[0]
check=r'''<script>(async()=>{
 const wait=async fn=>{for(let i=0;i<200;i++){if(fn())return;await new Promise(r=>setTimeout(r,50));}throw Error('Timed out');};
 const set=(id,value)=>document.getElementById(id).value=value;
 const submit=id=>document.getElementById(id).dispatchEvent(new Event('submit',{cancelable:true}));
 try{
  await wait(()=>!document.getElementById('new').disabled);
  document.getElementById('new').click();set('name','<img src=x onerror=alert(1)>');set('amount','55.00');set('gst','5.00');set('category','Food');submit('edit-form');
  await wait(()=>document.getElementById('status').textContent==='Saved.');
  if(document.querySelector('#records img')||!document.getElementById('records').textContent.includes('AUD 55.00 / GST AUD 5.00'))throw Error('unsafe rendering or wrong GST display');
  set('kind','subscriptions');document.getElementById('kind').dispatchEvent(new Event('change'));
  await wait(()=>document.getElementById('new').textContent==='Add subscription');
  document.getElementById('new').click();set('name','Streaming');set('amount','12.00');set('gst','');set('reminder','2');submit('edit-form');
  await wait(()=>[...document.querySelectorAll('#records button')].some(b=>b.textContent==='Record payment'));
  [...document.querySelectorAll('#records button')].find(b=>b.textContent==='Record payment').click();
  await wait(()=>document.getElementById('payment').open);set('paid-amount','11.00');submit('payment-form');
  await wait(()=>document.getElementById('status').textContent.includes('Payment recorded'));
  set('mode','patterns');submit('forecast');await wait(()=>document.getElementById('forecast-result').textContent.includes('AUD 31.00 estimated'));
  if(!document.getElementById('forecast-result').textContent.includes('Estimate only'))throw Error('forecast assumptions missing');
  await fetch('/result',{method:'POST',body:JSON.stringify({ok:true})});
 }catch(e){await fetch('/result',{method:'POST',body:JSON.stringify({ok:false,error:e.message})});}
})();</script>'''
page=page.replace('</html>',check+'</html>');result=None;records={'expenses':[],'subscriptions':[]};writes=[]
class Handler(http.server.BaseHTTPRequestHandler):
 def log_message(self,*args):pass
 def send(self,data,kind='application/json',status=200):
  content=data.encode() if isinstance(data,str) else json.dumps(data).encode();self.send_response(status);self.send_header('Content-Type',kind);self.send_header('Content-Length',str(len(content)));self.end_headers();self.wfile.write(content)
 def do_GET(self):
  path=urlsplit(self.path).path
  if path=='/budget/explorer':self.send(page,'text/html')
  elif path=='/auth/client.js':self.send('window.sparkSession={ready:Promise.resolve({mode:"open",permissions:[]})};','application/javascript')
  elif path in ['/budget/expenses','/budget/subscriptions']:
   kind=path.split('/')[-1];self.send({kind:records[kind],'has_more':False,'offset':0,'limit':25})
  elif path=='/budget/mortgage':self.send({'mortgage':None,'stats':None,'revision':0})
  elif path=='/budget/summary':self.send({'currencies':{}})
  elif path=='/budget/forecast':self.send({'mode':'patterns','currencies':{'AUD':{'total_cents':3100,'subscription_cents':1200,'pattern_cents':1900,'history_expense_count':1}},'assumptions':{'history_days':90,'history_from':'2026-01-01','history_to':'2026-03-31','warning':'Estimate only'},'overdue_subscriptions':[],'charges':[]})
  else:self.send({},status=404)
 def do_POST(self):
  global result
  p=json.loads(self.rfile.read(int(self.headers.get('Content-Length',0))) or b'{}')
  if self.path=='/result':result=p;self.send({});return
  writes.append((self.path,p))
  if self.path.endswith('/payments'):
   self.send({'already_recorded':False});return
  kind=self.path.split('/')[-1]
  if kind not in records:self.send({},status=404);return
  p.update({'id':'12345678-1234-1234-1234-123456789abc','revision':1});records[kind].append(p.copy());self.send(p,status=201)
server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler);threading.Thread(target=server.serve_forever,daemon=True).start()
with tempfile.TemporaryDirectory(prefix='spark-budget-ui-') as profile:
 process=subprocess.Popen(['firefox','--headless','--no-remote','--profile',profile,f'http://127.0.0.1:{server.server_port}/budget/explorer'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 try:
  for _ in range(350):
   if result is not None:break
   time.sleep(.1)
  assert result and result.get('ok'),result
  assert len(writes)==3,writes
  assert writes[0][1]['amount_cents']==5500 and writes[0][1]['gst_cents']==500,writes[0]
  assert writes[1][1]['gst_cents'] is None and writes[1][1]['reminder_days']==2,writes[1]
  assert writes[2][1]['amount_cents']==1100 and writes[2][1]['revision']==1 and writes[2][1]['scheduled_date'],writes[2]
  print('PASS: budget expense/GST form, literal names, subscription creation, payment confirmation and pattern forecast display')
 finally:process.terminate();process.wait(timeout=15);server.shutdown()
