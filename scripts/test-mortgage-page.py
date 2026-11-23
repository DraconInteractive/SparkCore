#!/usr/bin/env python3
"""Exercise mortgage forms, previews, conflicts and read-only controls in Firefox."""
import http.server,json,subprocess,tempfile,threading,time
from pathlib import Path
from urllib.parse import urlsplit
page=Path('src/budget_explorer.hpp').read_text().split('R"HTML(',1)[1].split(')HTML";',1)[0]
check=r'''<script>(async()=>{
 const wait=async fn=>{for(let i=0;i<200;i++){if(fn())return;await new Promise(r=>setTimeout(r,50));}throw Error('Timed out: '+document.getElementById('mortgage-message').textContent);};
 const n=id=>document.getElementById(id);window.confirm=()=>true;
 try{
  await wait(()=>!n('mortgage-preview').disabled);
  n('mortgage-principal').value='500000.00';n('mortgage-rate').value='6.25';n('mortgage-payment').value='3500.00';n('mortgage-offset').value='50000.00';
  n('mortgage-preview').click();await wait(()=>n('mortgage-stats').textContent.includes('Preview — not saved'));
  if(!n('mortgage-stats').textContent.includes('20 years, 0 months'))throw Error('Duration display');
  n('mortgage-form').requestSubmit();await wait(()=>n('mortgage-message').textContent==='Mortgage snapshot saved.');
  if(!n('mortgage-stats').textContent.includes('Saved snapshot estimate'))throw Error('Saved statistics');
  n('mortgage-principal').value='600000.00';n('mortgage-form').dispatchEvent(new Event('input'));n('mortgage-form').requestSubmit();await wait(()=>!mortgageBusy);
  if(n('mortgage-principal').value!=='600000.00'||!n('mortgage-message').textContent)throw Error('Conflict draft lost');
  n('mortgage-reload').click();await wait(()=>n('mortgage-principal').value==='500000.00');
  canWrite=false;mortgageLock(false);if(!n('mortgage-save').disabled||!n('mortgage-delete').disabled||n('mortgage-preview').disabled)throw Error('Read-only preview controls');
  canWrite=true;mortgageLock(false);n('mortgage-delete').click();await wait(()=>n('mortgage-message').textContent==='Saved mortgage cleared.');if(n('mortgage-principal').value!==''||!n('mortgage-delete').disabled)throw Error('Clear state');
  await fetch('/result',{method:'POST',body:JSON.stringify({ok:true})});
 }catch(e){await fetch('/result',{method:'POST',body:JSON.stringify({ok:false,error:e.message})});}
})();</script>'''
page=page.replace('</html>',check+'</html>');result=None;snapshot=None;revision=0;previews=0;saves=0
stats={'status':'amortizing','months_remaining':240,'years_remaining':20,'additional_months_remaining':0,'remaining_interest_cents':20000000,'remaining_payments_cents':70000000,'final_payment_cents':100000,'interest_bearing_balance_cents':45000000,'first_month_interest_cents':234375,'first_month_principal_reduction_cents':115625,'first_month_offset_interest_saving_cents':26042,'interest_saved_by_offset_cents':1000000,'months_saved_by_offset':12,'currency':'AUD','as_of':'2026-11-23'}
class Handler(http.server.BaseHTTPRequestHandler):
 def log_message(self,*args):pass
 def send(self,data,kind='application/json',status=200):
  raw=data.encode() if isinstance(data,str) else json.dumps(data).encode();self.send_response(status);self.send_header('Content-Type',kind);self.send_header('Content-Length',str(len(raw)));self.end_headers();self.wfile.write(raw)
 def state(self):return {'mortgage':snapshot,'stats':stats if snapshot else None,'revision':revision}
 def do_GET(self):
  path=urlsplit(self.path).path
  if path=='/budget/explorer':self.send(page,'text/html')
  elif path=='/auth/client.js':self.send('window.sparkSession={ready:Promise.resolve({mode:"open",permissions:[]})};','application/javascript')
  elif path=='/budget/mortgage':self.send(self.state())
  elif path=='/budget/expenses':self.send({'expenses':[],'has_more':False})
  elif path=='/budget/summary':self.send({'currencies':{}})
  else:self.send({},status=404)
 def do_POST(self):
  global result,previews
  p=json.loads(self.rfile.read(int(self.headers.get('Content-Length',0))))
  if self.path=='/result':result=p;self.send({});return
  assert self.path=='/budget/mortgage/estimate' and p['annual_interest_rate_bps']==625 and p['remaining_principal_cents']==50000000 and p['offset_balance_cents']==5000000 and p['monthly_payment_cents']==350000
  assert snapshot is None;previews+=1;self.send({'mortgage':p,'stats':stats})
 def do_PUT(self):
  global snapshot,revision,saves
  p=json.loads(self.rfile.read(int(self.headers.get('Content-Length',0))))
  if snapshot is not None:self.send({'error':'revision_conflict'},status=409);return
  assert p.pop('revision')==0;snapshot=p;revision=1;saves+=1;self.send(self.state())
 def do_DELETE(self):
  global snapshot,revision
  assert self.path=='/budget/mortgage?revision=1';snapshot=None;revision=2;self.send(self.state())
server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler);threading.Thread(target=server.serve_forever,daemon=True).start()
with tempfile.TemporaryDirectory(prefix='spark-mortgage-ui-') as profile:
 process=subprocess.Popen(['firefox','--headless','--no-remote','--profile',profile,f'http://127.0.0.1:{server.server_port}/budget/explorer'],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 try:
  for _ in range(400):
   if result is not None:break
   time.sleep(.1)
  assert result and result.get('ok'),result
  assert previews==1 and saves==1 and snapshot is None and revision==2
  print('PASS: mortgage form units, unsaved preview, saved statistics, conflict draft retention, reload, read-only controls and clear')
 finally:process.terminate();process.wait(timeout=15);server.shutdown()
