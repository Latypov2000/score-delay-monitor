import asyncio,csv,io,re,sqlite3,threading,time
from datetime import datetime,timezone
from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import HTMLResponse,StreamingResponse
from fastapi.staticfiles import StaticFiles
from playwright.async_api import async_playwright
ROOT=Path(__file__).parent; DB=ROOT/"data.sqlite"
SITES=[("CSGOEmpire","https://csgoempire.com/"),("Whale.io","https://whale.io/"),("HugoBet","https://hugobet.com/"),("Locas.bet","https://locas.bet/"),("BK8.io","https://bk8.io/"),("BetUS","https://www.betus.com.pa/"),("10bet","https://www.10bet.com/"),("MelBet","https://melbet.com/")]
PAT=re.compile(r"(?<!\\d)(\\d{1,2})\\s*[-:]\\s*(\\d{1,2})(?!\\d)")
def ts():return datetime.now(timezone.utc).isoformat(timespec="milliseconds")
def init():
 with sqlite3.connect(DB) as c:
  c.execute("CREATE TABLE IF NOT EXISTS obs(id INTEGER PRIMARY KEY,site TEXT,url TEXT,ts TEXT,score TEXT,context TEXT,source TEXT)")
  c.execute("CREATE TABLE IF NOT EXISTS statuses(site TEXT PRIMARY KEY,state TEXT,detail TEXT,ts TEXT)")
class Monitor:
 def __init__(self):
  init();self.running=False;self.thread=None;self.loop=None;self.stop_event=None;self.lock=threading.Lock();self.events=[];self.seen={};self.started=None;self.error=None
 def status(self,n,s,d):
  with sqlite3.connect(DB) as c:c.execute("INSERT OR REPLACE INTO statuses VALUES(?,?,?,?)",(n,s,d,ts()))
 def record(self,n,url,score,ctx,src):
  k=(n,score,ctx[:90]);t=time.monotonic()
  if t-self.seen.get(k,0)<20:return
  self.seen[k]=t;stamp=ts()
  with sqlite3.connect(DB) as c:c.execute("INSERT INTO obs(site,url,ts,score,context,source) VALUES(?,?,?,?,?,?)",(n,url,stamp,score,ctx,src))
  with self.lock:self.events.insert(0,dict(site=n,url=url,observed_utc=stamp,score=score,context=ctx[:180],source=src));self.events=self.events[:250]
 async def scan(self,n,url,txt,src):
  for m in PAT.finditer(txt):
   score=m.group(1)+"-"+m.group(2)
   if score=="0-0":continue
   ctx=re.sub(r"\\s+"," ",txt[max(0,m.start()-90):m.end()+90]).strip()
   self.record(n,url,score,ctx,src)
 async def response(self,n,r):
  try:
   if "json" in r.headers.get("content-type","").lower() and r.request.resource_type in ("xhr","fetch"):
    body=await r.text()
    if len(body)<750000:await self.scan(n,r.url,body,"JSON/XHR")
  except Exception:pass
 async def run(self):
  self.running=True;self.started=ts();self.error=None;self.stop_event=asyncio.Event();self.loop=asyncio.get_running_loop()
  try:
   async with async_playwright() as p:
    browser=await p.chromium.launch()
    pages={}
    try:
     for n,u in SITES:
      try:
       page=await browser.new_page();page.on("response",lambda r,name=n:asyncio.create_task(self.response(name,r)))
       await page.goto(u,wait_until="domcontentloaded",timeout=35000);pages[n]=page;self.status(n,"Открыт","Страница доступна")
      except Exception as e:self.status(n,"Недоступен",str(e)[:140])
     while not self.stop_event.is_set():
      for n,page in pages.items():
       try:await self.scan(n,page.url,await page.locator("body").inner_text(timeout=6000),"Текст страницы");self.status(n,"Сканируется","Последняя проверка "+ts())
       except Exception as e:self.status(n,"Ошибка",str(e)[:140])
      try:await asyncio.wait_for(self.stop_event.wait(),timeout=4)
      except asyncio.TimeoutError:pass
    finally:await browser.close()
  except Exception as e:self.error=str(e)
  finally:self.running=False
 def start(self):
  if self.running or (self.thread and self.thread.is_alive()):return
  self.thread=threading.Thread(target=lambda:asyncio.run(self.run()),daemon=True);self.thread.start()
 def stop(self):
  if self.loop and self.stop_event:self.loop.call_soon_threadsafe(self.stop_event.set)
 def state(self):
  with sqlite3.connect(DB) as c:
   c.row_factory=sqlite3.Row;statuses=[dict(x) for x in c.execute("SELECT * FROM statuses ORDER BY site")];history=[dict(x) for x in c.execute("SELECT site,url,ts observed_utc,score,context,source FROM obs ORDER BY id DESC LIMIT 150")]
  with self.lock:events=list(self.events)
  return {"running":self.running,"started":self.started,"error":self.error,"sites":[{"name":n,"url":u} for n,u in SITES],"statuses":statuses,"events":events or history}
m=Monitor();app=FastAPI(title="Score Delay Monitor");app.mount("/static",StaticFiles(directory=ROOT/"static"),name="static")
@app.get("/",response_class=HTMLResponse)
def home():return (ROOT/"static/index.html").read_text()
@app.get("/api/state")
def state():return m.state()
@app.post("/api/start")
def start():m.start();return {"ok":True}
@app.post("/api/stop")
def stop():m.stop();return {"ok":True}
@app.get("/api/export")
def export():
 with sqlite3.connect(DB) as c:rows=c.execute("SELECT site,url,ts,score,context,source FROM obs ORDER BY id").fetchall()
 f=io.StringIO();w=csv.writer(f);w.writerow(["site","url","observed_utc","score","context","source"]);w.writerows(rows)
 return StreamingResponse(io.BytesIO(f.getvalue().encode("utf-8-sig")),media_type="text/csv",headers={"Content-Disposition":"attachment; filename=score-monitor.csv"})
