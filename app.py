import asyncio,csv,io,re,sqlite3,threading,time
from datetime import datetime,timezone
from pathlib import Path
from fastapi import FastAPI,Request
from fastapi.responses import HTMLResponse,StreamingResponse
from fastapi.staticfiles import StaticFiles
from playwright.async_api import async_playwright
ROOT=Path(__file__).parent
DB=ROOT/"data.sqlite"
SITES=[("CSGOEmpire","https://csgoempire.com/"),("Whale.io","https://whale.io/"),("HugoBet","https://hugobet.com/"),("Locas.bet","https://locas.bet/"),("BK8.io","https://bk8.io/"),("BetUS","https://www.betus.com.pa/"),("10bet","https://www.10bet.com/"),("MelBet","https://melbet.com/")]
SCORE=re.compile(r"(?<!\d)(\d{1,2})\s*[-:]\s*(\d{1,2})(?!\d)")
def now():return datetime.now(timezone.utc).isoformat(timespec="milliseconds")
def init():
 with sqlite3.connect(DB) as c:
  c.execute("CREATE TABLE IF NOT EXISTS obs(id INTEGER PRIMARY KEY,site TEXT,url TEXT,ts TEXT,score TEXT,context TEXT,source TEXT)")
  c.execute("CREATE TABLE IF NOT EXISTS statuses(site TEXT PRIMARY KEY,state TEXT,detail TEXT,ts TEXT)")
class Monitor:
 def __init__(self):
  init();self.running=False;self.thread=None;self.loop=None;self.stop_event=None;self.lock=threading.Lock();self.target="";self.events=[];self.seen={};self.started=None;self.error=None
 def set_target(self,target):
  with self.lock:self.target=target[:160];self.events=[];self.seen={}
 def status(self,n,s,d):
  with sqlite3.connect(DB) as c:c.execute("INSERT OR REPLACE INTO statuses VALUES(?,?,?,?)",(n,s,d,now()))
 def record(self,n,url,score,ctx,src):
  stamp=now();key=(n,score,ctx[:120])
  with self.lock:
   if key in self.seen and time.monotonic()-self.seen[key]<10:return
   self.seen[key]=time.monotonic();self.events.insert(0,{"site":n,"url":url,"observed_utc":stamp,"score":score,"context":ctx[:260],"source":src});self.events=self.events[:300]
  with sqlite3.connect(DB) as c:c.execute("INSERT INTO obs(site,url,ts,score,context,source) VALUES(?,?,?,?,?,?)",(n,url,stamp,score,ctx[:260],src))
 async def scan(self,n,url,txt,src):
  with self.lock:target=self.target
  terms=[x.casefold() for x in re.findall(r"[\w]+",target,flags=re.UNICODE) if len(x)>1]
  if not terms:return 0
  clean=re.sub(r"\s+"," ",txt);found=0
  for hit in SCORE.finditer(clean):
   if hit.group(1)=="0" and hit.group(2)=="0":continue
   ctx=clean[max(0,hit.start()-220):min(len(clean),hit.end()+220)]
   if all(term in ctx.casefold() for term in terms):
    self.record(n,url,hit.group(1)+"-"+hit.group(2),ctx,src);found+=1
  return found
 async def on_response(self,n,r):
  try:
   if r.request.resource_type in ("xhr","fetch") and "json" in r.headers.get("content-type","").lower():
    body=await r.text()
    if len(body)<750000:await self.scan(n,r.url,body,"JSON/XHR")
  except Exception:pass
 async def run(self):
  self.running=True;self.started=now();self.error=None;self.stop_event=asyncio.Event();self.loop=asyncio.get_running_loop()
  pages={}
  for n,u in SITES:self.status(n,"Подключение","Открываем площадку")
  try:
   async with async_playwright() as p:
    browser=await p.chromium.launch()
    try:
     for n,u in SITES:
      try:
       page=await browser.new_page();page.on("response",lambda r,name=n:asyncio.create_task(self.on_response(name,r)))
       await page.goto(u,wait_until="domcontentloaded",timeout=25000);pages[n]=page;self.status(n,"Открыта","Страница загружена")
      except Exception as e:self.status(n,"Недоступна",str(e).replace("\n"," ")[:160])
     while not self.stop_event.is_set():
      for n,page in list(pages.items()):
       if self.stop_event.is_set():break
       try:
        body=await page.locator("body").inner_text(timeout=6000)
        with self.lock:target=self.target
        if target:
         count=await self.scan(n,page.url,body,"Текст страницы");self.status(n,"Мониторинг",f"Совпадений со счётом: {count}")
        else:self.status(n,"Открыта","Введи название матча для поиска")
       except Exception as e:self.status(n,"Ошибка чтения",str(e).replace("\n"," ")[:140])
      try:await asyncio.wait_for(self.stop_event.wait(),timeout=5)
      except asyncio.TimeoutError:pass
    finally:await browser.close()
  except Exception as e:
   self.error=str(e)[:300]
   for n,u in SITES:self.status(n,"Ошибка",self.error)
  finally:self.running=False
 def start(self):
  if self.running or (self.thread and self.thread.is_alive()):return
  self.thread=threading.Thread(target=lambda:asyncio.run(self.run()),daemon=True);self.thread.start()
 def stop(self):
  if self.loop and self.stop_event:self.loop.call_soon_threadsafe(self.stop_event.set)
 def state(self):
  with sqlite3.connect(DB) as c:
   c.row_factory=sqlite3.Row;statuses=[dict(x) for x in c.execute("SELECT * FROM statuses ORDER BY site")];history=[dict(x) for x in c.execute("SELECT site,url,ts observed_utc,score,context,source FROM obs ORDER BY id DESC LIMIT 200")]
  with self.lock:return {"running":self.running,"started":self.started,"error":self.error,"target":self.target,"sites":[{"name":n,"url":u} for n,u in SITES],"statuses":statuses,"events":self.events or history}
m=Monitor();app=FastAPI(title="Score Delay Monitor");app.mount("/static",StaticFiles(directory=ROOT/"static"),name="static")
@app.get("/",response_class=HTMLResponse)
def home():return (ROOT/"static/index.html").read_text()
@app.get("/api/state")
def state():return m.state()
@app.post("/api/start")
def start():m.start();return {"ok":True}
@app.post("/api/stop")
def stop():m.stop();return {"ok":True}
@app.post("/api/match")
async def match(request:Request):
 data=await request.json();target=str(data.get("target","")).strip()[:160];m.set_target(target);return {"ok":True,"target":target}
@app.get("/api/export")
def export():
 with sqlite3.connect(DB) as c:rows=c.execute("SELECT site,url,ts,score,context,source FROM obs ORDER BY id").fetchall()
 out=io.StringIO();w=csv.writer(out);w.writerow(["site","url","observed_utc","score","context","source"]);w.writerows(rows)
 return StreamingResponse(io.BytesIO(out.getvalue().encode("utf-8-sig")),media_type="text/csv",headers={"Content-Disposition":"attachment; filename=score-monitor.csv"})
