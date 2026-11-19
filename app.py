import asyncio,csv,io,re,sqlite3,threading,time
from datetime import datetime,timezone
from pathlib import Path
from fastapi import FastAPI
from fastapi.responses import HTMLResponse,StreamingResponse
from fastapi.staticfiles import StaticFiles
from playwright.async_api import async_playwright

ROOT=Path(__file__).parent
DB=ROOT/"data.sqlite"
SITES=[
 ("CSGOEmpire","https://csgoempire.com/"),
 ("Whale.io","https://whale.io/"),
 ("HugoBet","https://hugobet.com/"),
 ("Locas.bet","https://locas.bet/"),
 ("BK8.io","https://bk8.io/"),
 ("BetUS","https://www.betus.com.pa/"),
 ("10bet","https://www.10bet.com/"),
 ("MelBet","https://melbet.com/"),
]
# Deliberately conservative score-like pattern. It is a candidate detector,
# not proof that two observations refer to the same sporting event.
PAT=re.compile(r"(?<!\d)(\d{1,2})\s*[-:]\s*(\d{1,2})(?!\d)")
def ts():
 return datetime.now(timezone.utc).isoformat(timespec="milliseconds")
def init():
 with sqlite3.connect(DB) as c:
  c.execute("CREATE TABLE IF NOT EXISTS obs(id INTEGER PRIMARY KEY,site TEXT,url TEXT,ts TEXT,score TEXT,context TEXT,source TEXT)")
  c.execute("CREATE TABLE IF NOT EXISTS statuses(site TEXT PRIMARY KEY,state TEXT,detail TEXT,ts TEXT)")
class Monitor:
 def __init__(self):
  init()
  self.running=False;self.thread=None;self.loop=None;self.stop_event=None
  self.lock=threading.Lock();self.events=[];self.seen={};self.started=None;self.error=None
 def status(self,n,s,d):
  with sqlite3.connect(DB) as c:
   c.execute("INSERT OR REPLACE INTO statuses VALUES(?,?,?,?)",(n,s,d,ts()))
 def record(self,n,url,score,ctx,src):
  key=(n,score,ctx[:90]);now=time.monotonic()
  if now-self.seen.get(key,0)<25:return
  self.seen[key]=now;stamp=ts()
  with sqlite3.connect(DB) as c:
   c.execute("INSERT INTO obs(site,url,ts,score,context,source) VALUES(?,?,?,?,?,?)",(n,url,stamp,score,ctx,src))
  with self.lock:
   self.events.insert(0,dict(site=n,url=url,observed_utc=stamp,score=score,context=ctx[:180],source=src))
   self.events=self.events[:250]
 async def scan(self,n,url,txt,src):
  found=0
  clean=re.sub(r"\s+"," ",txt)
  for match in PAT.finditer(clean):
   a,b=match.group(1),match.group(2)
   if a=="0" and b=="0":continue
   context=clean[max(0,match.start()-100):match.end()+100]
   self.record(n,url,a+"-"+b,context,src);found+=1
  return found
 async def response(self,n,response):
  try:
   if "json" in response.headers.get("content-type","").lower() and response.request.resource_type in ("xhr","fetch"):
    body=await response.text()
    if len(body)<500000:await self.scan(n,response.url,body,"JSON/XHR")
  except Exception:
   pass
 async def run(self):
  self.running=True;self.started=ts();self.error=None
  self.stop_event=asyncio.Event();self.loop=asyncio.get_running_loop()
  for name,_ in SITES:self.status(name,"В очереди","Ожидает проверки")
  try:
   async with async_playwright() as p:
    browser=await p.chromium.launch()
    try:
     while not self.stop_event.is_set():
      cycle=time.monotonic()
      for name,url in SITES:
       if self.stop_event.is_set():break
       page=None
       try:
        self.status(name,"Подключение","Открываем страницу")
        page=await browser.new_page()
        page.on("response",lambda response,n=name:asyncio.create_task(self.response(n,response)))
        await page.goto(url,wait_until="domcontentloaded",timeout=18000)
        await page.wait_for_timeout(1200)
        body=await page.locator("body").inner_text(timeout=7000)
        count=await self.scan(name,page.url,body,"Текст страницы")
        self.status(name,"Доступен",f"Проверено; найдено кандидатов счёта: {count}")
       except Exception as exc:
        message=str(exc).replace("\n"," ")[:170]
        self.status(name,"Ограничен/ошибка",message)
       finally:
        if page:
         try:await page.close()
         except Exception:pass
      # Poll each source in sequence; one tab at a time to limit memory use.
      delay=max(2,30-(time.monotonic()-cycle))
      try:await asyncio.wait_for(self.stop_event.wait(),timeout=delay)
      except asyncio.TimeoutError:pass
    finally:
     await browser.close()
  except Exception as exc:
   self.error=str(exc)[:300]
   for name,_ in SITES:
    self.status(name,"Ошибка монитора",self.error)
  finally:
   self.running=False
 def start(self):
  if self.running or (self.thread and self.thread.is_alive()):return
  self.thread=threading.Thread(target=lambda:asyncio.run(self.run()),daemon=True)
  self.thread.start()
 def stop(self):
  if self.loop and self.stop_event:self.loop.call_soon_threadsafe(self.stop_event.set)
 def state(self):
  with sqlite3.connect(DB) as c:
   c.row_factory=sqlite3.Row
   statuses=[dict(row) for row in c.execute("SELECT * FROM statuses ORDER BY site")]
   history=[dict(row) for row in c.execute("SELECT site,url,ts observed_utc,score,context,source FROM obs ORDER BY id DESC LIMIT 150")]
  with self.lock:events=list(self.events)
  return {"running":self.running,"started":self.started,"error":self.error,
   "sites":[{"name":n,"url":u} for n,u in SITES],
   "statuses":statuses,"events":events or history}
m=Monitor()
app=FastAPI(title="Score Delay Monitor")
app.mount("/static",StaticFiles(directory=ROOT/"static"),name="static")
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
 with sqlite3.connect(DB) as c:
  rows=c.execute("SELECT site,url,ts,score,context,source FROM obs ORDER BY id").fetchall()
 output=io.StringIO();writer=csv.writer(output)
 writer.writerow(["site","url","observed_utc","score","context","source"]);writer.writerows(rows)
 return StreamingResponse(io.BytesIO(output.getvalue().encode("utf-8-sig")),
  media_type="text/csv",headers={"Content-Disposition":"attachment; filename=score-monitor.csv"})
