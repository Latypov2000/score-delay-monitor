import asyncio,csv,io,json,os,re,sqlite3,threading,time
from datetime import datetime,timezone
from pathlib import Path
from urllib.parse import urlencode
import httpx,websockets
from fastapi import FastAPI,Request
from fastapi.responses import HTMLResponse,StreamingResponse
from fastapi.staticfiles import StaticFiles

ROOT=Path(__file__).parent
DB=ROOT/"data.sqlite"
API="https://api.pulsescore.net"
SPORTS=("soccer","tennis","ice-hockey")
# Target names are mapped only where the aggregator's catalogue confirms coverage.
ALIASES={"10bet":"10bet","melbet":"melbet"}
DISPLAY={"10bet":"10bet","melbet":"MelBet"}
TARGETS=["CSGOEmpire","Whale.io","HugoBet","Locas.bet","BK8.io","BetUS","10bet","MelBet"]
def now(): return datetime.now(timezone.utc).isoformat(timespec="milliseconds")
def init():
 with sqlite3.connect(DB) as c:
  c.execute("CREATE TABLE IF NOT EXISTS obs(id INTEGER PRIMARY KEY,site TEXT,url TEXT,ts TEXT,score TEXT,context TEXT,source TEXT)")
  c.execute("CREATE TABLE IF NOT EXISTS statuses(site TEXT PRIMARY KEY,state TEXT,detail TEXT,ts TEXT)")
class Monitor:
 def __init__(self):
  init();self.running=False;self.thread=None;self.loop=None;self.stop_event=None
  self.lock=threading.RLock();self.target="";self.events=[];self.seen={};self.started=None;self.error=None;self.last_frame=None;self.connection_state="Остановлен"
 def set_target(self,target):
  with self.lock:self.target=target[:160];self.events=[];self.seen={}
 def status(self,n,state,detail):
  with sqlite3.connect(DB) as c:c.execute("INSERT OR REPLACE INTO statuses VALUES(?,?,?,?)",(n,state,detail[:240],now()))
 def score(self,e):
  s=e.get("score")
  if isinstance(s,dict):
   h=s.get("home",s.get("homeScore"));a=s.get("away",s.get("awayScore"))
   if h is not None and a is not None:return f"{h}-{a}"
  if isinstance(s,(str,int,float)):return str(s)
  return ""
 def match_ok(self,e,target):
  home=str(e.get("home") or e.get("homeTeam") or "")
  away=str(e.get("away") or e.get("awayTeam") or "")
  hay=(home+" "+away).casefold()
  words=[x.casefold() for x in re.findall(r"[\w]+",target,flags=re.UNICODE) if len(x)>1 and x.casefold() not in {"vs","versus","против"}]
  return bool(words) and all(x in hay for x in words)
 def save_event(self,book,sport,e):
  with self.lock:target=self.target
  if not target or not self.match_ok(e,target):return
  score=self.score(e)
  if not score:return
  site=DISPLAY.get(book,book)
  home=str(e.get("home") or e.get("homeTeam") or "")
  away=str(e.get("away") or e.get("awayTeam") or "")
  fixture=f"{home} — {away}"
  stamp=now();key=(site,fixture.casefold(),score)
  with self.lock:
   if key in self.seen:return
   self.seen[key]=stamp
   self.events.insert(0,{"site":site,"url":"","observed_utc":stamp,"score":score,"context":sport+" · "+str(e.get("league") or ""),"source":"PulseScore Stream","match":fixture})
   self.events=self.events[:500]
  with sqlite3.connect(DB) as c:c.execute("INSERT INTO obs(site,url,ts,score,context,source) VALUES(?,?,?,?,?,?)",(site,"",stamp,score,sport+" · "+str(e.get("league") or ""),"PulseScore Stream"))
  self.status(site,"Матч найден",fixture+" · "+score)
 async def run_stream(self):
  key=os.getenv("PULSESCORE_API_KEY","").strip()
  if not key:
   self.error="Не задан PULSESCORE_API_KEY в Render → Environment"
   for n in TARGETS:self.status(n,"Требуется ключ","Добавь API-ключ PulseScore")
   return
  params=urlencode({"key":key,"bookmakers":"all","sports":",".join(SPORTS),"kinds":"live"})
  uri="wss://api.pulsescore.net/api/stream/ws?"+params
  delay=2
  while not self.stop_event.is_set():
   try:
    self.connection_state="Подключение к PulseScore"
    for n in TARGETS:self.status(n,"Подключение","Ожидаем поток PulseScore")
    async with websockets.connect(uri,ping_interval=30,ping_timeout=150,max_size=None,max_queue=256,close_timeout=5) as ws:
     self.connection_state="Поток подключён";delay=2
     for n in TARGETS:self.status(n,"Поток подключён","Получаем live-события")
     async for raw in ws:
      if self.stop_event.is_set():break
      try:frame=json.loads(raw)
      except Exception:continue
      typ=frame.get("t")
      if typ=="error":
       self.error=str(frame.get("message") or frame)
       continue
      if typ=="connected":
       self.connection_state="Подключён · "+str(frame.get("plan",""));continue
      if typ in ("snapshot","delta"):
       stream=str(frame.get("stream",""));parts=stream.split("/")
       if len(parts)!=3:continue
       book,kind,sport=parts
       if kind!="live" or sport not in SPORTS:continue
       if typ=="snapshot":
        events=frame.get("events") or []
        for e in events:
         if isinstance(e,dict):self.save_event(book,sport,e)
       else:
        for change in frame.get("changes") or []:
         if change.get("op")=="upsert" and isinstance(change.get("event"),dict):self.save_event(book,sport,change["event"])
         elif change.get("op")=="patch":
          # Delta patches are applied to the last known event snapshot in memory.
          event_id=str(change.get("eventId"))
          board=self.boards.get(stream,{})
          e=board.get(event_id)
          if e:
           e.update(change.get("set") or {})
           if "score" in e:self.save_event(book,sport,e)
       elif typ=="snapshot_end":
        self.last_frame=now()
      elif typ=="snapshot_end":self.last_frame=now()
   except Exception as ex:
    self.connection_state="Переподключение"
    self.error=f"{type(ex).__name__}: {str(ex)[:180]}"
    for n in TARGETS:self.status(n,"Переподключение",self.error)
    try:await asyncio.wait_for(self.stop_event.wait(),timeout=delay)
    except asyncio.TimeoutError:pass
    delay=min(delay*2,60)
  self.connection_state="Остановлен"
 async def runner(self):
  self.boards={}
  await self.run_stream()
 def start(self):
  if self.running or (self.thread and self.thread.is_alive()):return
  self.running=True;self.started=now();self.error=None
  self.stop_event=asyncio.Event()
  def worker():
   try:asyncio.run(self.runner())
   finally:self.running=False
  self.thread=threading.Thread(target=worker,daemon=True);self.thread.start()
 def stop(self):
  if self.loop and self.stop_event:self.loop.call_soon_threadsafe(self.stop_event.set)
 def state(self):
  with sqlite3.connect(DB) as c:
   c.row_factory=sqlite3.Row
   statuses=[dict(x) for x in c.execute("SELECT * FROM statuses ORDER BY site")]
   history=[dict(x) for x in c.execute("SELECT site,url,ts observed_utc,score,context,source FROM obs ORDER BY id DESC LIMIT 300")]
  with self.lock:
   return {"running":self.running,"started":self.started,"error":self.error,"target":self.target,"last_poll":self.last_frame,"api_configured":bool(os.getenv("PULSESCORE_API_KEY","").strip()),"connection_state":self.connection_state,"sites":[{"name":n,"provider":ALIASES.get(n)} for n in TARGETS],"statuses":statuses,"events":list(self.events) or history}
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
@app.post("/api/match")
async def match(request:Request):
 data=await request.json();target=str(data.get("target","")).strip()[:160];m.set_target(target);return {"ok":True,"target":target}
@app.get("/api/export")
def export():
 with sqlite3.connect(DB) as c:rows=c.execute("SELECT site,url,ts,score,context,source FROM obs ORDER BY id").fetchall()
 out=io.StringIO();w=csv.writer(out);w.writerow(["site","url","observed_utc","score","context","source"]);w.writerows(rows)
 return StreamingResponse(io.BytesIO(out.getvalue().encode("utf-8-sig")),media_type="text/csv",headers={"Content-Disposition":"attachment; filename=score-monitor.csv"})
