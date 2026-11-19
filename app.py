import asyncio,csv,io,json,os,re,sqlite3,threading,time
from datetime import datetime,timezone
from pathlib import Path
from fastapi import FastAPI,Request
from fastapi.responses import HTMLResponse,StreamingResponse
from fastapi.staticfiles import StaticFiles
import httpx

ROOT=Path(__file__).parent
DB=ROOT/"data.sqlite"
API="https://api.pulsescore.net/api"
API_KEY=os.getenv("PULSESCORE_API_KEY","").strip()
SPORTS=("soccer","tennis","ice-hockey")
SITES=[("CSGOEmpire",None),("Whale.io",None),("HugoBet",None),("Locas.bet",None),("BK8.io",None),("BetUS",None),("10bet","10bet"),("MelBet","melbet")]
def now(): return datetime.now(timezone.utc).isoformat(timespec="milliseconds")
def init():
 with sqlite3.connect(DB) as c:
  c.execute("CREATE TABLE IF NOT EXISTS obs(id INTEGER PRIMARY KEY,site TEXT,url TEXT,ts TEXT,score TEXT,context TEXT,source TEXT)")
  c.execute("CREATE TABLE IF NOT EXISTS statuses(site TEXT PRIMARY KEY,state TEXT,detail TEXT,ts TEXT)")
class Monitor:
 def __init__(self):
  init(); self.running=False; self.thread=None; self.loop=None; self.stop_event=None
  self.lock=threading.RLock(); self.target=""; self.events=[]; self.seen={}; self.started=None; self.error=None; self.last_poll=None
 def set_target(self,target):
  with self.lock: self.target=target[:160]; self.events=[]; self.seen={}
 def status(self,n,state,detail):
  with sqlite3.connect(DB) as c: c.execute("INSERT OR REPLACE INTO statuses VALUES(?,?,?,?)",(n,state,detail[:240],now()))
 def record(self,n,score,match,ctx,source):
  stamp=now(); key=(n,match.casefold(),score)
  with self.lock:
   old=self.seen.get(key)
   if old and time.monotonic()-old<0.8: return
   self.seen[key]=time.monotonic()
   self.events.insert(0,{"site":n,"url":"","observed_utc":stamp,"score":score,"context":ctx[:260],"source":source,"match":match})
   self.events=self.events[:500]
  with sqlite3.connect(DB) as c: c.execute("INSERT INTO obs(site,url,ts,score,context,source) VALUES(?,?,?,?,?,?)",(n,"",stamp,score,ctx[:260],source))
 def matches_target(self,event,target):
  home=str(event.get("home") or event.get("homeTeam") or "")
  away=str(event.get("away") or event.get("awayTeam") or "")
  fixture=(home+" "+away).casefold()
  words=[w.casefold() for w in re.findall(r"[\w]+",target,flags=re.UNICODE) if len(w)>1 and w.casefold() not in {"vs","versus","против"}]
  return bool(words) and all(w in fixture for w in words)
 def score_text(self,event):
  score=event.get("score")
  if isinstance(score,dict):
   h=score.get("home",score.get("homeScore")); a=score.get("away",score.get("awayScore"))
   if h is not None and a is not None: return f"{h}-{a}"
   return json.dumps(score,ensure_ascii=False)
  if score is not None: return str(score)
  live=event.get("liveData") or event.get("scoreboard") or {}
  if isinstance(live,dict):
   h=live.get("home"); a=live.get("away")
   if h is not None and a is not None: return f"{h}-{a}"
  return ""
 async def fetch_book_sport(self,client,book,sport):
  try:
   r=await client.get(f"{API}/{book}/live-events",params={"sport":sport},headers={"X-Secret":API_KEY},timeout=12)
   r.raise_for_status(); payload=r.json()
   data=(payload.get("events") or payload.get("data") or []) if isinstance(payload,dict) else payload
   return data if isinstance(data,list) else []
  except Exception as e:
   name="10bet" if book=="10bet" else "MelBet"
   detail=f"{type(e).__name__}: {str(e)[:170]}"
   if isinstance(e,httpx.HTTPStatusError): detail=f"HTTP {e.response.status_code}: {e.response.text[:130]}"
   self.status(name,"Ошибка источника",detail)
   return []
 async def poll_once(self,client):
  with self.lock: target=self.target
  if not target: return
  tasks=[self.fetch_book_sport(client,b,s) for b in ("10bet","melbet") for s in SPORTS]
  results=await asyncio.gather(*tasks)
  for idx,items in enumerate(results):
   book=("10bet","melbet")[idx//len(SPORTS)]; name="10bet" if book=="10bet" else "MelBet"; sport=SPORTS[idx%len(SPORTS)]
   found=0
   for ev in items:
    if not isinstance(ev,dict) or not self.matches_target(ev,target): continue
    score=self.score_text(ev)
    if not score: continue
    home=str(ev.get("home") or ev.get("homeTeam") or ""); away=str(ev.get("away") or ev.get("awayTeam") or "")
    self.record(name,score,f"{home} — {away}",f"{sport} · {ev.get('league','')} · {ev.get('matchClock') or ev.get('period') or ''}","PulseScore REST")
    found+=1
   self.status(name,"Матч найден" if found else "Подключено",f"{sport}: совпадений со счётом — {found}")
  self.last_poll=now()
 async def run(self):
  self.running=True; self.started=now(); self.error=None; self.stop_event=asyncio.Event(); self.loop=asyncio.get_running_loop()
  for n,b in SITES:
   self.status(n,"Ожидание API" if b else "Нет покрытия","Ожидается API-ключ" if b else "Нет подтверждённого feed для этой конторы в выбранном агрегаторе")
  if not API_KEY:
   self.error="Не задан PULSESCORE_API_KEY в Render Environment"
   for n,b in SITES:
    if b: self.status(n,"Требуется ключ","Добавь PULSESCORE_API_KEY в Render → Environment")
   self.running=False; return
  try:
   async with httpx.AsyncClient(follow_redirects=True) as client:
    while not self.stop_event.is_set():
     await self.poll_once(client)
     try: await asyncio.wait_for(self.stop_event.wait(),timeout=5)
     except asyncio.TimeoutError: pass
  except Exception as e: self.error=f"{type(e).__name__}: {str(e)[:220]}"
  finally: self.running=False
 def start(self):
  if self.running or (self.thread and self.thread.is_alive()): return
  self.thread=threading.Thread(target=lambda:asyncio.run(self.run()),daemon=True); self.thread.start()
 def stop(self):
  if self.loop and self.stop_event: self.loop.call_soon_threadsafe(self.stop_event.set)
 def state(self):
  with sqlite3.connect(DB) as c:
   c.row_factory=sqlite3.Row
   statuses=[dict(x) for x in c.execute("SELECT * FROM statuses ORDER BY site")]
   history=[dict(x) for x in c.execute("SELECT site,url,ts observed_utc,score,context,source FROM obs ORDER BY id DESC LIMIT 300")]
  with self.lock:
   return {"running":self.running,"started":self.started,"error":self.error,"target":self.target,"last_poll":self.last_poll,"api_configured":bool(API_KEY),"sites":[{"name":n,"provider":b} for n,b in SITES],"statuses":statuses,"events":list(self.events) or history}
m=Monitor()
app=FastAPI(title="Score Delay Monitor")
app.mount("/static",StaticFiles(directory=ROOT/"static"),name="static")
@app.get("/",response_class=HTMLResponse)
def home(): return (ROOT/"static/index.html").read_text()
@app.get("/api/state")
def state(): return m.state()
@app.post("/api/start")
def start(): m.start(); return {"ok":True}
@app.post("/api/stop")
def stop(): m.stop(); return {"ok":True}
@app.post("/api/match")
async def match(request:Request):
 data=await request.json(); target=str(data.get("target","")).strip()[:160]
 m.set_target(target); return {"ok":True,"target":target}
@app.get("/api/export")
def export():
 with sqlite3.connect(DB) as c: rows=c.execute("SELECT site,url,ts,score,context,source FROM obs ORDER BY id").fetchall()
 out=io.StringIO(); w=csv.writer(out); w.writerow(["site","url","observed_utc","score","context","source"]); w.writerows(rows)
 return StreamingResponse(io.BytesIO(out.getvalue().encode("utf-8-sig")),media_type="text/csv",headers={"Content-Disposition":"attachment; filename=score-monitor.csv"})
