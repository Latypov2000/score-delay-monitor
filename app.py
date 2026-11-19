import csv,io,re,sqlite3,threading
from datetime import datetime,timezone
from pathlib import Path
import httpx
from fastapi import FastAPI,Request
from fastapi.responses import HTMLResponse,StreamingResponse
from fastapi.staticfiles import StaticFiles
ROOT=Path(__file__).parent
DB=ROOT/"data.sqlite"
SPORTS=("football","basketball","cricket","tennis")
BOOKMAKERS=["CSGOEmpire","Whale.io","HugoBet","Locas.bet","BK8.io","BetUS","10bet","MelBet"]
BASE="https://sportscore.com/api/v1/fixtures/"
def now(): return datetime.now(timezone.utc).isoformat(timespec="milliseconds")
def norm(s): return re.sub(r"[^a-z0-9]+"," ",str(s or "").casefold()).strip()
def init_db():
 with sqlite3.connect(DB) as c: c.execute("CREATE TABLE IF NOT EXISTS observations(id INTEGER PRIMARY KEY,sport TEXT,match TEXT,score TEXT,status TEXT,observed_at TEXT,provider_updated TEXT)")
class Monitor:
 def __init__(self):
  init_db();self.lock=threading.RLock();self.target="";self.thread=None;self.stop_event=threading.Event();self.last_poll=None;self.error=None;self.matches=[];self.provider_states={};self.last_scores={}
 def set_target(self,target):
  with self.lock:self.target=target[:160];self.matches=[];self.last_scores={}
 def matches_target(self,item,target):
  q=norm(target);hay=norm(str(item.get("home") or "")+" "+str(item.get("away") or ""))
  words=[w for w in q.split() if len(w)>1 and w not in ("vs","versus")]
  return bool(words) and all(w in hay for w in words)
 def poll_once(self):
  with httpx.Client(timeout=15,headers={"Accept":"application/json","User-Agent":"ScoreDelayMonitor/1.0"}) as client:
   for sport in SPORTS:
    if self.stop_event.is_set():break
    try:
     r=client.get(BASE,params={"sport":sport,"status":"live","limit":200});r.raise_for_status();payload=r.json()
     items=payload.get("matches",payload.get("fixtures",[]));updated=payload.get("updated")
     with self.lock:
      self.provider_states[sport]={"state":"Данные доступны","detail":f"Получено live-матчей: {len(items)}","checked_at":now()}
      target=self.target
      for item in items:
       if not isinstance(item,dict) or not self.matches_target(item,target):continue
       home=str(item.get("home") or "");away=str(item.get("away") or "")
       hs=item.get("home_score");ascore=item.get("away_score")
       score=f"{hs if hs is not None else '—'} : {ascore if ascore is not None else '—'}"
       fixture=f"{home} — {away}";key=(sport,fixture.casefold())
       if self.last_scores.get(key)==score:continue
       stamp=now();self.last_scores[key]=score
       row={"sport":sport,"match":fixture,"score":score,"status":str(item.get("status_text") or item.get("status") or ""),"observed_at":stamp,"provider_updated":updated,"source":"SportScore"}
       self.matches.insert(0,row);self.matches=self.matches[:100]
       with sqlite3.connect(DB) as db:db.execute("INSERT INTO observations(sport,match,score,status,observed_at,provider_updated) VALUES(?,?,?,?,?,?)",(sport,fixture,score,row["status"],stamp,str(updated or "")))
    except Exception as e:
     with self.lock:self.provider_states[sport]={"state":"Ошибка источника","detail":f"{type(e).__name__}: {str(e)[:160]}","checked_at":now()}
  with self.lock:
   self.last_poll=now()
   self.error=None if any(v["state"]=="Данные доступны" for v in self.provider_states.values()) else "Бесплатный источник недоступен"
 def run(self):
  while not self.stop_event.is_set():
   self.poll_once()
   # Provider edge cache is about 60 seconds; polling more often does not improve freshness.
   self.stop_event.wait(60)
 def start(self):
  if self.thread and self.thread.is_alive():return
  self.stop_event.clear();self.thread=threading.Thread(target=self.run,daemon=True);self.thread.start()
 def stop(self):self.stop_event.set()
 def state(self):
  with self.lock:return {"running":bool(self.thread and self.thread.is_alive() and not self.stop_event.is_set()),"target":self.target,"last_poll":self.last_poll,"error":self.error,"provider":"SportScore","provider_states":dict(self.provider_states),"matches":list(self.matches),"bookmakers":[{"name":n,"state":"Нет прямого источника","detail":"Общий спортивный счёт не является данными этой букмекерской конторы."} for n in BOOKMAKERS]}
m=Monitor();app=FastAPI(title="Score Delay Monitor");app.mount("/static",StaticFiles(directory=ROOT/"static"),name="static")
@app.get("/",response_class=HTMLResponse)
def home():return (ROOT/"static/index.html").read_text(encoding="utf-8")
@app.get("/api/state")
def state():return m.state()
@app.post("/api/start")
def start():m.start();return {"ok":True}
@app.post("/api/stop")
def stop():m.stop();return {"ok":True}
@app.post("/api/match")
async def match(request:Request):
 d=await request.json();target=str(d.get("target","")).strip()[:160];m.set_target(target);return {"ok":True,"target":target}
@app.get("/api/export")
def export():
 with sqlite3.connect(DB) as db:rows=db.execute("SELECT sport,match,score,status,observed_at,provider_updated FROM observations ORDER BY id DESC LIMIT 5000").fetchall()
 out=io.StringIO();w=csv.writer(out);w.writerow(["sport","match","score","status","observed_at_utc","provider_updated"]);w.writerows(rows)
 return StreamingResponse(io.BytesIO(out.getvalue().encode("utf-8-sig")),media_type="text/csv",headers={"Content-Disposition":"attachment; filename=score-monitor.csv"})
