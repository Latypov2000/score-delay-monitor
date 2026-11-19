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

def now():
 return datetime.now(timezone.utc).isoformat(timespec="milliseconds")
def norm(value):
 return re.sub(r"[^a-z0-9]+"," ",str(value or "").casefold()).strip()
def nested(obj,*path):
 for key in path:
  if not isinstance(obj,dict):return None
  obj=obj.get(key)
 return obj
def first(obj,*values):
 for value in values:
  if value is not None and value!="":return value
 return None
def unwrap_name(value):
 if isinstance(value,dict):return first(value.get("name"),value.get("title"),value.get("short_name"),value.get("display_name"),"")
 return value or ""
def team_names(item):
 home=first(item.get("home"),item.get("home_team"),item.get("homeTeam"),item.get("team_home"),nested(item,"teams","home"),nested(item,"participants","home"),"")
 away=first(item.get("away"),item.get("away_team"),item.get("awayTeam"),item.get("team_away"),nested(item,"teams","away"),nested(item,"participants","away"),"")
 if (not home or not away) and isinstance(item.get("participants"),list):
  ps=item["participants"]
  if len(ps)>=2:
   home=home or ps[0];away=away or ps[1]
 return str(unwrap_name(home)),str(unwrap_name(away))
def extract_items(payload):
 if isinstance(payload,list):return payload
 if not isinstance(payload,dict):return []
 for key in ("fixtures","matches","data","events","results"):
  value=payload.get(key)
  if isinstance(value,list):return value
  if isinstance(value,dict):
   for subkey in ("fixtures","matches","data","events"):
    if isinstance(value.get(subkey),list):return value[subkey]
 return []
def is_live(item):
 raw=first(item.get("status"),item.get("status_text"),item.get("state"),nested(item,"status","short"),nested(item,"status","name"))
 if isinstance(raw,dict):raw=first(raw.get("short"),raw.get("name"),raw.get("type"))
 if raw is None or raw=="":return True # API request itself is filtered to status=live
 s=norm(raw)
 return s in {"live","inplay","in play","in progress","1h","2h","ht","half time","q1","q2","q3","q4","period 1","period 2","period 3","period 4","set 1","set 2","set 3","set 4","set 5","overtime","ot","playing","in progress live"}
def score_value(item,side):
 keys=(side+"_score",side+"Score","score_"+side)
 val=first(*(item.get(k) for k in keys),nested(item,"scores",side),nested(item,"score",side))
 if isinstance(val,dict):val=first(val.get("current"),val.get("total"),val.get("value"))
 return val

def init_db():
 with sqlite3.connect(DB) as c:
  c.execute("CREATE TABLE IF NOT EXISTS observations(id INTEGER PRIMARY KEY,sport TEXT,match TEXT,score TEXT,status TEXT,observed_at TEXT,provider_updated TEXT)")

class Monitor:
 def __init__(self):
  init_db();self.lock=threading.RLock();self.target="";self.thread=None;self.stop_event=threading.Event();self.last_poll=None;self.error=None;self.matches=[];self.provider_states={};self.last_scores={}
 def set_target(self,target):
  with self.lock:self.target=target[:160];self.matches=[];self.last_scores={}
 def matches_target(self,item,target):
  home,away=team_names(item)
  query=norm(target)
  # Match against both teams, their combined names and competition/event title.
  extra=first(item.get("name"),item.get("match"),item.get("title"),item.get("event_name"),nested(item,"competition","name"),"")
  hay=norm(home+" "+away+" "+str(extra))
  words=[w for w in query.split() if len(w)>1 and w not in ("vs","versus","v")]
  return bool(words) and all(w in hay for w in words)
 def poll_once(self):
  with httpx.Client(timeout=18,headers={"Accept":"application/json","User-Agent":"ScoreDelayMonitor/1.0"}) as client:
   for sport in SPORTS:
    if self.stop_event.is_set():break
    try:
     # The documented v1 fixtures endpoint supports status=live and up to 200 fixtures.
     response=client.get(BASE,params={"sport":sport,"date":datetime.now(timezone.utc).date().isoformat(),"status":"live","limit":200})
     response.raise_for_status()
     payload=response.json()
     items=extract_items(payload)
     updated=first(payload.get("updated"),payload.get("generated_at"),payload.get("last_updated")) if isinstance(payload,dict) else None
     with self.lock:
      self.provider_states[sport]={"state":"Данные доступны","detail":f"Получено событий: {len(items)}","checked_at":now()}
      target=self.target
      for item in items:
       if not isinstance(item,dict) or not is_live(item) or not self.matches_target(item,target):continue
       home,away=team_names(item)
       if not home and not away:continue
       hs=score_value(item,"home");aws=score_value(item,"away")
       # Alternative documented score shapes.
       if hs is None:hs=first(nested(item,"score","home_score"),nested(item,"scores","home_score"))
       if aws is None:aws=first(nested(item,"score","away_score"),nested(item,"scores","away_score"))
       score=f"{hs if hs is not None else '—'} : {aws if aws is not None else '—'}"
       fixture=f"{home} — {away}"
       key=(sport,fixture.casefold())
       if self.last_scores.get(key)==score:continue
       stamp=now();self.last_scores[key]=score
       status=first(item.get("status_text"),item.get("status"),item.get("state"),"LIVE")
       if isinstance(status,dict):status=first(status.get("short"),status.get("name"),"LIVE")
       row={"sport":sport,"match":fixture,"score":score,"status":str(status),"observed_at":stamp,"provider_updated":updated,"source":"SportScore"}
       self.matches.insert(0,row);self.matches=self.matches[:100]
       with sqlite3.connect(DB) as db:
        db.execute("INSERT INTO observations(sport,match,score,status,observed_at,provider_updated) VALUES(?,?,?,?,?,?)",(sport,fixture,score,str(status),stamp,str(updated or "")))
    except Exception as exc:
     with self.lock:self.provider_states[sport]={"state":"Ошибка источника","detail":f"{type(exc).__name__}: {str(exc)[:160]}","checked_at":now()}
  with self.lock:
   self.last_poll=now()
   self.error=None if any(v["state"]=="Данные доступны" for v in self.provider_states.values()) else "Бесплатный источник недоступен"
 def run(self):
  while not self.stop_event.is_set():
   self.poll_once()
   self.stop_event.wait(60)
 def start(self):
  if self.thread and self.thread.is_alive():return
  self.stop_event.clear();self.thread=threading.Thread(target=self.run,daemon=True);self.thread.start()
 def stop(self):self.stop_event.set()
 def state(self):
  with self.lock:
   return {"running":bool(self.thread and self.thread.is_alive() and not self.stop_event.is_set()),"target":self.target,"last_poll":self.last_poll,"error":self.error,"provider":"SportScore","provider_states":dict(self.provider_states),"matches":list(self.matches),"bookmakers":[{"name":n,"state":"Нет подтверждённого источника букмекера","detail":"Общий live-score не подтверждает, что событие есть в линии этой конторы."} for n in BOOKMAKERS]}

m=Monitor()
app=FastAPI(title="Score Delay Monitor")
app.mount("/static",StaticFiles(directory=ROOT/"static"),name="static")
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
 data=await request.json();target=str(data.get("target","")).strip()[:160];m.set_target(target);return {"ok":True,"target":target}
@app.get("/api/export")
def export():
 with sqlite3.connect(DB) as db:rows=db.execute("SELECT sport,match,score,status,observed_at,provider_updated FROM observations ORDER BY id DESC LIMIT 5000").fetchall()
 out=io.StringIO();writer=csv.writer(out);writer.writerow(["sport","match","score","status","observed_at_utc","provider_updated"]);writer.writerows(rows)
 return StreamingResponse(io.BytesIO(out.getvalue().encode("utf-8-sig")),media_type="text/csv",headers={"Content-Disposition":"attachment; filename=score-monitor.csv"})
