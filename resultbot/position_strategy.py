#!/usr/bin/env python3
"""ResultBot standalone TEST-only strategy configuration: NO broker integration."""
import json, math, os, sys
from copy import deepcopy
from pathlib import Path

FILE = Path.home()/"resultbot-data"/"position_strategy.json"
DEFAULT = {
    "down_mode":"CUT",             # CUT, AVERAGE, STOP_ONLY
    "up_mode":"TAKE_PROFIT",       # TAKE_PROFIT, ADD_UP, TARGET_ONLY
    "cut_down":[[3,50],[5,75]],    # drop%, cumulative original quantity sold%
    "average_down":[3,6],         # drop% for each buy
    "take_profit":[[2,50],[4,75]], # rise%, cumulative original quantity sold%
    "add_up":[2,4],                # rise% for each buy
    "stop_pct":9, "target_pct":5, # absolute first-fill anchors
    "initial_budget_inr":500, "extra_budget_inr":500,
    "total_buy_cap_inr":2500, "execution_enabled":False
}

def valid(d):
    if d["down_mode"] not in ("CUT","AVERAGE","STOP_ONLY") or d["up_mode"] not in ("TAKE_PROFIT","ADD_UP","TARGET_ONLY"):
        raise ValueError("Unknown position-management mode")
    for k in ("stop_pct","target_pct","initial_budget_inr","extra_budget_inr","total_buy_cap_inr"):
        if not isinstance(d[k],(int,float)) or isinstance(d[k],bool) or not math.isfinite(d[k]) or d[k]<=0:
            raise ValueError(k+" must be a positive finite number")
    if d["stop_pct"]>=50 or d["target_pct"]>100 or d["initial_budget_inr"]>d["total_buy_cap_inr"]:
        raise ValueError("Invalid price exits or budget cap")
    for key in ("average_down","add_up"):
        xs=d[key]
        if len(xs)>5 or any(not isinstance(x,(int,float)) or not 0<x<100 for x in xs) or sorted(set(xs))!=xs:
            raise ValueError(key+": specify 0-5 ascending distinct percentages")
    for key in ("cut_down","take_profit"):
        xs=d[key]
        if len(xs)>5 or any(not isinstance(x,list) or len(x)!=2 for x in xs):
            raise ValueError(key+": specify up to 5 trigger:cumulative-exit pairs")
        if not all(0<x[0]<100 and 0<x[1]<100 for x in xs):
            raise ValueError(key+": triggers and exit fractions must be in 0..100")
        if [x[0] for x in xs]!=sorted(set(x[0] for x in xs)) or [x[1] for x in xs]!=sorted(set(x[1] for x in xs)):
            raise ValueError(key+": triggers and cumulative sold percentages must increase")
    down = d["average_down"] if d["down_mode"]=="AVERAGE" else [p for p,q in d["cut_down"]] if d["down_mode"]=="CUT" else []
    up = d["add_up"] if d["up_mode"]=="ADD_UP" else [p for p,q in d["take_profit"]] if d["up_mode"]=="TAKE_PROFIT" else []
    if down and max(down)>=d["stop_pct"]: raise ValueError("Final stop must be below all active downside triggers")
    if up and max(up)>=d["target_pct"]: raise ValueError("Final target must be above all active upside triggers")
    buy_steps = (len(d["average_down"]) if d["down_mode"]=="AVERAGE" else 0)+(len(d["add_up"]) if d["up_mode"]=="ADD_UP" else 0)
    if d["initial_budget_inr"]+buy_steps*d["extra_budget_inr"]>d["total_buy_cap_inr"]:
        raise ValueError("Total buy cap cannot cover all configured additional buys")

def load():
    d=deepcopy(DEFAULT)
    if FILE.exists():
        saved=json.loads(FILE.read_text())
        d.update({k:v for k,v in saved.items() if k in DEFAULT and k!="execution_enabled"})
    d["execution_enabled"]=False
    valid(d)
    return d

def ask(label, default):
    s=input(f"{label} [{default}]: ").strip()
    return s or str(default)

def edits(d):
    d["down_mode"]=ask("DOWN: CUT / AVERAGE / STOP_ONLY",d["down_mode"]).upper()
    d["up_mode"]=ask("UP: TAKE_PROFIT / ADD_UP / TARGET_ONLY",d["up_mode"]).upper()
    d["stop_pct"]=float(ask("Full exit stop % below FIRST fill",d["stop_pct"]))
    d["target_pct"]=float(ask("Full exit target % above FIRST fill",d["target_pct"]))
    for k in ("initial_budget_inr","extra_budget_inr","total_buy_cap_inr"):
        d[k]=float(ask(k.replace('_',' '),d[k]))
    key={"CUT":"cut_down","AVERAGE":"average_down","STOP_ONLY":None}[d["down_mode"]]
    if key: d[key]=read_levels(key,d[key])
    key={"TAKE_PROFIT":"take_profit","ADD_UP":"add_up","TARGET_ONLY":None}[d["up_mode"]]
    if key: d[key]=read_levels(key,d[key])
    d["execution_enabled"]=False
    valid(d)
    FILE.parent.mkdir(parents=True,exist_ok=True)
    FILE.parent.chmod(0o700)
    tmp=FILE.with_suffix('.tmp')
    tmp.write_text(json.dumps(d,indent=2)+'\n')
    tmp.chmod(0o600); tmp.replace(FILE)
    print("SAVED:",FILE,"(TEST CONFIG ONLY; NO ORDERS ENABLED)")
    return d

def read_levels(key,default):
    partial = key in ("cut_down","take_profit")
    old=','.join(f'{x}:{y}' for x,y in default) if partial else ','.join(map(str,default))
    s=ask(key+(' drop/rise%:cumulative original qty sold%, comma separated' if partial else ' trigger percentages, comma separated')+' (none for zero)',old or 'none')
    if s.lower() in ('none','off','0'): return []
    if partial: return [[float(a),float(b)] for a,b in (v.split(':') for v in s.split(','))]
    return [float(v) for v in s.split(',')]

def show(d,first):
    valid(d)
    print(f'\nRESULTBOT / EQUITY / EXCELLENT / TEST ONLY | First filled price ₹{first:.2f}')
    print('DOWN:',d['down_mode'],'| UP:',d['up_mode'])
    if d['down_mode']=='CUT':
        for p,q in d['cut_down']: print(f'  DROP -{p:g}% ₹{first*(1-p/100):.2f}: CUT to {q:g}% cumulative original shares sold')
    if d['down_mode']=='AVERAGE':
        for p in d['average_down']: print(f'  DROP -{p:g}% ₹{first*(1-p/100):.2f}: BUY extra up to ₹{d["extra_budget_inr"]:g}')
    if d['up_mode']=='TAKE_PROFIT':
        for p,q in d['take_profit']: print(f'  RISE +{p:g}% ₹{first*(1+p/100):.2f}: BOOK to {q:g}% cumulative original shares sold')
    if d['up_mode']=='ADD_UP':
        for p in d['add_up']: print(f'  RISE +{p:g}% ₹{first*(1+p/100):.2f}: BUY extra up to ₹{d["extra_budget_inr"]:g}')
    print(f'  STOP -{d["stop_pct"]:g}% ₹{first*(1-d["stop_pct"]/100):.2f}: close ALL remaining')
    print(f'  TARGET +{d["target_pct"]:g}% ₹{first*(1+d["target_pct"]/100):.2f}: close ALL remaining')
    print(f'Maximum total planned buying ₹{d["total_buy_cap_inr"]:g}')
    print('Cumulative exits use ORIGINAL filled share count; never % of remaining shares.')
    print('No live orders, no broker API calls, no Dhan services changed.')

def test():
    c=deepcopy(DEFAULT); valid(c)
    for down in ('CUT','AVERAGE','STOP_ONLY'):
        for up in ('TAKE_PROFIT','ADD_UP','TARGET_ONLY'):
            d=deepcopy(c); d['down_mode']=down; d['up_mode']=up; valid(d)
    for key in ('cut_down','take_profit'):
        d=deepcopy(c);d[key]=[[3,75],[4,50]]
        try:valid(d)
        except ValueError: pass
        else:raise AssertionError('Non-increasing cumulative exits accepted')
    print('PASS: 9 mode combinations + safety validation')

if __name__=='__main__':
    command=sys.argv[1] if len(sys.argv)>1 else 'show'
    if command=='self-test':test()
    elif command in ('show','configure'):
        d=load()
        if command=='configure':d=edits(d)
        first=float(sys.argv[2]) if len(sys.argv)>2 and command=='show' else 100
        if not math.isfinite(first) or first<=0:raise SystemExit('First fill must be positive')
        show(d,first)
    else:raise SystemExit('Usage: resultbot strategy [show [first_fill_price] | configure | self-test]')
