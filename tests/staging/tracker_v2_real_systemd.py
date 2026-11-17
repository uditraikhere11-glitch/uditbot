"""Real disposable systemd and authenticated late-ACK recheck acceptance."""
import json,os,subprocess,sys,time
from pathlib import Path
from unittest.mock import patch
from fastapi.testclient import TestClient
from src.engine.config_manager import ConfigChangeTracker
from src.engine.config_patch import revision
from src.engine.tracker_v2_integration import submit_v2_webui_patch
from src.engine.broker_handoff import _paths
from src import webui_password as auth
from src.control_center_entry import app
from src.setup_app import SETUP_CSRF_TOKEN

UNIT="uditbot-tracker-v2-staging.service"
PASSWORD="IsolatedGithubStaging2026!"

def cmd(*args):
    r=subprocess.run(args,capture_output=True,text=True,timeout=25)
    if r.returncode:
        raise AssertionError("TEST command failed: "+" ".join(args)+" "+(r.stderr or r.stdout)[:900])
    return r.stdout

def props():
    raw=cmd("systemctl","show",UNIT,"--property=ActiveState,SubState,Result,MainPID,ExecMainStatus")
    d={}
    for line in raw.splitlines():
        if "=" in line:
            k,v=line.split("=",1)
            d[k]=int(v) if k in ("MainPID","ExecMainStatus") else v
    return d

def require(ok,why):
    if not ok:
        raise AssertionError(why)

def wait_ack(tracker,pid):
    deadline=time.monotonic()+16
    while time.monotonic()<deadline:
        ack=tracker.get_active_running_version(require_live_pid=True)
        if ack and ack.get("pid")==pid:
            return ack
        time.sleep(0.08)
    raise AssertionError("Missing true /proc runtime ACK")

def main():
    stage=Path(sys.argv[1]).resolve()
    data=stage/"data"; env=stage/".env"
    require(os.environ.get("UDITBOT_TRACKER_V2_ENABLE")=="1","Flag missing")
    require(os.environ.get("AUDITBOT_TEST_ISOLATION")!="1","Fake systemd forbidden")
    tracker=ConfigChangeTracker(data_dir=str(data))
    old=props()
    require(old.get("MainPID",0)>0 and old.get("ActiveState")=="active","Real systemd unit not started")
    wait_ack(tracker,old["MainPID"])
    count={"n":0}
    def restart():
        count["n"]+=1
        cmd("sudo","systemctl","restart",UNIT)
        return True
    def save(budget,req):
        with patch.object(ConfigChangeTracker,"evaluate_trading_safety_gate_detailed",
                          return_value=(True,"synthetic broker GET, no network","",0)), \
             patch.object(ConfigChangeTracker,"dispatch_telegram_update"):
            return submit_v2_webui_patch(
                tracker,{"equity_budget_inr":budget},str(env),
                expected_revision=revision(env.read_text()),
                request_id=req,check_service_fn=props,systemctl_restart_fn=restart,
            )
    one=save("750","stage-real-systemd-first-001")
    require(one.runtime_verified and one.state.value=="APPLIED_VERIFIED","First restart not verified: "+one.reason)
    first_new=props()
    require(first_new["MainPID"]!=old["MainPID"] and first_new["ActiveState"]=="active","No PID replacement")
    wait_ack(tracker,first_new["MainPID"])
    require(count["n"]==1,"Duplicate restart")
    require(json.loads(_paths(data)[2].read_text())["active"] is False,"Fence not released")
    hold=data/"hold_runtime_ack";hold.touch()
    two=save("900","stage-real-systemd-late-ack-002")
    require(two.blocked_reason=="ACK_UNCERTAIN" and not two.runtime_verified,"False green on missing ACK")
    require(count["n"]==2,"Unexpected restart call")
    require(json.loads(_paths(data)[2].read_text())["active"] is True,"Fence released without ACK")
    newest=props()
    require(newest["MainPID"]!=first_new["MainPID"],"Second PID unchanged")
    hold.unlink()
    wait_ack(tracker,newest["MainPID"])
    old_cwd=os.getcwd()
    try:
        os.chdir(stage)
        os.environ["UDITBOT_WEBUI_AUTH_DIR"]=str(stage/"auth")
        # FastAPI TestClient is a synthetic peer, permitted ONLY in isolated
        # route tests. All real systemd restart checks above were non-isolated.
        os.environ["AUDITBOT_TEST_ISOLATION"]="1"
        auth.provision(PASSWORD)
        client=TestClient(app,base_url="https://control.example",follow_redirects=False)
        data_req={"csrf_token":SETUP_CSRF_TOKEN,"change_id":two.change_id}
        require(client.post("/control/tracker-v2/recheck",json=data_req).status_code==401,
                "Missing password accepted")
        require(client.post("/login",data={"password":PASSWORD}).status_code==303,
                "Password login failed")
        require(client.post("/control/tracker-v2/recheck",
                json={"csrf_token":"forged","change_id":two.change_id}).status_code==403,
                "Forged CSRF accepted")
        with patch("src.engine.tracker_v2_integration.inspect_systemd_service_properties",
                   side_effect=lambda unit,check_fn=None:props()), \
             patch.object(ConfigChangeTracker,"dispatch_telegram_update"):
            res=client.post("/control/tracker-v2/recheck",json=data_req)
        require(res.status_code==200,"Recovery route failed: "+res.text[:700])
        require(res.json().get("runtime_verified") is True,"Recovered false state")
        require(count["n"]==2,"Recovery reissued restart")
        require(json.loads(_paths(data)[2].read_text())["active"] is False,"Recovery fence retained")
        require(tracker.get_change_record(two.change_id).runtime_verified,"Tracker UI state not persisted")
        print("PASS genuine systemd replacement, delayed ACK, owner password+CSRF, recovery without restart")
    finally:
        os.chdir(old_cwd)
if __name__=="__main__":
    main()
