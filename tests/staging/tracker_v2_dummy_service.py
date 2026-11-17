"""Disposable TEST-only systemd worker. No trading, Dhan or Telegram."""
import os,sys,time
from pathlib import Path
from src.config import AppConfig,parse_env_dict
from src.engine.broker_handoff import register_producer
from src.engine.config_manager import ConfigChangeTracker,compute_config_version
from src.engine.tracker_credentials import fingerprint

if __name__=="__main__":
    env_path,data_dir=sys.argv[1:3]
    cfg=AppConfig.from_env(env=parse_env_dict(env_path),env_file="")
    assert cfg.trading_mode.value=="TEST"
    assert cfg.dhan_client_id=="STAGINGONLY1234"
    assert os.environ.get("UDITBOT_TRACKER_V2_ENABLE")=="1"
    register_producer(data_dir,cfg.dhan_client_id)
    while (Path(data_dir)/"hold_runtime_ack").exists():
        time.sleep(0.08)
    ConfigChangeTracker(data_dir=data_dir).record_active_running_version(
        compute_config_version(cfg),trading_mode="TEST",
        credential_fingerprint=fingerprint(cfg,data_dir),
    )
    print("TEST_ONLY runtime ACK published, pid",os.getpid(),flush=True)
    while True:
        time.sleep(0.5)
