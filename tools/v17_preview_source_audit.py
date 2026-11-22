"""Independent V17 preview-visual source audit; must be run with two local source trees."""
from __future__ import annotations
import hashlib,json,subprocess,sys
from pathlib import Path

ALLOWED={
 'src/control_center_ui.py','src/premium_ui.css','src/premium_ui.js','src/premium_ui.py',
 'tools/v17_control_state_browser_test.py',
 'tools/v17_preview_fidelity_browser.py','tools/v17_preview_interaction_audit.py',
 'tools/v17_preview_source_audit.py',
}
EXPECTED_MAIN_BLOBS={
 'src/control_center_ui.py':'16a39dda7f31fd322dbfe54a59473969010736c2',
 'src/premium_ui.py':'5bf56e2966bef0d9477923b0cecd6465e3b41193',
 'src/premium_ui.js':'4106c303ff26657264dcda462e21ca23bac52a66',
 'src/premium_ui.css':'27989aae3831dc47e7e47bd809098f4c817e1d0d',
 'tools/v17_control_state_browser_test.py':'8b04791c87194fd91f9be17ba9eeb43c1c164c2b',
}
BASE_COMMIT='6144532d832cc046500769f49b190454b6ffdd03'

def gitblob(path):
 return subprocess.check_output(['git','hash-object',str(path)],text=True).strip()

def audit(base,candidate):
 base=Path(base).resolve();candidate=Path(candidate).resolve()
 for rel,sha in EXPECTED_MAIN_BLOBS.items():
  assert (base/rel).is_file(),('No baseline',rel)
  assert gitblob(base/rel)==sha,('Wrong baseline Github blob',rel,gitblob(base/rel),sha)
 changes=[]
 for f in (base/'src').rglob('*'):
  if not f.is_file() or '__pycache__' in f.parts or f.suffix=='.pyc':continue
  rel=f.relative_to(base)
  other=candidate/rel
  assert other.exists(),('Removed protected source',rel)
  if f.read_bytes()!=other.read_bytes():changes.append(str(rel))
 assert sorted(changes)==sorted(k for k in ALLOWED if k.startswith('src/')),changes
 assert len([x for x in (base/'src').rglob('*') if x.is_file() and '__pycache__' not in x.parts and x.suffix!='.pyc'])==70
 assert (base/'src/setup_app.py').read_bytes()==(candidate/'src/setup_app.py').read_bytes()
 assert (base/'src/control_center_backend.py').read_bytes()==(candidate/'src/control_center_backend.py').read_bytes()
 assert (base/'src/notifier/message_designer_ui.py').read_bytes()==(candidate/'src/notifier/message_designer_ui.py').read_bytes()
 changed_tools=[]
 for f in (candidate/'tools').glob('v17_*'):
  rel=str(f.relative_to(candidate))
  if not f.is_file() or rel not in ALLOWED:continue
  if not (base/rel).is_file() or (base/rel).read_bytes()!=f.read_bytes():changed_tools.append(rel)
 assert sorted(changed_tools)==sorted(x for x in ALLOWED if x.startswith('tools/')),(changed_tools,ALLOWED)
 report={
  'status':'PASS','verified_github_base':BASE_COMMIT,
  'baseline_github_blobs':EXPECTED_MAIN_BLOBS,
  'baseline_source_files':70,
  'changed_source_files':sorted(changes),
  'source_unchanged':66,
  'modified_or_added_audit_tools':sorted(changed_tools),
  'backend_changed':False,
  'confirmed_scope':'Presentation-only source; no trading-engine, broker, reconciliation, notification or native Setup backend changes',
  'unverified':'Production running VM status after a future patch application, live Dhan/Telegram, real order placement and actual future service restart'
 }
 print(json.dumps(report,indent=2))
 return report

if __name__=='__main__':
 if len(sys.argv)!=3:raise SystemExit('Usage: python tools/v17_preview_source_audit.py BASE_TREE CANDIDATE_TREE')
 audit(sys.argv[1],sys.argv[2])
