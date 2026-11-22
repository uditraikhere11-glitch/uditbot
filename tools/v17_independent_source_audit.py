"""Independent second-pass offline V17 change-policy audit.

Compares a trusted current-main checkout/archive to proposed files without
executing trading code or touching GitHub, Dhan, Telegram, VM or .env.
"""
from pathlib import Path
import ast
import hashlib
import json
import re
import sys

PINNED_MAIN='03094f97efd00f9184e56ed2dae38f7e175ac366'
BASE_GIT_BLOBS={
 'src/control_center_ui.py':'638b1ab533cfbfde1969fe4f39541dbb5d08d9cc',
 'src/premium_ui.py':'4252db4e80bae747904ff7b2f0509963cec96547',
 'src/premium_ui.css':'a4c2a5edca02b768cd4534267e6a718d31e7912f',
 'src/premium_ui.js':'a5bebaa69f87a28d26184b0dbebff382a786a975',
 'src/setup_app.py':'bd416eaa76dfac7c5093ccc2dc164d64eb711139',
 'src/control_center_backend.py':'760fdf965e0d8aee5c8c685bb7b28302096ab940',
}
ALLOW={'src/control_center_ui.py','src/premium_ui.py','src/premium_ui.css','src/premium_ui.js'}
FORBIDDEN_PATHS=('src/broker/','src/engine/','src/notifier/','deploy/')


def githash(content:bytes):
    return hashlib.sha1(b'blob '+str(len(content)).encode()+b'\0'+content).hexdigest()


def audit(baseline:Path,candidate:Path):
    baseline=baseline.resolve();candidate=candidate.resolve()
    for rel,expected in BASE_GIT_BLOBS.items():
        value=githash((baseline/rel).read_bytes())
        assert value==expected,(rel,'not the verified GitHub main blob',value,expected)
    paths=[p.relative_to(baseline).as_posix() for p in (baseline/'src').rglob('*') if p.is_file() and '__pycache__' not in p.parts and not p.name.endswith('.pyc')]
    modifications=[]
    for rel in paths:
        old=(baseline/rel).read_bytes()
        new=(candidate/rel).read_bytes()
        if old != new:
            modifications.append(rel)
            assert rel in ALLOW,('Unauthorized backend modification',rel)
    assert set(modifications)==ALLOW,('Expected source changes not present',modifications)
    assert len(paths)>=60,('Incomplete source checkout',len(paths))
    for rel in modifications:
        assert not rel.startswith(FORBIDDEN_PATHS)
    for name in ('src/control_center_ui.py','src/premium_ui.py'):
        ast.parse((candidate/name).read_text(encoding='utf-8'))
    baseline_css=(baseline/'src/premium_ui.css').read_text()
    new_css=(candidate/'src/premium_ui.css').read_text()
    assert new_css.startswith(baseline_css), 'Existing V9 CSS bytes must remain untouched'
    assert '@media (max-width:700px)' in new_css[len(baseline_css):]
    baseline_js=(baseline/'src/premium_ui.js').read_text()
    new_js=(candidate/'src/premium_ui.js').read_text()
    assert new_js.startswith(baseline_js), 'Existing navigation behavior must be preserved as a prefix'
    assert 'auditbot-v17-appearance' in new_js
    original_control=(baseline/'src/control_center_ui.py').read_text()
    new_control=(candidate/'src/control_center_ui.py').read_text()
    for token in ('id="service-beacon"','id="mode-beacon"',"const verifiedRunning=state==='RUNNING'&&s.runtime_verified===true",
                  "$('service-beacon').classList.toggle('is-active',verifiedRunning)",
                  "$('mode-beacon').classList.toggle('is-active',verifiedRunning&&mode==='LIVE')",
                  '/setup/service-control','/control/status','/control/health',
                  '/control/amo/equity','/control/amo/fno','/control/amo/fno/cancel',
                  'id="start"','id="stop"','id="health"','id="fno-amo"','id="equity-amo"'):
        assert token in original_control and token in new_control,('Native contract changed',token)
    assert 'function requestJSON(' in new_control and 'await refreshStatus();' in new_control
    assert 'v17ShowView()' in new_control
    assert 'id="v17-health-run"' in new_control
    assert "if(current && resultMessage)setText('info',resultMessage)" in new_control
    original_setup=(baseline/'src/setup_app.py').read_bytes()
    assert (candidate/'src/setup_app.py').read_bytes()==original_setup
    native=original_setup.decode()
    form=native.split('<form method="POST" action="/setup">',1)[1].split('</form>',1)[0]
    names=re.findall(r'<(?:input|select|textarea)\b[^>]*\bname="([^"]+)"',form)
    assert len(names)==len(set(names))==54,('Native 54 Setup fields missing',len(names))
    report={'base_main_sha':PINNED_MAIN,'source_file_count':len(paths),
            'modified_source_paths':modifications,
            'unchanged_source_paths':len(paths)-len(modifications),
            'setup_unique_named_controls':len(names),'native_control_paths_unchanged':True,'native_view_states':['dashboard','control','amo','health'],
            'approved_artwork_ssim_gate':'NOT VERIFIED',
            'release_approval':'REJECT pending approved visual/e2e acceptance'}
    return report

if __name__=='__main__':
    if len(sys.argv)!=3:
        raise SystemExit('usage: python tools/v17_independent_source_audit.py /path/to/pinned-main /path/to/candidate')
    result=audit(Path(sys.argv[1]),Path(sys.argv[2]))
    print(json.dumps(result,indent=2))
