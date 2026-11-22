"""V17 genuine FastAPI Save & Apply transactions for all 49 native registry fields.

Each field uses a separate synthetic .env, stopped-service test fixture, and
real backend patch transaction. No Dhan, Telegram or systemd call is permitted.
"""
from pathlib import Path
import os
from tempfile import TemporaryDirectory
import unittest
from unittest.mock import patch
from fastapi.testclient import TestClient
from src.setup_app import app, SETUP_CSRF_TOKEN, HTML_TEMPLATE
from src.engine.config_patch import REGISTRY, revision, parse_env_strict
from src.engine.config_manager import ConfigChangeTracker, ServiceOperatingState
from tests.offline_stopped_service_fixture import stopped_service_test

BASE=(
    '# Isolated V17 Save & Apply test; no live account.\n'
    'TRADING_MODE=TEST\nEQUITY_ACTIVE=true\nFNO_ACTIVE=false\n'
    'TG_API_ID=0\nLOG_GROUP_ID=0\nTEST_GROUP_ID=0\n'
    'EQUITY_BUDGET_INR=125000\nMAX_ORDER_AMOUNT_INR=50000\n'
    'FNO_CAPITAL_PER_TRADE=19500\nMAX_TOTAL_EXPOSURE_INR=150000\n'
    'EQUITY_SL_PCT=3\nFNO_SL_PCT=3\nSL_PCT=3\nTARGET_PCT=3\n'
    'TG_API_HASH=FAKE_OLD_HASH\nDHAN_PIN=FAKE_OLD_PIN\nDHAN_TOTP_SECRET=FAKE_OLD_TOTP\n'
    'CUSTOM_UNTOUCHED_SETTING=keep_me\n'
)


def sample(name, spec):
    if name == 'is_live_mode': return 'true'
    if spec.value_type == 'bool': return 'true' if name in ('fno_active','allow_next_month_expiry_fallback') else 'false'
    if spec.value_type == 'mode': return 'LIVE'
    if spec.value_type == 'integer': return '123' if 'id' not in name else '-1001234567890' if name in ('log_group_id','test_group_id') else '12345'
    if spec.value_type == 'number': return '3.5' if 'pct' in name else '52.5'
    if spec.value_type == 'enum': return spec.choices[-1]
    if spec.value_type == 'groups': return '-1001234567890'
    if spec.value_type == 'response': return '-1001234567890'
    if spec.value_type == 'secret': return 'FAKE_V17_SECRETTEXT'
    return 'OfflineV17_TestValue'


class V17RealSetupTransactions(unittest.TestCase):
    @stopped_service_test
    def test_all_49_native_registry_fields_persist_via_real_route(self):
        named={name:spec for name,spec in REGISTRY.items() if name not in ('equity_amo_enabled','fno_amo_enabled')}
        self.assertEqual(len(named),49)
        prior=os.getcwd()
        for name,spec in named.items():
            with self.subTest(name=name):
                with TemporaryDirectory(prefix='v17_setup_') as temp:
                    try:
                        os.chdir(temp)
                        value=sample(name,spec)
                        initial=BASE
                        initial_vals=parse_env_strict(initial)[0]
                        if spec.value_type=='bool' and spec.key not in initial_vals:
                            initial += spec.key + '=' + ('false' if value == 'true' else 'true') + '\n'
                        file=Path('.env');file.write_text(initial,encoding='utf-8');file.chmod(0o600)
                        with patch.object(ConfigChangeTracker,'resolve_service_operating_state',return_value=(ServiceOperatingState.STOPPED_BY_OWNER,'synthetic stopped')):
                            client=TestClient(app)
                            request={'csrf_token':SETUP_CSRF_TOKEN,'expected_revision':revision(initial),'confirm_live':'true',name:value}
                            response=client.post('/setup/save-apply',data=request)
                            self.assertEqual(response.status_code,200,(name,response.text))
                            payload=response.json()
                            self.assertTrue(payload.get('ok'),(name,payload))
                            saved=parse_env_strict(file.read_text())[0]
                            self.assertEqual(saved.get('CUSTOM_UNTOUCHED_SETTING'),'keep_me')
                            from src.engine.config_patch import normalize
                            self.assertEqual(saved[spec.key],normalize(spec,value))
                            self.assertIn(spec.key,payload.get('changed_keys',[]))
                            self.assertFalse(payload.get('restart_performed'))
                            self.assertNotIn('FAKE_V17_SECRETTEXT',response.text)
                    finally:
                        os.chdir(prior)

    def test_form_field_set_still_exact(self):
        import re
        form=HTML_TEMPLATE.split('<form method="POST" action="/setup">',1)[1].split('</form>',1)[0]
        names=re.findall(r'<(?:input|select|textarea)\b[^>]*\bname="([^"]+)"',form)
        self.assertEqual(len(names),54)
        self.assertEqual(len(set(names)),54)
        self.assertEqual(set(REGISTRY)-{'equity_amo_enabled','fno_amo_enabled'},set(names)-{'csrf_token','expected_revision','full_setup_form','confirm_live','dhan_access_token'})

class V17SpecialWorkflowRejection(unittest.TestCase):
    @stopped_service_test
    def test_special_metadata_rejection_or_native_workflow(self):
        previous=os.getcwd()
        cases=[
            ('invalid_csrf',{'csrf_token':'bad','expected_revision':revision(BASE),'operator_name':'Safer'},403),
            ('revision_conflict',{'csrf_token':SETUP_CSRF_TOKEN,'expected_revision':'0'*64,'operator_name':'Safer'},409),
            ('live_without_confirmation',{'csrf_token':SETUP_CSRF_TOKEN,'expected_revision':revision(BASE),'is_live_mode':'true'},422),
            ('unknown_name',{'csrf_token':SETUP_CSRF_TOKEN,'expected_revision':revision(BASE),'unsupported_v17_field':'x'},422),
        ]
        for scenario,form,status in cases:
            with self.subTest(scenario=scenario):
                with TemporaryDirectory(prefix='v17_metadata_') as root:
                    try:
                        os.chdir(root)
                        Path('.env').write_text(BASE);Path('.env').chmod(0o600)
                        client=TestClient(app)
                        with patch.object(ConfigChangeTracker,'resolve_service_operating_state',return_value=(ServiceOperatingState.STOPPED_BY_OWNER,'synthetic')):
                            response=client.post('/setup/save-apply',data=form)
                            self.assertEqual(response.status_code,status,(scenario,response.text))
                            self.assertEqual(Path('.env').read_text(),BASE)
                    finally:
                        os.chdir(previous)

    @stopped_service_test
    def test_full_form_checkbox_workflow_uses_native_handler(self):
        previous=os.getcwd()
        with TemporaryDirectory(prefix='v17_fullform_') as root:
            try:
                os.chdir(root)
                Path('.env').write_text(BASE);Path('.env').chmod(0o600)
                with patch.object(ConfigChangeTracker,'resolve_service_operating_state',return_value=(ServiceOperatingState.STOPPED_BY_OWNER,'synthetic')):
                    response=TestClient(app).post('/setup/save-apply',data={
                        'csrf_token':SETUP_CSRF_TOKEN,'expected_revision':revision(BASE),'full_setup_form':'1'})
                    self.assertEqual(response.status_code,200,response.text)
                    self.assertEqual(parse_env_strict(Path('.env').read_text())[0]['EQUITY_ACTIVE'],'false')
            finally:
                os.chdir(previous)

    @stopped_service_test
    def test_token_coordinator_boundary_never_leaks_submitted_token(self):
        previous=os.getcwd()
        with TemporaryDirectory(prefix='v17_token_') as root:
            try:
                os.chdir(root)
                Path('.env').write_text(BASE);Path('.env').chmod(0o600)
                marker='NOT_A_REAL_TOKEN_V17_DO_NOT_STORE'
                with patch.object(ConfigChangeTracker,'resolve_service_operating_state',return_value=(ServiceOperatingState.STOPPED_BY_OWNER,'synthetic')):
                    response=TestClient(app).post('/setup/save-apply',data={
                        'csrf_token':SETUP_CSRF_TOKEN,'expected_revision':revision(BASE),'dhan_access_token':marker})
                    self.assertIn(response.status_code,(200,422),response.text)
                    self.assertNotIn(marker,response.text)
                    self.assertNotIn(marker,Path('.env').read_text())
                    self.assertFalse(Path('data/.dhan_token').exists())
            finally:
                os.chdir(previous)
