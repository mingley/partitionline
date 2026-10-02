"""Reject silent semantic changes and retain all attempts (no broker required)."""
import importlib.util
import os
from pathlib import Path
import unittest
from unittest.mock import patch

spec=importlib.util.spec_from_file_location('cpeer',Path(__file__).with_name('run.py'))
peer=importlib.util.module_from_spec(spec); spec.loader.exec_module(peer)

class ConfigTests(unittest.TestCase):
    def test_idempotence_requires_explicit_acks_and_flight(self):
        for env in ({'IDEMPOTENT':'1'},{'IDEMPOTENT':'1','ACKS':'-1','MAX_IN_FLIGHT':'6'}):
            with patch.dict(os.environ,env,clear=True),self.assertRaises(ValueError): peer.settings()
        with patch.dict(os.environ,{'IDEMPOTENT':'1','ACKS':'-1'},clear=True):
            self.assertTrue(peer.settings()['idempotence'])

    def test_security_does_not_silently_drop_credentials(self):
        with patch.dict(os.environ,{'SASL_USERNAME':'test','SASL_PASSWORD':'secret'},clear=True),self.assertRaises(ValueError): peer.settings()
        with patch.dict(os.environ,{'SASL_MECHANISM':'PLAIN','SASL_USERNAME':'test','SASL_PASSWORD':'secret','TLS_CA_PEM':'ca.pem'},clear=True):
            config=peer.settings()
            self.assertEqual(config['security_protocol'],'SASL_SSL')
            self.assertNotIn('secret',str(config))
            self.assertNotIn('test',str(config))

    def test_unsupported_features_fail_closed(self):
        for env in ({'TLS_SERVER_NAME':'override'}, {'SASL_MECHANISM':'OAUTHBEARER'}, {'KEY_MODE':'none'}, {'SECURITY_PROTOCOL':'SSL'}):
            with patch.dict(os.environ,env,clear=True),self.assertRaises(ValueError): peer.settings()

    def test_effective_library_settings_override_requested_values(self):
        with patch.dict(os.environ,{},clear=True): c=peer.settings()
        actual={'request.required.acks':'-1','queue.buffering.max.ms':'7.5','batch.size':'65536',
          'batch.num.messages':'1024','max.in.flight.requests.per.connection':'3','queue.buffering.max.messages':'2048',
          'queue.buffering.max.kbytes':'1024','message.timeout.ms':'2000','enable.idempotence':'true','security.protocol':'sasl_ssl'}
        result=peer.effective(c,actual)
        self.assertEqual((result['acks'],result['linger_ms'],result['max_in_flight']),(-1,7.5,3))
        self.assertEqual(result['security_protocol'],'SASL_SSL')

    def test_library_path_cannot_shadow_hashed_peer(self):
        with patch.dict(os.environ,{'LD_PRELOAD':'other.so','LD_LIBRARY_PATH':'other'},clear=True):
            e=peer.process_env(peer.settings())
            self.assertNotIn('LD_PRELOAD',e); self.assertNotIn('LD_LIBRARY_PATH',e)

    def test_write_retains_previous_failure(self):
        import tempfile
        with tempfile.TemporaryDirectory() as directory:
            path=Path(directory)/'attempt.json'
            peer.write(path,{'failed':True})
            with self.assertRaises(FileExistsError): peer.write(path,{'failed':False})
            self.assertIn('true',path.read_text())

if __name__=='__main__': unittest.main()
