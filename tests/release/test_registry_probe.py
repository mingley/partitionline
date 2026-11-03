"""Offline fail-closed checks for the crates.io API/index probe."""
import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[2]
CURL = '''#!/usr/bin/python3
import json, os, pathlib, sys
args = sys.argv[1:]
with open(os.environ['PROBE_LOG'], 'a') as f:
    f.write(json.dumps(args)+'\\n')
api = '/api/v1/crates/' in args[-1]
code = os.environ['PROBE_API_CODE' if api else 'PROBE_INDEX_CODE']
body = os.environ['PROBE_API_BODY' if api else 'PROBE_INDEX_BODY']
pathlib.Path(args[args.index('-o')+1]).write_text(body)
print(code, end='')
if os.environ['PROBE_API_FAIL' if api else 'PROBE_INDEX_FAIL'] == '1':
    sys.exit(18)
'''


def row(version, yanked=False, name='partitionline'):
    return json.dumps({'name':name,'vers':version,'yanked':yanked})+'\n'


class RegistryProbe(unittest.TestCase):
    def probe(self, api='404', index='200', body=None, api_fail=False, index_fail=False):
        with tempfile.TemporaryDirectory(prefix='pl-probe-') as temporary:
            folder=Path(temporary)
            curl=folder/'curl'
            curl.write_text(CURL)
            curl.chmod(0o755)
            env={'PATH':str(folder)+':/usr/bin:/bin', 'TMPDIR':str(folder),
                 'PROBE_LOG':str(folder/'calls'), 'PROBE_API_CODE':api,
                 'PROBE_INDEX_CODE':index, 'PROBE_API_BODY':'{}\n',
                 'PROBE_INDEX_BODY':row('0.1.0') if body is None else body,
                 'PROBE_API_FAIL':str(int(api_fail)),
                 'PROBE_INDEX_FAIL':str(int(index_fail))}
            command='source scripts/lib/crates-io.sh; pl_crates_probe_version partitionline 0.2.0 fixture; printf "%s\\n" "$PL_CRATES_PROBE_STATUS"'
            result=subprocess.run(['bash','-euo','pipefail','-c',command],cwd=ROOT,
                                  env=env,capture_output=True,text=True,timeout=10)
            self.assertEqual(result.returncode,0,result.stderr)
            calls=[json.loads(line) for line in (folder/'calls').read_text().splitlines()]
            for args in calls:
                self.assertIn('--connect-timeout',args)
                self.assertIn('--max-time',args)
            return result.stdout.strip()

    def test_actual_candidate_presence_and_absence(self):
        self.assertEqual(self.probe(body=row('0.1.0')), 'absent')
        self.assertEqual(self.probe(body=row('0.1.0')+row('0.2.0')), 'present')
        self.assertEqual(self.probe(api='503',body=row('0.2.0')), 'present')
        self.assertEqual(self.probe(api='200',body=row('0.1.0')), 'unknown')
        self.assertEqual(self.probe(api='404',index='404'), 'absent')

    def test_yanked_duplicate_or_invalid_index_is_unknown(self):
        bodies=[row('0.2.0',yanked=True), row('0.2.0')+row('0.2.0',yanked=True),
                row('0.1.0')+row('0.1.0',yanked=True),
                '', '{invalid json\n', row('0.1.0',name='other'),
                json.dumps({'vers':'0.1.0','yanked':False})+'\n',
                json.dumps({'name':'partitionline','vers':'0.1.0','yanked':'false'})+'\n',
                row('not-a-version'), row('0.01.0'), row('0.1.0-01'),
                '{"name":"partitionline","vers":"0.1.0","vers":"0.2.0","yanked":false}\n']
        for body in bodies:
            with self.subTest(body=body):
                self.assertEqual(self.probe(body=body), 'unknown')
        self.assertEqual(self.probe(body=row('0.1.0-alpha.1')+row('0.1.0+build.1')), 'absent')

    def test_non_json_constants_cannot_prove_presence_or_absence(self):
        for constant in ['NaN', 'Infinity', '-Infinity']:
            for version in ['0.1.0', '0.2.0']:
                with self.subTest(constant=constant, version=version):
                    body = row(version).rstrip()[:-1]+',"invalid":'+constant+'}\n'
                    self.assertEqual(self.probe(body=body), 'unknown')

    def test_status_errors_and_partial_transfers_cannot_mean_absent(self):
        for status in ['302','401','403','429','500','503','000']:
            with self.subTest(index=status):
                self.assertEqual(self.probe(index=status), 'unknown')
            with self.subTest(api=status):
                self.assertEqual(self.probe(api=status,index='404'), 'unknown')
        self.assertEqual(self.probe(api='404',index='404',api_fail=True),'unknown')
        self.assertEqual(self.probe(api='404',index='200',index_fail=True),'unknown')


if __name__ == '__main__':
    unittest.main()
