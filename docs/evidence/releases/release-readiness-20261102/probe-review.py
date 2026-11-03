#!/usr/bin/env python3
"""Offline controls for frozen release-rehearsal candidate v2.

The curl executable is replaced by a temporary stub; no service or publisher is
contacted. This is a targeted review, not a release qualification.
"""
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile

ROOT = Path(os.environ['REVIEW_SOURCE'])
CURL = r'''#!/usr/bin/env python3
import os, pathlib, sys
a = sys.argv[1:]
url = a[-1]
endpoint = 'INDEX' if url.startswith('https://index.crates.io/') else 'API'
if not (url.startswith('https://index.crates.io/') or url.startswith('https://crates.io/api/v1/crates/')):
    raise SystemExit(97)
with open(os.environ['REVIEW_LOG'], 'a') as f:
    f.write(endpoint+'\n')
code = os.environ[endpoint+'_CODE']
body = os.environ.get(endpoint+'_BODY', '')
if '-o' in a:
    pathlib.Path(a[a.index('-o')+1]).write_text(body)
else:
    print(body, end='')
if '-w' in a:
    print(code, end='')
rc = int(os.environ.get(endpoint+'_RC', '0'))
if rc == 0 and int(code) >= 400 and any(x.startswith('-') and not x.startswith('--') and 'f' in x for x in a):
    rc = 22
raise SystemExit(rc)
'''


def index(name='partitionline', version='0.1.0', yanked=False):
    return json.dumps({'name': name, 'vers': version, 'yanked': yanked})+'\n'


def main():
    cases = [
        ('both-404', '404', '404', '', 0, 0, 'absent'),
        ('index-503', '404', '503', '', 0, 0, 'unknown'),
        ('api-503-index-404', '503', '404', '', 0, 0, 'unknown'),
        ('api-present-index-missing', '200', '200', index(), 0, 0, 'unknown'),
        ('valid-version-absent', '404', '200', index(), 0, 0, 'absent'),
        ('index-present-api-unavailable', '503', '200', index(version='0.2.0'), 0, 0, 'present'),
        ('yanked-target', '200', '200', index(version='0.2.0', yanked=True), 0, 0, 'unknown'),
        ('empty-200-index', '404', '200', '', 0, 0, 'unknown'),
        ('malformed-index', '404', '200', index()+'{invalid\n', 0, 0, 'unknown'),
        ('missing-name', '404', '200', '{"vers":"0.1.0","yanked":false}\n', 0, 0, 'unknown'),
        ('missing-yanked', '404', '200', '{"name":"partitionline","vers":"0.1.0"}\n', 0, 0, 'unknown'),
        ('nonboolean-yanked', '404', '200', '{"name":"partitionline","vers":"0.1.0","yanked":0}\n', 0, 0, 'unknown'),
        ('invalid-other-version', '404', '200', index(version='banana'), 0, 0, 'unknown'),
        ('empty-other-version', '404', '200', index(version=''), 0, 0, 'unknown'),
        ('duplicate-json-keys', '404', '200', '{"name":"other","name":"partitionline","vers":"0.1.0","yanked":false}\n', 0, 0, 'unknown'),
        ('target-with-malformed-other', '404', '200', index(version='0.2.0')+'{invalid\n', 0, 0, 'unknown'),
        ('wrong-crate-absent', '404', '200', index(name='other'), 0, 0, 'unknown'),
        ('wrong-crate-present', '404', '200', index(name='other', version='0.2.0'), 0, 0, 'unknown'),
        ('incomplete-index-body', '404', '200', index(), 0, 18, 'unknown'),
        ('incomplete-api-response', '404', '404', '', 18, 0, 'unknown'),
        ('index-dns-failure', '404', '000', '', 0, 6, 'unknown'),
        ('index-timeout', '404', '000', '', 0, 28, 'unknown'),
        ('api-dns-failure', '000', '404', '', 6, 0, 'unknown'),
        ('duplicate-other-version', '404', '200', index()+index(), 0, 0, 'unknown'),
        ('duplicate-target-version', '404', '200', index(version='0.2.0')+index(version='0.2.0'), 0, 0, 'unknown'),
        ('api-redirect', '302', '404', '', 0, 0, 'unknown'),
        ('index-redirect', '404', '302', '', 0, 0, 'unknown'),
        ('api-forbidden', '403', '404', '', 0, 0, 'unknown'),
        ('api-unauthorized', '401', '404', '', 0, 0, 'unknown'),
        ('index-unauthorized', '404', '401', '', 0, 0, 'unknown'),
        ('index-forbidden', '404', '403', '', 0, 0, 'unknown'),
        ('index-rate-limited', '404', '429', '', 0, 0, 'unknown'),
        ('index-error', '404', '500', '', 0, 0, 'unknown'),
    ]
    with tempfile.TemporaryDirectory(prefix='pl-independent-review-', dir='/workspace/work') as d:
        tmp = Path(d)
        (tmp/'curl').write_text(CURL)
        (tmp/'curl').chmod(0o755)
        log = tmp/'requests.log'
        env = {'PATH':str(tmp)+':/usr/bin:/bin', 'LANG':'C.UTF-8', 'REVIEW_LOG':str(log)}
        rows=[]
        for name, api, idx, body, api_rc, idx_rc, expected in cases:
            current = {**env, 'API_CODE':api, 'INDEX_CODE':idx, 'INDEX_BODY':body,
                       'API_RC':str(api_rc), 'INDEX_RC':str(idx_rc)}
            completed = subprocess.run(['bash','-euo','pipefail','-c',
                'source "$1"; pl_crates_probe_version partitionline 0.2.0 review; printf "%s\\t%s\\n" "$PL_CRATES_PROBE_STATUS" "$PL_CRATES_PROBE_DETAIL"',
                '--',str(ROOT/'scripts/lib/crates-io.sh')], env=current, cwd=ROOT,
                text=True, capture_output=True, timeout=5, check=True)
            actual, detail = completed.stdout.rstrip('\n').split('\t', 1)
            rows.append({'case':name,'expected':expected,'actual':actual,'passed':actual == expected,'detail':detail})

        result={'probe_controls':rows,
                'scope':'Offline fake HTTP only; no Cargo, credentials, GitHub or publication.'}
        print(json.dumps(result,indent=2))
        if any(not row['passed'] for row in rows):
            return 1
        return 0


if __name__ == '__main__':
    raise SystemExit(main())
