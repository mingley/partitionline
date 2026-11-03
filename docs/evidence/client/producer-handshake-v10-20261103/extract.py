#!/usr/bin/env python3
"""Extract exact v10 mock failure evidence and a summarized wire timeline."""
from pathlib import Path
import argparse
import hashlib
import json
import re
import tarfile

parser=argparse.ArgumentParser(description=__doc__)
parser.add_argument('report_dir',type=Path,help='directory containing manifest.json and report-objects.tar.gz')
parser.add_argument('output_dir',type=Path,help='new output directory; must not exist')
args=parser.parse_args()
src=args.report_dir
dst=args.output_dir
dst.mkdir(exist_ok=False)
manifest=json.loads((src/'manifest.json').read_text())
archive=src/'report-objects.tar.gz'
assert hashlib.sha256(archive.read_bytes()).hexdigest()==manifest['object_archive_sha256']
names=['default-mock_produce.log','all-features-mock_produce.log',
       'default-producer_retained_backing.log','all-features-producer_retained_backing.log']
with tarfile.open(archive,'r:gz') as tar:
    objects={}
    for name in names:
        row=manifest['members'][name]
        data=tar.extractfile(row['sha256']).read()
        assert len(data)==row['bytes'] and hashlib.sha256(data).hexdigest()==row['sha256']
        (dst/name).write_bytes(data)
        objects[name]=data

profiles={}
event_re=re.compile(r'\((\d+(?:\.\d+)?)(µs|ms|s), (\d+), (\d+), '
                    r'(Accept|Close|Request \{ api_key: (\d+), api_version: (\d+) \}|'
                    r'Response \{ api_key: (\d+), api_version: (\d+) \}|'
                    r'ApiVersionsResponse)')
for profile in ('default','all-features'):
    mock=objects[profile+'-mock_produce.log'].decode()
    sections=re.findall(r'^---- (.+?) stdout ----$',mock,re.MULTILINE)
    assert len(sections)==15,sections
    line=next(x for x in mock.splitlines() if x.startswith('send_all replacement timeline'))
    events=[]
    for m in event_re.finditer(line):
        amount=float(m.group(1))
        micros=amount*({'µs':1,'ms':1000,'s':1000000}[m.group(2)])
        kind=m.group(5)
        row={'time_us':round(micros,3),'node':int(m.group(3)),'socket':int(m.group(4))}
        if kind.startswith('Request'):
            row.update(kind='request',api_key=int(m.group(6)),api_version=int(m.group(7)))
        elif kind.startswith('Response'):
            row.update(kind='response',api_key=int(m.group(8)),api_version=int(m.group(9)))
        else:
            row.update(kind=kind)
        events.append(row)
    assert any(e.get('api_key')==3 and e['kind']=='response' for e in events)
    assert len(events) >= 20,len(events)
    retained=objects[profile+'-producer_retained_backing.log'].decode()
    profiles[profile]={'failure_names':sections,'events':events,
                       'retained_timeout':'Error: Timeout' in retained,
                       'mock_log_sha256':hashlib.sha256(objects[profile+'-mock_produce.log']).hexdigest(),
                       'retained_log_sha256':hashlib.sha256(objects[profile+'-producer_retained_backing.log']).hexdigest()}
assert set(profiles['default']['failure_names'])==set(profiles['all-features']['failure_names'])
result={'checkout':manifest['checkout_commit'],'run':manifest['github_run'],'job':manifest['github_job'],
        'artifact_sha256':manifest['original_zip_sha256'],'source_spec':'3286ffa57a68cef353b2a09728924f6cc3710e634a2f98954246aa44ad94b2a5',
        'description':'Event projection; full unedited logs retained alongside this file.',
        'profiles':profiles}
(dst/'analysis.json').write_text(json.dumps(result,indent=2)+'\n')
for profile,row in profiles.items():
    print(profile,'failures',len(row['failure_names']),'retained timeout',row['retained_timeout'],
          'events',len(row['events']))
    for e in row['events']:
        if e.get('api_key') in (0,3,18) or e['kind'] in ('Accept','Close'):
            print(e)
