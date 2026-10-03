from pathlib import Path
import hashlib,json,re,sys
work=Path('/workspace/work/client-share-assessment');lane=Path(sys.argv[1]);toolchain,features=sys.argv[2:]
rows=[tuple(map(int,m)) for m in re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', (lane/'all-target-tests.log').read_text())]
docs=[tuple(map(int,m)) for m in re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', (lane/'doc-tests.log').read_text())]
assert rows and docs and all(row[1]==0 for row in rows+docs)
assert json.loads((lane/'exit.json').read_text())=={'phase':'complete','exit_code':0}
for name in ['run-qa-lane-checked.sh','verify-existing-source.py']:assert (work/name).read_bytes()==(lane/name).read_bytes()
commands=sorted(lane.glob('*-command.json'));before=sorted(lane.glob('*-source-before.json'));after=sorted(lane.glob('*-source-after.json'));assert len(commands)==9 and len(before)==len(after)==9
for name in commands:assert json.loads(name.read_text())['exit_code']==0
for name in before+after:
    row=json.loads(name.read_text());assert row['source_sha']=='96211da1cc63a5ef7c289e0135b74c00562403d6' and row['verified_git_blobs_and_modes']==44092
report={'schema_version':1,'source_sha':'96211da1cc63a5ef7c289e0135b74c00562403d6','toolchain':toolchain,'features':features,'all_target_counts':{'passed':sum(row[0] for row in rows),'failed':sum(row[1] for row in rows),'ignored':sum(row[2] for row in rows)},'doctest_counts':{'passed':sum(row[0] for row in docs),'failed':sum(row[1] for row in docs),'ignored':sum(row[2] for row in docs)},'command_receipts':len(commands),'source_before_after_receipts':len(before)+len(after),'limits':'Five live tests intentionally ignored by all-target unit/socket lanes; pinned live proof and three-release cells are separate. Only two required standalone examples built; all example harnesses compiled by all-targets.'}
(lane/'lane-receipt.json').write_text(json.dumps(report,indent=2)+'\n');print(json.dumps(report,indent=2))
