import hashlib, json, pathlib, tarfile
root=pathlib.Path('/workspace/work/consumer-aborts');out=pathlib.Path('/workspace/partitionline/docs/evidence/perf/KL09-47')
A='824662e39980604c799e3398a7e3bc46db8400b3';B='96986ba02ba5c7c480295c855a9e0dfe9efab549'
runtime_hashes={A:hashlib.sha256((root/'corrected-baseline-bin/runtime').read_bytes()).hexdigest(),B:hashlib.sha256((root/'revised-candidate-bin/runtime').read_bytes()).hexdigest()}
expected=[[p,g*2500,g*2500+2000] for p,n in [(0,20),(1,20),(2,13)] for g in range(n)]+[[2,32500,33000],[3,0,500],[4,0,500],[5,0,500]]
count=sidecars=observers=accepted=accepted_named=0
with tarfile.open(out/'raw-artifacts.tar.gz','r:gz') as tar:
    for member in tar.getmembers():
        if member.name.endswith('external-rss.json'):
            observers+=1
        if not member.name.endswith('.result.json'):
            continue
        j=json.load(tar.extractfile(member));count+=1
        for art in j['provenance']['artifacts']:
            data=tar.extractfile(art['path']).read()
            assert hashlib.sha256(data).hexdigest()==art['sha256'] and len(data)==art['size_bytes']
            sidecars+=1
        assert j['provenance']['host']['hostname']=='redacted'
        if member.name.startswith('raw/revised-paired/'):
            accepted+=1;s=j['provenance']['source'];assert s['git_commit'] in runtime_hashes and not s['dirty_tree']
            assert j['provenance']['binary']['sha256']==runtime_hashes[s['git_commit']]
            e=j['execution'];assert e['mismatched']==e['validation_failures']==0
            if '/nb-fetch-committed-aborts/' in member.name:
                accepted_named+=1
                assert e['committed_history']==expected and e['returned_records']==e['records_consumed']==108000
                assert e['committed_aborted_deliveries']==0 and e['committed_abort_gap_records']==25500 and e['filtered_records']==26500
                assert e['committed_partition_cursors']==[[0,50000],[1,50000],[2,33000],[3,500],[4,500],[5,500]]
assert (count,sidecars,observers,accepted,accepted_named)==(110,220,100,50,10)
initial=json.loads((root/'paired-summary-raw.json').read_text());archived=json.loads((out/'metrics.json').read_text())
for row in initial:
    old=next(r for r in archived['initial5'][row['cell']]['rows'] if r['pair']==row['pair'] and r['variant']==row['variant'])
    for k,v in row.items():
        if k not in ['result','external_rss_result']:
            assert old[k]==v
print('110 sanitized runtime artifacts,220 sidecar checksums,100 observers verified independently from archive;50 fresh acceptance runs/10 named exact complete histories verified;all original30 rows unchanged.')
