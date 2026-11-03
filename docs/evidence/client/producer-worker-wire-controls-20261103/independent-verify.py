from pathlib import Path
import hashlib,json,subprocess,tarfile,tempfile

B=Path('/workspace/work/producer-live-reuse-wire-controls-v3-20261103')
R=Path('/workspace/partitionline')
V=Path('/workspace/work/producer-live-reuse-wire-independent-20261103')

def sha(data): return hashlib.sha256(data).hexdigest()
def read(p): return p.read_bytes()
pins=json.loads(read(B/'pins.json'))
assert sha(read(B/'evidence.tar.gz'))=='1bfa871b70e5165acbc4d85bf6ba31fbf635247af530577bd6472169173c9aee'
assert sha(read(B/'wire-controls.patch'))=='edd7debcc4384c2da241489d9da12c0f057454d99734180f15ecc6a4e4d4b7e4'
assert sha(read(B/'receipt.json'))=='197accbc8b0fb24a6fbbbbd4ec026c8748ee150d2d846b1743c88f9f0dcc6185'
with tarfile.open(B/'evidence.tar.gz','r:gz') as tar:
    members=tar.getmembers()
    expected={'README.md','pins.json','verify.py','wire-controls.patch','static-validation.log','v1-to-v2-tests.diff','v2-to-v3.diff'}
    assert len(members)==len(expected) and {x.name for x in members}==expected
    for x in members:
        assert x.isfile() and not x.issym() and not x.islnk()
        assert tar.extractfile(x).read()==read(B/x.name)
catalog_path=R/'docs/evidence/client/candidate-20261030/v16/fixed-inputs.json'
catalog_bytes=read(catalog_path)
assert sha(catalog_bytes)==pins['fixed_catalog_sha256']
catalog=json.loads(catalog_bytes)
sources={
 'src/producer.rs': Path('/workspace/work/client-v14-deadline-fixture-20261103/after/src/producer.rs'),
 'src/net.rs': Path('/workspace/work/client-v10-wire-v3-thin-20261103/fixed/src/net.rs'),
 'src/producer/prepare.rs': Path('/workspace/work/client-v10-wire-v3-thin-20261103/fixed/src/producer/prepare.rs'),
 'src/producer/prepare_tests.rs': Path('/workspace/work/producer-pending-perf-20261103/v2/src/producer/prepare_tests.rs'),
 'src/protocol/api.rs': R/'src/protocol/api.rs',
}
for name,path in sources.items():
    assert sha(read(path))==catalog[name]==pins['source_sha256'][name],name
for name in ('src/net.rs','src/producer/prepare_tests.rs'):
    assert read(B/'before'/name)==read(sources[name]),name
for name,digest in pins['after_sha256'].items():
    assert sha(read(B/'after'/name))==digest,name
assert read(B/'after/src/producer/prepare_tests.rs').startswith(read(B/'before/src/producer/prepare_tests.rs'))
# Remove each explicitly test-gated addition and compare all remaining net bytes.
base=read(B/'before/src/net.rs').decode()
net=read(B/'after/src/net.rs').decode()
start=net.index('    /// Pause writes to this real TCP stream after `limit` accepted bytes.\n')
end=net.index('    /// Kafka `client.id` on this connection.\n',start)
net=net[:start]+net[end:]
pieces=['#[cfg(test)]\npub(crate) mod test_gated_tcp;\n','    #[cfg(test)]\n    GatedTcp(test_gated_tcp::GatedTcp),\n']
for method in ('poll_read(cx, buf)','poll_write(cx, buf)','poll_flush(cx)','poll_shutdown(cx)'):
    pieces.append('            #[cfg(test)]\n            ConnIo::GatedTcp(s) => Pin::new(s).'+method+',\n')
for piece in pieces:
    assert net.count(piece)==1
    net=net.replace(piece,'',1)
assert net==base
with tempfile.TemporaryDirectory(dir=V,prefix='roundtrip-') as temp:
    temp=Path(temp)
    for name in ('src/net.rs','src/producer/prepare_tests.rs'):
        path=temp/name
        path.parent.mkdir(parents=True,exist_ok=True)
        path.write_bytes(read(B/'before'/name))
    p=str(B/'wire-controls.patch')
    for flags in (['--check'],[],['--reverse','--check'],['--reverse']):
        subprocess.run(['git','apply',*flags,p],cwd=temp,check=True,capture_output=True)
        if not flags:
            for name in pins['after_sha256']:
                assert read(temp/name)==read(B/'after'/name),name
    for name in ('src/net.rs','src/producer/prepare_tests.rs'):
        assert read(temp/name)==read(B/'before'/name)
    assert not (temp/'src/net/test_gated_tcp.rs').exists()
print(json.dumps({'archive_exact_members':'pass','source_catalog_pins':'pass','old_tests_byte_prefix':'pass','non_test_network_bytes':'unchanged','patch_forward_reverse_byte_exact':'pass','source_hashes':pins['source_sha256'],'after_hashes':pins['after_sha256'],'limitations':['static analysis only','no Rust compile, native execution, network or performance test']},indent=2))
