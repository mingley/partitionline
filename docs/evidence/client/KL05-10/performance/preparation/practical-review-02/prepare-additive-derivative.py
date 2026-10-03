from pathlib import Path
import hashlib,json,shutil,stat
prior=Path('/workspace/work/client-sticky-performance-practical-497f')
root=Path('/workspace/work/client-sticky-performance-practical-review-02')
stage=prior/'stage-handoff.json'
assert hashlib.sha256(stage.read_bytes()).hexdigest()=='5c94ab5065e12bdc174fdd3abfd3520ee0ed893553a6f301d9719344995f37a1'
manifest=json.loads(stage.read_text())
history=root/'history/frozen-practical-5c94'
assert not history.exists()
for rel,expected in manifest['files'].items():
    source=prior/rel;raw=source.read_bytes()
    assert hashlib.sha256(raw).hexdigest()==expected['sha256'] and len(raw)==expected['bytes'] and stat.S_IMODE(source.stat().st_mode)==expected['full_mode']
    target=history/rel;target.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(source,target)
    assert target.read_bytes()==raw and stat.S_IMODE(target.stat().st_mode)==expected['full_mode']
shutil.copy2(stage,history/'stage-handoff.json')
bench=root/'benchmarks/sticky-partitioner'
for rel in manifest['files']:
    if rel.startswith('benchmarks/sticky-partitioner/'):
        dest=root/rel;dest.parent.mkdir(parents=True,exist_ok=True);shutil.copy2(prior/rel,dest)
(root/'original-preservation.json').write_text(json.dumps({'frozen_stage':str(stage),'frozen_stage_sha256':'5c94ab5065e12bdc174fdd3abfd3520ee0ed893553a6f301d9719344995f37a1','all71_original_payloads_SHA_bytes_full07777_verified':True,'prior_payload_bytes':532957,'copied_history_exact72files_including_manifest':True,'frozen_prior_not_modified':True},indent=2)+'\n')
# Fixed page geometry plus no spill bounds each DELETE transaction's rollback data.
p=bench/'audit.py';s=p.read_text()
insert='''
def pragma_scalar(conn, statement):
    row = conn.execute(statement).fetchone()
    assert row is not None and len(row) == 1, 'SQLite pragma result required'
    return row[0]


def configure_database(conn, qualification):
    page_bytes = 4096
    pages = 4096 if qualification else 524288
    conn.execute('PRAGMA page_size=4096')  # Before the first schema/page is created.
    actual_page_bytes = pragma_scalar(conn, 'PRAGMA page_size')
    assert type(actual_page_bytes) is int and actual_page_bytes == page_bytes
    journal_mode = pragma_scalar(conn, 'PRAGMA journal_mode=DELETE')
    assert journal_mode == 'delete', 'Require the forecasted DELETE journal mode'
    conn.execute('PRAGMA synchronous=FULL')
    synchronous = pragma_scalar(conn, 'PRAGMA synchronous')
    assert type(synchronous) is int and synchronous == 2
    conn.execute('PRAGMA cache_spill=OFF')
    cache_spill = pragma_scalar(conn, 'PRAGMA cache_spill')
    assert type(cache_spill) is int and cache_spill == 0
    # OFF prevents dirty-cache spill/repeated journal headers. The configured cache target
    # is soft: dirty pages may remain up to the database cap until each bounded commit.
    conn.execute('PRAGMA cache_size=-16384')
    actual_pages = pragma_scalar(conn, 'PRAGMA max_page_count=' + str(pages))
    assert type(actual_pages) is int and actual_pages == pages
    assert pragma_scalar(conn, 'PRAGMA max_page_count') == pages
    return {'page_bytes': actual_page_bytes, 'max_page_count': actual_pages,
            'database_byte_cap': actual_page_bytes * actual_pages,
            'journal_mode': journal_mode, 'synchronous': synchronous,
            'cache_spill': cache_spill, 'configured_cache_target_bytes': 16 * 1024**2,
            'dirty_cache_may_exceed_target_up_to_database_byte_cap': True,
            'rollback_journal_allocation_reserve_bytes': (32 * 1024**2 if qualification
                                                        else 2 * 1024**3 + 16 * 1024**2),
            'journal_bound_contract': 'one database/no ATTACH, cache_spill OFF; at most one old image per page plus8-byte page/checksum and at most64KiB header/alignment, rounded allocation reserve'}

'''
assert '\ndef audit(' in s;s=s.replace('\ndef audit(',insert+'\ndef audit(',1)
old='''    conn = sqlite3.connect(database)
    conn.execute('PRAGMA journal_mode=DELETE')
    conn.execute('PRAGMA cache_size=-16384')  # 16MiB bounded SQLite page cache.
    conn.execute('PRAGMA max_page_count=' + str(4096 if qualification else 524288))  # 2GiB hard database cap.
    conn.execute('CREATE TABLE records(id INTEGER PRIMARY KEY,phase INTEGER,partition INTEGER,'
                 'offset INTEGER,hash BLOB,seen INTEGER DEFAULT0,UNIQUE(partition,offset))'.replace('DEFAULT0', 'DEFAULT 0'))
    try:
'''
new='''    conn = sqlite3.connect(database)
    try:
        sqlite_configuration = configure_database(conn, qualification)
        conn.execute('CREATE TABLE records(id INTEGER PRIMARY KEY,phase INTEGER,partition INTEGER,'
                     'offset INTEGER,hash BLOB,seen INTEGER DEFAULT0,UNIQUE(partition,offset))'.replace('DEFAULT0', 'DEFAULT 0'))
'''
assert old in s;s=s.replace(old,new)
old="""        target = out / 'offline-delivery-audit.json'
"""
new="""        assert pragma_scalar(conn, 'PRAGMA page_size') == sqlite_configuration['page_bytes']
        page_count = pragma_scalar(conn, 'PRAGMA page_count')
        assert type(page_count) is int and 0 <= page_count <= sqlite_configuration['max_page_count']
        assert database.stat().st_size <= sqlite_configuration['database_byte_cap']
        rollback = Path(str(database) + '-journal')
        if rollback.exists():
            assert rollback.stat().st_size <= sqlite_configuration['rollback_journal_allocation_reserve_bytes']
        result['SQLite_version'] = sqlite3.sqlite_version
        result['SQLite_configuration'] = sqlite_configuration
        result['SQLite_observed_database'] = {'page_count': page_count,
            'logical_bytes': database.stat().st_size,
            'allocated_bytes': database.stat().st_blocks * 512,
            'rollback_journal_present_after_final_commit': rollback.exists()}
        target = out / 'offline-delivery-audit.json'
"""
assert old in s;s=s.replace(old,new)
p.write_text(s)
# Qualification explicitly includes a simultaneously live rollback journal.
p=bench/'qualification-resource-forecast.json';q=json.loads(p.read_text())
q['components']['SQLite_DELETE_rollback_journal_header_allocation_reserve_bytes']=32*1024**2
q['per_cell_generated_allocation_upper_bound_bytes']+=32*1024**2
q['per_cell_conservative_free_requirement_bytes']+=32*1024**2
q['six_cells_worst_case_accumulated_allocation_bytes']=6*q['per_cell_generated_allocation_upper_bound_bytes']
q['SQLite_contract']={'page_bytes':4096,'database_max_pages':4096,'journal_mode':'DELETE','synchronous':'FULL','cache_spill':'OFF','journal_reserve_bytes':32*1024**2,'dirty_cache':'16MiB configured target is not a hard memory cap; retained dirty pages bounded by16MiB DB plus page/cache overhead'}
q['memory']['SQLite_cache_bytes']=q['memory'].pop('SQLite_cache_bytes')
q['memory']['SQLite_cache_bytes_is_configured_target_not_hard_RAM_cap']=True
q['memory']['SQLite_dirty_page_bytes_upper']=16*1024**2
q['classification']='Unexecuted derivative forecast fixing page geometry and simultaneous rollback journal; frozen5c94 and original ranking forecast preserved'
p.write_text(json.dumps(q,indent=2)+'\n')
# Preserve original8.4GB verbatim while supplying a conservative actual-use ranking bound.
original_forecast=json.loads((bench/'resource-forecast.json').read_text())
rank={'classification':'Unexecuted supplemental ranking forecast; frozen original8.4GB source remains byte-for-byte. No original historical resource claim rewritten.',
      'original_forecast_file':'resource-forecast.json','original_forecast_sha256':hashlib.sha256((bench/'resource-forecast.json').read_bytes()).hexdigest(),
      'original_requirement_bytes':original_forecast['per_cell_conservative_free_requirement_bytes'],
      'SQLite_DELETE_rollback_journal_header_allocation_reserve_bytes':2*1024**3+16*1024**2,
      'per_cell_conservative_free_requirement_bytes':original_forecast['per_cell_conservative_free_requirement_bytes']+2*1024**3+16*1024**2,
      'SQLite_contract':{'page_bytes':4096,'database_max_pages':524288,'journal_mode':'DELETE','synchronous':'FULL','cache_spill':'OFF','configured_cache_target_bytes':16*1024**2,'dirty_page_bytes_upper':2*1024**3},
      'archive_reserve_not_reused_for_rollback_journal':True,
      'limits':'No Cargo/JVM/runtime/ranking lease; original>=60s/>=1M independent delivered/5paired gates unchanged. Any future mutually exclusive journal/archive budget reuse requires separately reviewed enforced lifecycle.',
      'build_or_image_provisioning_included':False}
(bench/'ranking-resource-forecast.json').write_text(json.dumps(rank,indent=2)+'\n')
# Route ranking to honest supplemental bound. Membership correction follows separately.
p=bench/'run-cell.py';s=p.read_text().replace("else 'resource-forecast.json'", "else 'ranking-resource-forecast.json'");p.write_text(s)
print(json.dumps({'original71_untouched':True,'qualification_guard_bytes':q['per_cell_conservative_free_requirement_bytes'],'supplemental_ranking_guard_bytes':rank['per_cell_conservative_free_requirement_bytes'],'SQLite_connections_processes_audit_main':False},indent=2))
