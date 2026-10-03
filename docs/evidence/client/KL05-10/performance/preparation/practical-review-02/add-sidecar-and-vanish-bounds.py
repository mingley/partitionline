from pathlib import Path
import json
root=Path('/workspace/work/client-sticky-performance-practical-review-02');bench=root/'benchmarks/sticky-partitioner'
p=bench/'audit.py';s=p.read_text()
s=s.replace("    conn.execute('PRAGMA cache_size=-16384')", "    conn.execute('PRAGMA temp_store=MEMORY')\n    temp_store = pragma_scalar(conn, 'PRAGMA temp_store')\n    assert type(temp_store) is int and temp_store == 2\n    conn.execute('PRAGMA cache_size=-16384')")
s=s.replace("'cache_spill': cache_spill, 'configured_cache_target_bytes'", "'cache_spill': cache_spill, 'temp_store': temp_store, 'configured_cache_target_bytes'")
s=s.replace("            'journal_bound_contract':", "            'statement_journal_allocation_reserve_bytes': (32 * 1024**2 if qualification\n                                                         else 2 * 1024**3 + 16 * 1024**2),\n            'statement_journal_bound_contract': 'conservative separate DB-sized allowance plus overhead; no actual subjournal allocation observed',\n            'journal_bound_contract':")
p.write_text(s)
p=bench/'qualification-resource-forecast.json';q=json.loads(p.read_text());q['components']['SQLite_statement_subjournal_allocation_reserve_bytes']=32*1024**2
q['per_cell_generated_allocation_upper_bound_bytes']+=32*1024**2;q['per_cell_conservative_free_requirement_bytes']+=32*1024**2
q['six_cells_worst_case_accumulated_allocation_bytes']=6*q['per_cell_generated_allocation_upper_bound_bytes']
q['SQLite_contract'].update(temp_store='MEMORY',statement_subjournal_reserve_bytes=32*1024**2,sidecar_forecast='Conservative unobserved normal and statement rollback reserves; cache_spill OFF does not prove every statement/temp-file category absent')
q['memory']['SQLite_dirty_and_statement_image_bytes_upper']=32*1024**2
p.write_text(json.dumps(q,indent=2)+'\n')
p=bench/'ranking-resource-forecast.json';d=json.loads(p.read_text())
d['SQLite_statement_subjournal_allocation_reserve_bytes']=2*1024**3+16*1024**2
d['per_cell_conservative_free_requirement_bytes']+=2*1024**3+16*1024**2
d['SQLite_contract'].update(temp_store='MEMORY',statement_journal_bytes_upper=2*1024**3+16*1024**2,dirty_and_statement_image_bytes_upper=4*1024**3)
p.write_text(json.dumps(d,indent=2)+'\n')
# ENOENT alone can also be a live hidden/reused PID: verify the disappearance.
p=bench/'run-cell.py';s=p.read_text()
old="""        except (FileNotFoundError, ProcessLookupError):
            continue  # Only an independently vanished PID can be absent.
"""
new="""        except (FileNotFoundError, ProcessLookupError) as vanished:
            try:
                os.kill(int(directory.name), 0)  # Existence check only; no signal is delivered.
            except ProcessLookupError:
                continue  # Independently confirmed vanished PID.
            except OSError as error:
                raise MembershipInspectionError('vanished PID unverifiable pid=' + directory.name
                                                + ' errno=' + str(error.errno)) from error
            raise MembershipInspectionError('stat missing for independently live PID=' + directory.name) from vanished
"""
assert old in s;s=s.replace(old,new);p.write_text(s)
print(json.dumps({'qualification_guard_bytes':q['per_cell_conservative_free_requirement_bytes'],'supplemental_ranking_guard_bytes':d['per_cell_conservative_free_requirement_bytes'],'runtime_execution':False},indent=2))
