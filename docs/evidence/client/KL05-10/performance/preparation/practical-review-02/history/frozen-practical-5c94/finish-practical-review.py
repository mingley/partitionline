from pathlib import Path
import hashlib,json,shutil,stat
root=Path('/workspace/work/client-sticky-performance-practical-497f')
bench=root/'benchmarks/sticky-partitioner'
history=root/'history/practical-review-01'
assert not history.exists()
paths=sorted(p for p in bench.rglob('*') if p.is_file())+[root/'offline-compile-plan.json']
entries=[]
for path in paths:
    rel=path.relative_to(root)
    dest=history/rel
    dest.parent.mkdir(parents=True,exist_ok=True)
    shutil.copy2(path,dest)
    before=path.read_bytes();mode=stat.S_IMODE(path.stat().st_mode)
    assert before==dest.read_bytes() and mode==stat.S_IMODE(dest.stat().st_mode)
    entries.append({'path':str(rel),'bytes':len(before),'sha256':hashlib.sha256(before).hexdigest(),'full_mode':mode})
(history/'preservation.json').write_text(json.dumps({'classification':'Source-only prepared practical candidate before failure-path review; no Cargo/JVM/performance commands','paths':entries},indent=2)+'\n')
# Preserve failure metadata above the true floor; the stop margin is available for receipts.
p=bench/'run-cell.py';s=p.read_text()
s=s.replace('CELL_SECONDS = 2400','CELL_SECONDS = 2400\nPROOF_TAIL_SECONDS = 90')
s=s.replace('FLOOR + STOP_MARGIN + len(data)','FLOOR + ((len(data) + 4095) // 4096) * 4096')
s=s.replace("help='Root must grant the future exclusive CPU3/resource lease first'","help='Root must grant the future qualification CPU0,1 or ranking CPU3/resource lease first'")
s=s.replace("assert args.execute, 'PREPARED only; future root CPU3/resource lease required'","assert args.execute, 'PREPARED only; future root mode-specific CPU/resource lease required'")
s=s.replace('deadline = time.monotonic() + (1000 if args.qualification else CELL_SECONDS)','deadline = time.monotonic() + (1000 if args.qualification else CELL_SECONDS)\n    runtime_deadline = deadline - PROOF_TAIL_SECONDS')
s=s.replace("inputs = {str(path): identity(path) for path in (rust, rust_build_path, Path(args.broker_receipt), origin_path, manifest)}","python = Path(shutil.which('python3')).resolve()\n    taskset = Path(shutil.which('taskset')).resolve()\n    inputs = {str(path): identity(path) for path in (rust, rust_build_path, Path(args.broker_receipt), origin_path, manifest, python, taskset)}")
s=s.replace("command = ['taskset', '-c', cpus, *command]","assert time.monotonic() < runtime_deadline, 'Reserve bounded source/input/process-closure proof tail'\n        command = [str(taskset), '-c', cpus, *command]")
s=s.replace("time.monotonic() >= deadline:","time.monotonic() >= runtime_deadline:")
s=s.replace("'physical350MiB floor+16MiB stop margin or whole-cell absolute deadline'","'physical350MiB floor+16MiB stop margin or whole-cell runtime deadline with90s proof tail'")
s=s.replace("min(seconds, deadline-time.monotonic())","min(seconds, runtime_deadline-time.monotonic())")
s=s.replace("'per-command/whole-cell timeout'","'per-command/whole-cell runtime timeout; bounded90s proof tail reserved'")
s=s.replace("'resource_samples': samples, 'owned_process_history': history}","'resource_samples': samples, 'owned_process_history': history,\n              'whole_cell_proof_tail_seconds': PROOF_TAIL_SECONDS}")
s=s.replace("[shutil.which('python3'), str(own_root / 'audit.py')","[str(python), str(own_root / 'audit.py')")
# An exception in output capture must fail/close the owned process, not silently lose its monitor.
old='''            with path.open('xb') as destination:
                while data := pipe.read(65536):
                    remaining = CAPTURE - written
                    destination.write(data[:max(0, remaining)])
                    written += len(data)
                    if written > CAPTURE:
                        reasons.append('captured stream1MiB cap; prefix retained, overflow caused failure')
                        stop(process.pid)
                        return
            pipe.close()'''
new='''            try:
                with path.open('xb') as destination:
                    while data := pipe.read(65536):
                        remaining = CAPTURE - written
                        destination.write(data[:max(0, remaining)])
                        written += len(data)
                        if written > CAPTURE:
                            reasons.append('captured stream1MiB cap; prefix retained, overflow caused failure')
                            stop(process.pid)
                            return
            except OSError as error:
                reasons.append('capture IO failure errno=' + str(error.errno))
                stop(process.pid)
            finally:
                pipe.close()'''
assert old in s;s=s.replace(old,new);p.write_text(s)
# Bound public close in the smaller mode and reject an empty bounded topic.
p=bench/'src/main.rs';s=p.read_text().replace('if args[4].len() > 120','if args[4].is_empty()\n            || args[4].len() > 120')
s=s.replace('producer.close_timeout(Duration::from_secs(120)).await','producer\n                    .close_timeout(Duration::from_secs(if settings.qualification { 30 } else { 120 }))\n                    .await')
p.write_text(s)
# Java receipts must remain valid JSON even for an exception containing control characters.
p=bench/'StickyBenchmark.java';s=p.read_text()
old='''        return "\\\"" + value.replace("\\\\", "\\\\\\\\").replace("\\\"", "\\\\\\\"").replace("\\n", "\\\\n") + "\\\"";'''
new='''        StringBuilder quoted = new StringBuilder("\\\"");
        for (int index=0; index<value.length(); index++) {
            char character=value.charAt(index);
            if (character=='"' || character=='\\\\') quoted.append('\\\\').append(character);
            else if (character<0x20) {
                String hex=Integer.toHexString(character);
                quoted.append("\\\\u");
                for (int padding=hex.length(); padding<4; padding++) quoted.append('0');
                quoted.append(hex);
            } else quoted.append(character);
        }
        return quoted.append('"').toString();'''
assert old in s;s=s.replace(old,new)
s=s.replace('throw new IOException("bounded300s phase timeout")','throw new IOException("bounded"+phaseDeadlineSeconds+"s phase timeout")')
s=s.replace('throw new IOException("10M total record cap before duration/minimum; unqualified run")','throw new IOException(maxRecords+" total record cap before duration/minimum; unqualified run")')
s=s.replace('if (acknowledged!=submitted || acknowledged<minimum || elapsed<duration)','if (acknowledged!=submitted || acknowledged<minimum || elapsed<duration || elapsed>phaseDeadlineSeconds*1_000_000_000L)')
s=s.replace(',\\\"record_window\\\":8192',',\\\"enable_metrics_push\\\":false,\\\"record_window\\\":8192')
old='''                if(consumer.endOffsets(assignment,Duration.ofSeconds(30)).values().stream().anyMatch(x->x!=0L))throw new IOException("fresh empty topic required");'''
new='''                Map<TopicPartition,Long> ends=consumer.endOffsets(assignment,Duration.ofSeconds(30));
                if(ends.size()!=6 || !ends.keySet().containsAll(assignment) || ends.values().stream().anyMatch(x->x!=0L))throw new IOException("fresh empty six-partition topic required");'''
assert old in s;s=s.replace(old,new)
s=s.replace('Run only under a separately granted CPU3/disk/source/image-qualified lease.','Run only under a separately granted qualification CPU0,1 or ranking CPU3 lease.')
p.write_text(s)
# Explicit compile stop margin is not also silently spent on cold graph growth.
p=root/'offline-compile-plan.json';d=json.loads(p.read_text())
d['development_cache_forecast']['minimum_free_before_cold_graph_bytes']+=d['development_cache_forecast']['live_stop_margin_bytes']
d['development_cache_forecast']['minimum_free_before_cold_graph_includes_stop_margin']=True
p.write_text(json.dumps(d,indent=2)+'\n')
p=bench/'qualification-resource-forecast.json';d=json.loads(p.read_text())
d['time']['whole_host_proof_tail_seconds']=90
d['time']['whole_host_runtime_deadline_seconds']=910
p.write_text(json.dumps(d,indent=2)+'\n')
p=bench/'README.md';s=p.read_text()
s=s.replace('1,000 seconds. Source guards and process cleanup remain part of that host budget.','1,000 seconds, reserving its final 90 seconds for bounded source/input guards and\nowned process closure. Small failure receipts may use the stop margin while\nretaining the actual 350 MiB floor. Rust close is 30 seconds in qualification.\nSource guards and process cleanup remain part of that host budget.')
s=s.replace('accepted release ELF, genuine SDK classes/JAR','accepted source-bound ELF (debug allowed only for qualification; release required\nfor ranking), genuine SDK classes/JAR')
s=s.replace('- Rust: source_commit, release=true, driver_source_sha256,','- Rust: source_commit, an actual boolean release (true required for ranking;\n  verified debug allowed only for qualification), driver_source_sha256,')
s=s.replace('  Broker ack is not fsync durability proof.','  Qualification additionally binds completed topic creation, actual post-readiness\n  physical free bytes and no broker/image provisioning inside the cell.\n  Broker ack is not fsync durability proof.')
s += '\nJava metrics push is explicitly disabled to avoid unmatched background telemetry.\nJava metadata snapshots bind partition IDs and leader IDs; the public PartitionInfo\nAPI does not expose the leader epoch. Rust snapshots also bind its public leader\nepoch. Both require exactly six available leaders and a fresh zero-offset topic.\nThe 450 MiB cold development graph forecast plus 40 MiB retained executable/log\nreserve, 16 MiB metadata, 16 MiB stop margin and 350 MiB floor requires\n914,358,272 free bytes before an authorized cold compilation. It is a forecast,\nnot an observed build; actual cache growth can still stop the cell.\n'
p.write_text(s)
print(json.dumps({'classification':'Source-only WORK review; no Cargo/JVM/runtime executed','preserved_pre_review_paths':len(entries),'cold_compile_forecast_bytes':d.get('per_cell_conservative_free_requirement_bytes',620298256)},indent=2))
