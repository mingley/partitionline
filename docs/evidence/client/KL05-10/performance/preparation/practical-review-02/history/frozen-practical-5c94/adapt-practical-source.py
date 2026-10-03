from pathlib import Path
import difflib
ROOT = Path(__file__).parent
BENCH = ROOT / 'benchmarks/sticky-partitioner'
ORIGINAL = Path('/workspace/work/client-sticky-performance-source-04f6bc29/benchmarks/sticky-partitioner')
p = BENCH / 'src/main.rs'
s = p.read_text()
s = s.replace('const MAX_RECORDS: u64 = 10_000_000;', 'const MAX_RECORDS: u64 = 10_000_000;\nconst QUALIFICATION_RECORDS: u64 = 24_576;\nconst QUALIFICATION_WARMUP: u64 = 8192;\nconst QUALIFICATION_EXERCISE: u64 = 16_384;')
s = s.replace('fn require_measurement_cpu() -> Outcome<()> {', 'fn require_producer_cpu(qualification: bool) -> Outcome<()> {')
s = s.replace('if !status.lines().any(|line| line == "Cpus_allowed_list:\\t3")', 'let expected = if qualification { "Cpus_allowed_list:\\t0-1" } else { "Cpus_allowed_list:\\t3" };\n    if !status.lines().any(|line| line == expected)')
s = s.replace('"measurement producer requires exclusive taskset CPU3 lease"', '"producer CPU affinity differs from qualification0,1 or ranking3 lease"')
s = s.replace('    seed: u64,\n}', '    seed: u64,\n    qualification: bool,\n}', 1)
s = s.replace('if args.len() != 7 || args[1] != "produce" && args[1] != "verify"', 'if args.len() != 7 || !["produce", "verify", "qualify-produce", "qualify-verify"].contains(&args[1].as_str())')
s = s.replace('sticky-benchmark produce|verify PROFILE BOOTSTRAP TOPIC OUT SEED', 'sticky-benchmark produce|verify|qualify-produce|qualify-verify PROFILE BOOTSTRAP TOPIC OUT SEED')
s = s.replace('            seed: args[6].parse()?,', '            seed: args[6].parse()?,\n            qualification: args[1].starts_with("qualify-"),')
marker = '    fn keyed(&self) -> bool {'
addition = '''    fn max_records(&self) -> u64 {
        if self.qualification { QUALIFICATION_RECORDS } else { MAX_RECORDS }
    }
    fn exercise_minimum(&self) -> u64 {
        if self.qualification { QUALIFICATION_EXERCISE } else { MIN_MEASURE_RECORDS }
    }
'''
s = s.replace(marker, addition + marker)
start = s.index('    let duration = Duration::from_secs(if phase == 0 { 15 } else { 60 });')
end = s.index('    let phase_start = Instant::now();', start)
s = s[:start] + '''    let duration = Duration::from_secs(if settings.qualification { 0 } else if phase == 0 { 15 } else { 60 });
    let minimum = if phase == 0 {
        if settings.qualification { QUALIFICATION_WARMUP } else { 10_000 }
    } else { settings.exercise_minimum() };
''' + s[end:]
s = s.replace('    let phase_start_ns = ns(epoch)?;', '    let deadline = phase_start + Duration::from_secs(if settings.qualification { 90 } else { 300 });\n    let phase_start_ns = ns(epoch)?;')
s = s.replace('if phase_start.elapsed() > Duration::from_secs(300)', 'if Instant::now() >= deadline')
s = s.replace('if *next_id >= MAX_RECORDS', 'if *next_id >= settings.max_records()')
s = s.replace('"10M total record cap reached before duration/minimum; run is unqualified, enlarge forecast separately"', '"bounded record cap reached before required count/time; unqualified, preserve history"')
s = s.replace('        if let Some(completed) = futures.join_next().await {', '        if let Some(completed) = tokio::time::timeout(deadline.saturating_duration_since(Instant::now()), futures.join_next()).await? {')
s = s.replace('    producer.flush_timeout(Duration::from_secs(120)).await?;', '    let remaining = deadline.saturating_duration_since(Instant::now());\n    if remaining.is_zero() { return Err(fail("phase absolute deadline before flush")); }\n    producer.flush_timeout(remaining.min(Duration::from_secs(120))).await?;', 1)
s = s.replace('"acknowledged_records_per_second":acknowledged as f64/seconds,', '"acknowledged_records_per_second":if settings.qualification { None } else { Some(acknowledged as f64/seconds) },\n        "performance_qualified":false,"purpose":if settings.qualification {"qualification"} else {"ranking"},')
s = s.replace('    require_measurement_cpu()?;', '    require_producer_cpu(settings.qualification)?;')
s = s.replace('"profile":settings.profile,"seed":settings.seed,', '"profile":settings.profile,"purpose":if settings.qualification {"qualification"} else {"ranking"},\n        "performance_qualified":false,"seed":settings.seed,', 1)
s = s.replace('"record_window":WINDOW,"max_records":MAX_RECORDS,', '"record_window":WINDOW,"max_records":settings.max_records(),')
s = s.replace('if total > MAX_RECORDS', 'if total > settings.max_records()')
s = s.replace('if phase_counts[1] < MIN_MEASURE_RECORDS', 'if phase_counts[1] < settings.exercise_minimum()')
s = s.replace('if std::env::args().nth(1).as_deref() == Some("produce")', 'if std::env::args().nth(1).is_some_and(|action| action.ends_with("produce"))')
p.write_text(s)
p = BENCH / 'Cargo.toml'
s = p.read_text()
s = s.replace('[dependencies]', '[features]\ndefault = []\ntracing = ["partitionline/tracing"]\n\n[dependencies]')
p.write_text(s)
(ROOT / 'rust-qualification-and-deadline.patch').write_text(''.join(difflib.unified_diff(
    (ORIGINAL/'src/main.rs').read_text().splitlines(True), (BENCH/'src/main.rs').read_text().splitlines(True),
    fromfile='frozen-ebb7/src/main.rs', tofile='practical-497f/src/main.rs')))
print('WORK source adapted; uncompiled')
