import pathlib,hashlib,json,stat,difflib
base=pathlib.Path('/workspace/work/raft-runtime-76/coverage-proposal-01/candidate')
root=pathlib.Path('/workspace/work/raft-runtime-76/tcp-qualification-02')
files=['partitionline-broker/src/raft/runtime.rs','partitionline-broker/tests/common/raft_runtime.rs']
rows=[]
for name in files:
 src=base/name;b=src.read_bytes();mode=stat.S_IMODE(src.stat().st_mode)
 q=root/'before'/name;q.parent.mkdir(parents=True,exist_ok=True);assert not q.exists();q.write_bytes(b);q.chmod(mode)
 rows.append({'path':name,'origin':str(src),'sha256':hashlib.sha256(b).hexdigest(),'bytes':len(b),'full07777':mode})
s=(base/files[0]).read_text()
old='''            write!(
                out,
                "{{\\\"current\\\":{},\\\"peak\\\":{}}}",
                g.current.load(Ordering::Acquire),
                g.peak.load(Ordering::Acquire)
            )'''
new='''            // A constructor may be preempted between its current increment
            // and peak publication. This observed current value is itself a
            // genuine high-water observation; publish it before emitting both.
            let current = g.current.load(Ordering::Acquire);
            let peak = g.peak.fetch_max(current, Ordering::AcqRel).max(current);
            write!(out, "{{\\\"current\\\":{},\\\"peak\\\":{}}}", current, peak)'''
assert s.count(old)==1;s=s.replace(old,new)
assert s.count('if self.disk_bytes > 128 * 1024 * 1024')==1
s=s.replace('if self.disk_bytes > 128 * 1024 * 1024','if self.disk_bytes > 16 * 1024 * 1024')
q=root/'candidate'/files[0];q.parent.mkdir(parents=True,exist_ok=True);q.write_text(s);q.chmod(rows[0]['full07777'])
s=(base/files[1]).read_text()
s=s.replace('    clock: std::time::Instant,\n}', '    clock: std::time::Instant,\n    captured_bytes: AtomicU64,\n}')
s=s.replace('            clock: std::time::Instant::now(),\n', '            clock: std::time::Instant::now(),\n            captured_bytes: AtomicU64::new(0),\n')
needle='''    fn set(&self, a: usize, b: usize, value: bool) {'''
insert='''    fn capture_file(&self, name: &str, bytes: &[u8]) -> TestResult {
        let Some(root) = &self.capture else { return Ok(()); };
        let size = u64::try_from(bytes.len())?;
        self.captured_bytes.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n|
            n.checked_add(size).filter(|next| *next <= 16 * 1024 * 1024)
        ).map_err(|_| "finite per-case proxy capture bytes")?;
        let mut file = fs::OpenOptions::new().write(true).create_new(true).open(root.join(name))?;
        file.write_all(bytes)?;
        file.sync_all()?;
        Ok(())
    }
    fn set(&self, a: usize, b: usize, value: bool) {'''
assert s.count(needle)==1;s=s.replace(needle,insert)
start=s.index('async fn pipe<');end=s.index('async fn proxy(',start)
pipe='''struct ForwardReceipt {
    source: usize,
    target: usize,
    connection: u64,
    reply: bool,
    ordinal: u64,
    received_ms: u128,
    delay_ms: u64,
    forward_started_ms: Option<u128>,
    forward_finished_ms: Option<u128>,
    write_ok: Option<bool>,
    disposition: &'static str,
}
impl Gate {
    fn forward_receipt(&self, name: &str, bytes: &[u8], r: ForwardReceipt) -> TestResult {
        let rpc = u64::from_be_bytes(bytes[16..24].try_into()?);
        let time = |v: Option<u128>| v.map_or_else(|| "null".into(), |n| n.to_string());
        let ok = r.write_ok.map_or_else(|| "null".into(), |b| b.to_string());
        let metadata = format!("{{\\\"schema_version\\\":2,\\\"packet_file\\\":\\\"{name}\\\",\\\"proxy_pid\\\":{},\\\"clock_basis\\\":\\\"parent proxy process monotonic Instant epoch; not owner process clock\\\",\\\"source\\\":{},\\\"target\\\":{},\\\"connection\\\":{},\\\"reply\\\":{},\\\"ordinal\\\":{},\\\"kind\\\":{},\\\"rpc\\\":{rpc},\\\"received_ms\\\":{},\\\"selected_delay_ms\\\":{},\\\"forward_started_ms\\\":{},\\\"forward_finished_ms\\\":{},\\\"forward_write_ok\\\":{ok},\\\"disposition\\\":\\\"{}\\\"}}}\\n", std::process::id(), r.source, r.target, r.connection, r.reply, r.ordinal, bytes[14], r.received_ms, r.delay_ms, time(r.forward_started_ms), time(r.forward_finished_ms), r.disposition);
        self.capture_file(&format!("{name}.forward.json"), metadata.as_bytes())
    }
}
async fn pipe<R: AsyncRead + Unpin, W: AsyncWrite + Unpin>(
    mut r: R,
    mut w: W,
    gate: Arc<Gate>,
    source: usize,
    target: usize,
    reply: bool,
    connection: u64,
    mut stop: watch::Receiver<bool>,
) -> TestResult {
    let mut ordinal = 0;
    loop {
        let bytes = tokio::select! {_ = stop.changed()=>return Ok(()),p=packet(&mut r)=>p?};
        let received_ms = gate.clock.elapsed().as_millis();
        let name = format!("wire-{source}-{target}-{connection}-{}-{ordinal}.bin", if reply { "response" } else { "request" });
        // Capture actual read bytes before waiting, including a response whose
        // source times out while the proxy is holding it. Forwarding is a later
        // fact; only the owner's ACK command proves response consumption.
        gate.capture_file(&name, &bytes)?;
        let delay = if reply { gate.delay[source * gate.count + target].swap(0, Ordering::AcqRel) } else { 0 };
        let mut receipt = ForwardReceipt { source, target, connection, reply, ordinal,
            received_ms, delay_ms: delay, forward_started_ms: None,
            forward_finished_ms: None, write_ok: None, disposition: "partition-drop" };
        if !gate.permits(source, target) {
            gate.forward_receipt(&name, &bytes, receipt)?;
            return Ok(());
        }
        if delay > 0 {
            tokio::select! {
                _ = stop.changed()=>{
                    receipt.disposition="shutdown-before-forward";
                    gate.forward_receipt(&name, &bytes, receipt)?;
                    return Ok(());
                },
                _ = tokio::time::sleep(Duration::from_millis(delay))=>{}
            }
        }
        if !gate.permits(source, target) {
            gate.forward_receipt(&name, &bytes, receipt)?;
            return Ok(());
        }
        receipt.forward_started_ms = Some(gate.clock.elapsed().as_millis());
        let result = w.write_all(&bytes).await;
        receipt.forward_finished_ms = Some(gate.clock.elapsed().as_millis());
        receipt.write_ok = Some(result.is_ok());
        receipt.disposition = if result.is_ok() { "forward-write-ok" } else { "forward-write-error" };
        gate.forward_receipt(&name, &bytes, receipt)?;
        result?;
        ordinal += 1;
        if ordinal > 10000 { return Err("finite capture count".into()); }
    }
}
'''
s=s[:start]+pipe+s[end:]
old='''                    if let Some(root)=&gate.capture{let mut file=File::create(root.join(format!("hello-{source}-{target}-{id}.bin")))?;file.write_all(&first)?;file.sync_all()?;}
                    let (cr,cw)=client.into_split();let (sr,sw)=server.into_split();
                    tokio::select!{r=pipe(cr,sw,gate.clone(),source,target,false,id,stop.clone())=>r,r=pipe(sr,cw,gate,source,target,true,id,stop)=>r}'''
new='''                    gate.capture_file(&format!("hello-{source}-{target}-{id}.bin"), &first)?;
                    let (cr,cw)=client.into_split();let (sr,sw)=server.into_split();
                    // EOF in one direction must not cancel the other future
                    // while it holds an already-read delayed response. Dropping
                    // its write half shuts down that direction; stop also wakes
                    // both pipes, so proxy shutdown still joins every task.
                    let (request,response)=tokio::join!(pipe(cr,sw,gate.clone(),source,target,false,id,stop.clone()),pipe(sr,cw,gate,source,target,true,id,stop));
                    request.and(response)'''
assert s.count(old)==1;s=s.replace(old,new)
s=s.replace('''            let configured_managed_bytes = cfg.managed_bytes();''','''            let configured_managed_bytes = cfg.managed_bytes();
            let configured_client_slots = 4;
            let configured_socket_bound = 2 * routes.len().saturating_sub(1).max(1);''')
s=s.replace('''                let observed = handle.diagnostics();''','''                let observed = handle.diagnostics();
                assert!(observed.network_tasks <= configured_task_bound);
                assert!(observed.sockets <= configured_socket_bound);
                assert!(observed.transport_bytes <= configured_managed_bytes);
                assert!(observed.client_slots <= configured_client_slots);''')
s=s.replace('''                        assert_eq!(handle.diagnostics().transport_bytes, 0);''','''                        assert_eq!(handle.diagnostics().transport_bytes, 0);
                        assert_eq!(handle.diagnostics().client_slots, 0);''')
s=s.replace('''    assert!(checkpoint.starts_with("image "));
    let base:''','''    assert!(checkpoint.starts_with("image "));
    let image_bytes: u64 = checkpoint.split_whitespace().nth(3).ok_or("image byte count")?.parse()?;
    assert!(image_bytes > 64 * 1024, "real transfer must exceed 64KiB");
    let base:''')
q=root/'candidate'/files[1];q.parent.mkdir(parents=True,exist_ok=True);q.write_text(s);q.chmod(rows[1]['full07777'])
(root/'before-manifest.json').write_text(json.dumps({'schema_version':1,'basis':'earlier uncompiled WORK coverage-proposal-01 candidate, derived from exact ea9; new actual main identity bridge pending root source checkpoint','rows':rows},indent=2)+'\n')
for name in files:
 a=(root/'before'/name).read_text();b=(root/'candidate'/name).read_text()
 p=root/(pathlib.Path(name).name+'.patch');p.write_text(''.join(difflib.unified_diff(a.splitlines(keepends=True),b.splitlines(keepends=True),fromfile='before/'+name,tofile='candidate/'+name)))
print('prepared two WORK files; no Cargo or runtime')
