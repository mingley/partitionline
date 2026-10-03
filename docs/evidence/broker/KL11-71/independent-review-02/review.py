import hashlib,json,os,stat
from pathlib import Path
ROOT=Path('/workspace/work/broker-sasl-reauth-71'); OUT=ROOT/'independent-review-02'
PINS={'source-01':'cb925db28cb2717ece2aaeb8e51225b976a15efe09d479ae41e023ef29bbc74b','source-02':'b2fff7b8d0491a70f94a5df5dca9bd8242cde1613eef89df1d8e74d2f2dd3d97','source-03':'c00a626c99721a2351dfaf4c722e11ad3e763707703bc3c08376301e699fcad5'}
def row(p):
 p=Path(p); s=p.lstat(); assert stat.S_ISREG(s.st_mode) and not p.is_symlink()
 b=p.read_bytes(); return {'path':str(p),'bytes':len(b),'sha256':hashlib.sha256(b).hexdigest(),'full_07777':s.st_mode&0o7777}
paths={}; packets={}
for name,sha in PINS.items():
 p=ROOT/name/'handoff.json'; r=row(p); assert r['sha256']==sha; paths[str(p)]=r
 d=json.loads(p.read_bytes()); packets[name]=d
 for entry in d['files']:
  for label in ('candidate','original_copy'):
   if label not in entry: continue
   e=entry[label]; r=row(e['path']); assert (r['bytes'],r['sha256'],r['full_07777'])==(e['bytes'],e['sha256'],e['mode'])
   paths[r['path']]=r
old_review=ROOT/'independent-review-01/validation.json'; r=row(old_review); assert r['sha256']=='dcd8c8b33152f88f2d8fce6a57e241580cf21ffeb9fc818fe1c0978df852a3aa'; paths[str(old_review)]=r
files={e['repo_path']:Path(e['candidate']['path']) for e in packets['source-03']['files']}
t=files['partitionline-broker/src/transport.rs'].read_text(); test=files['partitionline-broker/tests/sasl_sessions.rs'].read_text(); session=files['partitionline-broker/src/security/session.rs'].read_text(); docs=files['docs/sasl-reauthentication.md'].read_text()
exchange=t[t.index('async fn exchange<'):t.index('#[cfg(all(test, feature = "sasl"))]')]
read=exchange[exchange.index('let request = match read.await'):exchange.index('let operation = async')]
admit=exchange[exchange.index('let permit = handlers.acquire()'):exchange.index('let response = handler')]
ready=exchange[exchange.index('let response = match operation.await'):exchange.index('if response.len()')]
write=exchange[exchange.index('let write = async'):exchange.index('if let Err(exit) = write.await')]
reg=test[test.index('#[derive(Default)]\nstruct ReadyAfterExpiryProbe'):]
checks={
 'ready_read_clock_rechecked': 'if Instant::now() >= read_deadline' in read and 'return Exit::ReadDeadline' in read,
 'original_received_instant_before_handler_admission': exchange.index('let received = Instant::now()') < exchange.index('handlers.acquire()') and '.request_deadline(&request, received)' in exchange,
 'post_permit_fence_precedes_authentication': admit.index('if Instant::now() >= deadline') < admit.index('auth\n                        .handle') and 'return Err(Exit::HandlerDeadline)' in admit,
 'ready_handler_clock_precedes_response_or_none': ready.index('if Instant::now() >= deadline') < ready.index('match response') and 'None => continue' in ready,
 'write_bound_retains_earlier_and_new_auth_deadlines': '(Some(a), Some(b)) => Some(a.min(b))' in exchange and 'operation_deadline(write_authority, config.write_timeout)' in exchange,
 'three_write_clock_fences': write.count('if Instant::now() >= deadline')==3,
 'prefix_io_maps_to_exit_io': '.write_all(&length.to_be_bytes())\n                    .await\n                    .map_err(|_| Exit::Io)?' in write,
 'body_io_maps_to_exit_io': 'socket.write_all(&response).await.map_err(|_| Exit::Io)?;' in write,
 'write_timeout_maps_to_write_deadline': '.map_err(|_| Exit::WriteDeadline)?' in write,
 'write_success_is_unit': 'Ok(())' in write,
 'final_identity_commit_after_successful_write': exchange.index('if let Err(exit) = write.await') < exchange.index('auth.response_written()'),
 'renewal_identity_hidden': 'if self.renewal.is_some() {\n            return None;' in session,
 'mechanism_exactly_continuous': 'self.mechanism != Some(mechanism)' in session,
 'principal_and_authority_continuous': 'identity.name() != renewal.identity.name()' in session and 'identity.authority() != renewal.identity.authority()' in session,
 'previous_and_current_lease_checked_at_commit': 'self.lease().is_some_and(|l| l.failure().is_some())' in session and 'self.lease.as_ref().is_some_and(|l| l.failure().is_some())' in session,
 'legacy_authenticate_zero_cannot_renew': 'if self.renewal.is_some() && version == 0' in session,
 'handshake_one_required_for_renewal': 'if version != 1 || self.session_deadline.is_none()' in session,
 'renewal_budget_uses_original_received_instant': 'expires.min(received + self.profile.0.limits.preauth_timeout)' in session,
 'api18_disallowed_during_renewal': 'if key == 18 {\n            if self.renewal.is_some()' in session,
 'regression_uses_actual_transport': 'renewable_plaintext(' in reg and 'TcpStream::connect(server.local_addr())' in reg,
 'regression_authenticates_before_dispatch': 'scram_lifetime(' in reg and reg.index('scram_lifetime(') < reg.index('send(&mut socket'),
 'regression_uses_100ms_and_synchronous_300ms': 'Duration::from_millis(100)' in reg and 'std::thread::sleep(Duration::from_millis(300))' in reg,
 'nonvacuous_exact_one_handler_entry': 'self.calls.fetch_add(1, Ordering::SeqCst)' in reg and 'assert_eq!(probe.calls.load(Ordering::SeqCst), 1)' in reg,
 'sleep_lint_expectation_local_with_reason': '#[expect(\n        clippy::disallowed_methods,' in reg and 'deliberate synchronous work exercises timeout_at Ready completion after expiry' in reg,
 'regression_requires_connection_joins': 'assert_eq!(report.accepted_connections, report.joined_connections)' in reg,
 'regression_rejects_complete_reply': 'assert!(result.is_err(), "complete expired application reply was delivered")' in reg,
 'physical_kernel_delivery_limit_stated': 'already accepted by the socket cannot be withdrawn' in docs and 'hard real-time transmission guarantee' in docs,
 'uncompiled_18_test_limit_stated': '18 new draft tests have not yet been compiled or run' in docs,
}
print(json.dumps(checks,sort_keys=True))
assert all(checks.values()), [k for k,v in checks.items() if not v]
before=list(paths.values()); after=[row(r['path']) for r in before]; assert before==after
receipt={'schema_version':1,'disposition':'source-review-pass-no-runtime-qualification','handoff':row(ROOT/'source-03/handoff.json'),'supersedes_prior_blocking_review_for_corrected_fences':row(old_review),'candidate_count':6,'inputs_before':before,'inputs_after':after,'all_inputs_unchanged':True,'source_controls':[{'name':k,'passed':v} for k,v in checks.items()],'source_controls_passed':len(checks),'findings':[], 'review_conclusions':['The corrected transport checks the original absolute read/admission/handler bounds after Ready completion, and checks before prefix, between prefix/body and after body writing. Source-level Exit/Result typing is coherent but not compiled.','Renewal mechanism, principal and authority continuity, old/new lease fences and final identity commitment remain in the byte-identical Session source.','The final test includes an actual authenticated handler-entry counter and a narrowly reasoned sleep lint expectation; source-only assertions do not establish timing/runtime outcomes.','Documentation explicitly limits physical delivery guarantees: synchronous work cannot be preempted, kernel-accepted bytes cannot be retracted, and a late partial write fails closed before renewed identity commitment.'], 'commands':['Read-only source inspection of exact source-03 six-file closure, source-02 transport/write fences, byte-identical source-01 session continuity, and final regression/operator docs.','Python exact SHA/bytes/full07777 guards before and after review and 28 named source predicates; no Rust parser/compiler/runtime.'],'actual_compiler_executions':0,'actual_rust_test_executions':0,'actual_runtime_executions':0,'actual_network_actions':0,'limits':['The 18 new draft Rust/socket tests remain uncompiled and unexecuted in this review.','Old/new OAuth lease admission/revocation/cancellation and blocked final-write cases still need actual runtime verification.','Current Java/native C/public Rust renewal histories, stable/MSRV strict checks, resource/Drop/join gates remain necessary.','No hard real-time or kernel-delivery guarantee is inferred.'],'receipt_helper_failure':row(OUT/'helper-attempt-01.failure.txt')}
p=OUT/'validation.json'; p.write_text(json.dumps(receipt,indent=2,sort_keys=True)+'\n'); p.chmod(0o600)
print(json.dumps({'validation':row(p),'inputs':len(before),'controls':len(checks)}))
