import gzip
import hashlib
import json
from pathlib import Path
import re
import shutil

repo=Path('/workspace/partitionline');out=repo/'docs/evidence/broker/KL11-37/production/development/jwks-media-01';commands=json.loads((out/'commands.json').read_text())['commands'];checks=json.loads((out/'source-per-command.json').read_text())['checks'];preparation=json.loads((out/'preparation.json').read_text());review=json.loads((out/'independent-source-review.json').read_text());baseline='0b9797ef166a5d067869be1fe988ee21051c86ad'
assert len(commands)==12 and len(checks)==24
primary=[row for row in commands if row['label'] in ['stable-rustc','stable-fmt','stable-oidc-tests','stable-oidc-clippy','1.85.0-rustc','1.85.0-fmt','1.85.0-oidc-tests','1.85.0-oidc-clippy']];assert len(primary)==8
for row in commands:
    log=out/row['log'];assert hashlib.sha256(log.read_bytes()).hexdigest()==row['sha256'];assert row['exit_code']==row['expected_exit']
for row in primary:assert row['exit_code']==0
behavior=[row['test_totals']for row in primary if 'test_totals'in row];assert sum(row['passed']for row in behavior)==194 and all(row[key]==0 for row in behavior for key in ['failed','ignored','measured','filtered'])
controls=[row for row in commands if row not in primary];assert len(controls)==4 and sorted(row['exit_code']for row in controls)==[0,0,101,101]
source=preparation['changed_source']
for row in source:
    actual=(repo/row['path']).read_bytes();assert hashlib.sha256(actual).hexdigest()==row['sha256']and (out/'source'/row['path']).read_bytes()==actual;assert review['reviewed_files'][row['path']]==row['sha256']
socket=json.loads((repo/'docs/evidence/broker/KL11-37/production/socket-checkpoint-source20/manifest.json').read_text())['source'];assert all(hashlib.sha256((repo/row['path']).read_bytes()).hexdigest()==row['sha256']for row in socket)
raw=gzip.decompress((out/'source-before.json.gz').read_bytes());assert hashlib.sha256(raw).hexdigest()==preparation['audit']['original_sha256'];assert hashlib.sha256((out/'source-before.json.gz').read_bytes()).hexdigest()==preparation['audit']['gzip_sha256']
producer=out/'producer';producer.mkdir(exist_ok=True)
for name in ['prepare-jwks-media.py','run-jwks-media-focus.py','retain-jwks-controls.py','seal-jwks-media.py']:
    shutil.copy2('/workspace/work/broker-oidc/'+name,producer/name)
validation=dict(base_source_sha=baseline,candidate_remote_source_sha=None,qualification='source-checkpoint-ready development overlay only; actual pushed candidate and OAuth sockets not qualified',changed_paths=len(source),changed_source=source,primary_commands=8,primary_commands_all_pass=True,unfiltered_behavior={'passed':194,'failed':0,'ignored':0,'filtered':0},controls={'original_baseline_exit':101,'reproduced_baseline_exit':101,'restored_focused_passes':2,'filtered_each':9,'purpose':'actual TLS JWKS media and dual Accept regression plus separate exact-source failing binary retention'},total_runtime_commands=12,total_runtime_expected_exits_match=True,complete_source_files=64610,source_before_after_checks=24,bytes_and_full_modes_preserved=True,source_aggregate_variants=sorted(set(row['aggregate_sha256']for row in checks)),source_aggregate_variants_scope='candidate plus intentional two-file original-HTTP baseline override only',six_socket_files_unchanged=True,source_only_independent_review={'reviewer':review['reviewer'],'blocking_findings':review['blocking_findings'],'runtime_claim':False},original_baseline_binary_limitation=json.loads((out/'bin/manifest.json').read_text())['original_baseline_retention_limitation'],current_candidate_ELF_archive=json.loads((out/'bin/manifest.json').read_text()),reproduced_baseline_ELF=json.loads((out/'bin/reproduced-baseline.json').read_text()),raw_source_audit_lossless_packaging=preparation['audit'],full_KL11_37_status='open: actual-pushed HTTP followup/socket qualification and independent real peers still required')
(out/'validation.json').write_text(json.dumps(validation,indent=2)+'\n')
(out/'README.md').write_text('''This isolated followup changes only the JWKS response media policy. Signing-key requests advertise and accept `application/jwk-set+json` and `application/json`; discovery and introspection continue to advertise and require `application/json`. The registered JWK Set media type is defined by RFC 7517. TLS, trusted origins, response status, redirects, encoding, JSON parsing, resource limits, work admission, deadlines and freshness are unchanged.

The complete source baseline is pushed commit `0b9797ef166a5d067869be1fe988ee21051c86ad`. Exactly four retained source files overlay that immutable Git tree. All 64,610 source files have byte and full-permission checks before and after each of the twelve runtime commands. Six draft OAuth socket files remain excluded and unchanged. This is development evidence prepared for a source checkpoint, not qualification of a yet-unpublished commit or full OIDC listener behavior.

Eight primary commands pass on stable Rust and Rust 1.85: toolchain identification, formatting, selected library/HTTP/public-validation behavior, and strict all-target Clippy on each toolchain. The two unfiltered behavior commands pass 194 cases, with no failures, ignored cases or filtering. The new TLS fixture rejects JWK Set media in discovery and introspection, accepts it for keys while checking the dual Accept header, and validates a lease through ordinary JSON introspection. Existing trust, size, JSON, outage, rotation, lifetime and shutdown regressions remain in the unfiltered HTTP suite.

The old HTTP/cache implementation fails only the new regression, with exit 101 and nine other HTTP tests intentionally filtered by the focused command. Its exact raw log and source checks are retained. The first baseline executable was overwritten by the candidate rebuild before its binary hash and bytes were retained. A second, separately labeled exact-source reproduction also fails the intended regression; its executable is preserved. No identity with the unretained original executable is claimed. Both restored focused controls pass. These four controls are separate from the 194 unfiltered cases.

All 56 current cache ELF executables/shared objects are preserved in the byte-and-mode-verified scratch archive identified by `bin/manifest.json`; the reproduced failing binary has its own receipt. Large source audit JSON is stored losslessly as gzip with its original raw SHA256, mode and scratch restore path. A Python preparation syntax error and the initial unconfigured rustfmt attempt are retained as tooling history, separate from Rust behavior qualification. The first rustfmt description is reconstructed from the prior tool response, because no original raw log was captured.

An independent source-only review by `/root/c_peer` found no blocker. It ran no Cargo commands or live peers. Full KL11-37 remains open pending actual pushed source, socket profile checks and the independent HTTPS issuer plus genuine OAuth client/authority lifecycle proofs.
''')
files=sorted(path for path in out.rglob('*')if path.is_file()and path.name!='SHA256SUMS');(out/'SHA256SUMS').write_text(''.join(hashlib.sha256(path.read_bytes()).hexdigest()+'  '+str(path.relative_to(out))+'\n'for path in files));print(json.dumps(dict(files=len(files),total_bytes=sum(p.stat().st_size for p in files),validation_sha256=hashlib.sha256((out/'validation.json').read_bytes()).hexdigest(),checksum_manifest_sha256=hashlib.sha256((out/'SHA256SUMS').read_bytes()).hexdigest(),source_paths=[row['path']for row in source]),indent=2))
