#!/usr/bin/env python3
"""Seal twelve completed actual strict cells as an exact text-only WORK packet."""
from pathlib import Path
import hashlib
import importlib.util
import json
import shutil
import stat

ROOT=Path('/workspace/work/membership-strict-403be1e3')
PACKET=ROOT/'publication-packet'
WRAPPER_SHA='c7b21841320b09f0d3a6952990995f92b8abce81fd2cfb1dd1dd42b06bb68c01'

def sha(p):
    return hashlib.sha256(p.read_bytes()).hexdigest()

def dump(p,x):
    p.write_text(json.dumps(x,indent=2)+'\n')

def main():
    assert sha(ROOT/'run-strict.py')==WRAPPER_SHA, 'frozen wrapper changed'
    assert not PACKET.exists(), 'publication overwrite refused'
    spec=importlib.util.spec_from_file_location('strict_wrapper',ROOT/'run-strict.py')
    module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
    matrix=json.loads((ROOT/'matrix-result.json').read_bytes())
    assert matrix['passed'] and matrix['completed_cells']==12 and matrix['actual_history_commands']==24, 'complete actual matrix required'
    assert matrix['failed_cells']==matrix['pending_cells']==matrix['checker_exit_failures']==matrix['all_attempt_cell_failures']==matrix['all_attempt_checker_exit_failures']==0, 'no failed or hidden attempts'
    sources=module.sources()
    aggregate={'events':0,'raw_checkpoint_pairs':0,'remote_authorities_bound':0,'actual_admission_change_proofs':0}
    trace_hashes=set();result_hashes=set();review=[]
    inputs=[ROOT/n for n in ['run-strict.py','expected-cells.json','preparation.json','prior-proof-reference.json','matrix-result.json','prepare-publication.py']]
    for cell in matrix['cells']:
        assert cell['status']=='passed' and len(cell['attempts'])==1, 'exact successful attempts'
        p=Path(cell['attempts'][0]['path']);receipt=json.loads(p.read_bytes())
        assert sha(p)==cell['attempts'][0]['sha256'] and receipt['passed'], 'sealed actual cell receipt'
        before=json.loads((p.parent/'capture-before.json').read_bytes())
        current=module.inventory(Path(receipt['capture_root']))
        assert before==current==json.loads((p.parent/'capture-after.json').read_bytes()), 'original full capture bytes/modes remain unchanged'
        assert sources==json.loads((p.parent/'source-before.json').read_bytes())==json.loads((p.parent/'source-after.json').read_bytes()), 'source modes/hashes remain unchanged'
        actual_completion=module.review_completion(p.parent/'completion.json',Path(receipt['capture_root']),cell['cell'],current)
        assert receipt['completion_before']==receipt['completion_after']==actual_completion, 'actual runtime provenance remains exact'
        for cmd in receipt['commands']:
            assert cmd['exit_code']==0 and cmd['strict_result_valid'] and cmd['admission_mode']=='observed-settings', 'strict actual command'
            result_path=p.parent/cmd['result']['path'];result=json.loads(result_path.read_bytes())
            assert sha(result_path)==cmd['result']['sha256'], 'exact actual checker result'
            aggregate['events']+=result['events']
            aggregate['raw_checkpoint_pairs']+=result['paired_raw_checkpoints']
            aggregate['remote_authorities_bound']+=result['remote_wal_authorities_bound']
            aggregate['actual_admission_change_proofs']+=len(result['admission_change_proofs'])
            trace_hashes.add(result['trace_sha256']);result_hashes.add(sha(result_path))
        review.append({'cell':cell['cell'],'original_capture_unchanged_at_seal':True,'complete_actual_command_source_env_harness_rechecked':True})
        inputs.extend(q for q in p.parent.iterdir() if q.is_file())
    assert aggregate=={k:matrix[k] for k in aggregate}, 'independent aggregate count agreement'
    assert len(inputs)==len(set(inputs)), 'distinct publication inputs'
    PACKET.mkdir()
    for source in sorted(inputs):
        # No captured journals/images, gzip ELF, class or jar is copied. Every
        # own retained command stream is empty or exact valid UTF-8 text.
        source.read_bytes().decode('utf-8')
        target=PACKET/source.relative_to(ROOT);target.parent.mkdir(parents=True,exist_ok=True)
        shutil.copy2(source,target)
        assert source.read_bytes()==target.read_bytes() and stat.S_IMODE(source.stat().st_mode)==stat.S_IMODE(target.stat().st_mode), 'exact packet bytes/full modes'
    validation={'schema_version':1,'passed':True,'source_sha':module.PIN,'actual_cells':12,'actual_history_checker_executions':24,
                'all_actual_commands_exit_zero':True,'all_capture_bytes_full_modes_unchanged':True,'all_source_command_env_completion_harness_bindings_verified_before_after_and_at_seal':True,
                **aggregate,'distinct_trace_content_sha256':len(trace_hashes),'distinct_checker_result_content_sha256':len(result_hashes),
                'trace_content_sha256':sorted(trace_hashes),'observed_settings':module.SETTINGS,'synthetic_executions_in_this_packet':0,
                'sources':sources,'publication_files_exact_utf8_or_empty':True,'compiled_executables_jars_classes_copied':0,
                'capture_journals_or_images_transformed_or_copied':0,'cell_final_reviews':review,
                'scope':'Finite typed current/prior-end admission and directory-qualified durable history from actual3/5process captures across six profiles/two toolchains. Twelve actual executions are reported separately from unique byte contents. No native wire, autonomous election scheduling, exhaustive consensus safety, or broader broker strict-gate claim.',
                'prior_source_proof':'prior-proof-reference.json','matrix_receipt':'matrix-result.json'}
    dump(PACKET/'validation.json',validation)
    manifest={str(p.relative_to(PACKET)):{'sha256':sha(p),'bytes':p.stat().st_size,'full_mode':oct(stat.S_IMODE(p.stat().st_mode))}
              for p in sorted(PACKET.rglob('*')) if p.is_file()}
    dump(PACKET/'packet-manifest.json',{'schema_version':1,'source_sha':module.PIN,'files':manifest,'payloads_utf8_or_empty':True,'compiled_artifacts':0})
    print(json.dumps({'packet':str(PACKET),'files':len(manifest)+1,'validation_sha256':sha(PACKET/'validation.json'),'manifest_sha256':sha(PACKET/'packet-manifest.json'),'actual_counts':aggregate,'distinct_trace_contents':len(trace_hashes)}))

if __name__=='__main__':
    main()
