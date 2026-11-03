#!/usr/bin/env python3
"""Build an inactive v17 queue-witness bundle after receiving exact candidate pins.

Only static Python and git-apply controls are run. No native build or benchmark.
"""
import argparse
import ast
import difflib
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile

HERE = Path(__file__).resolve().parent
V16_ARCHIVE_SHA = '35d1718b9ca85274ce4e1f13acee78306dbf0ecbcbc7ace6dbb1e5ffb1d162c5'
V16_MANIFEST_SHA = '8e90cbd3f103a4959fe3995795ec9311e22d583c741e21c19616d9471bb1ad6d'
V16_REVIEW_SHA = '0c697edbe2ccbec1890a3c50e5e0fe5a76398bf101652e2d7a9e6d746aaa3475'
V16_SPEC_SHA = 'dcca8ef98344e960ced94ef0aa560032950ce96556dab16820cffe007e3a6be2'
V16_IMPORT_SHA = '327f9f05c1ab9e321951ea86d841006943ec8f93c74a5b5d960c63a68252e58c'
FIXED_SHA = '15da704200222b0b3b8de7d74274b9cf1fb3933a9339112c09b3467aa07868ea'
DEADLINE_SHA = 'bff6fc134996b97cfc493fd4657f360c911127213ec88f0ca728548c7d9dfeeb'
IMPORT_PATCH_SHA = '560658682de8033e09ba973f2d4be5e493c140665f57c491cae0a1fa5f3e0d72'
V4_HELPERS = {
    'scripts/ci-qualify-client-candidate.py': 'f567f7456c92ce1f39e6d7b4476f3cc3300bfef9c92dd036f8cc3ef0a5514608',
    'tests/ci/test_client_candidate_qualification.py': '2d84bbb8884f059b1974659dc6d1c0c4b6785a5b01702d855e8302a73ab4ce1f',
}
TEMPLATES = ('README.md', 'workflow-fragment.yml', 'bind_qualification.py', 'run.py', 'test_plan.py')
UNCHANGED = ('example.rs', 'pending_queue_bench.rs.in', 'queue-v2.patch.gz',
             'v10-queue-hook-inputs.json', 'v10-queue-inputs.json',
             'v10-queue-worker-initializer.rs.txt', 'v16-fixed-inputs.json',
             'v16-hook-inputs.json', 'v16-worker-initializer.rs.txt')
CORE = ('safe_copy', 'inventory', 'short_command', 'checked_patch', 'inject',
        'case_plan', 'pair_orders', 'validate_sample', 'owned_helper', 'execute_owned')


def digest(data):
    return hashlib.sha256(data).hexdigest()


def read_file(path, sha=None):
    if path.is_symlink() or not path.is_file():
        raise RuntimeError(f'missing or unsafe file: {path}')
    data = path.read_bytes()
    if sha is not None and digest(data) != sha:
        raise RuntimeError(f'file hash mismatch: {path}')
    return data


def parse_json(path, sha=None):
    return json.loads(read_file(path, sha))


def function_text(source, name):
    tree = ast.parse(source)
    matches = [n for n in tree.body if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef)) and n.name == name]
    if len(matches) != 1:
        raise RuntimeError(f'expected one function: {name}')
    return ast.get_source_segment(source, matches[0])


def write_file(base, name, data):
    target = base / name
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_bytes(data)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--checkout', type=Path, required=True, help='existing checkout containing v16 evidence and four unchanged helpers')
    parser.add_argument('--v17-assets', type=Path, required=True, help='frozen v17 candidate asset directory')
    parser.add_argument('--helper-overlay', type=Path, required=True, help='frozen v4 helper overlay root')
    parser.add_argument('--spec-sha256', required=True, help='independently supplied exact v17 spec digest')
    parser.add_argument('--import-sha256', required=True, help='independently supplied exact v17 import-manifest digest')
    parser.add_argument('--producer', type=Path, required=True, help='exact v17 fixed src/producer.rs')
    parser.add_argument('--prepare-tests', type=Path, required=True, help='exact v17 fixed src/producer/prepare_tests.rs')
    parser.add_argument('--cargo-toml', type=Path, required=True, help='exact v17 fixed Cargo.toml (read only)')
    parser.add_argument('--output', type=Path, required=True, help='new directory outside checkout')
    args = parser.parse_args()
    for value in (args.spec_sha256, args.import_sha256):
        if not re.fullmatch('[0-9a-f]{64}', value):
            raise RuntimeError('expected SHA-256 in lowercase hexadecimal')
    checkout=args.checkout.resolve(); assets=args.v17_assets.resolve(); overlay=args.helper_overlay.resolve()
    output=args.output.resolve()
    if output.exists() or output.is_relative_to(checkout):
        raise RuntimeError('output must be new and outside checkout')
    old_dir=checkout/'docs/evidence/client/candidate-20261030/v16'
    evidence=checkout/'docs/evidence/perf/pending-queue-v16-20261103'
    archive=read_file(evidence/'prototype.tar.gz', V16_ARCHIVE_SHA)
    old_review=read_file(evidence/'independent-review.json', V16_REVIEW_SHA)
    old_spec=parse_json(old_dir/'spec.json', V16_SPEC_SHA)
    old_manifest=parse_json(old_dir/'import-manifest.json', V16_IMPORT_SHA)
    spec=parse_json(assets/'spec.json', args.spec_sha256)
    manifest=parse_json(assets/'import-manifest.json', args.import_sha256)
    fixed_raw=read_file(assets/'fixed-inputs.json', FIXED_SHA)
    deadline_raw=read_file(assets/'deadline-fixture.patch', DEADLINE_SHA)
    for key in ('base_inputs','baseline_inputs','fixed_inputs','controls','baseline_patch','fixed_delta','baseline_cases','fixed_full_targets'):
        if spec.get(key)!=old_spec.get(key):
            raise RuntimeError(f'v17 changed a benchmark or correctness contract field: {key}')
    if len(spec['baseline_cases'])!=23 or len(spec['fixed_full_targets'])!=10:
        raise RuntimeError('unexpected baseline or full-target count')
    controls_raw=read_file(assets/spec['controls']['path'], spec['controls']['sha256'])
    if len(controls_raw)!=spec['controls']['bytes']:
        raise RuntimeError('v17 control asset size differs')
    controls=json.loads(controls_raw)
    if len(controls['library'])!=166 or len({row['test'] for row in controls['library']})!=166 or \
       len(controls['integrations'])!=17 or len({(row['target'],row['test']) for row in controls['integrations']})!=17:
        raise RuntimeError('v17 control counts or unique identities differ')
    fixed=json.loads(fixed_raw)
    if len(fixed)!=1755:
        raise RuntimeError('v17 fixed source catalog size differs')
    if len(fixed_raw)!=spec['fixed_inputs']['bytes'] or digest(fixed_raw)!=spec['fixed_inputs']['sha256']:
        raise RuntimeError('v17 fixed source catalog does not match spec')
    for path,key in ((args.producer,'src/producer.rs'),(args.prepare_tests,'src/producer/prepare_tests.rs'),(args.cargo_toml,'Cargo.toml')):
        read_file(path, fixed[key])
    pinned=spec['qualification_helper_overlay']['helper_pins']
    if pinned != manifest['required_existing_root_files']:
        raise RuntimeError('v17 helper manifest and spec differ')
    previous=old_manifest['required_existing_root_files']
    if set(pinned)!=set(previous) or len(pinned)!=6:
        raise RuntimeError('v17 requires a different helper set')
    changed={k for k in pinned if pinned[k]!=previous[k]}
    if changed!=set(V4_HELPERS) or any(pinned[k]['sha256']!=v for k,v in V4_HELPERS.items()):
        raise RuntimeError('v17 helper changes differ from reviewed phase projection v4')
    helpers={}
    for name,pin in pinned.items():
        candidate=overlay/name if (overlay/name).is_file() else checkout/name
        data=read_file(candidate,pin['sha256'])
        if len(data)!=pin['bytes']:
            raise RuntimeError(f'helper size mismatch: {name}')
        helpers[name]=data
    guard=pinned['scripts/ci-verify-client-candidate-helpers.py']
    # The spec and manifest are independently pinned; the guard is also
    # verified against the actual six helper bytes before running static tests.
    spec_key='docs/evidence/client/candidate-20261030/v17/spec.json'
    if manifest['files'][spec_key] != {'sha256':args.spec_sha256,'bytes':len(read_file(assets/'spec.json'))}:
        raise RuntimeError('v17 import manifest does not bind the exact specification')
    for name in ('fixed-inputs.json','controls.json','deadline-fixture.patch'):
        key='docs/evidence/client/candidate-20261030/v17/'+name
        data=read_file(assets/name)
        if manifest['files'][key]!={'sha256':digest(data),'bytes':len(data)}:
            raise RuntimeError(f'v17 import manifest does not bind {name}')
    import_patch=checkout/'docs/evidence/client/candidate-20261030/v14/pending-work-import.patch'
    read_file(import_patch,IMPORT_PATCH_SHA)
    source_data={}
    with tarfile.open(fileobj=io.BytesIO(archive),mode='r:gz') as tar:
        names=tar.getnames()
        if len(names)!=21 or len(names)!=len(set(names)) or any('/' in x or x in ('.','..') for x in names):
            raise RuntimeError('unexpected v16 archive layout')
        manifest_raw=tar.extractfile('manifest.json').read()
        if digest(manifest_raw)!=V16_MANIFEST_SHA:
            raise RuntimeError('unexpected v16 archived manifest')
        old_bundle=json.loads(manifest_raw)
        for name,pin in old_bundle['assets'].items():
            data=tar.extractfile(name).read()
            if len(data)!=pin['bytes'] or digest(data)!=pin['sha256']:
                raise RuntimeError(f'v16 archived asset mismatch: {name}')
            source_data[name]=data
    if source_data['v16-fixed-inputs.json']!=fixed_raw:
        raise RuntimeError('the v17 and reviewed v16 fixed inventories differ')
    if output.parent.is_symlink():
        raise RuntimeError('unsafe output parent')
    output.parent.mkdir(parents=True,exist_ok=True)
    with tempfile.TemporaryDirectory(prefix='pending-v17-finalize-',dir=output.parent) as temp:
        root=Path(temp); bundle=root/'bundle';bundle.mkdir()
        for name in UNCHANGED:write_file(bundle,name,source_data[name])
        write_file(bundle,'prior-v16-independent-review.json',old_review)
        replacement={'__V17_SPEC_SHA__':args.spec_sha256,'__V17_MANIFEST_SHA__':args.import_sha256,
                     '__V17_GUARD_SHA__':guard['sha256'],'__V17_GUARD_SIZE__':str(guard['bytes'])}
        for name in TEMPLATES:
            text=(HERE/'templates'/name).read_text()
            for old,new in replacement.items():text=text.replace(old,new)
            if re.search(r'__V17_[A-Z_]+__',text):raise RuntimeError('unresolved template pin')
            write_file(bundle,name,text.encode())
        old_run=source_data['run.py'].decode();new_run=(bundle/'run.py').read_text()
        for name in CORE:
            if function_text(old_run,name)!=function_text(new_run,name):
                raise RuntimeError(f'benchmark function changed: {name}')
        start="        state['phase']='sampling';write_json(out/'state.json',state)"
        end="        state.update(phase='complete'"
        if old_run[old_run.index(start):old_run.index(end)]!=new_run[new_run.index(start):new_run.index(end)]:
            raise RuntimeError('benchmark sampling and timing loop changed')
        for name in ('test_bounded_and_balanced_pairs','test_sample_validation_rejects_lost_or_ambiguous_identities',
                     'test_same_job_witness_rejects_stale_run_and_attempt','test_exact_patch_roundtrip_and_hook_parity'):
            # The method bodies are nested in a class; compare exact text from
            # their source lines, bounded by the next test method.
            def method(source):
                begin=source.index('    def '+name+'(')
                after=source.find('\n    def test_',begin+1)
                if after<0:after=source.index("\nif __name__",begin)
                return source[begin:after]
            if method(source_data['test_plan.py'].decode())!=method((bundle/'test_plan.py').read_text()):
                raise RuntimeError(f'original benchmark test changed: {name}')
        delta=[]
        for name in TEMPLATES:
            delta.extend(difflib.unified_diff(source_data[name].decode().splitlines(keepends=True),
                                              (bundle/name).read_text().splitlines(keepends=True),
                                              fromfile='v16/'+name,tofile='v17/'+name))
        write_file(bundle,'v16-to-v17.diff',''.join(delta).encode())
        # Assemble only the six pinned helpers and the few spec assets needed
        # by source-only unit tests, never a full Rust source checkout.
        scratch=root/'checkout';v17=scratch/'docs/evidence/client/candidate-20261030/v17';v17.mkdir(parents=True)
        v14=scratch/'docs/evidence/client/candidate-20261030/v14';v14.mkdir(parents=True)
        for name,data in helpers.items():write_file(scratch,name,data)
        for name in ('spec.json','import-manifest.json','controls.json','fixed-inputs.json','deadline-fixture.patch'):
            write_file(v17,name,read_file(assets/name))
        write_file(v14,'spec.json',read_file(checkout/'docs/evidence/client/candidate-20261030/v14/spec.json'))
        guard_dir=root/'guard-bin';guard_dir.mkdir()
        for name in ('cargo','rustc','rustup','cc','c++','curl','wget','gh'):
            p=guard_dir/name;p.write_text('#!/bin/sh\nprintf "forbidden native or network command\\n" >&2\nexit 99\n');p.chmod(0o755)
        env=os.environ.copy();env.update(PYTHONDONTWRITEBYTECODE='1',PATH=str(guard_dir)+os.pathsep+env.get('PATH',''),
            PL_BENCH_CHECKOUT_PATH=str(scratch),PL_BENCH_PRODUCER_PATH=str(args.producer.resolve()),
            PL_BENCH_PREPARE_TESTS_PATH=str(args.prepare_tests.resolve()),PL_BENCH_CARGO_PATH=str(args.cargo_toml.resolve()),
            PL_BENCH_IMPORT_PATH=str(import_patch),PL_BENCH_DEADLINE_PATH=str(assets/'deadline-fixture.patch'))
        check=subprocess.run([sys.executable,'-B',str(bundle/'test_plan.py'),'-v'],env=env,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,timeout=120)
        if check.returncode:
            raise RuntimeError('static controls failed:\n'+check.stdout.decode(errors='replace'))
        log=check.stdout.decode(errors='replace')
        if 'skipped=' in log or 'Ran 9 tests' not in log or '\nOK\n' not in log:
            raise RuntimeError('static controls were skipped or incomplete:\n'+log)
        write_file(bundle,'static-validation.log',check.stdout)
        validation={'scope':'source-only static checks; no native build or benchmark',
            'v17_spec_sha256':args.spec_sha256,'v17_import_manifest_sha256':args.import_sha256,
            'prior_v16_archive_sha256':V16_ARCHIVE_SHA,'prior_v16_review_sha256':V16_REVIEW_SHA,
            'fixed_source_catalog_sha256':FIXED_SHA,'helper_pins':pinned,'v4_changed_helpers':sorted(changed),
            'unchanged_core_function_bytes':list(CORE),'sampling_and_timing_loop_bytes_identical':True,
            'rust_harness_template_identical':True,'example_identical':True,'queue_patch_identical':True,
            'same_job_scope':'trusted workflow ordering only; not an independently signed attestation',
            'complete_gate_requirement':'format, clippy and every specified/control target in both feature profiles, with no missing, duplicate, extra or failed gates',
            'baseline_cases':23,'fixed_full_targets':10,'library_controls':166,'integration_controls':17,
            'static_controls':'nine passed with no skips; native/network command stubs on PATH',
            'limitations':['no Cargo, Rust/native tests, actual benchmark or hosted run','synthetic completion does not prove Kafka delivery','no performance ranking or production-readiness conclusion']}
        write_file(bundle,'validation.json',(json.dumps(validation,indent=2,sort_keys=True)+'\n').encode())
        records={p.name:{'sha256':digest(p.read_bytes()),'bytes':p.stat().st_size} for p in sorted(bundle.iterdir()) if p.is_file()}
        out_manifest={'schema':1,'status':'inactive source-only candidate; independent review required',
            'v17_spec_sha256':args.spec_sha256,'v17_import_manifest_sha256':args.import_sha256,
            'prior_v16_archive_sha256':V16_ARCHIVE_SHA,'prior_v16_manifest_sha256':V16_MANIFEST_SHA,
            'prior_v16_review_sha256':V16_REVIEW_SHA,'fixed_source_catalog_sha256':FIXED_SHA,
            'qualification_required':'complete same-job v17 qualification before any benchmark execution',
            'historical_arm_label':'v16; exact v17 fixed source catalog is byte-identical',
            'assets':records}
        manifest_raw=(json.dumps(out_manifest,indent=2,sort_keys=True)+'\n').encode();write_file(bundle,'manifest.json',manifest_raw)
        archive_path=root/'prototype.tar.gz'
        with archive_path.open('wb') as raw:
            with gzip.GzipFile(filename='',mode='wb',fileobj=raw,mtime=0) as gz:
                with tarfile.open(fileobj=gz,mode='w') as tar:
                    for path in sorted(bundle.iterdir()):
                        data=path.read_bytes();info=tarfile.TarInfo(path.name);info.size=len(data);info.mode=0o644
                        info.uid=info.gid=0;info.uname=info.gname='';info.mtime=0
                        tar.addfile(info,io.BytesIO(data))
        receipt={'scope':'inactive v17 source-only benchmark witness; no native execution',
                 'archive_sha256':digest(archive_path.read_bytes()),'manifest_sha256':digest(manifest_raw),
                 'v17_spec_sha256':args.spec_sha256,'v17_import_manifest_sha256':args.import_sha256,
                 'prior_v16_archive_sha256':V16_ARCHIVE_SHA,'prior_v16_review_sha256':V16_REVIEW_SHA,
                 'finalizer_sha256':digest(read_file(HERE/'finalize.py')),
                 'template_sha256':{name:digest(read_file(HERE/'templates'/name)) for name in TEMPLATES}}
        write_file(root,'receipt.json',(json.dumps(receipt,indent=2,sort_keys=True)+'\n').encode())
        final=root/'final';final.mkdir();shutil.move(bundle,final/'bundle');shutil.move(archive_path,final/'prototype.tar.gz')
        shutil.move(root/'receipt.json',final/'receipt.json');final.rename(output)
    print(json.dumps({'result':'static pass; inactive', 'output':str(output), **receipt},indent=2))


if __name__ == '__main__':
    try:main()
    except Exception as error:print(f'finalization failed: {error}',file=sys.stderr);raise SystemExit(1)
