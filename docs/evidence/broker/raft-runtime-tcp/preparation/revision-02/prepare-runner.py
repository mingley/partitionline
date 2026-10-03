import ast,pathlib,hashlib,json
root=pathlib.Path(__file__).parent
old=root.parent/'diagnostic-runner-02/guard-functions.py';text=old.read_text();tree=ast.parse(text);node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='executed_elfs');lines=text.splitlines(keepends=True)
replacement='''def executed_elfs(log, command_name):
    from artifact_paths import harness_paths
    records = []
    for name in harness_paths(log, ENV['CARGO_TARGET_DIR'], TREE):
        path = Path(name)
        assert path.is_file(), ('executed test ELF missing before retention', path)
        record = retain_elf(path, 'exact Cargo test harness execution', command_name, force_verify=True)
        assert record is not None, ('test harness was not ELF', path)
        records.append(record)
    return records
'''
helper=''.join(lines[:node.lineno-1])+replacement+''.join(lines[node.end_lineno:]);ast.parse(helper);(root/'guard-functions.py').write_text(helper)
provider=root.parent/'coverage-proposal-01/run-selective-candidate-03.py';st=provider.read_text();parsed=ast.parse(st);names=['free_bytes','guard_write','work_inodes','verify_rows','cache_map','combined_forecast','preserve_whole_cache','decode_cache_receipt','validate_reference_archive'];parts=[]
for name in names:
 n=next(n for n in parsed.body if isinstance(n,ast.FunctionDef) and n.name==name);parts.append(ast.get_source_segment(st,n)+'\n')
cache='\n'.join(parts)
# A per-step retained-output reserve includes its own finite raw capture ceiling.
cache=cache.replace("'additional_generated_growth': 64 * 1024 * 1024, 'future_delta_and_elf_retention':80*1024*1024, 'raw_capture_and_facts': plan['raw_capture_bytes_upper_bound']", "'additional_generated_growth': plan['generated_growth_bytes'], 'future_delta_and_elf_retention': plan['future_retention_bytes'], 'raw_capture_and_facts': plan['raw_capture_bytes_upper_bound'], 'scenario_temporary_data': plan['scenario_temporary_bytes_upper_bound']")
ast.parse(cache);(root/'cache-guards.py').write_text(cache)
(root/'guard-provenance.json').write_text(json.dumps({'schema_version':1,'baseline_guard':{'path':str(old),'sha256':hashlib.sha256(old.read_bytes()).hexdigest(),'original_frozen_provenance':str(old.with_name('guard-provenance.json'))},'extracted_sha256':hashlib.sha256(helper.encode()).hexdigest(),'change':'only executed_elfs is replaced with actual test-harness parser; all other original function bytes preserved','artifact_parser_sha256':hashlib.sha256((root/'artifact_paths.py').read_bytes()).hexdigest(),'cache_provider':{'path':str(provider),'sha256':hashlib.sha256(provider.read_bytes()).hexdigest(),'functions':names},'cache_functions_sha256':hashlib.sha256(cache.encode()).hexdigest(),'cache_change':'dynamic selected-step reserves for raw/scenario/generated/retention; original cache map and archive verification semantics retained'},indent=2)+'\n')
print('prepared unchanged core guards with additive exact harness and bounded step forecasts; source only')
