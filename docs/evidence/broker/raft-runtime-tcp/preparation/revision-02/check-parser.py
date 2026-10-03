import hashlib,json,pathlib,importlib.util
root=pathlib.Path(__file__).parent
spec=importlib.util.spec_from_file_location('artifact_paths',root/'artifact_paths.py');mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
target='/workspace/work/target-broker-segments';tree='/workspace/work/broker-merged-source-ea9ff293'
cases=[('/workspace/work/raft-runtime-76/development/incoming-lifetime-candidate47a9-01/candidate-four-regressions/command.log',['raft_membership-df70681fff0cc180'],('raft_membership',)),('/workspace/work/raft-runtime-76/development/diagnostic-ea9ff293-01/stable-default-runtime-tests/command.log',['raft_runtime-10d4014cbed1908f'],('raft_runtime',)),('/workspace/work/raft-runtime-76/development/diagnostic-ea9ff293-01/stable-default-runtime-lib-tests/command.log',['partitionline_broker-c952dbf8a96b13dc'],('partitionline_broker',))]
results=[]
for source,names,expected in cases:
 p=pathlib.Path(source);data=p.read_bytes();paths=mod.harness_paths(data.decode(),target,tree,expected)
 assert [pathlib.Path(n).name for n in paths]==names
 results.append({'actual_log':source,'sha256':hashlib.sha256(data).hexdigest(),'harnesses':paths})
noise="     Running `/usr/bin/rustc --check-cfg 'cfg(feature, values(\"codecs\", \"default\"))'`\n     Running `/workspace/work/target-broker-segments/debug/build/crc32c-abc/build-script-build`\n"
assert mod.harness_paths(noise,target,tree)==[]
for wrong in ['/elsewhere/debug/deps/raft_runtime-abcdef','/workspace/work/target-broker-segments/debug/deps/rustc']:
 try:mod.harness_paths('Running tests/raft_runtime.rs ('+wrong+')',target,tree)
 except ValueError:results.append({'negative_path':wrong,'rejected':True})
 else:raise AssertionError('invalid harness path accepted')
(root/'artifact-parser-validation.json').write_text(json.dumps({'scope':'source/Python-only parser controls on retained real Cargo logs; no Rust launch','parser_sha256':hashlib.sha256((root/'artifact_paths.py').read_bytes()).hexdigest(),'actual_log_cases':3,'negative_controls':4,'results':results},indent=2)+'\n')
print('3 actual Cargo log forms and 4 compiler/buildscript/outside/wrong-harness controls pass')
