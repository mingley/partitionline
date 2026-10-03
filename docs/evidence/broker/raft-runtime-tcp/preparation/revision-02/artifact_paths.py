"""Parse only actual Cargo test-harness executions; compiler arguments are not paths."""
import pathlib,re,shlex
NON_VERBOSE=re.compile(r'^(?:unittests src/lib\.rs|tests/[A-Za-z0-9_./-]+\.rs) \(([^()]*)\)$')
def harness_paths(log, target, tree, expected=('partitionline_broker','raft_runtime')):
    target=pathlib.Path(target).resolve();tree=pathlib.Path(tree).resolve();found=set()
    name_pattern=re.compile(r'^(?:'+ '|'.join(re.escape(n) for n in expected)+r')-[0-9a-f]+$')
    for line in log.splitlines():
        line=line.strip()
        if not line.startswith('Running '):continue
        payload=line.removeprefix('Running ')
        m=NON_VERBOSE.fullmatch(payload)
        if m: candidate=pathlib.Path(m.group(1))
        elif payload.startswith('`') and payload.endswith('`'):
            words=shlex.split(payload[1:-1])
            if not words:continue
            candidate=pathlib.Path(words[0])
            # Rustc, rustdoc and build scripts are retained by the cache scan;
            # none is a test-harness execution, regardless of parentheses.
            if not name_pattern.fullmatch(candidate.name):continue
        else:continue
        if not candidate.is_absolute():candidate=tree/candidate
        resolved=candidate.resolve()
        if not resolved.is_relative_to(target):raise ValueError('test harness outside owned target: '+str(candidate))
        if resolved.parent.name!='deps' or not name_pattern.fullmatch(resolved.name):
            raise ValueError('unexpected owned test harness: '+str(candidate))
        found.add(str(resolved))
    return sorted(found)
