#!/usr/bin/env python3
"""Execute the verified-byte workflow prelude and tests, stopping before Cargo."""
import ast,os,sys,textwrap
from pathlib import Path
root=Path(os.environ['GITHUB_WORKSPACE'])
text=(root/'.github/workflows/candidates.yml').read_text()
start="          python3 -B - <<'PYJOIN'\n"
assert text.count(start)==1
block=textwrap.dedent(text.split(start,1)[1].split('\n          PYJOIN',1)[0])
tree=ast.parse(block)
cut=None
for i,node in enumerate(tree.body):
 if isinstance(node,ast.Expr) and isinstance(node.value,ast.Call) and isinstance(node.value.func,ast.Name) and node.value.func.id=='run_static':
  assert cut is None;cut=i+1
assert cut is not None
assert any(isinstance(n,ast.FunctionDef) and n.name=='run_static' for n in tree.body[:cut])
exec(compile(ast.fix_missing_locations(ast.Module(body=tree.body[:cut],type_ignores=[])),'workflow-loader','exec'),{})
print('verified workflow-byte loader and 12 tests passed; stopped before qualification',flush=True)
