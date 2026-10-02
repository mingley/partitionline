import pathlib, resource, subprocess
root=pathlib.Path('/workspace/work/pending-sparse-memory')
print('Python parent peak bytes',resource.getrusage(resource.RUSAGE_SELF).ru_maxrss*1024,flush=True)
for i in range(3):
    ret=subprocess.run([str(root/'low-rss-launcher'),str(root/f'control-final-child-{i}.pid'),str(root/'rss-exec-control')],cwd=root,text=True,check=True)
    print('control_exit',ret.returncode,flush=True)
ret=subprocess.run([str(root/'low-rss-launcher'),str(root/'control-final-exit.pid'),'/usr/bin/sh','-c','exit 17'],cwd=root)
assert ret.returncode==17
print('exit_propagation',ret.returncode,flush=True)
print(subprocess.check_output(['cc','--version'],text=True).splitlines()[0],flush=True)
