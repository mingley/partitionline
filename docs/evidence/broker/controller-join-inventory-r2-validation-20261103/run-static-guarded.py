#!/usr/bin/env python3
"""Run only pinned Python inventory tools with no network/Cargo/native tests."""
import ctypes, errno, os, resource, socket, sys
from pathlib import Path
root=Path('/workspace/work/broker-join-inventory-investigation-20261103')
guard=root/'guard-bin';guard.mkdir(exist_ok=True)
for name in ('cargo','rustc','rustup','curl','wget','gh'):
 p=guard/name
 p.write_text('#!/bin/sh\nprintf "%s\\n" "$0" >> "$PL_FORBIDDEN_LOG"\nexit 91\n')
 p.chmod(0o755)
lib=ctypes.CDLL('libseccomp.so.2',use_errno=True)
lib.seccomp_init.argtypes=[ctypes.c_uint32];lib.seccomp_init.restype=ctypes.c_void_p
lib.seccomp_syscall_resolve_name.argtypes=[ctypes.c_char_p];lib.seccomp_syscall_resolve_name.restype=ctypes.c_int
lib.seccomp_rule_add.argtypes=[ctypes.c_void_p,ctypes.c_uint32,ctypes.c_int,ctypes.c_uint];lib.seccomp_rule_add.restype=ctypes.c_int
lib.seccomp_load.argtypes=[ctypes.c_void_p];lib.seccomp_load.restype=ctypes.c_int
lib.seccomp_release.argtypes=[ctypes.c_void_p]
ctx=lib.seccomp_init(0x7fff0000)
assert ctx
for name in ('socket','connect','sendto','sendmsg','sendmmsg'):
 n=lib.seccomp_syscall_resolve_name(name.encode())
 assert n>=0 and lib.seccomp_rule_add(ctx,0x00050000|errno.EPERM,n,0)==0
assert lib.seccomp_load(ctx)==0
lib.seccomp_release(ctx)
try: socket.socket()
except PermissionError: print('socket guard active',flush=True)
else: raise SystemExit('socket guard failed')
resource.setrlimit(resource.RLIMIT_FSIZE,(16*1024**2,16*1024**2))
os.chdir(root/'source')
env={'PATH':str(guard)+':/usr/local/bin:/usr/bin:/bin','LANG':'C.UTF-8','PYTHONDONTWRITEBYTECODE':'1','TMPDIR':str(root/'tmp'),'PL_FORBIDDEN_LOG':str(root/'forbidden.log')}
env['CAPACITY_CHECKOUT']='/workspace/partitionline'
env['RUNNER_TEMP']=str(root/'tmp')
env['CAPACITY_WORKFLOW_FILE']='/workspace/work/broker-join-inventory-r2-20261103/workflow-r2.yml'
args=['-m','unittest','discover','-s','/workspace/work/broker-join-inventory-r2-20261103','-p','test_static.py','-v']
os.execve(sys.executable,[sys.executable,'-B',*args],env)
