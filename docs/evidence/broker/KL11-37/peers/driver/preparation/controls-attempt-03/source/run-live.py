#!/usr/bin/env python3
"""Source-only prepared live driver; no qualification has been run.

The operator supplies a predeclared finite step history and immutable runtime
bindings. Only parsed, bounded safe JSON receipts are retained. Private issuer
files and bearers stay in a separate mode0700 scratch tree.
"""
import argparse
import base64
import hashlib
import importlib.util
import json
import os
from pathlib import Path
import queue
import re
import resource
import signal
import socket
import ssl
import subprocess
import sys
import threading
import time
import urllib.request


def digest(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


def require(condition, label):
    if not condition:
        raise RuntimeError(label)


def bind(path, expected):
    path = Path(path).resolve(strict=True)
    require(path.is_file() and digest(path) == expected, 'runtime input hash')
    return path


def child_setup():
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    os.umask(0o077)


def interruption(_signal, _frame):
    raise RuntimeError('controlled external interruption')


def process_identity(pid):
    # Linux /proc fields are parsed after the final comm parenthesis because
    # executable names may contain whitespace or parentheses.
    fields = (Path('/proc')/str(pid)/'stat').read_text().rsplit(')',1)[1].split()
    return {'pid':pid,'pgid':int(fields[2]),'starttime_ticks':int(fields[19])}


class OwnedProcess:
    def __init__(self, identity, argv, receipt, deadline, on_spawn):
        self.identity, self.receipt, self.deadline = identity, receipt, deadline
        self.events = queue.Queue(maxsize=1024)
        self.stderr_bytes = 0
        self.stderr_digest = hashlib.sha256()
        self.failure = None
        self.forced = False
        self.readers = []
        self.process = subprocess.Popen(argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, start_new_session=True,
                                        umask=0o077, bufsize=0)
        on_spawn(self)
        self.readers = [threading.Thread(target=self.stdout, name=identity+'-stdout'),
                        threading.Thread(target=self.stderr, name=identity+'-stderr')]
        for reader in self.readers:
            reader.start()

    def stdout(self):
        try:
            while True:
                line = self.process.stdout.readline(65537)
                if not line:
                    break
                require(len(line) <= 65536 and line.endswith(b'\n'), 'finite JSON line')
                event = json.loads(line)
                require(isinstance(event, dict) and isinstance(event.get('event'), str),
                        'typed JSON event')
                require(not re.search(rb'[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}\.[A-Za-z0-9_-]{20,}',
                                      line), 'no compact bearer in receipt')
                forbidden = {'access_token', 'authorization', 'client_secret', 'password',
                             'private_key', 'proof', 'token'}
                def inspect(value, depth=0):
                    require(depth <= 12, 'finite receipt nesting')
                    if isinstance(value, dict):
                        require(len(value) <= 128 and not forbidden.intersection(value),
                                'safe receipt keys')
                        for field in value.values():
                            inspect(field, depth+1)
                    elif isinstance(value, list):
                        require(len(value) <= 128, 'finite receipt array')
                        for field in value:
                            inspect(field, depth+1)
                    elif isinstance(value, str):
                        require(len(value) <= 2048, 'finite receipt string')
                    else:
                        require(value is None or type(value) in (int, float, bool),
                                'typed receipt scalar')
                inspect(event)
                self.receipt(self.identity, event)
                self.events.put(event, timeout=1)
        except BaseException:
            # Do not preserve a raw line or exception: a rejected value can
            # contain SDK/provider secrets. The failure remains explicit.
            self.failure = 'rejected stdout receipt'

    def stderr(self):
        while True:
            data = self.process.stderr.read(4096)
            if not data:
                break
            self.stderr_bytes += len(data)
            self.stderr_digest.update(data)
            if self.stderr_bytes > 4*1024*1024:
                self.failure = 'stderr byte ceiling'
                break

    def await_event(self, name, phase=None, seconds=20):
        deadline = min(self.deadline, time.monotonic()+seconds)
        while time.monotonic() < deadline:
            require(self.failure is None, 'process receipt failure')
            try:
                event = self.events.get(timeout=min(0.2, deadline-time.monotonic()))
            except queue.Empty:
                if self.process.poll() is not None:
                    raise RuntimeError('process exited before expected event')
                continue
            if event['event'] == name and (phase is None or event.get('phase') == phase):
                return event
            require(event['event'] not in ('fatal', 'failed'), 'peer fatal event')
        raise RuntimeError('expected process event deadline')

    def send(self, line):
        require(time.monotonic() < self.deadline and len(line) <= 100, 'operator deadline')
        self.process.stdin.write((line+'\n').encode('ascii'))
        self.process.stdin.flush()

    def join(self, seconds=30, expected_code=0):
        limit = max(0.1, min(seconds, self.deadline-time.monotonic()))
        try:
            code = self.process.wait(timeout=limit)
        except subprocess.TimeoutExpired:
            self.abort()
            raise RuntimeError('process join deadline')
        for reader in self.readers:
            reader.join(timeout=2)
        require(not any(reader.is_alive() for reader in self.readers), 'reader threads joined')
        require(self.failure is None, 'process receipt failure')
        self.receipt(self.identity, {'event':'process-joined','exit_code':code,
                     'stderr_bytes':self.stderr_bytes,'stderr_sha256':self.stderr_digest.hexdigest(),
                     'stderr_retained':False})
        require(code == expected_code and not self.forced, 'process predeclared graceful exit')

    def group_exists(self):
        try:
            os.killpg(self.process.pid, 0)
            return True
        except ProcessLookupError:
            return False

    def abort(self):
        # The leader can already have exited while descendants retain stdout
        # or stderr. Ownership is the new process group, not only its leader.
        if self.group_exists():
            self.forced = True
            try:
                os.killpg(self.process.pid, signal.SIGTERM)
            except ProcessLookupError:
                pass
            deadline = time.monotonic()+3
            while self.group_exists() and time.monotonic()<deadline:
                self.process.poll()  # reap the owned leader when possible
                time.sleep(0.05)
            if self.group_exists():
                try:
                    os.killpg(self.process.pid, signal.SIGKILL)
                except ProcessLookupError:
                    pass
            try:
                self.process.wait(timeout=3)
            except subprocess.TimeoutExpired:
                self.failure = 'owned group leader did not reap'
        require(not self.group_exists(), 'owned process group gone')
        for reader in self.readers:
            reader.join(timeout=4)
        require(not any(reader.is_alive() for reader in self.readers), 'owned readers joined')


class Run:
    def __init__(self, config, output, private):
        self.config, self.output, self.private = config, output, private
        self.deadline = time.monotonic()+240
        self.lock = threading.Lock()
        self.records, self.processes, self.peers = [], [], {}
        self.history = []
        self.server = self.issuer_thread = self.broker = None
        self.issuer_started = False
        self.cleanup_threads = []
        self.positive_peers = set()
        self.peer_issue_boundary = {}
        self.peer_proofs = {}
        self.peer_generation = {}
        self.pending_recovery = set()
        self.control_boundary = 0
        self.failed_step = None
        self.failure_stage = None
        self.registry = []
        self.steps = []
        self.source_before = None

    def register_process(self, process):
        self.processes.append(process)
        row = {'actor':process.identity,**process_identity(process.process.pid)}
        require(row['pgid']==row['pid'], 'owned child new session group')
        self.registry.append(row)
        self.write_registry()

    def write_registry(self):
        # The outer owner additionally scans descendants while the driver is
        # still alive, closing the spawn-to-registration race. It must sweep
        # these separate sessions before any fallback driver SIGKILL.
        value = {'driver':process_identity(os.getpid()),'owned_groups':self.registry,
                 'scope':'PID/starttime bound process groups; no secret argv or token data'}
        temporary = self.output/'owned-process-groups.json.tmp'
        temporary.write_text(json.dumps(value,indent=2)+'\n')
        os.replace(temporary,self.output/'owned-process-groups.json')

    def source_guard(self):
        """Verify every committed file against its actual Git object, plus mode.

        This is outside timed SDK phases. It runs before startup and after all
        joins, and binds the entire complete immutable tree rather than a
        selected production-file list.
        """
        source = Path(self.config['source_root']).resolve(strict=True)
        require(not self.private.resolve().is_relative_to(source)
                and not self.output.resolve().is_relative_to(source)
                and not self.private.resolve().is_relative_to(self.output.resolve())
                and not self.output.resolve().is_relative_to(self.private.resolve()),
                'private scratch, public receipts and immutable source are disjoint')
        result = subprocess.run(['git','ls-tree','-r','-z',self.config['source_sha']],
                                cwd=self.config['git_repository'],capture_output=True,
                                check=True,timeout=15)
        rows = []
        expected_paths = set()
        for item in result.stdout.split(b'\0'):
            if not item:
                continue
            metadata, relative = item.split(b'\t',1)
            mode, kind, object_id = metadata.decode('ascii').split()
            require(kind=='blob', 'complete source contains supported Git blobs')
            relative = os.fsdecode(relative)
            expected_paths.add(relative)
            path = source/relative
            info = path.lstat()
            if mode=='120000':
                require(path.is_symlink(), 'source symlink mode')
                data = os.fsencode(os.readlink(path))
            else:
                require(mode in ('100644','100755') and path.is_file()
                        and not path.is_symlink(), 'source regular Git file mode')
                data = path.read_bytes()
                require(bool(info.st_mode&0o111)==(mode=='100755'), 'source executable Git mode')
            computed = hashlib.sha1(('blob '+str(len(data))+'\0').encode()+data).hexdigest()
            require(computed==object_id, 'complete immutable source Git byte identity')
            rows.append({'path':relative,'git_mode':mode,'git_object':object_id,
                         'full_mode':oct(info.st_mode&0o7777),'bytes':len(data),
                         'sha256':hashlib.sha256(data).hexdigest()})
        require(rows, 'nonempty complete source tree')
        require(not (source/'.git').exists(), 'immutable archive has no mutable Git metadata')
        actual_paths = {str(path.relative_to(source)) for path in source.rglob('*')
                        if path.is_file() or path.is_symlink()}
        require(actual_paths==expected_paths, 'complete immutable filesystem pathset equals Git')
        return rows

    def receipt(self, identity, event):
        with self.lock:
            require(len(self.records) < 16384, 'finite receipt history')
            require(not {'driver_sequence','actor'}.intersection(event), 'reserved receipt authority')
            self.records.append({'driver_sequence':len(self.records),'actor':identity,**event})

    def setup(self):
        self.output.mkdir(mode=0o700, parents=True, exist_ok=False)
        self.write_registry()
        self.private.mkdir(mode=0o700, parents=True, exist_ok=False)
        require(len(self.config.get('steps', [])) <= 96, 'finite predeclared steps')
        (self.output/'declared-lifecycle.json').write_text(json.dumps(self.config,indent=2)+'\n')
        source = Path(self.config['source_root']).resolve(strict=True)
        actual = subprocess.run(['git','rev-parse',self.config['source_sha']+'^{commit}'],
                                cwd=self.config['git_repository'], capture_output=True,
                                check=True, timeout=5).stdout.decode().strip()
        require(actual == self.config['source_sha'], 'exact pushed source identifier')
        origin_path = bind(self.config['immutable_origin_receipt'],
                           self.config['immutable_origin_receipt_sha256'])
        origin = json.loads(origin_path.read_text())
        require(origin['source_sha']==actual and origin['source_root']==str(source)
                and origin['complete_git_tree_verified'] is True
                and origin['mutable_worktree_inodes_disjoint'] is True,
                'complete immutable origin receipt')
        driver = source/self.config['driver_relative_path']
        require(driver.resolve(strict=True)==Path(__file__).resolve(strict=True),
                'execute the immutable published driver source')
        bind(driver,self.config['driver_sha256'])
        self.source_before = self.source_guard()
        build_path = bind(self.config['normal_example_build_receipt'],
                          self.config['normal_example_build_receipt_sha256'])
        build = json.loads(build_path.read_text())
        require(build['source_sha'] == actual and build['exit_code'] == 0
                and build['artifact_kind'] == 'normal-example-executable'
                and 'build' in build['command'] and '--example' in build['command']
                and 'oidc_live_probe' in build['command'], 'normal daemon build proof')
        self.broker_executable = bind(build['executable'], build['executable_sha256'])
        issuer_path = bind(source / self.config['issuer_relative_path'],
                           self.config['issuer_sha256'])
        spec = importlib.util.spec_from_file_location('frozen_independent_issuer', issuer_path)
        issuer = importlib.util.module_from_spec(spec)
        sys.dont_write_bytecode = True
        spec.loader.exec_module(issuer)
        issuer_directory = self.private/'issuer'
        issuer.prepare(issuer_directory)
        self.server = issuer.Server(issuer_directory)
        self.issuer_thread = threading.Thread(target=self.server.serve_forever,
                                              kwargs={'poll_interval':0.1}, name='issuer-owner')
        self.issuer_thread.start()
        self.issuer_started = True
        self.issuer = self.server.state.issuer
        self.trust = ssl.create_default_context(cafile=str(issuer_directory/'ca.pem'))
        for command in (
            ['openssl','x509','-in',str(issuer_directory/'leaf.pem'),'-outform','DER',
             '-out',str(issuer_directory/'leaf.der')],
            ['openssl','pkcs8','-topk8','-nocrypt','-in',str(issuer_directory/'leaf.key'),
             '-outform','DER','-out',str(issuer_directory/'leaf.key.der')],
        ):
            result = subprocess.run(command, capture_output=True, timeout=10)
            require(result.returncode == 0, 'ephemeral TLS conversion')
        with socket.socket() as port:
            port.bind(('127.0.0.1',0))
            broker_port = port.getsockname()[1]
        self.bootstrap = 'localhost:'+str(broker_port)
        profile = self.config['profile']
        require(profile in ('metadata','read-write'), 'finite real API profile')
        self.stop_file = self.private/'stop'
        broker_config = {
            'state_dir':str(self.private/'broker-state'),'stop_file':str(self.stop_file),
            'broker_port':broker_port,'profile':profile,'maximum_runtime_secs':240,
            'issuer':self.issuer,'jwks_origin':self.issuer.rsplit('/issuer',1)[0],
            'audience':'partitionline','client_id':'partitionline-probe',
            'introspection_endpoint':self.issuer+'/introspect',
            'client_secret_file':str(issuer_directory/'client-secret'),
            'issuer_ca_der_file':str(issuer_directory/'ca.der'),
            'broker_certificate_der_file':str(issuer_directory/'leaf.der'),
            'broker_key_der_file':str(issuer_directory/'leaf.key.der'),
            'key_freshness_ms':3000,'key_refresh_ms':200,'revocation_lease_ms':500,
            'revocation_refresh_ms':100,'http_timeout_ms':1000,
        }
        self.broker_config = self.private/'broker.json'
        self.broker_config.write_text(json.dumps(broker_config)+'\n')
        self.start_broker()

    def start_broker(self):
        self.stop_file.unlink(missing_ok=True)
        self.broker = OwnedProcess('broker', [str(self.broker_executable),str(self.broker_config)],
                                   self.receipt, self.deadline,self.register_process)
        ready = self.broker.await_event('ready')
        require(ready['broker'] == self.bootstrap and ready['session_lifetime_ms'] == 0,
                'actual endpoint and reconnect lifetime')
        expected = [3,17,18,19,20,36] if self.config['profile']=='metadata' else [0,1,2,3,17,18,19,20,36]
        require(ready['api_keys'] == expected, 'actual finite advertisement')

    def control(self, value):
        with self.server.state.lock:
            self.control_boundary = len(self.server.state.events)
        secret = (self.private/'issuer'/'control-secret').read_text()
        authorization = base64.b64encode(('control:'+secret).encode()).decode()
        request = urllib.request.Request(self.issuer.rsplit('/issuer',1)[0]+'/control',
                    json.dumps(value).encode(), {'Authorization':'Basic '+authorization,
                    'Content-Type':'application/json'}, method='POST')
        with urllib.request.urlopen(request, context=self.trust, timeout=3) as response:
            require(response.status == 200 and json.loads(response.read(4097)) == {'changed':True},
                    'actual authenticated HTTPS control')
        self.receipt('driver', {'event':'issuer-control', **value})

    def start_peer(self, identity, expected_ready=True, witness=None):
        require(identity not in self.peers, 'unique active peer')
        with self.server.state.lock:
            self.peer_issue_boundary[identity] = set(self.server.state.issued)
            self.peer_generation[identity] = self.server.state.generation
        peer = next(row for row in self.config['peers'] if row['id']==identity)
        require(peer['kind'] in ('java','native','rust'), 'genuine peer kind')
        for row in peer['runtime_inputs']:
            bind(row['path'], row['sha256'])
        require(peer['runtime_inputs'] and peer['source_inputs'], 'complete peer runtime/source bindings')
        for row in peer['source_inputs']:
            bind(Path(self.config['source_root'])/row['path'],row['sha256'])
        build_path = bind(peer['build_receipt'],peer['build_receipt_sha256'])
        build = json.loads(build_path.read_text())
        require(build['source_sha']==self.config['source_sha'] and build['exit_code']==0
                and build['runtime_inputs']==peer['runtime_inputs'],
                'exact peer compiler/runtime receipt')
        issuer_directory = self.private/'issuer'
        common = {'bootstrap':self.bootstrap,'token_url':self.issuer+'/token',
                  'client_id':'partitionline-probe','client_secret_file':str(issuer_directory/'client-secret'),
                  'ca_pem':str(issuer_directory/'ca.pem'),'topic':'oidc-probe','max_runtime_seconds':180}
        config_path = self.private/(identity+'.properties' if peer['kind']=='java' else identity+'.json')
        if peer['kind']=='java':
            common.update(release=peer['release'], issuer=self.issuer, scope='kafka')
            config_path.write_text(''.join(key+'='+str(value)+'\n' for key,value in common.items()))
        else:
            config_path.write_text(json.dumps(common)+'\n')
        values = {'config':str(config_path),'bootstrap':self.bootstrap,
                  'ca_pem':common['ca_pem'],'token_url':common['token_url'],
                  'client_secret_file':common['client_secret_file']}
        argv = [argument.format_map(values) for argument in peer['command']]
        require(len(argv) <= 64 and all(len(argument) <= 32768 for argument in argv), 'bounded argv')
        process = OwnedProcess(identity, argv, self.receipt, self.deadline,self.register_process)
        self.peers[identity] = (peer, process)
        if expected_ready:
            process.await_event('ready')
        else:
            # A real official login constructor may fail before creating its
            # client objects. This is a separately predeclared acquisition
            # failure, rather than a fabricated metadata operation verdict.
            event = process.await_event('fatal', seconds=40)
            self.negative(identity,peer,event,witness,acquisition=True)
            process.join(expected_code=1)
            del self.peers[identity]

    def negative(self, identity, peer, event, witness, acquisition=False):
        require(identity in self.positive_peers, 'negative case follows same configured positive peer')
        require(len(self.peers)==1, 'negative attribution has exactly one active public peer')
        require(isinstance(witness,dict), 'predeclared causal authority witness')
        if peer['kind']=='java':
            error_types = event.get('error_types',[])
            permitted = {'org.apache.kafka.common.errors.SaslAuthenticationException',
                         'org.apache.kafka.common.errors.AuthenticationException',
                         'org.apache.kafka.common.errors.TimeoutException',
                         'java.io.IOException','java.net.ConnectException',
                         'java.net.SocketTimeoutException',
                         'javax.security.auth.login.LoginException',
                         'org.apache.kafka.common.security.oauthbearer.JwtValidatorException',
                         'org.apache.kafka.common.security.oauthbearer.internals.unsecured.OAuthBearerIllegalTokenException'}
            require(any(error_type in permitted for error_type in error_types),
                    'safe Java authentication/transport/provider failure class')
        elif peer['kind']=='native':
            require(event.get('code') in (-195,-185,-169,58),
                    'safe native transport/timeout/authentication code')
        else:
            require(event.get('error_category') in ('io','timeout','client-protocol','broker-authentication'),
                    'safe Rust public error category')
        with self.server.state.lock:
            events = self.server.state.events[self.control_boundary:].copy()
            outage = self.server.state.outage.copy()
            issued = self.server.state.issued.copy()
            revoked = self.server.state.revoked.copy()
            issued_metadata = getattr(self.server.state,'issued_metadata',{}).copy()
        condition = witness.get('condition')
        if condition=='outage':
            route = witness['route']
            require(route in outage and any(row['endpoint']==route and row['status']==503 for row in events),
                    'actual declared HTTPS authority outage response')
        elif condition=='revoked':
            require(issued and set(issued)<=revoked and '/issuer/token' in outage,
                    'all actual issued proofs revoked and replacement acquisition blocked')
            require(any(row['endpoint']=='/issuer/introspect' and row['status']==200 for row in events),
                    'actual revocation refresh HTTP observation')
        elif condition=='expired':
            proofs = self.peer_proofs.get(identity,set())
            require(proofs and all(issued[token_hash]['exp']<=int(time.time()) for token_hash in proofs)
                    and '/issuer/token' in outage,
                    'actual active-peer proof expiry with acquisition blocked')
        elif condition=='key-removed':
            require(self.server.state.generation==1 and '/issuer/token' in outage
                    and self.peer_generation.get(identity)==0
                    and any(row['endpoint']=='/issuer/keys' and row['status']==200 for row in events),
                    'actual replacement JWKS fetched with new acquisition blocked')
        elif condition=='signed-policy':
            hashes = set(issued)-self.peer_issue_boundary[identity]
            policy = witness['policy']
            require(policy in ('wrong_issuer','wrong_audience','expired','future_nbf',
                              'wrong_typ','missing_typ','missing_subject')
                    and hashes and all(issued_metadata.get(token_hash,{}).get('policy')==policy
                                       for token_hash in hashes),
                    'independently signed active-peer negative schema')
            require(any(row['endpoint']=='/issuer/token' and row['status']==200 for row in events),
                    'actual HTTPS negative signed token acquisition')
        else:
            raise RuntimeError('unknown causal authority condition')
        require(not acquisition or condition=='signed-policy'
                or (condition=='outage' and witness['route']=='/issuer/token'),
                'constructor failure is explicitly acquisition/policy condition')
        broker_rejection = (event.get('error_category')=='broker-authentication'
            or event.get('code') in (-169,58)
            or 'org.apache.kafka.common.errors.SaslAuthenticationException' in event.get('error_types',[]))
        layer = 'broker-authentication' if broker_rejection else 'client-or-transport'
        if witness.get('expected_rejection_layer') is not None:
            require(witness['expected_rejection_layer']==layer, 'predeclared rejection layer')
        self.pending_recovery.add(identity)
        self.receipt('driver',{'event':'negative-causal-witness','peer':identity,
            'condition':condition,'acquisition_before_ready':acquisition,
            'observed_rejection_layer':layer,
            'classification':'public failure with actual controlled authority condition; precise socket lease semantics are separately tested'})

    def operation(self, step):
        peer, process = self.peers[step['peer']]
        action, phase, expected = step['action'], step['phase'], step['expected_pass']
        require(re.fullmatch('[a-z0-9_-]{1,48}',phase), 'synthetic phase')
        if peer['kind']=='native':
            require(action == 'query', 'native callback metadata scope')
            process.send('METADATA '+('pass' if expected else 'fail'))
            event = process.await_event('metadata')
            passed = event['accepted']
        else:
            require(action in ('query','recreate','write','read'), 'public operator action')
            process.send(action+' '+phase)
            kind = {'query':'query','recreate':'query','write':'produce','read':'fetch'}[action]
            event = process.await_event(kind, phase, seconds=40)
            passed = event['passed']
        require(passed is expected, 'predeclared SDK operation verdict')
        if not passed:
            self.negative(step['peer'],peer,event,step.get('authority_witness'))
        else:
            self.positive_peers.add(step['peer'])
            with self.server.state.lock:
                self.peer_proofs[step['peer']] = set(self.server.state.issued)-self.peer_issue_boundary[step['peer']]
            if step['peer'] in self.pending_recovery:
                self.pending_recovery.remove(step['peer'])
                self.receipt('driver',{'event':'positive-recovery','peer':step['peer'],'phase':phase})
        if action == 'write' and passed:
            require(self.config['profile']=='read-write', 'real record profile selected')
            require(event['offset']==len(self.history) and event['partition']==0, 'exact acknowledged offset')
            release = peer.get('release','rust')
            key = ('oauth-'+release+'-'+phase).encode().hex()
            value = ('public-java-'+release if peer['kind']=='java' else 'public-rust-oidc').encode().hex()
            require(event['key_hex']==key and event['value_hex']==value, 'predeclared public record ID')
            self.history.append({'offset':len(self.history),'timestamp':1700000000123,
                'timestamp_type':'CREATE_TIME','key_hex':key,'value_hex':value,
                'headers':[{'name':'peer','value_hex':release.encode().hex()},
                           {'name':'d','value_hex':None},{'name':'d','value_hex':''}]})
        if action == 'read' and passed:
            require(event['log_start']==0 and event['log_end']==len(self.history)
                    and event['records']==self.history, 'every public returned field and offset')

    def stop_peer(self, identity):
        peer, process = self.peers.pop(identity)
        process.send('STOP' if peer['kind']=='native' else 'close shutdown')
        process.await_event('joined' if peer['kind']=='native' else 'shutdown')
        process.join()

    def stop_broker(self):
        self.stop_file.touch(mode=0o600)
        joined = self.broker.await_event('joined', seconds=30)
        require(joined['accepted']==joined['joined'] and joined['worker_failures']==0,
                'all broker connection workers joined')
        self.broker.join()
        self.broker = None

    def run_steps(self):
        for ordinal, step in enumerate(self.config['steps']):
            self.failed_step = ordinal
            self.failure_stage = 'predeclared-lifecycle-step'
            require(time.monotonic() < self.deadline, 'absolute live history deadline')
            action = step['operation']
            if action=='start-peer':
                self.start_peer(step['peer'], step.get('expected_ready',True),step.get('authority_witness'))
            elif action=='stop-peer':
                self.stop_peer(step['peer'])
            elif action=='send':
                self.operation(step)
            elif action=='control':
                self.control(step['value'])
            elif action=='revoke-issued':
                with self.server.state.lock:
                    hashes = list(self.server.state.issued)
                require(hashes, 'actual issued token before revocation')
                for token_hash in hashes:
                    self.control({'revoke_sha256':token_hash})
            elif action=='wait':
                seconds = step['seconds']
                require(type(seconds) in (int,float) and 0<=seconds<=10, 'finite explicit authority wait')
                require(time.monotonic()+seconds < self.deadline, 'wait within absolute deadline')
                time.sleep(seconds)
            elif action=='restart-broker':
                require(not self.peers, 'restart after client joins')
                self.stop_broker()
                self.start_broker()
            else:
                raise RuntimeError('unknown predeclared lifecycle step')
            self.steps.append({'ordinal':ordinal,'operation':action,'passed':True})
        self.failed_step = None
        self.failure_stage = None

    def bounded_cleanup(self, name, function, seconds):
        result = []
        def invoke():
            try:
                function()
                result.append(True)
            except BaseException:
                result.append(False)
        thread = threading.Thread(target=invoke,name=name)
        self.cleanup_threads.append(thread)
        try:
            thread.start()
        except BaseException:
            return False
        thread.join(timeout=seconds)
        # A timed-out OS/third-party join is an explicit failure. The outer
        # 300s process-group owner must then terminate the failed driver.
        return not thread.is_alive() and result==[True]

    def finalize(self, passed):
        cleanup = []
        # Attempt every owned resource even when an earlier close fails.
        for identity in list(self.peers):
            try:
                self.stop_peer(identity)
                cleanup.append({'owner':identity,'joined':True})
            except BaseException:
                cleanup.append({'owner':identity,'joined':False})
        if self.broker is not None:
            try:
                self.stop_broker()
                cleanup.append({'owner':'broker','joined':True})
            except BaseException:
                cleanup.append({'owner':'broker','joined':False})
        for ordinal, process in enumerate(self.processes):
            try:
                process.abort()
                cleanup.append({'owner':process.identity+'-process-'+str(ordinal),
                                'joined':not process.forced,
                                'forced_close':process.forced})
            except BaseException:
                cleanup.append({'owner':process.identity+'-process-'+str(ordinal),'joined':False})
        if self.server is not None:
            issuer_ok = True
            if self.issuer_started:
                issuer_ok = self.bounded_cleanup('issuer-stop',self.server.shutdown,5)
            issuer_ok = self.bounded_cleanup('issuer-close',self.server.server_close,20) and issuer_ok
            if self.issuer_started:
                self.issuer_thread.join(timeout=5)
            cleanup.append({'owner':'issuer','joined':issuer_ok
                and (not self.issuer_started or not self.issuer_thread.is_alive())
                and not any(thread.is_alive() for thread in self.cleanup_threads)})
            with self.server.state.lock:
                issuer_events = self.server.state.events.copy()
                issued = [{'token_sha256':token_hash,'issued_epoch':claims['iat'],
                           'expires_epoch':claims['exp'],'subject':claims.get('sub'),
                           'policy':getattr(self.server.state,'issued_metadata',{}).get(token_hash,{}).get('policy'),
                           'generation':getattr(self.server.state,'issued_metadata',{}).get(token_hash,{}).get('generation')}
                          for token_hash,claims in self.server.state.issued.items()]
            (self.output/'issuer-events.json').write_text(json.dumps(issuer_events,indent=2)+'\n')
            (self.output/'issued-public-metadata.json').write_text(json.dumps(issued,indent=2)+'\n')
        source_unchanged = False
        if self.source_before is not None:
            try:
                source_after = self.source_guard()
                source_unchanged = source_after==self.source_before
                manifest = json.dumps({'source_sha':self.config['source_sha'],
                    'identical_before_after':source_unchanged,'files':self.source_before},
                    separators=(',',':')).encode()+b'\n'
                import gzip
                compressed = gzip.compress(manifest,mtime=0)
                (self.output/'complete-source-identity.json.gz').write_bytes(compressed)
                restore = {'original_path':'complete-source-identity.json',
                           'original_bytes':len(manifest),'original_mode':'0o600',
                           'original_sha256':hashlib.sha256(manifest).hexdigest(),
                           'compressed_path':'complete-source-identity.json.gz',
                           'compressed_sha256':hashlib.sha256(compressed).hexdigest(),
                           'roundtrip_verified':gzip.decompress(compressed)==manifest}
                (self.output/'complete-source-identity.restore.json').write_text(json.dumps(restore,indent=2)+'\n')
                self.receipt('driver',{'event':'complete-source-identity','file_count':len(self.source_before),
                    'before_after_identical':source_unchanged,'raw_sha256':hashlib.sha256(manifest).hexdigest()})
            except BaseException:
                source_unchanged = False
        validation = {'source_sha':self.config['source_sha'],'passed':passed and source_unchanged and not self.pending_recovery and all(row['joined'] for row in cleanup),
                      'complete_source_unchanged':source_unchanged,
                      'unresolved_negative_recovery_peers':sorted(self.pending_recovery),
                      'failed_step':self.failed_step,'failure_stage':self.failure_stage,
                      'phase_dispatch_denial_proved':False,
                      'audit_limit':'Independent stdout readers do not create a synchronized phase barrier. Retained complete joined audit totals are distinct from precise denial proven by source-pinned core socket tests.',
                      'steps':self.steps,'cleanup':cleanup,'ordinary_expected_history':self.history,
                      'scope':'real token acquisition/socket metadata and explicitly selected ordinary RF1 records; reconnect lifetime0'}
        (self.output/'events.json').write_text(json.dumps(self.records,indent=2)+'\n')
        (self.output/'validation.json').write_text(json.dumps(validation,indent=2)+'\n')
        return validation['passed']


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--config',type=Path,required=True)
    parser.add_argument('--output',type=Path,required=True)
    parser.add_argument('--private-scratch',type=Path,required=True)
    args = parser.parse_args()
    child_setup()
    signal.signal(signal.SIGTERM,interruption)
    signal.signal(signal.SIGINT,interruption)
    require(args.config.stat().st_size <= 65536, 'finite lifecycle configuration')
    config = json.loads(args.config.read_text())
    run = Run(config,args.output,args.private_scratch)
    passed = False
    try:
        run.failure_stage = 'immutable-source-runtime-and-authority-setup'
        run.setup()
        run.run_steps()
        passed = True
    except BaseException:
        # No arbitrary SDK/token/provider or exception data is persisted.
        pass
    finally:
        require(run.output.exists(), 'output setup completed')
        passed = run.finalize(passed)
    raise SystemExit(0 if passed else 1)


if __name__=='__main__':
    main()
