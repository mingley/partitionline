#!/usr/bin/env python3
"""Build the pinned Apache SDK peer with a full JDK; --offline uses verified jars."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import urllib.request

HERE = Path(__file__).resolve().parent
ROOT = HERE.parents[2]

def sha(path):
    return hashlib.sha256(Path(path).read_bytes()).hexdigest()

def environment():
    env = os.environ.copy()
    for key in ('JAVA_TOOL_OPTIONS', 'JDK_JAVA_OPTIONS', '_JAVA_OPTIONS', 'CLASSPATH',
                'LD_PRELOAD', 'LD_LIBRARY_PATH'):
        env.pop(key, None)
    env['TZ'] = 'UTC'
    return env

def sources():
    files = ['BenchmarkPeer.java', 'Build.java', 'build.py', 'run.py', 'source-pin.json']
    paths = [HERE / file for file in files]
    paths.append(HERE.parent / 'librdkafka/run.py')
    return {str(path.relative_to(ROOT)): sha(path) for path in paths}

def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    parser.add_argument('--cache', type=Path, required=True)
    parser.add_argument('--offline', action='store_true')
    args = parser.parse_args()
    pin = json.loads((HERE / 'source-pin.json').read_text())
    args.output.mkdir(parents=True, exist_ok=False)
    bindings = sources()
    for name in bindings:
        destination = args.output / 'executed-source' / name
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(ROOT / name, destination)
    with (args.output / 'build-inputs.json').open('x') as file:
        json.dump(dict(sources=bindings, pins=pin, offline=args.offline), file, indent=2)
        file.write('\n')
    java = Path(shutil.which('java') or 'missing-java').resolve()
    env = environment()
    version = subprocess.check_output([str(java), '-XshowSettings:properties', '-version'],
                                     env=env, stderr=subprocess.STDOUT, text=True)
    actual = next(line.split('=', 1)[1].strip() for line in version.splitlines()
                  if line.strip().startswith('java.version ='))
    (args.output / 'toolchain.txt').write_text(version)
    if actual != pin['java_version']:
        raise ValueError(f"JDK {pin['java_version']} required; found {actual}")
    args.cache.mkdir(parents=True, exist_ok=True)
    jars = []
    for dependency in pin['dependencies']:
        filename = Path(dependency['path']).name
        cached = args.cache / filename
        if not cached.exists():
            if args.offline:
                raise ValueError(f'missing offline jar: {filename}')
            # Download to a disposable file; never replace a cache artifact.
            with tempfile.NamedTemporaryFile(dir=args.cache, delete=False) as tmp:
                temporary = Path(tmp.name)
            try:
                urllib.request.urlretrieve('https://repo.maven.apache.org/maven2/' + dependency['path'], temporary)
                if sha(temporary) != dependency['sha256']:
                    raise ValueError(f'download hash mismatch: {filename}')
                with cached.open('xb') as output:
                    output.write(temporary.read_bytes())
            finally:
                temporary.unlink(missing_ok=True)
        if sha(cached) != dependency['sha256']:
            raise ValueError(f'jar hash mismatch: {filename}')
        destination = args.output / filename
        shutil.copyfile(cached, destination)
        jars.append(destination.resolve())
    archive = args.output / 'java-peer.jar'
    command = [str(java), str(HERE / 'Build.java'), str(HERE / 'BenchmarkPeer.java'),
               os.pathsep.join(map(str, jars)), str(args.output / 'classes'), str(archive)]
    subprocess.run(command, env=env, check=True, timeout=120)
    if sources() != bindings:
        raise ValueError('sources changed during build')
    manifest = dict(version=1, tag=pin['tag'], java_version=actual, compiler=version.strip(),
                    build_tool='Pinned JDK 21 JavaCompiler -source 21 -target 21; deterministic JarOutputStream',
                    binary_sha256=sha(archive), sources=bindings, java_path=str(java),
                    java_sha256=sha(java), dependencies=pin['dependencies'], command=command,
                    hermetic_os_image=False)
    with (args.output / 'build-manifest.json').open('x') as file:
        json.dump(manifest, file, indent=2); file.write('\n')
    print(json.dumps({'jar': str(archive), 'sha256': sha(archive), 'java_version': actual}))

if __name__ == '__main__':
    main()
