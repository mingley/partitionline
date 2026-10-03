from pathlib import Path
import datetime
import gzip
import hashlib
import json
import os
import shutil
import subprocess
import sys
import tarfile

reference, output_name = sys.argv[1:]
out = Path(output_name)
out.mkdir(parents=True, exist_ok=False)
shutil.copyfile(__file__, out / 'executed-retention-helper.py')
env = os.environ.copy()
for key in ['DOCKER_CONTEXT', 'DOCKER_TLS', 'DOCKER_TLS_VERIFY', 'DOCKER_CERT_PATH']:
    env.pop(key, None)
env['DOCKER_HOST'] = 'unix:///var/run/docker.sock'
image = json.loads(subprocess.check_output(['docker', 'image', 'inspect', reference], env=env))[0]
(out / 'original-image-identity.json').write_text(json.dumps(image, indent=2) + '\n')
containers = subprocess.check_output(['docker', 'container', 'ls', '-a', '--filter', 'ancestor=' + image['Id'], '--format', '{{.ID}}'], env=env).decode().split()
assert not containers, containers
assert shutil.disk_usage('/workspace').free >= image['Size'] + 750 * 1024 * 1024
archive = out / 'docker-save.tar.gz'
raw_sha = hashlib.sha256()
raw_bytes = 0
minimum = shutil.disk_usage('/workspace').free
with (out / 'docker-save.stderr.log').open('wb') as stderr:
    process = subprocess.Popen(['docker', 'image', 'save', reference], env=env, stdout=subprocess.PIPE, stderr=stderr)
    with archive.open('wb') as destination, gzip.GzipFile(fileobj=destination, mode='wb', compresslevel=3, mtime=0, filename='') as compressed:
        while True:
            block = process.stdout.read(1024 * 1024)
            if not block:
                break
            minimum = min(minimum, shutil.disk_usage('/workspace').free)
            assert minimum >= 350 * 1024 * 1024
            raw_sha.update(block)
            raw_bytes += len(block)
            compressed.write(block)
    result = process.wait()
assert result == 0
verify_sha = hashlib.sha256()
verify_bytes = 0
with gzip.open(archive, 'rb') as stream:
    while block := stream.read(1024 * 1024):
        verify_sha.update(block)
        verify_bytes += len(block)
assert verify_sha.digest() == raw_sha.digest() and verify_bytes == raw_bytes
members = {}
documents = {}
with tarfile.open(archive, 'r|gz') as tar:
    for member in tar:
        if not member.isfile():
            continue
        stream = tar.extractfile(member)
        digest = hashlib.sha256()
        data = [] if member.name.endswith('.json') or member.name.endswith('/json') else None
        count = 0
        while block := stream.read(1024 * 1024):
            digest.update(block)
            count += len(block)
            if data is not None:
                data.append(block)
        members[member.name] = {'sha256': digest.hexdigest(), 'bytes': count, 'tar_mode': member.mode, 'tar_uid': member.uid, 'tar_gid': member.gid}
        if data is not None:
            try:
                documents[member.name] = json.loads(b''.join(data))
            except ValueError:
                pass
manifest = documents['manifest.json']
assert len(manifest) == 1
config_name = manifest[0]['Config']
assert members[config_name]['sha256'] == image['Id'].removeprefix('sha256:')
config = documents[config_name]
assert config['rootfs']['diff_ids'] == image['RootFS']['Layers']
layer_names = manifest[0]['Layers']
assert len(layer_names) == len(image['RootFS']['Layers'])
for name, expected in zip(layer_names, image['RootFS']['Layers']):
    assert members[name]['sha256'] == expected.removeprefix('sha256:'), name
receipt = {'schema_version': 1, 'captured_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'original_reference': reference, 'image_id': image['Id'], 'repo_digests': image['RepoDigests'], 'os': image['Os'], 'architecture': image['Architecture'], 'logical_image_bytes': image['Size'], 'archive_path': str(archive), 'archive_bytes': archive.stat().st_size, 'archive_sha256': hashlib.sha256(archive.read_bytes()).hexdigest(), 'exact_raw_docker_save_bytes': raw_bytes, 'exact_raw_docker_save_sha256': raw_sha.hexdigest(), 'gzip_roundtrip_bytes_and_hash_verified': True, 'image_config_sha_equals_image_id': True, 'all_layer_tar_hashes_equal_rootfs_diff_ids': True, 'layer_count': len(layer_names), 'archive_members': members, 'active_or_stopped_container_references': containers, 'minimum_observed_free_bytes': minimum, 'restore_argv': ['docker', 'image', 'load', '--input', str(archive)], 'restore_scope': 'The Docker save archive preserves image configuration and every exact layer tar. Original RepoDigest identity is retained separately; archive load alone may not restore registry RepoDigest metadata. Any future genuine qualification must recheck registry identity independently.', 'deletion_performed': False}
(out / 'retention.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps({k: v for k, v in receipt.items() if k != 'archive_members'}), flush=True)
