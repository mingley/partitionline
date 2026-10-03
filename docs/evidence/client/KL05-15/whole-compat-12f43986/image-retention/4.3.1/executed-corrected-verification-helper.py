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

out = Path(sys.argv[1])
shutil.copyfile(__file__, out / 'executed-corrected-verification-helper.py')
original = json.loads((out / 'original-image-identity.json').read_text())
archive = out / 'docker-save.tar.gz'
env = os.environ.copy()
for key in ['DOCKER_CONTEXT', 'DOCKER_TLS', 'DOCKER_TLS_VERIFY', 'DOCKER_CERT_PATH']:
    env.pop(key, None)
env['DOCKER_HOST'] = 'unix:///var/run/docker.sock'
live = json.loads(subprocess.check_output(['docker', 'image', 'inspect', original['Id']], env=env))[0]
assert live['Id'] == original['Id'] and live['RootFS'] == original['RootFS'] and live['RepoDigests'] == original['RepoDigests']
containers = subprocess.check_output(['docker', 'container', 'ls', '-a', '--filter', 'ancestor=' + original['Id'], '--format', '{{.ID}}'], env=env).decode().split()
assert not containers, containers
raw_sha = hashlib.sha256()
raw_bytes = 0
with gzip.open(archive, 'rb') as stream:
    while block := stream.read(1024 * 1024):
        raw_sha.update(block)
        raw_bytes += len(block)
members = {}
documents = {}
with tarfile.open(archive, 'r|gz') as tar:
    for member in tar:
        if not member.isfile():
            continue
        stream = tar.extractfile(member)
        digest = hashlib.sha256()
        data = [] if member.size <= 1024 * 1024 else None
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
assert members[config_name]['sha256'] == original['Id'].removeprefix('sha256:')
assert documents[config_name]['rootfs']['diff_ids'] == original['RootFS']['Layers']
layer_names = manifest[0]['Layers']
assert len(layer_names) == len(original['RootFS']['Layers'])
for name, expected in zip(layer_names, original['RootFS']['Layers']):
    assert members[name]['sha256'] == expected.removeprefix('sha256:'), name
receipt = {'schema_version': 1, 'captured_at': datetime.datetime.now(datetime.timezone.utc).isoformat(), 'image_id': original['Id'], 'repo_digests': original['RepoDigests'], 'os': original['Os'], 'architecture': original['Architecture'], 'logical_image_bytes': original['Size'], 'archive_path': str(archive), 'archive_bytes': archive.stat().st_size, 'archive_sha256': hashlib.sha256(archive.read_bytes()).hexdigest(), 'exact_raw_docker_save_bytes': raw_bytes, 'exact_raw_docker_save_sha256': raw_sha.hexdigest(), 'gzip_full_stream_crc_and_size_verified': True, 'original_gzip_roundtrip_comparison_passed_before_first_verifier_config_suffix_failure': True, 'original_gzip_roundtrip_proof': 'Retained executed-retention-helper.py completed raw tarstream hash+size comparison before KeyError shown in retention-4.3.1-command.log. Corrected verifier independently reads the unchanged full archive and checks every image layer.', 'image_config_sha_equals_image_id': True, 'all_layer_tar_hashes_equal_rootfs_diff_ids': True, 'layer_count': len(layer_names), 'archive_members': members, 'active_or_stopped_container_references': containers, 'free_bytes_after_validation': shutil.disk_usage('/workspace').free, 'restore_argv': ['docker', 'image', 'load', '--input', str(archive)], 'restore_scope': 'Load restores exact image configuration and every layer tar. Original RepoDigest identity is retained separately; archive load alone may not restore registry RepoDigest metadata. Any future genuine qualification must independently recheck registry identity.', 'deletion_performed': False}
(out / 'retention.json').write_text(json.dumps(receipt, indent=2) + '\n')
print(json.dumps({k: v for k, v in receipt.items() if k != 'archive_members'}), flush=True)
