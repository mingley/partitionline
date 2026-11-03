#!/usr/bin/env python3
"""Check retained evidence, and optionally reconstruct the test-only patch."""
import argparse
import hashlib
import json
from pathlib import Path
import subprocess
import tarfile
import tempfile

HERE = Path(__file__).resolve().parent


def digest(data):
    return hashlib.sha256(data).hexdigest()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--source-root', type=Path, help='reconstructed v16/v17 fixed source root')
    parser.add_argument('--catalog', type=Path, help='v16/v17 fixed-inputs.json')
    args = parser.parse_args()
    if bool(args.source_root) != bool(args.catalog):
        parser.error('--source-root and --catalog must be supplied together')
    receipt = json.loads((HERE / 'receipt.json').read_text())
    pins = json.loads((HERE / 'pins.json').read_text())
    v2 = json.loads((HERE / 'v2-receipt.json').read_text())
    v1 = json.loads((HERE / 'v1-receipt.json').read_text())
    assert digest((HERE / 'v2-evidence.tar.gz').read_bytes()) == receipt['v2_archive_sha256']
    assert digest((HERE / 'v2-receipt.json').read_bytes()) == receipt['v2_receipt_sha256']
    assert digest((HERE / 'v1-evidence.tar.gz').read_bytes()) == v2['v1_archive_sha256']
    assert digest((HERE / 'v1-receipt.json').read_bytes()) == v2['v1_receipt_sha256']
    for version, prior in (('v1', v1), ('v2', v2)):
        with tarfile.open(HERE / (version + '-evidence.tar.gz'), 'r:gz') as archived:
            assert digest(archived.extractfile('wire-controls.patch').read()) == prior['patch_sha256']
    assert digest((HERE / 'evidence.tar.gz').read_bytes()) == receipt['archive_sha256']
    assert digest((HERE / 'wire-controls.patch').read_bytes()) == receipt['patch_sha256'] == pins['patch_sha256']
    assert digest((HERE / 'pins.json').read_bytes()) == receipt['pins_sha256']
    assert digest((HERE / 'README.md').read_bytes()) == receipt['readme_sha256']
    with tarfile.open(HERE / 'evidence.tar.gz', 'r:gz') as archive:
        names = archive.getnames()
        assert len(names) == len(set(names)) and set(names) == set(receipt['files'])
        for name in names:
            data = archive.extractfile(name).read()
            pin = receipt['files'][name]
            assert digest(data) == pin['sha256'] and len(data) == pin['bytes'], name
        for name in ('README.md', 'pins.json', 'wire-controls.patch'):
            assert archive.extractfile(name).read() == (HERE / name).read_bytes(), name
    status = 'retained archive and patch verified'
    if args.source_root:
        catalog_bytes = args.catalog.read_bytes()
        assert digest(catalog_bytes) == pins['fixed_catalog_sha256']
        catalog = json.loads(catalog_bytes)
        for name, expected in pins['source_sha256'].items():
            path = args.source_root / name
            assert not path.is_symlink() and path.is_file() and digest(path.read_bytes()) == expected == catalog[name], name
        with tempfile.TemporaryDirectory(prefix='producer-wire-import-') as temp:
            temp = Path(temp)
            for name in ('src/net.rs', 'src/producer/prepare_tests.rs'):
                dst = temp / name
                dst.parent.mkdir(parents=True, exist_ok=True)
                dst.write_bytes((args.source_root / name).read_bytes())
            patch = HERE / 'wire-controls.patch'
            for flags in (['--check'], [], ['--reverse', '--check'], ['--reverse']):
                subprocess.run(['git', 'apply', *flags, str(patch)], cwd=temp, check=True, capture_output=True)
                if not flags:
                    for name, expected in pins['after_sha256'].items():
                        assert digest((temp / name).read_bytes()) == expected, name
                    assert (temp / 'src/producer/prepare_tests.rs').read_bytes().startswith(
                        (args.source_root / 'src/producer/prepare_tests.rs').read_bytes())
            for name in ('src/net.rs', 'src/producer/prepare_tests.rs'):
                assert (temp / name).read_bytes() == (args.source_root / name).read_bytes()
            assert not (temp / 'src/net/test_gated_tcp.rs').exists()
        status = 'archive, exact source pins, test prefix and patch forward/reverse verified'
    print(json.dumps({'result': status, 'limits': 'No Rust compile, native test, socket transfer or benchmark was run.'}, indent=2))


if __name__ == '__main__':
    main()
