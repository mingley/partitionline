#!/usr/bin/env python3
"""Check docs/navigation and the license/exclusion inventory in an extracted .crate."""
import argparse
import hashlib
import importlib.util
import json
import re
import sys
import tomllib
from pathlib import Path
from urllib.parse import urlsplit

SPEC = importlib.util.spec_from_file_location('doc_examples', Path(__file__).with_name('check-doc-examples.py'))
DOC = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(DOC)


def check(root):
    root = root.resolve()
    manifest = tomllib.loads((root / 'Cargo.toml').read_text())['package']
    files = sorted(p.relative_to(root).as_posix() for p in root.rglob('*') if p.is_file())
    forbidden = ('benchmarks/', 'fuzz/', 'docs/evidence/', 'docs/plan/', 'docs/audits/', '.git/', '.aws/', '.codex/')
    for name in files:
        if name.startswith(forbidden) or '.env' in Path(name).parts:
            raise DOC.CheckError(f'excluded repository/private material in package: {name}')
    licenses = {}
    if manifest.get('license') != 'MIT OR Apache-2.0':
        raise DOC.CheckError('unexpected package license expression')
    for name in ('LICENSE-MIT', 'LICENSE-APACHE', 'NOTICE'):
        data = (root / name).read_bytes()
        if not data.strip():
            raise DOC.CheckError(f'empty required license/notice: {name}')
        licenses[name] = hashlib.sha256(data).hexdigest()
    documents = [root / 'README.md', *sorted((root / 'docs').glob('*.md'))]
    links = DOC.check_links(root, documents)
    pinned = []
    for document in documents:
        for line, target in DOC.link_targets(document.read_text()):
            parsed = urlsplit(target)
            if parsed.netloc == 'github.com' and parsed.path.startswith('/mingley/partitionline/'):
                match = re.match(r'/mingley/partitionline/(blob|tree)/([^/]+)/', parsed.path)
                if match:
                    if not re.fullmatch(r'[0-9a-f]{40}|v\d+\.\d+\.\d+(?:[-+][A-Za-z0-9.-]+)?', match[2]):
                        raise DOC.CheckError(f'{document}:{line}: repository docs require an immutable commit or version tag')
                    pinned.append(target)
            if parsed.netloc == 'raw.githubusercontent.com' and parsed.path.startswith('/mingley/partitionline/'):
                ref = parsed.path.split('/')[3]
                if not re.fullmatch(r'[0-9a-f]{40}|v\d+\.\d+\.\d+(?:[-+][A-Za-z0-9.-]+)?', ref):
                    raise DOC.CheckError(f'{document}:{line}: raw repository docs require a version pin')
                pinned.append(target)
    return {'package_name': manifest['name'], 'package_version': manifest['version'],
            'license': manifest['license'], 'license_notice_sha256': licenses,
            'file_count': len(files), 'unpacked_file_bytes': sum((root / p).stat().st_size for p in files),
            'documents': [p.relative_to(root).as_posix() for p in documents],
            'local_links': links, 'versioned_repository_links': sorted(set(pinned)),
            'excluded_prefixes': list(forbidden), 'files': files}


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('package', type=Path)
    parser.add_argument('--report', type=Path)
    args = parser.parse_args(argv)
    try:
        report = check(args.package)
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2) + '\n')
        print(f'package-docs: {len(report["documents"])} documents, {report["local_links"]} local links, {len(report["versioned_repository_links"])} pinned repository targets; license and exclusion inventory passed')
        return 0
    except (DOC.CheckError, OSError, ValueError, KeyError, IndexError) as error:
        print(f'package-docs: FAIL: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
