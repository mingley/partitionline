#!/usr/bin/env python3
"""Compile registered Markdown Rust fences against a .crate; check local links.

Fences use rustdoc's hidden '# ' setup and standard no_run syntax. All Rust
fences compile with no_run: this gate never starts a broker-dependent program.
An intentional non-compiling fence must say rust,ignore and give a reason in
an adjacent <!-- doc-example-ignore: reason --> comment. Ignored blocks are
reported separately. Registration selects documents, not copied snippets or
paragraph text, so new Rust fences in those documents join the gate automatically.
"""
import argparse
import hashlib
import html
import json
import re
import subprocess
import sys
import tarfile
import tempfile
import tomllib
from pathlib import Path
from urllib.parse import unquote, urlsplit


class CheckError(Exception):
    """A documentation validation failure."""


def fences(text):
    """Yield (line, info, actual body, preceding text) for fenced blocks."""
    lines = text.splitlines(keepends=True)
    index = 0
    while index < len(lines):
        opening = re.match(r'^ {0,3}(`{3,}|~{3,})([^\n]*)\n?$', lines[index])
        if not opening:
            index += 1
            continue
        delimiter, info = opening.groups()
        start = index
        index += 1
        while index < len(lines) and not re.match(
            rf'^ {{0,3}}{re.escape(delimiter[0])}{{{len(delimiter)},}}\s*$', lines[index]
        ):
            index += 1
        if index == len(lines):
            raise CheckError(f'unclosed fence at line {start + 1}')
        yield start + 1, info.strip(), ''.join(lines[start + 1:index]), ''.join(lines[max(0, start - 3):start])
        index += 1


def snippets(path):
    selected, ignored = [], []
    for line, info, body, preceding in fences(path.read_text()):
        flags = [flag.strip() for flag in re.split(r'[,\s]+', info) if flag.strip()]
        if not flags or flags[0] not in ('rust', 'rs'):
            continue
        if 'ignore' in flags:
            reason = re.search(r'<!--\s*doc-example-ignore:\s*(.*?)\s*-->', preceding)
            if not reason or not reason.group(1).strip():
                raise CheckError(f'{path}:{line}: ignored Rust fence needs doc-example-ignore reason')
            ignored.append({'line': line, 'reason': reason.group(1)})
            continue
        if any(flag not in ('rust', 'rs', 'no_run', 'edition2021') for flag in flags):
            raise CheckError(f'{path}:{line}: unsupported Rust fence flags: {info}')
        selected.append({'line': line, 'body': body, 'sha256': hashlib.sha256(body.encode()).hexdigest()})
    return selected, ignored


def outside_fences(text):
    """Mask code blocks while preserving line numbers for diagnostics."""
    lines = text.splitlines(keepends=True)
    for line, _, body, _ in fences(text):
        end = line + len(body.splitlines()) + 1
        for index in range(line - 1, end):
            lines[index] = '\n'
    return ''.join(lines)


def anchors(path):
    text = outside_fences(path.read_text())
    found = set(re.findall(r'<a\s+(?:id|name)=["\']([^"\']+)["\']', text, flags=re.I))
    counts = {}
    for line in text.splitlines():
        match = re.match(r'^ {0,3}#{1,6}\s+(.+?)\s*#*\s*$', line)
        if not match:
            continue
        heading = html.unescape(re.sub(r'<[^>]*>', '', match.group(1)))
        heading = re.sub(r'\[([^]]+)\]\([^)]*\)', r'\1', heading)
        slug = re.sub(r'[^\w\- ]', '', heading.lower()).replace(' ', '-')
        count = counts.get(slug, 0)
        counts[slug] = count + 1
        found.add(slug if count == 0 else f'{slug}-{count}')
    return found


def link_targets(text):
    """Inline links/images and reference definitions outside code fences."""
    text = outside_fences(text)
    # Do not treat literal Markdown in inline code as navigation.
    text = re.sub(r'(`+).*?\1', lambda m: ' ' * len(m.group(0)), text)
    pattern = r'!?\[[^]\n]*\]\(\s*(<[^>\n]+>|[^\s()]+(?:\([^\s()]*\)[^\s()]*)*)(?:\s+["\'][^\n]*?["\'])?\s*\)'
    for match in re.finditer(pattern, text):
        yield text.count('\n', 0, match.start()) + 1, match.group(1).strip('<>')
    for match in re.finditer(r'^ {0,3}\[[^]\n]+\]:\s*(<[^>\n]+>|\S+)', text, re.M):
        yield text.count('\n', 0, match.start()) + 1, match.group(1).strip('<>')


def check_links(root, documents):
    root = root.resolve()
    count = 0
    for document in documents:
        for line, target in link_targets(document.read_text()):
            parsed = urlsplit(target)
            if parsed.scheme or parsed.netloc:
                continue  # This gate checks local navigation; no external requests.
            destination = (root / unquote(parsed.path).lstrip('/') if parsed.path.startswith('/')
                           else document.parent / unquote(parsed.path)) if parsed.path else document
            destination = destination.resolve()
            if not destination.is_relative_to(root) or not destination.is_file():
                raise CheckError(f'{document}:{line}: missing local file: {target}')
            if parsed.fragment and destination.suffix.lower() == '.md':
                if unquote(parsed.fragment) not in anchors(destination):
                    raise CheckError(f'{document}:{line}: missing local anchor: {target}')
            count += 1
    return count


def rustdoc_source(selected):
    """Use the exact Markdown body; rustdoc handles hidden setup and main wrapping."""
    blocks = []
    for document, snippet in selected:
        blocks.append(f'{document}:{snippet["line"]}\n\n```rust,no_run\n{snippet["body"]}```\n')
    markdown = '\n'.join(blocks)
    hashes = '#'
    while '"' + hashes in markdown:
        hashes += '#'
    return '#![doc = r' + hashes + '"' + markdown + '"' + hashes + ']\n' 


def run(command, cwd=None):
    result = subprocess.run(command, cwd=cwd, text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT)
    if result.returncode:
        raise CheckError(f'command failed ({result.returncode}): {" ".join(map(str, command))}\n{result.stdout}')
    print(result.stdout, end='')
    return result.stdout


def compile_against_package(archive, selected, work, feature_mode="tracing", toolchain=None):
    package = work / 'package'
    package.mkdir()
    with tarfile.open(archive) as packed:
        packed.extractall(package, filter='data')
    manifests = list(package.glob('*/Cargo.toml'))
    if len(manifests) != 1:
        raise CheckError('packed crate must contain exactly one top-level Cargo.toml')
    consumer = work / 'consumer'
    (consumer / 'src').mkdir(parents=True)
    package_info = tomllib.loads(manifests[0].read_text())
    feature_table = package_info.get('features', {})
    defaults = set(feature_table.get('default', []))
    declared = set(feature_table) - {'default'}
    # Keep every optional feature explicit in the packaged consumer matrix.
    if not defaults.issubset({'zlib-rs'}) or not (declared - defaults).issubset({'tracing', 'zstd'}):
        raise CheckError('packaged optional-feature matrix needs an explicit update')
    features = [] if feature_mode == 'default' else (sorted(declared - defaults) if feature_mode == 'all' else [feature_mode])
    if not set(features).issubset(declared - defaults):
        raise CheckError('requested optional feature is not declared by the package')
    cargo = ['cargo'] + ([f'+{toolchain}'] if toolchain else [])
    dependency = json.dumps(str(manifests[0].parent))
    (consumer / 'Cargo.toml').write_text(f'''[package]
name = "partitionline-markdown-check"
version = "0.0.0"
edition = "2021"
[workspace]
[dependencies]
partitionline = {{ path = {dependency}, features = {json.dumps(features)} }}
tokio = {{ version = "1", features = ["macros", "rt-multi-thread", "time"] }}
''')
    (consumer / 'src/lib.rs').write_text(rustdoc_source(selected))
    manifest = str(consumer / 'Cargo.toml')
    run(cargo + ['generate-lockfile', '--manifest-path', manifest])
    output = run(cargo + ['test', '--locked', '--doc', '--manifest-path', manifest])
    counts = re.findall(r'test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;', output)
    if len(counts) != 1 or tuple(map(int, counts[0])) != (len(selected), 0, 0):
        raise CheckError(f'rustdoc did not compile all {len(selected)} registered snippets without skips: {counts}')
    vcs = manifests[0].parent / '.cargo_vcs_info.json'
    return {
        'package_name': package_info['package']['name'],
        'package_version': package_info['package']['version'],
        'package_vcs_info': json.loads(vcs.read_text()) if vcs.exists() else None,
        'feature_mode': feature_mode, 'dependency_features': features,
        'toolchain': toolchain or 'current',
        'rustc': run(['rustc'] + ([f'+{toolchain}'] if toolchain else []) + ['--version']).strip(),
        'consumer_lock_sha256': hashlib.sha256((consumer / 'Cargo.lock').read_bytes()).hexdigest(),
    }



def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--root', type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument('--registry', type=Path, default=Path('tests/docs/registry.json'))
    parser.add_argument('--crate', type=Path, help='already-built .crate; otherwise cargo package --locked --no-verify')
    parser.add_argument('--allow-dirty', action='store_true', help='local development packaging only')
    parser.add_argument('--links-only', action='store_true')
    parser.add_argument('--report', type=Path)
    parser.add_argument('--features', choices=('default', 'tracing', 'zstd', 'all'), default='tracing')
    parser.add_argument('--toolchain', help='installed Rust toolchain; otherwise current')
    args = parser.parse_args(argv)
    root = args.root.resolve()
    try:
        registry = json.loads((root / args.registry).read_text())
        documents = [(root / name).resolve() for name in registry['documents']]
        if not documents or any(not path.is_relative_to(root) for path in documents):
            raise CheckError('registry must select at least one document inside the repository')
        selected, ignored = [], []
        for path in documents:
            blocks, skipped = snippets(path)
            selected.extend((str(path.relative_to(root)), block) for block in blocks)
            ignored.extend({'document': str(path.relative_to(root)), **block} for block in skipped)
        if not selected:
            raise CheckError('registry selected zero Rust snippets')
        link_count = check_links(root, documents)
        report = {'documents': registry['documents'], 'compiled_snippets': 0, 'local_links': link_count,
                  'ignored_snippets': ignored, 'snippets': [{'document': name, 'line': block['line'], 'sha256': block['sha256']} for name, block in selected]}
        if not args.links_only:
            archive = args.crate.resolve() if args.crate else None
            if archive is None:
                command = ['cargo', 'package', '--locked', '--no-verify']
                if args.allow_dirty:
                    command.append('--allow-dirty')
                run(command, root)
                metadata = json.loads(subprocess.check_output(['cargo', 'metadata', '--no-deps', '--format-version', '1'], cwd=root, text=True))
                package = tomllib.loads((root / 'Cargo.toml').read_text())['package']
                archive = Path(metadata['target_directory']) / 'package' / f'{package["name"]}-{package["version"]}.crate'
            report['package_sha256'] = hashlib.sha256(archive.read_bytes()).hexdigest()
            with tempfile.TemporaryDirectory(prefix='partitionline-markdown-') as directory:
                report.update(compile_against_package(archive, selected, Path(directory), args.features, args.toolchain))
            report['compiled_snippets'] = len(selected)
        if args.report:
            args.report.parent.mkdir(parents=True, exist_ok=True)
            args.report.write_text(json.dumps(report, indent=2) + '\n')
        print(f'doc-examples: {report["compiled_snippets"]} compiled, {link_count} local links checked, {len(ignored)} explicitly ignored')
        return 0
    except (CheckError, OSError, ValueError, KeyError, tarfile.TarError) as error:
        print(f'doc-examples: FAIL: {error}', file=sys.stderr)
        return 1


if __name__ == '__main__':
    sys.exit(main())
