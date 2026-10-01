#!/usr/bin/env python3
"""Retain native platform/toolchain and fail closed on incomplete qualification."""
import hashlib
import json
import os
import platform
import re
import subprocess
import sys
from pathlib import Path

REQUIRED_TESTS = (
    'tls_produce_fetch',
    'tls_rejects_wrong_hostname',
    'tls_mtls_allows_valid_client_identity',
    'tls_no_plaintext_fallback_and_handshake_deadline',
)


def command(*args):
    return subprocess.check_output(args, text=True).strip()


def validate_versions(data):
    if int(data['bash_version'].split('.')[0]) < 5:
        raise ValueError('Bash 5+ package-check prerequisite missing')
    if tuple(map(int, data['python_version'].split('.')[:2])) < (3, 11):
        raise ValueError('Python 3.11+ package-check prerequisite missing')
    if (data['system'], data['machine'], data['rustc_host']) != (
            'Darwin', 'arm64', 'aarch64-apple-darwin'):
        raise ValueError('native Darwin arm64 compiler/runtime required')
    if data['requested_toolchain'] not in ('stable', '1.85.0'):
        raise ValueError('unqualified toolchain')
    if data['requested_toolchain'] == '1.85.0' and data['rustc_release'] != '1.85.0':
        raise ValueError('MSRV compiler mismatch')
    if not re.fullmatch(r'[0-9a-f]{40}', data['source_sha']):
        raise ValueError('source commit missing')
    if not data['openssl_version'].startswith('OpenSSL 3.'):
        raise ValueError('OpenSSL 3 fixture prerequisite missing')


def capture(toolchain):
    rustc = command('rustc', '-vV')
    fields = dict(line.split(': ', 1) for line in rustc.splitlines() if ': ' in line)
    data = {
        'system': platform.system(), 'machine': platform.machine(),
        'macos_version': platform.mac_ver()[0],
        'python_version': platform.python_version(),
        'bash_version': command('bash', '-c', 'printf "%s" "$BASH_VERSION"'),
        'requested_toolchain': toolchain, 'rustc_verbose': rustc,
        'rustc_release': fields['release'], 'rustc_host': fields['host'],
        'cargo_version': command('cargo', '--version'),
        'openssl_version': command('openssl', 'version'),
        'source_sha': command('git', 'rev-parse', 'HEAD'),
    }
    validate_versions(data)
    return data


def summary_counts(output):
    matches = re.findall(
        r'^test result: (ok|FAILED)\. (\d+) passed; (\d+) failed; (\d+) ignored; '
        r'(\d+) measured; (\d+) filtered out;', output, re.M)
    if not matches:
        raise ValueError('test result summaries missing')
    if any(state != 'ok' or int(failed) or int(filtered)
           for state, _, failed, _, _, filtered in matches):
        raise ValueError('failed or filtered runtime tests')
    passed = sum(int(m[1]) for m in matches)
    if not passed:
        raise ValueError('zero passing tests')
    return {'passed': passed, 'failed': 0, 'suites': len(matches),
            'ignored': sum(int(m[3]) for m in matches),
            'ignored_lines': re.findall(r'^test .* \.\.\. ignored.*$', output, re.M)}


def test_counts(output):
    counts = summary_counts(output)
    for test in REQUIRED_TESTS:
        if not re.search(r'^test '+re.escape(test)+r' \.\.\. ok$', output, re.M):
            raise ValueError('required runtime test missing: '+test)
    return {**counts, 'required_runtime_tests': list(REQUIRED_TESTS)}


def validate_package(summary, versions):
    sha = versions['source_sha']
    vcs = summary['source_vcs_info']
    if vcs['git']['sha1'] != sha or vcs['git'].get('dirty', False):
        raise ValueError('package source is stale or dirty')
    cells = summary['feature_matrix']
    if len(cells) != 2 or {c['feature_mode'] for c in cells} != {'default', 'tracing'}:
        raise ValueError('approved package feature matrix incomplete')
    for cell in cells:
        if (cell['package_vcs_info'] != vcs or cell['package_sha256'] != summary['package_sha256']
                or cell['toolchain'] != versions['requested_toolchain']):
            raise ValueError('package cell provenance mismatch')
        if (cell['compiled_snippets'] <= 0 or cell['ignored_snippets']
                or cell['compiled_snippets'] != len(cell['snippets'])):
            raise ValueError('missing or skipped packaged snippets')
        if cell['dependency_features'] != ([] if cell['feature_mode'] == 'default' else ['tracing']):
            raise ValueError('unapproved feature combination')
        if not cell['rustc'].startswith('rustc '+versions['rustc_release']+' '):
            raise ValueError('packed consumer compiler mismatch')
    expected_locks = {f"operator-{versions['requested_toolchain']}-{mode}.lock"
                      for mode in ('default', 'tracing')}
    if set(summary['operator_lock_sha256']) != expected_locks:
        raise ValueError('operator consumer reports incomplete')
    return sum(c['compiled_snippets'] for c in cells)


def finish(directory):
    versions = json.loads((directory / 'versions.json').read_text())
    validate_versions(versions)
    counts = {mode: test_counts((directory / f'{mode}-tests.log').read_text())
              for mode in ('default', 'tracing')}
    summary = json.loads((directory / 'package/summary.json').read_text())
    snippets = validate_package(summary, versions)
    docs_output = (directory / 'docs.log').read_text()
    doctests = summary_counts(docs_output)
    if doctests['ignored'] or 'ci-docs: ok (strict rustdoc warnings denied; doctests passed)' not in docs_output:
        raise ValueError('strict documentation gate incomplete')
    if versions['requested_toolchain'] == 'stable' and not (directory / 'clippy.log').read_text().strip():
        raise ValueError('stable Clippy log missing')
    for name, expected in summary['operator_lock_sha256'].items():
        if Path(name).name != name:
            raise ValueError('invalid operator lock path')
        if hashlib.sha256((directory / 'package' / name).read_bytes()).hexdigest() != expected:
            raise ValueError('operator lock checksum mismatch')
    label = (f"macos-arm64-{versions['requested_toolchain']}-rust{versions['rustc_release']}"
             f"-os{versions['macos_version']}-ssl{versions['openssl_version'].split()[1]}"
             f"-tests{counts['default']['passed']}-{counts['tracing']['passed']}-snips{snippets}")
    return {'status': 'passed', 'versions': versions, 'runtime': counts, 'doctests': doctests,
            'packaged_snippets': snippets, 'operator_cells': 2,
            'package_sha256': summary['package_sha256'],
            'artifact_name': re.sub(r'[^A-Za-z0-9_.-]', '_', label),
            'limits': ['Native mock/runtime and package qualification; live brokers/performance remain Linux lanes.',
                       'Pre-existing opt-in live tests are reported as ignored, with no cross-platform live claim.',
                       'Clippy runs on stable; both Rust toolchains run default/tracing and strict documentation.']}


def diagnose(path, phase, code):
    lines = path.read_text(errors='replace').splitlines()
    errors = [line for line in lines if re.search(r'error(?:\[|:)|doc-examples:|package-docs:|Traceback|Error:', line)]
    detail = (errors[-1] if errors else (lines[-1] if lines else 'no output'))[:800]
    detail = re.sub(r'https?://[^\s]+', '<url>', detail)
    message = f'{phase} exited {code}: {detail}'
    if os.environ.get('GITHUB_ACTIONS') == 'true':
        escaped = message.replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A')
        print('::error title=macOS qualification phase::'+escaped)
    else:
        print(message, file=sys.stderr)


def main():
    try:
        if len(sys.argv) == 5 and sys.argv[1] == 'diagnose':
            diagnose(Path(sys.argv[2]), sys.argv[3], int(sys.argv[4]))
            return 0
        if len(sys.argv) not in (3, 4):
            raise ValueError('capture <report-dir> <toolchain> | finish <report-dir>')
        mode, directory = sys.argv[1], Path(sys.argv[2])
        if mode == 'capture' and len(sys.argv) == 4:
            report, name = capture(sys.argv[3]), 'versions.json'
        elif mode == 'finish' and len(sys.argv) == 3:
            report, name = finish(directory), 'report.json'
        else:
            raise ValueError('invalid report mode')
        directory.mkdir(parents=True, exist_ok=True)
        (directory / name).write_text(json.dumps(report, indent=2)+'\n')
        print(json.dumps(report, indent=2))
        return 0
    except (ValueError, KeyError, TypeError, OSError, subprocess.CalledProcessError) as exc:
        print('macos-arm64 qualification: '+str(exc), file=sys.stderr)
        if os.environ.get('GITHUB_ACTIONS') == 'true':
            message = str(exc).replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A')
            print('::error title=macOS qualification report::'+message)
        return 1


if __name__ == '__main__':
    sys.exit(main())
