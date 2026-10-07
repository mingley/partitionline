#!/usr/bin/env python3
"""Fail-closed native Windows MSVC runtime and packed-consumer qualification."""
import hashlib
import importlib.util
import json
import os
import platform
import re
import shutil
import struct
import sys
from pathlib import Path

SPEC = importlib.util.spec_from_file_location('platform_report_base', Path(__file__).with_name('report-macos-arm64.py'))
BASE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(BASE)


def validate_versions(data):
    if (data['system'], data['machine'], data['pointer_bits'], data['rustc_host']) != (
            'Windows', 'AMD64', 64, 'x86_64-pc-windows-msvc'):
        raise ValueError('native Windows x86_64 MSVC compiler/runtime required')
    if data['requested_toolchain'] != 'stable':
        raise ValueError('unqualified Rust toolchain')
    if not re.fullmatch(r'1\.\d+\.\d+', data['rustc_release']) or tuple(map(int, data['rustc_release'].split('.'))) < (1, 99, 0):
        raise ValueError('latest stable Rust required')
    if tuple(map(int, data['python_version'].split('.')[:2])) < (3, 11):
        raise ValueError('Python 3.11+ package-check prerequisite missing')
    if data['python_utf8_mode'] != 1:
        raise ValueError('native Python UTF-8 mode required for packaged Markdown')
    if int(data['bash_version'].split('.')[0]) < 5:
        raise ValueError('Bash 5+ package-check prerequisite missing')
    if not data['openssl_version'].startswith('OpenSSL 3.'):
        raise ValueError('OpenSSL 3 certificate-tool prerequisite missing')
    if not re.fullmatch(r'[0-9a-f]{40}', data['source_sha']):
        raise ValueError('source commit missing')
    if not data['windows_version'] or not data['openssl_executable'] or not data['bash_executable']:
        raise ValueError('exact platform or certificate-tool identity missing')


def capture(toolchain):
    bash_executable = os.environ.get('PL_WINDOWS_BASH_EXECUTABLE')
    if not bash_executable:
        raise ValueError('driver-selected native Git Bash executable missing')
    rustc = BASE.command('rustc', '-vV')
    fields = dict(line.split(': ', 1) for line in rustc.splitlines() if ': ' in line)
    data = {
        'system': platform.system(), 'machine': platform.machine(),
        'pointer_bits': struct.calcsize('P') * 8,
        'windows_release': platform.release(), 'windows_version': platform.version(),
        'python_version': platform.python_version(),
        'python_utf8_mode': sys.flags.utf8_mode,
        # Windows PATH can resolve bare bash to the WSL launcher. Use the exact
        # native Git Bash executable selected by the running driver instead.
        'bash_version': BASE.command(bash_executable, '-c', 'printf "%s" "$BASH_VERSION"'),
        'bash_executable': bash_executable,
        'requested_toolchain': toolchain, 'rustc_verbose': rustc,
        'rustc_release': fields['release'], 'rustc_host': fields['host'],
        'cargo_version': BASE.command('cargo', '--version'),
        'openssl_version': BASE.command('openssl', 'version'),
        'openssl_executable': shutil.which('openssl'),
        'source_sha': BASE.command('git', 'rev-parse', 'HEAD'),
    }
    validate_versions(data)
    return data


def artifact_name(versions, suffix):
    label = (f"windows-{versions['requested_toolchain']}-rust{versions['rustc_release']}"
             f"-os{versions['windows_version']}-ssl{versions['openssl_version'].split()[1]}"
             f"-py{versions['python_version']}-utf8{versions['python_utf8_mode']}-bash{versions['bash_version'].split('(')[0]}-{suffix}")
    return re.sub(r'[^A-Za-z0-9_.-]', '_', label)


def finish(directory):
    versions = json.loads((directory / 'versions.json').read_text())
    validate_versions(versions)
    runtime = {mode: BASE.test_counts((directory / f'{mode}-tests.log').read_text())
               for mode in ('default', 'tracing')}
    package = json.loads((directory / 'package/summary.json').read_text())
    snippets = BASE.validate_package(package, versions)
    docs_output = (directory / 'docs.log').read_text()
    doctests = BASE.summary_counts(docs_output)
    if doctests['ignored'] or 'ci-docs: ok (strict rustdoc warnings denied; doctests passed)' not in docs_output:
        raise ValueError('strict documentation gate incomplete')
    if versions['requested_toolchain'] == 'stable' and not (directory / 'clippy.log').read_text().strip():
        raise ValueError('stable Clippy log missing')
    for name, expected in package['operator_lock_sha256'].items():
        if Path(name).name != name:
            raise ValueError('invalid operator lock path')
        if hashlib.sha256((directory / 'package' / name).read_bytes()).hexdigest() != expected:
            raise ValueError('operator lock checksum mismatch')
    return {'status': 'passed', 'versions': versions, 'runtime': runtime,
            'doctests': doctests, 'packaged_snippets': snippets, 'operator_cells': 2,
            'package_sha256': package['package_sha256'],
            'artifact_name': artifact_name(versions, f"tests{runtime['default']['passed']}-{runtime['tracing']['passed']}-snips{snippets}"),
            'limits': ['Native x86_64 Windows MSVC mock/runtime and package qualification only.',
                       'Live broker, auth-service and performance campaigns remain Linux lanes.',
                       'Pre-existing opt-in live tests remain explicitly reported as ignored.',
                       'Latest stable Rust runs strict Clippy, docs and the declared package feature matrix.']}


def main():
    try:
        if len(sys.argv) == 5 and sys.argv[1] == 'diagnose':
            lines = Path(sys.argv[2]).read_text(errors='replace').splitlines()
            errors = [line for line in lines if re.search(r'error(?:\[|:)|doc-examples:|package-docs:|ci-crate-consumer:|Traceback|Error:', line)]
            detail = (errors[-1] if errors else (lines[-1] if lines else 'no output'))[:800]
            detail = re.sub(r'https?://[^\s]+', '<url>', detail)
            message = f'{sys.argv[3]} exited {int(sys.argv[4])}: {detail}'
            if os.environ.get('GITHUB_ACTIONS') == 'true':
                escaped = message.replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A')
                print('::error title=Windows qualification phase::'+escaped)
            else:
                print(message, file=sys.stderr)
            return 0
        if len(sys.argv) not in (3, 4):
            raise ValueError('capture <report-dir> <toolchain> | finish <report-dir>')
        mode, directory = sys.argv[1], Path(sys.argv[2])
        if mode == 'capture' and len(sys.argv) == 4:
            report = capture(sys.argv[3])
            report['artifact_name'] = artifact_name(report, 'partial-'+report['source_sha'])
            name = 'versions.json'
        elif mode == 'finish' and len(sys.argv) == 3:
            report, name = finish(directory), 'report.json'
        else:
            raise ValueError('invalid report mode')
        directory.mkdir(parents=True, exist_ok=True)
        (directory / name).write_text(json.dumps(report, indent=2)+'\n')
        print(json.dumps(report, indent=2))
        return 0
    except (ValueError, KeyError, TypeError, OSError, BASE.subprocess.CalledProcessError) as error:
        print('Windows qualification: '+str(error), file=sys.stderr)
        if os.environ.get('GITHUB_ACTIONS') == 'true':
            message = str(error).replace('%', '%25').replace('\r', '%0D').replace('\n', '%0A')
            print('::error title=Windows qualification report::'+message)
        return 1


if __name__ == '__main__':
    sys.exit(main())
