#!/usr/bin/env python3
"""Run the clean client example build and default all-target matrix with resource receipts."""
import importlib.util
import sys

spec = importlib.util.spec_from_file_location('qualification_runner', '/workspace/work/broker-full-validation-runner/run.py')
r = importlib.util.module_from_spec(spec)
spec.loader.exec_module(r)
expected = '53b5af47fd18898a68364d9b88edc181c225bfe4'
if r.git('rev-parse', 'HEAD') != expected:
    raise SystemExit('refusing unexpected HEAD')
r.CASES = [
    ('build-verifiable-examples', ['build', '--locked', '--example', 'verifiable_producer', '--example', 'verifiable_consumer']),
    ('default-all-targets', ['test', '--locked', '--all-targets', '--no-fail-fast']),
]
sys.exit(r.main())
