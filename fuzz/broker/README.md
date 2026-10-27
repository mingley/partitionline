# Broker fuzz campaigns

The five targets exercise request headers and voter records, KRaft decoders,
controller responses, journal recovery and epoch-log recovery. Each target
uses the broker's bounded default-feature implementation. The controller
target keeps state between inputs on its single libFuzzer thread.

Build with the latest stable Rust on Linux x86_64:

```sh
python3 scripts/broker-fuzz-campaign.py build --output ../work/broker-fuzz-build
```

The build uses stable LLVM coverage instrumentation and the locked Cargo
dependencies. Its receipt records source hashes, compiler version, build
command and executable hashes. Changed inputs require a new build.

Check the supervisor with the actual instrumented executables:

```sh
PL_BROKER_FUZZ_BUILD=../work/broker-fuzz-build \
  python3 -B -m unittest discover -s tests/ci -p test_broker_fuzz_campaign.py -v
```

Run each target for at least 24 hours:

```sh
python3 scripts/broker-fuzz-campaign.py run \
  --build ../work/broker-fuzz-build --output ../work/broker-fuzz-day-01 \
  --kind campaign --seconds 86400
```

Run the command under a persistent supervisor when the terminal may close.
The runner writes worker PIDs, raw libFuzzer logs, evolving corpora, failure
inputs and resource samples. Workers have private temporary directories,
512 MiB RSS limits and two-second input deadlines. The supervisor caps disk
and file growth, joins every worker, and stops the attempt after a failure.
Workers are killed if their supervisor disappears.

Recovery fixture creation, writes, cleanup and limit setup must succeed. An
infrastructure error exits with status 86 and fails the attempt; it cannot
silently skip recovery while increasing the execution counter.

Inspect or stop an attempt:

```sh
python3 scripts/broker-fuzz-campaign.py audit --output ../work/broker-fuzz-day-01
touch ../work/broker-fuzz-day-01/STOP
```

A missing final receipt is incomplete. Smoke runs, interrupted runs and
changed artifacts cannot qualify a 24-hour campaign. A stopped attempt can
seed a new attempt with `--resume-from ../work/broker-fuzz-day-01`; use a new
output directory. Each attempt must complete the full duration independently.
Retain and minimize any failure before turning it into a regression.

Campaign qualification covers these parser and recovery targets. Broker
resource soaks, operational continuity, additional feature profiles and
comparative performance have separate qualification gates.
