#!/usr/bin/env python3
"""Extract Ir counts from iai-callgrind JSONL output (KL09-05).

Stdlib only. Reads the `--output-format=json` stream (one object per
line) and prints `<bench_id>\\t<Ir>` per line, where bench_id is
`<function_name>/<id>`.

Metric shapes (iai-callgrind-runner EitherOrBoth: left is new):
  {"Left": new}          fresh target dir, no previous run
  {"Both": [new, old]}   repeat run; the previous measurement rides along
  {"Right": old}         bench missing from this run (no new value)

Only Ir (instructions retired) is extracted; every other event is
ignored. A Right entry is an error: the bench disappeared and there
is nothing to compare.

Usage:
  python3 scripts/ci-perf-gate-extract.py [--self-test] [JSONL ...]
  --self-test runs the fixture suite and prints nothing on success.
"""

import json
import sys


def ir_of(metrics, bench_id):
    """Return the NEW Ir value, or raise LookupError."""
    if not isinstance(metrics, dict) or len(metrics) != 1:
        raise ValueError("expected one Ir measurement variant")
    if "Left" in metrics:
        count = metrics["Left"]
    elif "Both" in metrics:
        pair = metrics["Both"]
        if not isinstance(pair, list) or len(pair) != 2:
            raise ValueError("expected new and prior counts in Both measurement")
        count = pair[0]
    elif "Right" in metrics:
        raise LookupError(f"{bench_id}: no new measurement (Right-only entry)")
    else:
        raise ValueError("unknown Ir measurement variant")
    if type(count) is not int or count <= 0:
        raise ValueError("new Ir count must be a positive integer")
    return count


def extract(path):
    """Parse JSONL file; return {bench_id: Ir}. Fail closed on bad rows."""
    results = {}
    with open(path) as f:
        for lineno, line in enumerate(f, 1):
            line = line.strip()
            if not line.startswith("{"):
                continue
            try:
                d = json.loads(line)
                bid = f"{d['function_name']}/{d['id']}"
                if bid in results:
                    raise ValueError(f"{bid}: duplicate measurement")
                m = d["callgrind_summary"]["callgrind_run"]["total"]["summary"][
                    "Ir"
                ]["metrics"]
                results[bid] = ir_of(m, bid)
            except (KeyError, TypeError, ValueError) as e:
                raise ValueError(f"{path}:{lineno}: unparseable bench row: {e}")
    if not results:
        raise ValueError(f"{path}: no bench results parsed")
    return results


def self_test():
    """Fixture suite: Left vs Both vs Right vs malformed."""

    def row(fn, bid, metrics):
        return json.dumps(
            {
                "function_name": fn,
                "id": bid,
                "callgrind_summary": {
                    "callgrind_run": {
                        "total": {"summary": {"Ir": {"metrics": metrics}}}
                    }
                },
            }
        )

    import tempfile, os

    cases = [
        # (metrics, expected Ir or None for error)
        ({"Left": 100}, 100),
        ({"Both": [200, 150]}, 200),  # left is new; old must be ignored
        ({"Both": [150, 200]}, 150),  # improvement still reads new
        ({"Right": 300}, None),  # no new value: error
        ({"Left": 0}, None),
        ({"Left": "100"}, None),
        ({"Left": True}, None),
        ({"Left": 100, "Right": 300}, None),
        ({"Both": {"new": 100, "old": 200}}, None),
        ({"Both": [-1, 200]}, None),
    ]
    for i, (metrics, want) in enumerate(cases):
        fd, path = tempfile.mkstemp(suffix=".jsonl")
        try:
            with os.fdopen(fd, "w") as f:
                f.write("not json\n")
                f.write(row("bench_f", f"c{i}", metrics) + "\n")
            if want is None:
                try:
                    extract(path)
                except (LookupError, ValueError):
                    continue
                raise AssertionError(f"case {i}: expected error, got value")
            got = extract(path)
            assert got == {f"bench_f/c{i}": want}, f"case {i}: {got} != {want}"
        finally:
            os.unlink(path)
    with tempfile.TemporaryDirectory() as directory:
        from pathlib import Path

        duplicate = Path(directory) / "duplicate.jsonl"
        duplicate.write_text(
            row("bench_f", "same", {"Left": 500}) + "\n"
            + row("bench_f", "same", {"Left": 100}) + "\n",
            encoding="utf-8",
        )
        try:
            extract(duplicate)
        except ValueError as error:
            assert "duplicate measurement" in str(error), error
        else:
            raise AssertionError("duplicate measurement accepted")

    # malformed + empty inputs fail closed
    fd, path = tempfile.mkstemp(suffix=".jsonl")
    try:
        with os.fdopen(fd, "w") as f:
            f.write('{"function_name": "x"}\n')
        try:
            extract(path)
        except ValueError:
            pass
        else:
            raise AssertionError("malformed row accepted")
    finally:
        os.unlink(path)
    fd, path = tempfile.mkstemp(suffix=".jsonl")
    os.close(fd)
    try:
        try:
            extract(path)
        except ValueError:
            pass
        else:
            raise AssertionError("empty input accepted")
    finally:
        os.unlink(path)


def main(argv):
    if len(argv) == 2 and argv[1] == "--self-test":
        self_test()
        return 0
    if len(argv) < 2 or argv[1].startswith("-"):
        print(
            "usage: ci-perf-gate-extract.py [--self-test] JSONL ...",
            file=sys.stderr,
        )
        return 2
    try:
        for path in argv[1:]:
            for bid, ir in sorted(extract(path).items()):
                print(f"{bid}\t{ir}")
    except (ValueError, LookupError, OSError) as e:
        print(f"ci-perf-gate-extract: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv))
