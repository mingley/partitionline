from pathlib import Path
import sys

path = Path("partitionline-broker/tests/common/raft_runtime.rs")
text = path.read_text()
mode = sys.argv[1]
if mode == "identity":
    old = "            if term == *want_term\n"
    new = "            if term != 0\n"
elif mode == "bound":
    old = "    const MAX_ENTRIES: usize = 64;\n    const MAX_DETAILS: usize = 7 * 1024;\n"
    new = "    const MAX_ENTRIES: usize = 10000;\n    const MAX_DETAILS: usize = 1024 * 1024;\n"
elif mode == "force-image":
    condition = "    if image_bytes <= 64 * 1024 || base != committed {\n"
    assertion = '    assert!(image_bytes > 64 * 1024, "real transfer must exceed 64KiB");\n'
    if text.count(condition) != 1 or text.count(assertion) != 1:
        raise SystemExit("force-image anchors did not match exactly once")
    text = text.replace(condition, "    if true || image_bytes <= 64 * 1024 || base != committed {\n", 1)
    text = text.replace(assertion, '    assert!(false, "synthetic forced observer diagnostic");\n', 1)
    path.write_text(text)
    print("installed observer diagnostic mutation", mode)
    raise SystemExit(0)
else:
    raise SystemExit("expected identity, bound, or force-image")
if text.count(old) != 1:
    raise SystemExit("mutation anchor did not match exactly once: " + mode)
path.write_text(text.replace(old, new, 1))
print("installed observer diagnostic mutation", mode)
