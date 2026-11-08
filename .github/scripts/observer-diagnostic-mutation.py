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
    old = """    let (base_term, base, image_bytes) = cluster.response(
        leader,
        "observer-prefix-checkpoint",
        image_response(&checkpoint),
    )?;
    if image_bytes <= 64 * 1024 || base != committed {
"""
    new = """    let (base_term, base, _image_bytes) = cluster.response(
        leader,
        "observer-prefix-checkpoint",
        image_response(&checkpoint),
    )?;
    // Diagnostic-only mutation of the reported size after a real checkpoint.
    let image_bytes = 0_u64;
    if image_bytes <= 64 * 1024 || base != committed {
"""
else:
    raise SystemExit("expected identity, bound, or force-image")
if text.count(old) != 1:
    raise SystemExit("mutation anchor did not match exactly once: " + mode)
path.write_text(text.replace(old, new, 1))
print("installed observer diagnostic mutation", mode)
