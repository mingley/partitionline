from pathlib import Path

path = Path("partitionline-broker/tests/operator_quorum.rs")
text = path.read_text()
old = "    std::str::from_utf8(message)\n        .map(Some)\n"
new = "    std::str::from_utf8(message)\n        .map(|_| None)\n"
if text.count(old) != 1:
    raise SystemExit("decoder mutation anchor did not match exactly once")
path.write_text(text.replace(old, new))
print("installed synthetic message-loss mutation")
