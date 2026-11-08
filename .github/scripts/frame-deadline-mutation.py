from pathlib import Path
import sys

path = Path("partitionline-broker/src/transport.rs")
text = path.read_text()
start = text.index("async fn read_exact_spare<")
end = text.index("\nenum Ahead", start)
section = text[start:end]
mode = sys.argv[1]

if mode == "deadline":
    before = """        if Instant::now() >= deadline {
            return Err(Exit::ReadDeadline);
        }
"""
    final = """    if Instant::now() >= deadline {
        Err(Exit::ReadDeadline)
    } else {
        Ok(())
    }
"""
    if section.count(before) != 1 or section.count(final) != 1:
        raise SystemExit("deadline mutation anchors did not match exactly once")
    section = section.replace(before, "", 1).replace(final, "    Ok(())\n", 1)
elif mode == "yield":
    needle = "            tokio::task::yield_now().await;\n"
    if section.count(needle) != 1:
        raise SystemExit("yield mutation anchor did not match exactly once")
    section = section.replace(needle, "", 1)
else:
    raise SystemExit("expected deadline or yield")
path.write_text(text[:start] + section + text[end:])
print("installed", mode, "mutant")
