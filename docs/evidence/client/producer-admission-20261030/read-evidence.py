"""Read or verify content-addressed producer qualification evidence."""
import hashlib
import json
import sys
import tarfile
from pathlib import Path

archive_path = Path(sys.argv[1])
name = sys.argv[2]
with tarfile.open(archive_path, "r:gz") as archive:
    with archive.extractfile("files.json") as source:
        files = json.load(source)["files"]
    selected = files.items() if name == "--verify" else [(name, files[name])]
    checked = {}
    for path, record in selected:
        object_path = record["object"]
        actual = checked.get(object_path)
        if actual is None:
            with archive.extractfile(object_path) as source:
                data = source.read()
            actual = (len(data), hashlib.sha256(data).hexdigest())
            checked[object_path] = actual
        if actual != (record["bytes"], record["sha256"]):
            raise RuntimeError("evidence bytes differ: " + path)
        if name != "--verify":
            sys.stdout.buffer.write(data)
    if name == "--verify":
        print(json.dumps(dict(virtual_files=len(files), verified_objects=len(checked))))
