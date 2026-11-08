#!/usr/bin/env python3
"""Temporary counterexamples for the observed transport-deadline fixture."""
from pathlib import Path
import sys


def once(value, before, after):
    count = value.count(before)
    if count != 1:
        raise AssertionError(f"expected one hunk, got {count}: {before[:80]!r}")
    return value.replace(before, after)


mode = sys.argv[1]
if mode == "before-admission":
    path = Path("partitionline-broker/tests/fetch.rs")
    value = path.read_text()
    before = "            let is_fetch = request.get(..2) == Some(&[0, 1]);\n            let inner ="
    after = (
        "            let is_fetch = request.get(..2) == Some(&[0, 1]);\n"
        "            if is_fetch {\n"
        "                self.admitted.send_replace(true);\n"
        "                std::future::pending::<()>().await;\n"
        "            }\n"
        "            let inner ="
    )
    path.write_text(once(value, before, after))
elif mode == "leaked-permit":
    path = Path("partitionline-broker/src/metadata.rs")
    value = path.read_text()
    before = "type DataSender = (oneshot::Sender<DataCompletion>, OwnedSemaphorePermit);"
    after = """type DataSender = (oneshot::Sender<DataCompletion>, DataPermit);
struct DataPermit(Option<OwnedSemaphorePermit>);
impl Drop for DataPermit {
    fn drop(&mut self) {
        if let Some(permit) = self.0.take() {
            permit.forget();
        }
    }
}
"""
    value = once(value, before, after)
    value = once(value, "    permit: OwnedSemaphorePermit,\n}", "    permit: DataPermit,\n}")
    before = """            let permit = Arc::clone(&self.data_slots)
                .try_acquire_owned()
                .map_err(|_| Error::QueueFull)?;
            let mut lease = DataLease {"""
    after = """            let permit = Arc::clone(&self.data_slots)
                .try_acquire_owned()
                .map_err(|_| Error::QueueFull)?;
            let permit = DataPermit(Some(permit));
            let mut lease = DataLease {"""
    value = once(value, before, after)
    path.write_text(value)
else:
    raise ValueError(mode)
print(f"installed {mode} counterexample")
