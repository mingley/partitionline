#!/usr/bin/env python3
"""Inject test-only reservation faults into a throwaway qualification checkout."""
import hashlib
import pathlib
import sys

path = pathlib.Path("src/producer.rs")
original = path.read_text()
digest = hashlib.sha256(original.encode()).hexdigest()
expected = "3d65fcceecbb43c1a974f7f6b7bd58c8a310930f6bcced9052a00e88700eb37f"
if digest != expected:
    raise SystemExit(f"unexpected original producer SHA-256: {digest}")

needle = """impl Drop for BufferReservation<'_> {
    fn drop(&mut self) {
        if self.active {
            self.shared.release_buffer(self.bytes);
        }
    }
}"""
replacement = """impl Drop for BufferReservation<'_> {
    fn drop(&mut self) {
        if self.active {
            if self.bytes == 113 {
                match std::env::var("PL_RETAINED_NEGATIVE").as_deref() {
                    Ok("omit") => return,
                    Ok("double") => {
                        self.shared.release_buffer(self.bytes);
                        self.shared.release_buffer(self.bytes);
                        return;
                    }
                    _ => {}
                }
            }
            self.shared.release_buffer(self.bytes);
        }
    }
}"""
if original.count(needle) != 1:
    raise SystemExit("reservation Drop anchor was not unique")
path.write_text(original.replace(needle, replacement, 1))
print("NEGATIVE_SOURCE_SHA256", hashlib.sha256(path.read_bytes()).hexdigest())
print("The injected fault is qualification-only and must not be merged.")
