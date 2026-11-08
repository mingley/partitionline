#!/usr/bin/env python3
"""Apply and instrument the narrow retry-budget candidate in a disposable checkout."""
from pathlib import Path
import hashlib

path = Path("src/producer.rs")
source = path.read_text()
start = source.index("    async fn add_txn_partitions(&self, groups:")
end = source.index("\n    fn park_batch(", start)
method = source[start:end]

def once(text, old, new):
    if text.count(old) != 1:
        raise SystemExit("repair anchor mismatch")
    return text.replace(old, new, 1)

method = once(
    method,
    "        let mut attempt = 0u32;\n        loop {",
    "        let mut attempt = 0u32;\n        let mut last_retriable = None;\n        loop {",
)
method = once(
    method,
    """                Err(error) => {
                    forget_added_txn_parts(&self.shared.txn_partitions, &added).await;
                    return Err(error);
                }
            };
            let mut payload""",
    """                Err(error) => {
                    if let Some(code) = last_retriable {
                        self.shared.apply_txn_disposition(
                            TxnErrorDisposition::Retriable,
                            Error::broker(code, "AddPartitionsToTxn"),
                        );
                    }
                    forget_added_txn_parts(&self.shared.txn_partitions, &added).await;
                    return Err(error);
                }
            };
            let mut payload""",
)
method = once(
    method,
    """                Err(e) => {
                    forget_added_txn_parts(&self.shared.txn_partitions, &added).await;
                    return Err(e);
                }
            };
            let err =""",
    """                Err(e) => {
                    if let (Error::Timeout, Some(code)) = (&e, last_retriable) {
                        self.shared.apply_txn_disposition(
                            TxnErrorDisposition::Retriable,
                            Error::broker(code, "AddPartitionsToTxn"),
                        );
                    }
                    forget_added_txn_parts(&self.shared.txn_partitions, &added).await;
                    return Err(e);
                }
            };
            let err =""",
)
method = once(
    method,
    "            {\n                attempt += 1;\n                txn_retry_sleep(&self.shared, attempt, deadline).await;",
    "            {\n                last_retriable = Some(err);\n"
    "                attempt += 1;\n                txn_retry_sleep(&self.shared, attempt, deadline).await;",
)
source = source[:start] + method + source[end:]
test_anchor = "#[cfg(test)]\nmod tests {\n    use super::*;\n"
test_import = (
    "#[cfg(test)]\nmod tests {\n    use super::*;\n"
    "    use crate::protocol::txn::decode_add_partitions_to_txn_response_cooperative;\n"
)
source = once(source, test_anchor, test_import)
digest = hashlib.sha256(source.encode()).hexdigest()
if digest != "b03577efa883ea0bd162c2af71db0618aa3d3bfd8e49819b6a55a788757a9fa8":
    raise SystemExit("repaired source hash mismatch: " + digest)
print("repaired uninstrumented source sha256:", digest)

start = source.index("    async fn add_txn_partitions(&self, groups:")
end = source.index("\n    fn park_batch(", start)
method = source[start:end]
a = method.index("            let remaining = match timeout_remaining(deadline)")
b = method.index("            let mut payload", a)
part = method[a:b]
part = once(
    part,
    '                            Error::broker(code, "AddPartitionsToTxn"),\n'
    '                        );\n'
    '                    }\n',
    '                            Error::broker(code, "AddPartitionsToTxn"),\n'
    '                        );\n'
    '                        #[cfg(test)]\n'
    '                        add_partitions_retry_deadline::after_disposition(&self.shared).await;\n'
    '                    }\n',
)
method = method[:a] + part + method[b:]
method = once(
    method,
    "                txn_retry_sleep(&self.shared, attempt, deadline).await;\n"
    "                if Instant::now() < deadline {\n"
    "                    continue;\n"
    "                }",
    "                txn_retry_sleep(&self.shared, attempt, deadline).await;\n"
    "                if Instant::now() < deadline {\n"
    "                    #[cfg(test)]\n"
    "                    add_partitions_retry_deadline::checkpoint(&self.shared, deadline).await;\n"
    "                    continue;\n"
    "                }",
)
source = source[:start] + method + source[end:]
source += '\n#[cfg(test)]\n#[path = "producer/add_partitions_retry_deadline.rs"]\nmod add_partitions_retry_deadline;\n'
digest = hashlib.sha256(source.encode()).hexdigest()
if digest != "c93a8137a95160d04c466ca3d69221a0228df7b1a78b9b7c2daa175a511989a4":
    raise SystemExit("instrumented source hash mismatch: " + digest)
path.write_text(source)
print("instrumented source sha256:", digest)
