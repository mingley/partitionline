# DescribeQuorum fixtures

Apache Kafka SDKs 4.1.2, 4.2.1 and 4.3.1 generated these bodies twice.
Each release has45 identical request/response bodies across versions0–2.
source.json records SDK and generator hashes. The generator checks every
incomplete prefix, SDK error factories, error counts and throttle behavior.
The Rust offline test reads all135 bodies; the guarded runner also has each
SDK independently parse Rust output and actual public Admin traffic.

The SDK accepts8193 empty topics; Rust's local array limit is8192.
The SDK accepts32767-byte strings and refuses larger ones. Nonzero directory
IDs and nonempty node lists cannot be encoded before version2. Opaque tags
are retained by Java and skipped by Rust; reverse comparison removes them.

Regenerate with tests/conformance/java/generate_describe_quorum.py.
Qualification receipts retain the original source, process results and scope.
