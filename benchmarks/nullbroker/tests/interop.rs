//! End-to-end null-broker checks over raw TCP (no `partitionline` dependency).

#![allow(clippy::unwrap_used)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::Duration;

use nullbroker::{crc32c, NullBroker};

fn put_uvarint(out: &mut Vec<u8>, mut value: u32) {
    loop {
        let mut byte = (value & 0x7f) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if value == 0 {
            return;
        }
    }
}

fn put_compact_string(out: &mut Vec<u8>, s: Option<&str>) {
    match s {
        None => put_uvarint(out, 0),
        Some(text) => {
            put_uvarint(out, text.len() as u32 + 1);
            out.extend_from_slice(text.as_bytes());
        }
    }
}

fn put_compact_bytes(out: &mut Vec<u8>, bytes: Option<&[u8]>) {
    match bytes {
        None => put_uvarint(out, 0),
        Some(b) => {
            put_uvarint(out, b.len() as u32 + 1);
            out.extend_from_slice(b);
        }
    }
}

/// Request frame: len + (key, version, correlation, classic client id,
/// tagged fields) + body. Header v2 keeps the classic client id.
fn frame(api_key: i16, api_version: i16, correlation: i32, body: &[u8]) -> Vec<u8> {
    let mut payload = Vec::new();
    payload.extend_from_slice(&api_key.to_be_bytes());
    payload.extend_from_slice(&api_version.to_be_bytes());
    payload.extend_from_slice(&correlation.to_be_bytes());
    payload.extend_from_slice(&7i16.to_be_bytes());
    payload.extend_from_slice(b"interop");
    put_uvarint(&mut payload, 0);
    payload.extend_from_slice(body);
    let mut out = (payload.len() as u32).to_be_bytes().to_vec();
    out.extend_from_slice(&payload);
    out
}

fn read_frame(stream: &mut TcpStream) -> Vec<u8> {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).unwrap();
    let len = u32::from_be_bytes(len_buf) as usize;
    let mut frame = vec![0u8; len];
    stream.read_exact(&mut frame).unwrap();
    frame
}

fn put_zigzag32(out: &mut Vec<u8>, value: i32) {
    let mut v = ((value << 1) ^ (value >> 31)) as u32;
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if v == 0 {
            return;
        }
    }
}

fn put_zigzag64(out: &mut Vec<u8>, value: i64) {
    let mut v = ((value << 1) ^ (value >> 63)) as u64;
    loop {
        let mut byte = (v & 0x7f) as u8;
        v >>= 7;
        if v != 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if v == 0 {
            return;
        }
    }
}

/// Minimal v2 batch: `n` records with `payload`-byte values.
fn build_batch(
    base_offset: i64,
    producer_id: i64,
    base_sequence: i32,
    n: usize,
    payload: u8,
) -> Vec<u8> {
    let mut records = Vec::new();
    for i in 0..n {
        let mut body = Vec::new();
        body.push(0);
        put_zigzag64(&mut body, 0);
        put_zigzag32(&mut body, i as i32);
        put_zigzag32(&mut body, -1);
        put_zigzag32(&mut body, 4);
        body.extend_from_slice(&[payload; 4]);
        put_zigzag32(&mut body, -1);
        put_zigzag32(&mut records, body.len() as i32);
        records.extend_from_slice(&body);
    }
    let mut out = Vec::new();
    out.extend_from_slice(&base_offset.to_be_bytes());
    out.extend_from_slice(&0i32.to_be_bytes());
    out.extend_from_slice(&(-1i32).to_be_bytes());
    out.push(2);
    out.extend_from_slice(&0u32.to_be_bytes());
    out.extend_from_slice(&0i16.to_be_bytes());
    out.extend_from_slice(&(n as i32 - 1).to_be_bytes());
    out.extend_from_slice(&0i64.to_be_bytes());
    out.extend_from_slice(&0i64.to_be_bytes());
    out.extend_from_slice(&producer_id.to_be_bytes());
    out.extend_from_slice(&0i16.to_be_bytes());
    out.extend_from_slice(&base_sequence.to_be_bytes());
    out.extend_from_slice(&(n as i32).to_be_bytes());
    out.extend_from_slice(&records);
    let batch_len = (out.len() - 12) as i32;
    out[8..12].copy_from_slice(&batch_len.to_be_bytes());
    let crc = crc32c(&out[21..]);
    out[17..21].copy_from_slice(&crc.to_be_bytes());
    out
}

/// One topic's `(partition, records)` batches in a produce request.
type TopicBatches<'a> = (&'a str, &'a [(i32, &'a [u8])]);

fn produce_body(acks: i16, topics: &[TopicBatches<'_>]) -> Vec<u8> {
    let mut body = Vec::new();
    put_compact_string(&mut body, None);
    body.extend_from_slice(&acks.to_be_bytes());
    body.extend_from_slice(&30_000i32.to_be_bytes());
    put_uvarint(&mut body, topics.len() as u32 + 1);
    for (name, parts) in topics {
        put_compact_string(&mut body, Some(name));
        put_uvarint(&mut body, parts.len() as u32 + 1);
        for (index, records) in *parts {
            body.extend_from_slice(&index.to_be_bytes());
            put_compact_bytes(&mut body, Some(records));
            put_uvarint(&mut body, 0);
        }
        put_uvarint(&mut body, 0);
    }
    put_uvarint(&mut body, 0);
    body
}

/// Minimal response cursor.
struct Cur<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cur<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    fn take(&mut self, n: usize) -> &'a [u8] {
        let start = self.pos;
        self.pos += n;
        &self.buf[start..start + n]
    }

    fn i16(&mut self) -> i16 {
        i16::from_be_bytes(self.take(2).try_into().unwrap())
    }

    fn i32(&mut self) -> i32 {
        i32::from_be_bytes(self.take(4).try_into().unwrap())
    }

    fn i64(&mut self) -> i64 {
        i64::from_be_bytes(self.take(8).try_into().unwrap())
    }

    fn uvarint(&mut self) -> u32 {
        let mut value = 0u32;
        let mut shift = 0;
        loop {
            let byte = self.take(1)[0];
            value |= u32::from(byte & 0x7f) << shift;
            if byte & 0x80 == 0 {
                return value;
            }
            shift += 7;
        }
    }

    fn compact_string(&mut self) -> Option<String> {
        let n = self.uvarint();
        if n == 0 {
            return None;
        }
        let bytes = self.take((n - 1) as usize);
        Some(String::from_utf8(bytes.to_vec()).unwrap())
    }

    fn skip_tags(&mut self) {
        let n = self.uvarint();
        for _ in 0..n {
            let _tag = self.uvarint();
            let size = self.uvarint() as usize;
            self.take(size);
        }
    }
}

/// Parse a `Produce` v12 response into `(topic, partition, error, base_offset)`.
fn parse_produce_response(frame: &[u8]) -> Vec<(String, i32, i16, i64)> {
    let mut cur = Cur::new(frame);
    let _correlation = cur.i32();
    cur.skip_tags();
    let topics = cur.uvarint() - 1;
    let mut out = Vec::new();
    for _ in 0..topics {
        let name = cur.compact_string().unwrap();
        let parts = cur.uvarint() - 1;
        for _ in 0..parts {
            let index = cur.i32();
            let error = cur.i16();
            let base = cur.i64();
            let _append_time = cur.i64();
            let _log_start = cur.i64();
            let record_errors = cur.uvarint() - 1;
            for _ in 0..record_errors {
                let _batch_index = cur.i32();
                let _message = cur.compact_string();
                cur.skip_tags();
            }
            let _error_message = cur.compact_string();
            cur.skip_tags();
            out.push((name.clone(), index, error, base));
        }
        cur.skip_tags();
    }
    out
}

/// Request helper borrowing the stream and correlation counter.
struct Rpc<'a> {
    stream: &'a mut TcpStream,
    corr: &'a mut i32,
}

impl Rpc<'_> {
    fn call(&mut self, api_key: i16, version: i16, body: &[u8]) -> Vec<u8> {
        *self.corr += 1;
        let req = frame(api_key, version, *self.corr, body);
        self.stream.write_all(&req).unwrap();
        read_frame(self.stream)
    }

    fn correlation(&self) -> i32 {
        *self.corr
    }

    fn send_only(&mut self, api_key: i16, version: i16, body: &[u8]) {
        *self.corr += 1;
        let req = frame(api_key, version, *self.corr, body);
        self.stream.write_all(&req).unwrap();
    }
}

#[test]
fn handshake_produce_and_rejections() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let broker = Arc::new(NullBroker::with_listener(&listener, 2).unwrap());
    let server = Arc::clone(&broker);
    let handle = std::thread::spawn(move || {
        server.serve(&listener, Duration::from_secs(30)).unwrap();
    });

    let mut stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    let mut corr = 0;
    let mut rpc = Rpc {
        stream: &mut stream,
        corr: &mut corr,
    };

    // ApiVersions v4.
    let mut body = Vec::new();
    put_compact_string(&mut body, Some("interop"));
    put_compact_string(&mut body, Some("0.1.0"));
    put_uvarint(&mut body, 0);
    let resp = rpc.call(18, 4, &body);
    let mut cur = Cur::new(&resp);
    assert_eq!(cur.i32(), 1); // correlation, header v0 (no tags)
    assert_eq!(cur.i16(), 0); // error
    let apis = cur.uvarint() - 1;
    let mut ranges = Vec::new();
    for _ in 0..apis {
        let key = cur.i16();
        let min = cur.i16();
        let max = cur.i16();
        cur.skip_tags();
        ranges.push((key, min, max));
    }
    assert!(ranges.contains(&(0, 3, 12)), "produce range: {ranges:?}");
    assert!(ranges.contains(&(3, 1, 13)), "metadata range: {ranges:?}");
    assert!(ranges.contains(&(22, 0, 5)), "ipid range: {ranges:?}");
    assert!(ranges.contains(&(10, 1, 6)), "find-coord range: {ranges:?}");

    // Metadata v13 for ["t"]: 2 partitions, leader 0.
    let mut body = Vec::new();
    put_uvarint(&mut body, 2); // 1 topic
    body.extend_from_slice(&[0u8; 16]); // topic_id
    put_compact_string(&mut body, Some("t"));
    put_uvarint(&mut body, 0);
    body.push(1); // allow_auto
    body.push(0); // include_topic_authorized
    put_uvarint(&mut body, 0);
    let resp = rpc.call(3, 13, &body);
    let mut cur = Cur::new(&resp);
    let _correlation = cur.i32();
    cur.skip_tags();
    let _throttle = cur.i32();
    assert_eq!(cur.uvarint(), 2); // 1 broker
    assert_eq!(cur.i32(), 0); // node id
    assert_eq!(cur.compact_string().as_deref(), Some("127.0.0.1"));
    assert_eq!(cur.i32(), i32::from(port));
    assert_eq!(cur.compact_string(), None); // rack
    cur.skip_tags();
    assert_eq!(cur.compact_string().as_deref(), Some("nullbroker"));
    assert_eq!(cur.i32(), 0); // controller
    assert_eq!(cur.uvarint(), 2); // 1 topic
    assert_eq!(cur.i16(), 0); // topic error
    assert_eq!(cur.compact_string().as_deref(), Some("t"));
    cur.take(16); // topic_id
    assert_eq!(cur.take(1)[0], 0); // is_internal
    assert_eq!(cur.uvarint(), 3); // 2 partitions
    for index in 0..2 {
        assert_eq!(cur.i16(), 0);
        assert_eq!(cur.i32(), index);
        assert_eq!(cur.i32(), 0); // leader
        assert_eq!(cur.i32(), 0); // leader epoch
        assert_eq!(cur.uvarint(), 2); // 1 replica
        assert_eq!(cur.i32(), 0);
        assert_eq!(cur.uvarint(), 2); // 1 isr
        assert_eq!(cur.i32(), 0);
        assert_eq!(cur.uvarint(), 1); // 0 offline
        cur.skip_tags();
    }

    // InitProducerId v5.
    let mut body = Vec::new();
    put_compact_string(&mut body, None);
    body.extend_from_slice(&30_000i32.to_be_bytes());
    body.extend_from_slice(&(-1i64).to_be_bytes());
    body.extend_from_slice(&(-1i16).to_be_bytes());
    put_uvarint(&mut body, 0);
    let resp = rpc.call(22, 5, &body);
    let mut cur = Cur::new(&resp);
    let _correlation = cur.i32();
    cur.skip_tags();
    let _throttle = cur.i32();
    assert_eq!(cur.i16(), 0);
    let pid = cur.i64();
    assert!(pid >= 1, "pid: {pid}");
    assert_eq!(cur.i16(), 0); // epoch

    // FindCoordinator v6: keys map to node 0.
    let mut body = Vec::new();
    body.push(0); // key_type: group
    put_uvarint(&mut body, 2); // 1 key
    put_compact_string(&mut body, Some("g"));
    put_uvarint(&mut body, 0);
    let resp = rpc.call(10, 6, &body);
    let mut cur = Cur::new(&resp);
    let _correlation = cur.i32();
    cur.skip_tags();
    let _throttle = cur.i32();
    assert_eq!(cur.uvarint(), 2); // 1 coordinator
    assert_eq!(cur.compact_string().as_deref(), Some("g"));
    assert_eq!(cur.i32(), 0); // node
    assert_eq!(cur.compact_string().as_deref(), Some("127.0.0.1"));
    assert_eq!(cur.i32(), i32::from(port));
    assert_eq!(cur.i16(), 0); // error

    // Produce v12: idempotent batch (seq 0, 3 recs) on p0, plain (2 recs) on p1.
    let idem = build_batch(-1, pid, 0, 3, b'a');
    let plain = build_batch(-1, -1, 0, 2, b'b');
    let body = produce_body(1, &[("t", &[(0, &idem[..]), (1, &plain[..])])]);
    let out = parse_produce_response(&rpc.call(0, 12, &body));
    assert_eq!(
        out,
        vec![("t".to_owned(), 0, 0, 0), ("t".to_owned(), 1, 0, 0),]
    );

    // Next idempotent batch continues the sequence; offset advances.
    let idem2 = build_batch(-1, pid, 3, 2, b'c');
    let body = produce_body(1, &[("t", &[(0, &idem2[..])])]);
    let out = parse_produce_response(&rpc.call(0, 12, &body));
    assert_eq!(out, vec![("t".to_owned(), 0, 0, 3)]);

    // An exact retry deduplicates: success with the original base.
    let body = produce_body(1, &[("t", &[(0, &idem2[..])])]);
    let out = parse_produce_response(&rpc.call(0, 12, &body));
    assert_eq!(out, vec![("t".to_owned(), 0, 0, 3)]);

    // A skipped sequence is out of order.
    let skipped = build_batch(-1, pid, 99, 1, b'z');
    let body = produce_body(1, &[("t", &[(0, &skipped[..])])]);
    let out = parse_produce_response(&rpc.call(0, 12, &body));
    assert_eq!(out[0].2, 45, "skipped sequence: {out:?}");

    // Corrupted CRC is rejected.
    let mut corrupt = build_batch(-1, -1, 0, 2, b'd');
    let last = corrupt.len() - 1;
    corrupt[last] ^= 0xff;
    let body = produce_body(1, &[("t", &[(1, &corrupt[..])])]);
    let out = parse_produce_response(&rpc.call(0, 12, &body));
    assert_eq!(out[0].2, 87, "corrupt crc: {out:?}");

    // The client-stated base offset is ignored; the broker assigns.
    let misplaced = build_batch(999, -1, 0, 1, b'e');
    let body = produce_body(1, &[("t", &[(1, &misplaced[..])])]);
    let out = parse_produce_response(&rpc.call(0, 12, &body));
    assert_eq!(out, vec![("t".to_owned(), 1, 0, 2)]);

    // acks=0 produces no response: the next RPC's reply arrives undisturbed.
    let fire = build_batch(-1, -1, 0, 1, b'f');
    let body = produce_body(0, &[("t", &[(1, &fire[..])])]);
    rpc.send_only(0, 12, &body);
    let mut probe = Vec::new();
    put_compact_string(&mut probe, Some("interop"));
    put_compact_string(&mut probe, Some("0.1.0"));
    put_uvarint(&mut probe, 0);
    let resp = rpc.call(18, 4, &probe);
    let mut cur = Cur::new(&resp);
    assert_eq!(cur.i32(), rpc.correlation()); // this RPC's correlation, no stray reply

    broker.shutdown();
    handle.join().unwrap();
    let report = broker.report();
    // 3+2, then 2, then a deduped retry (uncounted), an accepted stated-base
    // batch, and one acks=0 record.
    assert_eq!(report.accepted_records, 3 + 2 + 2 + 1 + 1);
    assert_eq!(report.produce_requests, 7);
    assert_eq!(report.failures.crc, 1);
    assert_eq!(report.failures.sequence, 1);
    assert_eq!(report.failures.total(), 2);
    assert!(report.accepted_wire_bytes > 0);
    assert_eq!(
        report.end_offsets,
        vec![("t".to_owned(), 0, 5), ("t".to_owned(), 1, 4)]
    );
}

#[test]
fn artifact_is_labeled_client_ceiling() {
    let dir = std::env::temp_dir().join(format!("nullbroker-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("ceiling.json");
    let report = nullbroker::RunReport {
        accepted_records: 42,
        accepted_wire_bytes: 4200,
        produce_requests: 7,
        failures: nullbroker::FailureCounts {
            framing: 1,
            crc: 2,
            count: 0,
            sequence: 0,
            transactional: 0,
        },
        end_offsets: vec![("t".to_owned(), 0, 42)],
    };
    nullbroker::write_artifact(&path, &report).unwrap();
    let text = std::fs::read_to_string(&path).unwrap();
    assert!(text.contains("\"label\": \"client-ceiling\""), "{text}");
    assert!(text.contains("\"accepted_records\": 42"), "{text}");
    assert!(text.contains("\"crc\": 2"), "{text}");
    assert!(text.contains("\"t/0\": 42"), "{text}");
    // parseable JSON object
    assert!(text.trim_start().starts_with('{') && text.trim_end().ends_with('}'));
    std::fs::remove_file(&path).unwrap();
    std::fs::remove_dir(&dir).unwrap();
}
