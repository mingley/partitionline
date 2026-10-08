//! End-to-end null-broker checks over raw TCP (no `partitionline` dependency).

#![allow(clippy::unwrap_used)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::Arc;
use std::time::{Duration, Instant};

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
    if !(api_key == 10 && api_version < 3) {
        put_uvarint(&mut payload, 0);
    }
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
    let broker = Arc::new(
        NullBroker::with_listener(&listener, 2, nullbroker::synth::SynthConfig::default()).unwrap(),
    );
    let server = Arc::clone(&broker);
    let handle = std::thread::spawn(move || {
        server.serve(0, &listener, Duration::from_secs(30)).unwrap();
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
    assert!(ranges.contains(&(0, 9, 12)), "produce range: {ranges:?}");
    assert!(ranges.contains(&(3, 12, 13)), "metadata range: {ranges:?}");
    assert!(ranges.contains(&(22, 5, 5)), "ipid range: {ranges:?}");
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
    assert_eq!(cur.take(16), nullbroker::synth::topic_id("t")); // topic_id
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

fn fetch_body(topic_id: &[u8; 16], parts: &[(i32, i64, i32)], max_bytes: i32) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&100i32.to_be_bytes()); // max_wait
    body.extend_from_slice(&1i32.to_be_bytes()); // min_bytes
    body.extend_from_slice(&max_bytes.to_be_bytes());
    body.push(0); // isolation
    body.extend_from_slice(&0i32.to_be_bytes()); // session
    body.extend_from_slice(&(-1i32).to_be_bytes()); // epoch
    put_uvarint(&mut body, 2); // 1 topic
    body.extend_from_slice(topic_id);
    put_uvarint(&mut body, parts.len() as u32 + 1);
    for (partition, fetch_offset, partition_max) in parts {
        body.extend_from_slice(&partition.to_be_bytes());
        body.extend_from_slice(&(-1i32).to_be_bytes()); // leader epoch
        body.extend_from_slice(&fetch_offset.to_be_bytes());
        body.extend_from_slice(&(-1i32).to_be_bytes()); // last fetched epoch
        body.extend_from_slice(&0i64.to_be_bytes()); // log start
        body.extend_from_slice(&partition_max.to_be_bytes());
        put_uvarint(&mut body, 0);
    }
    put_uvarint(&mut body, 0);
    put_uvarint(&mut body, 1); // forgotten: empty
    put_compact_string(&mut body, Some(""));
    put_uvarint(&mut body, 0);
    body
}

/// Parsed fetch partition: `(index, error, hw, lso, aborted, records)`.
#[allow(clippy::type_complexity)]
fn parse_fetch_response(frame: &[u8]) -> Vec<(i32, i16, i64, i64, Vec<(i64, i64)>, Vec<u8>)> {
    let mut cur = Cur::new(frame);
    let _correlation = cur.i32();
    cur.skip_tags();
    let _throttle = cur.i32();
    assert_eq!(cur.i16(), 0);
    assert_eq!(cur.i32(), 0); // session_id: full response
    assert_eq!(cur.uvarint(), 2); // 1 topic
    cur.take(16); // topic_id
    let parts = cur.uvarint() - 1;
    let mut out = Vec::new();
    for _ in 0..parts {
        let index = cur.i32();
        let error = cur.i16();
        let hw = cur.i64();
        let lso = cur.i64();
        let _log_start = cur.i64();
        let aborted_n = cur.uvarint() - 1;
        let mut aborted = Vec::new();
        for _ in 0..aborted_n {
            let pid = cur.i64();
            let first = cur.i64();
            cur.skip_tags();
            aborted.push((pid, first));
        }
        let _preferred = cur.i32();
        let n = cur.uvarint();
        let records = if n == 0 {
            Vec::new()
        } else {
            cur.take((n - 1) as usize).to_vec()
        };
        cur.skip_tags();
        out.push((index, error, hw, lso, aborted, records));
    }
    out
}

/// Split concatenated batches and count records via the public validator.
fn count_fetched_records(records: &[u8]) -> (u32, usize) {
    let mut pos = 0;
    let mut count = 0;
    let mut batches = 0;
    while pos < records.len() {
        let len = i32::from_be_bytes([
            records[pos + 8],
            records[pos + 9],
            records[pos + 10],
            records[pos + 11],
        ]) as usize;
        let total = 12 + len;
        let parsed = nullbroker::parse_batch(&records[pos..pos + total]).unwrap();
        count += parsed.records;
        batches += 1;
        pos += total;
    }
    (count, batches)
}

fn list_offsets_body(topic: &str, partition: i32, timestamp: i64) -> Vec<u8> {
    let mut body = Vec::new();
    body.extend_from_slice(&(-1i32).to_be_bytes());
    body.push(0);
    put_uvarint(&mut body, 2);
    put_compact_string(&mut body, Some(topic));
    put_uvarint(&mut body, 2);
    body.extend_from_slice(&partition.to_be_bytes());
    body.extend_from_slice(&(-1i32).to_be_bytes());
    body.extend_from_slice(&timestamp.to_be_bytes());
    put_uvarint(&mut body, 0);
    put_uvarint(&mut body, 0);
    body.extend_from_slice(&5000i32.to_be_bytes());
    put_uvarint(&mut body, 0);
    body
}

fn parse_list_offsets_response(frame: &[u8]) -> (i16, i64, i64) {
    let mut cur = Cur::new(frame);
    let _correlation = cur.i32();
    cur.skip_tags();
    let _throttle = cur.i32();
    assert_eq!(cur.uvarint(), 2);
    let _name = cur.compact_string();
    assert_eq!(cur.uvarint(), 2);
    let _index = cur.i32();
    let error = cur.i16();
    let timestamp = cur.i64();
    let offset = cur.i64();
    (error, timestamp, offset)
}

#[test]
fn fetch_and_list_offsets_roundtrip() {
    let synth = nullbroker::synth::SynthConfig {
        seed: 99,
        records_per_partition: 2500,
        records_per_batch: 500,
        payload_bytes: 100,
        header_count: 2,
        codec: 0,
        abort_every: 5,
    };
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let broker = Arc::new(NullBroker::with_listener(&listener, 1, synth).unwrap());
    let server = Arc::clone(&broker);
    let handle = std::thread::spawn(move || {
        server.serve(0, &listener, Duration::from_secs(30)).unwrap();
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

    // Metadata first so the server learns the topic id.
    let mut body = Vec::new();
    put_uvarint(&mut body, 2);
    body.extend_from_slice(&[0u8; 16]);
    put_compact_string(&mut body, Some("ft"));
    put_uvarint(&mut body, 0);
    body.push(1);
    body.push(0);
    put_uvarint(&mut body, 0);
    rpc.call(3, 13, &body);
    let topic_id = nullbroker::synth::topic_id("ft");

    // ListOffsets: earliest, latest, explicit timestamp.
    let (error, _, offset) =
        parse_list_offsets_response(&rpc.call(2, 10, &list_offsets_body("ft", 0, -2)));
    assert_eq!((error, offset), (0, 0));
    let (error, _, offset) =
        parse_list_offsets_response(&rpc.call(2, 10, &list_offsets_body("ft", 0, -1)));
    assert_eq!((error, offset), (0, 2500));
    let (error, _, offset) = parse_list_offsets_response(&rpc.call(
        2,
        10,
        &list_offsets_body("ft", 0, 1_700_000_000_042),
    ));
    assert_eq!((error, offset), (0, 42));

    // Fetch from 0 with a generous budget: 5 batches, the last aborted.
    let body = fetch_body(&topic_id, &[(0, 0, 1_000_000)], 16_777_216);
    let out = parse_fetch_response(&rpc.call(1, 17, &body));
    assert_eq!(out.len(), 1);
    let (index, error, hw, lso, aborted, records) = &out[0];
    assert_eq!((*index, *error, *hw, *lso), (0, 0, 2500, 2500));
    assert_eq!(count_fetched_records(records), (2500, 5));
    assert_eq!(*aborted, vec![(1_000_004, 2000)]);

    // Partition budget smaller than one batch still makes progress.
    let body = fetch_body(&topic_id, &[(0, 0, 10)], 16_777_216);
    let out = parse_fetch_response(&rpc.call(1, 17, &body));
    assert_eq!(count_fetched_records(&out[0].5).1, 1);

    // Fetch at log end is empty; past it is empty too.
    for offset in [2500, 999_999] {
        let body = fetch_body(&topic_id, &[(0, offset, 1_000_000)], 16_777_216);
        let out = parse_fetch_response(&rpc.call(1, 17, &body));
        assert_eq!(out[0].1, 0);
        assert!(out[0].5.is_empty());
    }

    // Unknown topic id and negative offset are protocol errors.
    let body = fetch_body(&[0xabu8; 16], &[(0, 0, 1_000_000)], 16_777_216);
    let out = parse_fetch_response(&rpc.call(1, 17, &body));
    assert_eq!(out[0].1, 100);
    let body = fetch_body(&topic_id, &[(0, -5, 1_000_000)], 16_777_216);
    let out = parse_fetch_response(&rpc.call(1, 17, &body));
    assert_eq!(out[0].1, 1);

    // Malformed frame closes only that connection; the server survives.
    let mut bad = TcpStream::connect(("127.0.0.1", port)).unwrap();
    bad.set_read_timeout(Some(Duration::from_secs(2))).unwrap();
    bad.write_all(&1000i32.to_be_bytes()).unwrap();
    bad.write_all(&[0u8; 4]).unwrap();
    drop(bad);
    let body = fetch_body(&topic_id, &[(0, 0, 1_000_000)], 16_777_216);
    let out = parse_fetch_response(&rpc.call(1, 17, &body));
    assert_eq!(out[0].1, 0);

    broker.shutdown();
    handle.join().unwrap();
    let report = broker.report();
    assert!(report.fetch_requests >= 7, "{}", report.fetch_requests);
    assert!(
        report.fetched_records >= 2500 + 500,
        "{}",
        report.fetched_records
    );
}

#[test]
fn artifact_is_labeled_client_ceiling() {
    let dir = std::env::temp_dir().join(format!("nullbroker-test-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("ceiling.json");
    let report = nullbroker::RunReport {
        api_versions: Vec::new(),
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
        fetch_requests: 3,
        fetched_records: 100,
        fetched_wire_bytes: 9000,
        injected_errors: 0,
        injected_requests: 0,
        metadata_requests: 0,
        leader_mismatches: 0,
        modes: nullbroker::Modes::default(),
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

/// Spin up `live` node listeners; returns the broker, per-node ports,
/// and server handles. Dead ids come from `config.dead_nodes`.
fn serve_nodes(
    live: u16,
    config: &nullbroker::Config,
) -> (Arc<NullBroker>, Vec<u16>, Vec<std::thread::JoinHandle<()>>) {
    let listeners: Vec<TcpListener> = (0..live)
        .map(|_| TcpListener::bind("127.0.0.1:0").unwrap())
        .collect();
    let ports: Vec<u16> = listeners
        .iter()
        .map(|l| l.local_addr().unwrap().port())
        .collect();
    let bound: Vec<(i32, TcpListener)> = listeners
        .iter()
        .enumerate()
        .map(|(i, l)| (i as i32, l.try_clone().unwrap()))
        .collect();
    let broker = Arc::new(NullBroker::with_bound(&bound, config).unwrap());
    let mut handles = Vec::new();
    for (id, listener) in &bound {
        let server = Arc::clone(&broker);
        let listener = listener.try_clone().unwrap();
        let id = *id;
        handles.push(std::thread::spawn(move || {
            server
                .serve(id, &listener, Duration::from_secs(30))
                .unwrap()
        }));
    }
    (broker, ports, handles)
}

fn connect(port: u16) -> TcpStream {
    let stream = TcpStream::connect(("127.0.0.1", port)).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();
    stream
}

fn metadata_body(topic: &str) -> Vec<u8> {
    let mut body = Vec::new();
    put_uvarint(&mut body, 2); // 1 topic
    body.extend_from_slice(&[0u8; 16]); // topic_id
    put_compact_string(&mut body, Some(topic));
    put_uvarint(&mut body, 0);
    body.push(1); // allow_auto
    body.push(0); // include_topic_authorized
    put_uvarint(&mut body, 0);
    body
}

/// Advertised broker: `(node_id, host, port)`.
type BrokerAddr = (i32, String, i32);
/// Partition leadership: `(partition, leader_node_id)`.
type PartitionLeader = (i32, i32);

/// Parse Metadata v13 brokers and per-partition leaders.
fn parse_metadata_topology(frame: &[u8]) -> (Vec<BrokerAddr>, Vec<PartitionLeader>) {
    let mut cur = Cur::new(frame);
    let _correlation = cur.i32();
    cur.skip_tags();
    let _throttle = cur.i32();
    let brokers = cur.uvarint() - 1;
    let mut nodes = Vec::new();
    for _ in 0..brokers {
        let id = cur.i32();
        let host = cur.compact_string().unwrap();
        let port = cur.i32();
        assert_eq!(cur.compact_string(), None); // rack
        cur.skip_tags();
        nodes.push((id, host, port));
    }
    assert_eq!(cur.compact_string().as_deref(), Some("nullbroker"));
    let _controller = cur.i32();
    assert_eq!(cur.uvarint(), 2); // 1 topic
    assert_eq!(cur.i16(), 0);
    let _name = cur.compact_string();
    let _topic_id = cur.take(16);
    let _internal = cur.take(1);
    let partitions = cur.uvarint() - 1;
    let mut leaders = Vec::new();
    for _ in 0..partitions {
        assert_eq!(cur.i16(), 0);
        let index = cur.i32();
        let leader = cur.i32();
        let _epoch = cur.i32();
        assert_eq!(cur.uvarint(), 2); // 1 replica
        let _replica = cur.i32();
        assert_eq!(cur.uvarint(), 2); // 1 isr
        let _isr = cur.i32();
        assert_eq!(cur.uvarint(), 1); // 0 offline
        cur.skip_tags();
        leaders.push((index, leader));
    }
    (nodes, leaders)
}

#[test]
fn multinode_metadata_advertises_topology() {
    let config = nullbroker::Config {
        partitions: 6,
        nodes: 3,
        dead_nodes: 1,
        ..nullbroker::Config::default()
    };
    let (broker, ports, handles) = serve_nodes(3, &config);

    let mut stream = connect(ports[0]);
    let mut corr = 0;
    let mut rpc = Rpc {
        stream: &mut stream,
        corr: &mut corr,
    };
    let (nodes, leaders) = parse_metadata_topology(&rpc.call(3, 13, &metadata_body("t")));
    let ids: Vec<i32> = nodes.iter().map(|n| n.0).collect();
    assert_eq!(ids, vec![0, 1, 2, 3], "advertised nodes: {nodes:?}");
    // Live nodes carry their listener ports; the dead id advertises a
    // distinct port with no listener behind it. (No connect assertion: with
    // ephemeral ports a parallel test may hold any given port.)
    assert_eq!(nodes[0].2, i32::from(ports[0]));
    assert_eq!(nodes[1].2, i32::from(ports[1]));
    assert_eq!(nodes[2].2, i32::from(ports[2]));
    assert!(
        !ports.contains(&(nodes[3].2 as u16)),
        "dead port must differ from live ports: {nodes:?}"
    );
    // Default leadership is round-robin over live nodes.
    let leaders: Vec<i32> = leaders.iter().map(|(_, l)| *l).collect();
    assert_eq!(leaders, vec![0, 1, 2, 0, 1, 2]);

    broker.shutdown();
    for handle in handles {
        handle.join().unwrap();
    }
}

#[test]
fn requests_to_wrong_node_return_not_leader() {
    let config = nullbroker::Config {
        partitions: 6,
        nodes: 3,
        ..nullbroker::Config::default()
    };
    let (broker, ports, handles) = serve_nodes(3, &config);

    // Node 0 leads partitions 0 and 3; partition 1 belongs to node 1.
    let mut stream = connect(ports[0]);
    let mut corr = 0;
    let mut rpc = Rpc {
        stream: &mut stream,
        corr: &mut corr,
    };
    rpc.call(3, 13, &metadata_body("t")); // register the topic id

    let wrong = build_batch(-1, -1, 0, 2, b'x');
    let body = produce_body(1, &[("t", &[(1, &wrong[..])])]);
    let out = parse_produce_response(&rpc.call(0, 12, &body));
    assert_eq!(out, vec![("t".to_owned(), 1, 6, -1)]);

    let right = build_batch(-1, -1, 0, 2, b'y');
    let body = produce_body(1, &[("t", &[(0, &right[..])])]);
    let out = parse_produce_response(&rpc.call(0, 12, &body));
    assert_eq!(out, vec![("t".to_owned(), 0, 0, 0)]);

    let topic_id = nullbroker::synth::topic_id("t");
    let body = fetch_body(&topic_id, &[(2, 0, 1_000_000)], 16_777_216);
    let out = parse_fetch_response(&rpc.call(1, 17, &body));
    assert_eq!(out[0].1, 6); // partition 2 lives on node 2
    assert!(out[0].5.is_empty());

    let body = fetch_body(&topic_id, &[(0, 0, 1_000_000)], 16_777_216);
    let out = parse_fetch_response(&rpc.call(1, 17, &body));
    assert_eq!(out[0].1, 0);

    broker.shutdown();
    for handle in handles {
        handle.join().unwrap();
    }
    let report = broker.report();
    assert_eq!(report.leader_mismatches, 2, "{report:?}");
    assert_eq!(report.injected_errors, 0);
    assert_eq!(report.accepted_records, 2);
}

#[test]
fn slow_node_delays_responses() {
    let config = nullbroker::Config {
        partitions: 1,
        nodes: 2,
        slow_node: Some(1),
        slow_delay: Duration::from_millis(100),
        ..nullbroker::Config::default()
    };
    let (broker, ports, handles) = serve_nodes(2, &config);

    let mut probe = Vec::new();
    put_compact_string(&mut probe, Some("interop"));
    put_compact_string(&mut probe, Some("0.1.0"));
    put_uvarint(&mut probe, 0);

    let mut slow = connect(ports[1]);
    let mut corr = 0;
    let start = Instant::now();
    let mut rpc = Rpc {
        stream: &mut slow,
        corr: &mut corr,
    };
    let resp = rpc.call(18, 4, &probe);
    let elapsed = start.elapsed();
    let mut cur = Cur::new(&resp);
    assert_eq!(cur.i32(), 1);
    assert_eq!(cur.i16(), 0);
    assert!(
        elapsed >= Duration::from_millis(95),
        "slow node answered in {elapsed:?}"
    );

    // The fast node still answers (no upper bound: loaded hosts stall).
    let mut fast = connect(ports[0]);
    let mut corr = 0;
    let mut rpc = Rpc {
        stream: &mut fast,
        corr: &mut corr,
    };
    let resp = rpc.call(18, 4, &probe);
    let mut cur = Cur::new(&resp);
    assert_eq!(cur.i32(), 1);
    assert_eq!(cur.i16(), 0);

    broker.shutdown();
    for handle in handles {
        handle.join().unwrap();
    }
}

/// One seeded fault run: the per-request error sequence plus the injected
/// counter. Identical seeds must replay identical sequences.
fn fault_run(seed: u64, ppm: u32, requests: usize) -> (Vec<i16>, u64) {
    let config = nullbroker::Config {
        partitions: 1,
        nodes: 1,
        fault_seed: seed,
        fault_rate_per_million: ppm,
        ..nullbroker::Config::default()
    };
    let (broker, ports, handles) = serve_nodes(1, &config);

    let mut stream = connect(ports[0]);
    let mut corr = 0;
    let mut rpc = Rpc {
        stream: &mut stream,
        corr: &mut corr,
    };
    rpc.call(3, 13, &metadata_body("t"));
    let mut errors = Vec::with_capacity(requests);
    for _ in 0..requests {
        let batch = build_batch(-1, -1, 0, 1, b'f');
        let body = produce_body(1, &[("t", &[(0, &batch[..])])]);
        let out = parse_produce_response(&rpc.call(0, 12, &body));
        assert_eq!(out.len(), 1);
        errors.push(out[0].2);
        if out[0].2 == 6 {
            assert_eq!(out[0].3, -1, "injected fault base: {out:?}");
        }
    }

    broker.shutdown();
    for handle in handles {
        handle.join().unwrap();
    }
    let report = broker.report();
    assert_eq!(report.leader_mismatches, 0);
    (errors, report.injected_errors)
}

#[test]
fn seeded_faults_are_deterministic() {
    let (first, first_count) = fault_run(0x5EED_F001, 100_000, 50);
    let (second, second_count) = fault_run(0x5EED_F001, 100_000, 50);
    assert_eq!(first, second, "same seed must replay the same faults");
    assert_eq!(first_count, second_count);
    let injected = first.iter().filter(|e| **e == 6).count() as u64;
    assert_eq!(first_count, injected, "counter matches responses");
    assert!(
        (1..50).contains(&injected),
        "10% of 50 requests should fault partially, got {injected}"
    );

    // A zero rate never faults.
    let (clean, clean_count) = fault_run(0x5EED_F001, 0, 10);
    assert!(clean.iter().all(|e| *e == 0), "{clean:?}");
    assert_eq!(clean_count, 0);
}

#[test]
fn all_advertised_flexible_versions_accept_validated_records() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let config = nullbroker::Config {
        partitions: 1,
        trace_api_versions: true,
        synth: nullbroker::synth::SynthConfig {
            records_per_partition: 3,
            ..nullbroker::synth::SynthConfig::default()
        },
        ..nullbroker::Config::default()
    };
    let broker =
        Arc::new(NullBroker::with_bound(&[(0, listener.try_clone().unwrap())], &config).unwrap());
    let server = Arc::clone(&broker);
    let handle =
        std::thread::spawn(move || server.serve(0, &listener, Duration::from_secs(10)).unwrap());
    let mut stream = connect(port);
    let mut corr = 0;
    let mut rpc = Rpc {
        stream: &mut stream,
        corr: &mut corr,
    };
    for version in 12..=13 {
        let response = rpc.call(3, version, &metadata_body("versions"));
        let (_, leaders) = parse_metadata_topology(&response);
        assert_eq!(leaders, vec![(0, 0)]);
        // v13 adds exactly its int16 top-level error before the final tags.
        if version == 12 {
            assert_eq!(response[response.len() - 1], 0);
        } else {
            assert_eq!(&response[response.len() - 3..], &[0, 0, 0]);
        }
    }
    for version in 9..=12 {
        let batch = build_batch(-1, -1, 0, 1, b'x');
        let response = rpc.call(
            0,
            version,
            &produce_body(-1, &[("versions", &[(0, &batch)])]),
        );
        assert_eq!(
            parse_produce_response(&response),
            vec![("versions".to_owned(), 0, 0, i64::from(version - 9))]
        );
        let mut corrupt = batch.clone();
        *corrupt.last_mut().unwrap() ^= 1;
        let response = rpc.call(
            0,
            version,
            &produce_body(-1, &[("versions", &[(0, &corrupt)])]),
        );
        assert_eq!(parse_produce_response(&response)[0].2, 87);
    }
    for version in 15..=17 {
        let response = rpc.call(
            1,
            version,
            &fetch_body(
                &nullbroker::synth::topic_id("versions"),
                &[(0, 0, 1_000_000)],
                1_000_000,
            ),
        );
        let parts = parse_fetch_response(&response);
        assert_eq!(parts[0].1, 0);
        assert_eq!(parts[0].2, 3);
        assert_eq!(count_fetched_records(&parts[0].5).0, 3);
    }
    for version in 7..=10 {
        for (timestamp, expected) in [(-2, 0), (-1, 3)] {
            let response = rpc.call(2, version, &list_offsets_body("versions", 0, timestamp));
            let (error, _, offset) = parse_list_offsets_response(&response);
            assert_eq!((error, offset), (0, expected));
        }
    }
    drop(stream);
    broker.shutdown();
    handle.join().unwrap();
    let report = broker.report();
    assert_eq!(report.accepted_records, 4);
    assert_eq!(report.failures.crc, 4);
    assert_eq!(report.fetched_records, 9);
    assert_eq!(report.api_versions.len(), 13);
    assert_eq!(
        report.api_versions.iter().map(|(_, _, n)| n).sum::<u64>(),
        21
    );
}

#[test]
fn coordinator_classic_and_flexible_versions_share_one_node() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let broker = Arc::new(
        NullBroker::with_listener(&listener, 1, nullbroker::synth::SynthConfig::default()).unwrap(),
    );
    let server = Arc::clone(&broker);
    let handle =
        std::thread::spawn(move || server.serve(0, &listener, Duration::from_secs(10)).unwrap());
    let mut stream = connect(port);
    let mut corr = 0;
    let mut rpc = Rpc {
        stream: &mut stream,
        corr: &mut corr,
    };
    for version in 1..=6 {
        let mut body = Vec::new();
        if version < 3 {
            body.extend_from_slice(&1i16.to_be_bytes());
            body.push(b'g');
        } else if version == 3 {
            put_compact_string(&mut body, Some("g"));
        }
        body.push(0);
        if version >= 4 {
            put_uvarint(&mut body, 2);
            put_compact_string(&mut body, Some("g"));
        }
        if version >= 3 {
            put_uvarint(&mut body, 0);
        }
        let response = rpc.call(10, version, &body);
        let mut cur = Cur::new(&response);
        assert_eq!(cur.i32(), rpc.correlation());
        if version >= 3 {
            cur.skip_tags();
        }
        assert_eq!(cur.i32(), 0);
        if version >= 4 {
            assert_eq!(cur.uvarint(), 2);
            assert_eq!(cur.compact_string().as_deref(), Some("g"));
        } else {
            assert_eq!(cur.i16(), 0);
            if version == 3 {
                assert_eq!(cur.compact_string(), None);
            } else {
                assert_eq!(cur.i16(), -1);
            }
        }
        assert_eq!(cur.i32(), 0);
        if version >= 3 {
            assert_eq!(cur.compact_string().as_deref(), Some("127.0.0.1"));
        } else {
            let len = cur.i16() as usize;
            assert_eq!(cur.take(len), b"127.0.0.1");
        }
        assert_eq!(cur.i32(), i32::from(port));
        if version >= 4 {
            assert_eq!(cur.i16(), 0);
            assert_eq!(cur.compact_string(), None);
            cur.skip_tags();
        }
        if version >= 3 {
            cur.skip_tags();
        }
        assert_eq!(cur.pos, response.len());
    }
    drop(stream);
    broker.shutdown();
    handle.join().unwrap();
}

#[test]
fn unsupported_api_versions_negotiate_on_the_same_connection() {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let broker = Arc::new(
        NullBroker::with_listener(&listener, 1, nullbroker::synth::SynthConfig::default()).unwrap(),
    );
    let server = Arc::clone(&broker);
    let handle =
        std::thread::spawn(move || server.serve(0, &listener, Duration::from_secs(10)).unwrap());
    let mut stream = connect(port);
    let mut corr = 0;
    let mut rpc = Rpc {
        stream: &mut stream,
        corr: &mut corr,
    };
    let mut body = Vec::new();
    put_compact_string(&mut body, Some("future-sdk"));
    put_compact_string(&mut body, Some("1"));
    put_uvarint(&mut body, 0);
    let response = rpc.call(18, 5, &body);
    let mut cur = Cur::new(&response);
    assert_eq!(cur.i32(), rpc.correlation());
    assert_eq!(cur.i16(), 35);
    assert_eq!(cur.i32(), 7); // classic v0 array, no flexible header tags
    for expected in nullbroker::advertised_apis() {
        assert_eq!((cur.i16(), cur.i16(), cur.i16()), expected);
    }
    assert_eq!(cur.pos, response.len());
    // The next ordinary request succeeds without redialing.
    let response = rpc.call(3, 13, &metadata_body("negotiated"));
    assert_eq!(parse_metadata_topology(&response).1, vec![(0, 0)]);
    drop(stream);
    broker.shutdown();
    handle.join().unwrap();
}
