//! KL01-09: verifiable producer/consumer CLI + JSON event contract.
//!
//! Drives the real `examples/verifiable_producer` and
//! `examples/verifiable_consumer` binaries (pinned to Apache Kafka 3.9.1)
//! as subprocesses: CLI validation without a broker, a full
//! produce-consume roundtrip against the mock broker with every stdout
//! event validated, deterministic `producer_send_error` events via mock
//! fault injection, and the Java `--producer.config` file-wins quirk.

mod common;

use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::time::{Duration, Instant};

/// Minimal JSON value (test-only validator; the repo has no serde dependency).
#[derive(Debug)]
enum Json {
    Null,
    Bool(bool),
    Num(String),
    Str(String),
    Arr(Vec<Json>),
    Obj(Vec<(String, Json)>),
}

impl Json {
    /// Object member lookup.
    fn get(&self, key: &str) -> Option<&Json> {
        match self {
            Json::Obj(pairs) => pairs.iter().find(|(k, _)| k == key).map(|(_, v)| v),
            _ => None,
        }
    }

    /// Event `name` field.
    fn name(&self) -> Option<&str> {
        self.get("name").and_then(Json::as_str)
    }

    /// String accessor.
    fn as_str(&self) -> Option<&str> {
        match self {
            Json::Str(s) => Some(s),
            _ => None,
        }
    }

    /// Integer accessor (strict).
    fn as_i64(&self) -> Option<i64> {
        match self {
            Json::Num(raw) => raw.parse().ok(),
            _ => None,
        }
    }

    /// Float accessor (strict).
    fn as_f64(&self) -> Option<f64> {
        match self {
            Json::Num(raw) => raw.parse().ok(),
            _ => None,
        }
    }

    /// Boolean accessor.
    fn as_bool(&self) -> Option<bool> {
        match self {
            Json::Bool(b) => Some(*b),
            _ => None,
        }
    }

    /// Array accessor.
    fn as_arr(&self) -> Option<&[Json]> {
        match self {
            Json::Arr(v) => Some(v),
            _ => None,
        }
    }

    /// Null check.
    fn is_null(&self) -> bool {
        matches!(self, Json::Null)
    }
}

/// Strict single-value JSON parser over one event line.
struct Parser<'a> {
    bytes: &'a [u8],
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(line: &'a str) -> Self {
        Self {
            bytes: line.as_bytes(),
            pos: 0,
        }
    }

    fn peek(&self) -> Option<u8> {
        self.bytes.get(self.pos).copied()
    }

    fn next(&mut self) -> Option<u8> {
        let c = self.peek()?;
        self.pos += 1;
        Some(c)
    }

    /// Advance past one byte known to be present (result unused by design).
    fn bump(&mut self) {
        self.pos += 1;
    }

    fn whitespace(&mut self) {
        while matches!(self.peek(), Some(b' ' | b'\t' | b'\n' | b'\r')) {
            self.pos += 1;
        }
    }

    fn literal(&mut self, word: &str) -> Result<(), String> {
        let end = self.pos + word.len();
        if self.bytes.get(self.pos..end) == Some(word.as_bytes()) {
            self.pos = end;
            Ok(())
        } else {
            Err(format!("expected '{word}' at byte {}", self.pos))
        }
    }

    fn value(&mut self) -> Result<Json, String> {
        self.whitespace();
        match self.peek() {
            Some(b'{') => self.object(),
            Some(b'[') => self.array(),
            Some(b'"') => Ok(Json::Str(self.string()?)),
            Some(b't') => {
                self.literal("true")?;
                Ok(Json::Bool(true))
            }
            Some(b'f') => {
                self.literal("false")?;
                Ok(Json::Bool(false))
            }
            Some(b'n') => {
                self.literal("null")?;
                Ok(Json::Null)
            }
            Some(c) if c == b'-' || c.is_ascii_digit() => Ok(Json::Num(self.number()?)),
            other => Err(format!("unexpected byte {other:?} at {}", self.pos)),
        }
    }

    fn object(&mut self) -> Result<Json, String> {
        self.bump();
        let mut pairs = Vec::new();
        self.whitespace();
        if self.peek() == Some(b'}') {
            self.bump();
            return Ok(Json::Obj(pairs));
        }
        loop {
            self.whitespace();
            if self.peek() != Some(b'"') {
                return Err(format!("expected object key at {}", self.pos));
            }
            let key = self.string()?;
            self.whitespace();
            if self.next() != Some(b':') {
                return Err(format!("expected ':' at {}", self.pos));
            }
            pairs.push((key, self.value()?));
            self.whitespace();
            match self.next() {
                Some(b',') => {}
                Some(b'}') => return Ok(Json::Obj(pairs)),
                other => return Err(format!("expected ',' or '}}', got {other:?}")),
            }
        }
    }

    fn array(&mut self) -> Result<Json, String> {
        self.bump();
        let mut items = Vec::new();
        self.whitespace();
        if self.peek() == Some(b']') {
            self.bump();
            return Ok(Json::Arr(items));
        }
        loop {
            items.push(self.value()?);
            self.whitespace();
            match self.next() {
                Some(b',') => {}
                Some(b']') => return Ok(Json::Arr(items)),
                other => return Err(format!("expected ',' or ']', got {other:?}")),
            }
        }
    }

    fn string(&mut self) -> Result<String, String> {
        if self.next() != Some(b'"') {
            return Err(format!("expected string at {}", self.pos));
        }
        let mut raw: Vec<u8> = Vec::new();
        loop {
            match self.next() {
                None => return Err("unterminated string".to_string()),
                Some(b'"') => break,
                Some(b'\\') => match self.next() {
                    Some(b'"') => raw.push(b'"'),
                    Some(b'\\') => raw.push(b'\\'),
                    Some(b'/') => raw.push(b'/'),
                    Some(b'b') => raw.push(0x08),
                    Some(b'f') => raw.push(0x0C),
                    Some(b'n') => raw.push(b'\n'),
                    Some(b'r') => raw.push(b'\r'),
                    Some(b't') => raw.push(b'\t'),
                    Some(b'u') => {
                        let hi = self.hex4()?;
                        let cp = if (0xD800..0xDC00).contains(&hi) {
                            if self.next() != Some(b'\\') || self.next() != Some(b'u') {
                                return Err("lone UTF-16 surrogate".to_string());
                            }
                            let lo = self.hex4()?;
                            if !(0xDC00..0xE000).contains(&lo) {
                                return Err("lone UTF-16 surrogate".to_string());
                            }
                            0x1_0000 + ((hi - 0xD800) << 10) + (lo - 0xDC00)
                        } else if (0xDC00..0xE000).contains(&hi) {
                            return Err("lone UTF-16 surrogate".to_string());
                        } else {
                            hi
                        };
                        let c = char::from_u32(cp)
                            .ok_or_else(|| format!("invalid codepoint {cp:#X}"))?;
                        let mut buf = [0u8; 4];
                        raw.extend_from_slice(c.encode_utf8(&mut buf).as_bytes());
                    }
                    other => return Err(format!("bad escape {other:?}")),
                },
                Some(c) if c < 0x20 => return Err("unescaped control byte in string".to_string()),
                Some(c) => raw.push(c),
            }
        }
        String::from_utf8(raw).map_err(|_| "string is not valid UTF-8".to_string())
    }

    fn hex4(&mut self) -> Result<u32, String> {
        let mut value: u32 = 0;
        for _ in 0..4 {
            let digit = match self.next() {
                Some(c @ b'0'..=b'9') => u32::from(c - b'0'),
                Some(c @ b'a'..=b'f') => u32::from(c - b'a') + 10,
                Some(c @ b'A'..=b'F') => u32::from(c - b'A') + 10,
                other => return Err(format!("bad \\u escape, got {other:?}")),
            };
            value = value * 16 + digit;
        }
        Ok(value)
    }

    fn number(&mut self) -> Result<String, String> {
        let start = self.pos;
        if self.peek() == Some(b'-') {
            self.pos += 1;
        }
        match self.peek() {
            Some(b'0') => self.pos += 1,
            Some(c) if c.is_ascii_digit() => {
                while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                    self.pos += 1;
                }
            }
            other => return Err(format!("bad number, got {other:?}")),
        }
        if self.peek() == Some(b'.') {
            self.pos += 1;
            if !self.peek().is_some_and(|c| c.is_ascii_digit()) {
                return Err("bad number fraction".to_string());
            }
            while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                self.pos += 1;
            }
        }
        if matches!(self.peek(), Some(b'e' | b'E')) {
            self.pos += 1;
            if matches!(self.peek(), Some(b'+' | b'-')) {
                self.pos += 1;
            }
            if !self.peek().is_some_and(|c| c.is_ascii_digit()) {
                return Err("bad number exponent".to_string());
            }
            while self.peek().is_some_and(|c| c.is_ascii_digit()) {
                self.pos += 1;
            }
        }
        self.bytes
            .get(start..self.pos)
            .and_then(|b| std::str::from_utf8(b).ok())
            .map(str::to_string)
            .ok_or_else(|| "bad number bytes".to_string())
    }
}

/// Parse one event line (strict: trailing bytes fail).
fn parse_json(line: &str) -> Result<Json, String> {
    let mut parser = Parser::new(line);
    let value = parser.value()?;
    parser.whitespace();
    if parser.peek().is_some() {
        return Err(format!("trailing bytes at {}", parser.pos));
    }
    Ok(value)
}

/// Parse every non-empty stdout line as a JSON event.
fn parse_events(tag: &str, stdout: &str) -> Result<Vec<Json>, String> {
    let mut out = Vec::new();
    for (n, line) in stdout.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        out.push(parse_json(line).map_err(|e| format!("{tag} line {}: {e}: {line}", n + 1))?);
    }
    Ok(out)
}

/// Locate a built example binary next to the test executable.
///
/// `cargo test --all-targets` builds example test harnesses rather than these
/// standalone binaries. CI and CONTRIBUTING.md build them explicitly first;
/// fail closed with the exact rebuild command when that prerequisite is missing.
fn example_bin(name: &str) -> Result<PathBuf, String> {
    let exe = std::env::current_exe().map_err(|e| format!("current_exe: {e}"))?;
    let deps = exe
        .parent()
        .ok_or_else(|| "test executable has no parent dir".to_string())?;
    let profile = deps
        .parent()
        .ok_or_else(|| "test executable has no profile dir".to_string())?;
    let file = if cfg!(windows) {
        format!("{name}.exe")
    } else {
        name.to_string()
    };
    let bin = profile.join("examples").join(file);
    if bin.is_file() {
        Ok(bin)
    } else {
        Err(format!(
            "missing example binary {}; run `cargo build --locked --examples` first",
            bin.display()
        ))
    }
}

/// One finished child run.
struct Run {
    status: i32,
    stdout: String,
    stderr: String,
}

/// Run an example binary with piped stdio, killing it on timeout.
///
/// Event streams here are small (bounded `--max-messages` runs), so polling
/// `try_wait` cannot pipe-block the child; the timeout still kills fail-closed.
async fn run_example(bin: &Path, args: &[&str], timeout: Duration) -> Result<Run, String> {
    let mut child = std::process::Command::new(bin)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()
        .map_err(|e| format!("spawn {}: {e}", bin.display()))?;
    let deadline = Instant::now() + timeout;
    loop {
        match child.try_wait() {
            Ok(Some(_)) => break,
            Ok(None) => {
                if Instant::now() >= deadline {
                    drop(child.kill());
                    return Err(format!(
                        "timed out after {timeout:?}: {} {args:?}",
                        bin.display()
                    ));
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
            Err(e) => return Err(format!("try_wait {}: {e}", bin.display())),
        }
    }
    let output = child
        .wait_with_output()
        .map_err(|e| format!("wait {}: {e}", bin.display()))?;
    let status = output
        .status
        .code()
        .ok_or_else(|| format!("{} died by signal", bin.display()))?;
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    let stderr = String::from_utf8_lossy(&output.stderr).into_owned();
    Ok(Run {
        status,
        stdout,
        stderr,
    })
}

/// Assert every event carries a numeric `timestamp` (Java base event).
fn assert_timestamps(events: &[Json], tag: &str) {
    for (n, ev) in events.iter().enumerate() {
        assert!(
            ev.get("timestamp").and_then(Json::as_i64).is_some(),
            "{tag} event {n} lacks a numeric timestamp: {ev:?}"
        );
    }
}

/// Filter events by `name`.
fn named<'a>(events: &'a [Json], name: &str) -> Vec<&'a Json> {
    events.iter().filter(|ev| ev.name() == Some(name)).collect()
}

const TIMEOUT: Duration = Duration::from_secs(60);

/// Producer CLI contract without a broker (exit codes + usage, Java argparse shape).
#[tokio::test]
async fn producer_cli_contract_no_broker() {
    let bin = example_bin("verifiable_producer").unwrap();
    // Bare invocation prints help and exits 0 (Java main).
    let run = run_example(&bin, &[], TIMEOUT).await.unwrap();
    assert_eq!(
        run.status, 0,
        "bare run must exit 0, stderr: {}",
        run.stderr
    );
    assert!(
        run.stdout.starts_with("usage: verifiable_producer"),
        "bare run must print usage, got: {}",
        run.stdout.lines().next().unwrap_or("")
    );
    assert!(run.stderr.is_empty(), "help must not warn: {}", run.stderr);
    let run = run_example(&bin, &["--help"], TIMEOUT).await.unwrap();
    assert_eq!(run.status, 0);
    assert!(run.stdout.starts_with("usage: verifiable_producer"));

    // Each failure exits 1 with the usage on stderr (Java handleError).
    for (args, needle) in [
        (&["--topic", "t"] as &[&str], "bootstrap-server"),
        (
            &[
                "--topic",
                "t",
                "--bootstrap-server",
                "x",
                "--broker-list",
                "y",
            ] as &[&str],
            "not allowed",
        ),
        (&["--bootstrap-server", "x"] as &[&str], "required: --topic"),
        (
            &["--topic", "t", "--bootstrap-server", "x", "--acks", "2"] as &[&str],
            "invalid choice",
        ),
        (
            &["--topic", "t", "--bootstrap-server", "x", "--bogus"] as &[&str],
            "unrecognized",
        ),
        (
            &[
                "--topic",
                "t",
                "--bootstrap-server",
                "x",
                "--max-messages",
                "zzz",
            ] as &[&str],
            "invalid integer",
        ),
        (&["positional"] as &[&str], "unexpected argument"),
    ] {
        let run = run_example(&bin, args, TIMEOUT).await.unwrap();
        assert_eq!(run.status, 1, "args {args:?} must exit 1");
        assert!(
            run.stderr.contains(needle),
            "args {args:?} stderr must mention '{needle}', got: {}",
            run.stderr.lines().next().unwrap_or("")
        );
        assert!(
            run.stderr.contains("usage: verifiable_producer"),
            "args {args:?} must print usage on stderr"
        );
        assert!(run.stdout.is_empty(), "args {args:?} must not print events");
    }

    // `--opt=value` form parses (dies later at connect, proving the parse worked).
    let run = run_example(
        &bin,
        &[
            "--topic=t",
            "--bootstrap-server=127.0.0.1:9",
            "--max-messages=1",
        ] as &[&str],
        TIMEOUT,
    )
    .await
    .unwrap();
    assert_eq!(run.status, 1);
    assert!(
        run.stderr.contains("cannot create producer"),
        "inline form must reach connect, got: {}",
        run.stderr.lines().next().unwrap_or("")
    );
}

/// Consumer CLI contract without a broker.
#[tokio::test]
async fn consumer_cli_contract_no_broker() {
    let bin = example_bin("verifiable_consumer").unwrap();
    let run = run_example(&bin, &[], TIMEOUT).await.unwrap();
    assert_eq!(
        run.status, 0,
        "bare run must exit 0, stderr: {}",
        run.stderr
    );
    assert!(run.stdout.starts_with("usage: verifiable_consumer"));
    assert!(run.stderr.is_empty());

    let base = ["--topic", "t", "--group-id", "g", "--bootstrap-server", "x"];
    // Missing-connection and missing-group cases spell their own argv.
    for (args, needle) in [
        (
            &["--topic", "t", "--group-id", "g"] as &[&str],
            "bootstrap-server",
        ),
        (
            &["--topic", "t", "--bootstrap-server", "x"] as &[&str],
            "required: --group-id",
        ),
        (
            &["--group-id", "g", "--bootstrap-server", "x"] as &[&str],
            "required: --topic",
        ),
    ] {
        let run = run_example(&bin, args, TIMEOUT).await.unwrap();
        assert_eq!(run.status, 1, "args {args:?} must exit 1");
        assert!(run.stderr.contains(needle), "args {args:?}: {}", run.stderr);
        assert!(run.stdout.is_empty());
    }
    for (extra, needle) in [
        (
            &["--reset-policy", "sometimes"] as &[&str],
            "invalid choice",
        ),
        (
            &["--assignment-strategy", "foo.RoundRobinAssignor"] as &[&str],
            "unsupported assignor",
        ),
        (
            &["--group-remote-assignor", "range"] as &[&str],
            "unsupported assignor",
        ),
        (&["--bogus"] as &[&str], "unrecognized"),
        (&["--session-timeout", "zzz"] as &[&str], "invalid integer"),
    ] {
        let mut args: Vec<&str> = base.to_vec();
        args.extend(extra);
        let run = run_example(&bin, &args, TIMEOUT).await.unwrap();
        assert_eq!(run.status, 1, "args {args:?} must exit 1");
        assert!(
            run.stderr.contains(needle),
            "args {args:?} stderr must mention '{needle}', got: {}",
            run.stderr.lines().next().unwrap_or("")
        );
        assert!(
            run.stderr.contains("usage: verifiable_consumer"),
            "args {args:?} must print usage on stderr"
        );
        assert!(run.stdout.is_empty(), "args {args:?} must not print events");
    }
}

/// One pinned produce-consume compatibility case end to end (KL01-09 proof).
///
/// The producer's event stream pins every ID, value, partition and offset;
/// the consumer's stream must reproduce the same IDs and positions, commit
/// `maxOffset + 1`, and close with `shutdown_complete`.
#[tokio::test]
async fn verifiable_produce_consume_roundtrip() {
    const N: usize = 25;
    let n_i64 = i64::try_from(N).unwrap();
    let producer_bin = example_bin("verifiable_producer").unwrap();
    let consumer_bin = example_bin("verifiable_consumer").unwrap();
    let mock = common::Mock::start().await;

    let run = run_example(
        &producer_bin,
        &[
            "--topic",
            "t",
            "--bootstrap-server",
            &mock.addr,
            "--max-messages",
            "25",
            "--acks",
            "-1",
        ],
        TIMEOUT,
    )
    .await
    .unwrap();
    assert_eq!(
        run.status, 0,
        "producer must exit 0, stderr: {}",
        run.stderr
    );
    assert!(run.stderr.is_empty(), "producer stderr: {}", run.stderr);
    let events = parse_events("producer", &run.stdout).unwrap();
    assert_timestamps(&events, "producer");
    assert_eq!(
        events.first().and_then(Json::name),
        Some("startup_complete")
    );
    assert_eq!(
        events.iter().rev().nth(1).and_then(Json::name),
        Some("shutdown_complete")
    );
    assert_eq!(events.last().and_then(Json::name), Some("tool_data"));
    let successes = named(&events, "producer_send_success");
    assert_eq!(successes.len(), N, "expected {N} send successes");
    assert!(named(&events, "producer_send_error").is_empty());
    for (i, ev) in successes.iter().enumerate() {
        assert!(
            ev.get("key").is_some_and(Json::is_null),
            "key must be null: {ev:?}"
        );
        let expected = i.to_string();
        assert_eq!(
            ev.get("value").and_then(Json::as_str),
            Some(expected.as_str())
        );
        assert_eq!(ev.get("topic").and_then(Json::as_str), Some("t"));
        assert_eq!(ev.get("partition").and_then(Json::as_i64), Some(0));
        assert_eq!(
            ev.get("offset").and_then(Json::as_i64),
            Some(i64::try_from(i).unwrap()),
            "offsets must be dense from 0"
        );
    }
    let tool = events.last().unwrap();
    assert_eq!(tool.get("sent").and_then(Json::as_i64), Some(n_i64));
    assert_eq!(tool.get("acked").and_then(Json::as_i64), Some(n_i64));
    assert_eq!(
        tool.get("target_throughput").and_then(Json::as_i64),
        Some(-1)
    );
    let avg = tool.get("avg_throughput").and_then(Json::as_f64).unwrap();
    assert!(avg.is_finite() && avg >= 0.0, "avg_throughput: {avg}");

    let run = run_example(
        &consumer_bin,
        &[
            "--topic",
            "t",
            "--group-id",
            "vgroup",
            "--bootstrap-server",
            &mock.addr,
            "--max-messages",
            "25",
            "--verbose",
        ],
        TIMEOUT,
    )
    .await
    .unwrap();
    assert_eq!(
        run.status, 0,
        "consumer must exit 0, stderr: {}",
        run.stderr
    );
    assert!(run.stderr.is_empty(), "consumer stderr: {}", run.stderr);
    let events = parse_events("consumer", &run.stdout).unwrap();
    assert_timestamps(&events, "consumer");
    assert_eq!(
        events.first().and_then(Json::name),
        Some("startup_complete")
    );
    assert_eq!(
        events.last().and_then(Json::name),
        Some("shutdown_complete")
    );

    // Assignment: the single member owns t-0; close revokes it once.
    let assigned = named(&events, "partitions_assigned");
    assert_eq!(assigned.len(), 1, "one assignment expected");
    let parts = assigned[0]
        .get("partitions")
        .and_then(Json::as_arr)
        .unwrap();
    assert_eq!(parts.len(), 1);
    assert_eq!(parts[0].get("topic").and_then(Json::as_str), Some("t"));
    assert_eq!(parts[0].get("partition").and_then(Json::as_i64), Some(0));
    let revoked = named(&events, "partitions_revoked");
    assert_eq!(revoked.len(), 1, "Java close revokes the owned assignment");
    assert_eq!(event_partitions(revoked[0]).unwrap(), [0]);

    // record_data reproduces every produced ID at its position.
    let records = named(&events, "record_data");
    assert_eq!(records.len(), N, "expected {N} record_data events");
    let mut seen: Vec<(i64, String)> = records
        .iter()
        .map(|ev| {
            assert!(ev.get("key").is_some_and(Json::is_null));
            assert_eq!(ev.get("topic").and_then(Json::as_str), Some("t"));
            assert_eq!(ev.get("partition").and_then(Json::as_i64), Some(0));
            (
                ev.get("offset").and_then(Json::as_i64).unwrap(),
                ev.get("value").and_then(Json::as_str).unwrap().to_string(),
            )
        })
        .collect();
    seen.sort();
    for (i, (offset, value)) in seen.iter().enumerate() {
        assert_eq!(*offset, i64::try_from(i).unwrap());
        assert_eq!(*value, i.to_string(), "value must equal its offset ID");
    }

    // records_consumed summaries tile offsets 0..N contiguously.
    let consumed_events = named(&events, "records_consumed");
    assert!(!consumed_events.is_empty());
    let total: i64 = consumed_events
        .iter()
        .map(|ev| ev.get("count").and_then(Json::as_i64).unwrap())
        .sum();
    assert_eq!(total, n_i64, "consumed counts must sum to {N}");
    let mut covered = [false; N];
    for ev in &consumed_events {
        for summary in ev.get("partitions").and_then(Json::as_arr).unwrap() {
            assert_eq!(summary.get("topic").and_then(Json::as_str), Some("t"));
            assert_eq!(summary.get("partition").and_then(Json::as_i64), Some(0));
            let (count, min, max) = (
                summary.get("count").and_then(Json::as_i64).unwrap(),
                summary.get("minOffset").and_then(Json::as_i64).unwrap(),
                summary.get("maxOffset").and_then(Json::as_i64).unwrap(),
            );
            assert_eq!(
                count,
                max - min + 1,
                "summary must be contiguous: {summary:?}"
            );
            for off in min..=max {
                covered[usize::try_from(off).unwrap()] = true;
            }
        }
    }
    assert!(covered.iter().all(|c| *c), "summaries must cover 0..{N}");

    // Manual commits: one success per poll, error omitted, final offset N.
    let commits = named(&events, "offsets_committed");
    assert_eq!(
        commits.len(),
        consumed_events.len(),
        "one commit per consumed poll"
    );
    for ev in &commits {
        assert_eq!(ev.get("success").and_then(Json::as_bool), Some(true));
        assert!(
            ev.get("error").is_none(),
            "error must be omitted on success"
        );
        assert!(!ev.get("offsets").and_then(Json::as_arr).unwrap().is_empty());
    }
    let last = commits.last().unwrap();
    let committed: Vec<i64> = last
        .get("offsets")
        .and_then(Json::as_arr)
        .unwrap()
        .iter()
        .map(|o| o.get("offset").and_then(Json::as_i64).unwrap())
        .collect();
    assert_eq!(committed, &[n_i64], "final commit must be maxOffset + 1");
}

/// `producer_send_error` shape with IDs preserved, via mock fault injection.
///
/// `MESSAGE_TOO_LARGE` (10) is non-retriable, so sends fail immediately
/// without waiting out `delivery.timeout.ms`.
#[tokio::test]
async fn verifiable_producer_error_events() {
    let bin = example_bin("verifiable_producer").unwrap();
    let mock = common::Mock::start().await;
    mock.set_produce_error(10);
    let run = run_example(
        &bin,
        &[
            "--topic",
            "t",
            "--bootstrap-server",
            &mock.addr,
            "--max-messages",
            "3",
        ],
        TIMEOUT,
    )
    .await
    .unwrap();
    assert_eq!(
        run.status, 0,
        "error run must exit 0, stderr: {}",
        run.stderr
    );
    assert!(run.stderr.is_empty(), "stderr: {}", run.stderr);
    let events = parse_events("producer-error", &run.stdout).unwrap();
    assert_eq!(
        events.first().and_then(Json::name),
        Some("startup_complete")
    );
    let errors = named(&events, "producer_send_error");
    assert_eq!(errors.len(), 3);
    for (i, ev) in errors.iter().enumerate() {
        assert!(ev.get("key").is_some_and(Json::is_null));
        let expected = i.to_string();
        assert_eq!(
            ev.get("value").and_then(Json::as_str),
            Some(expected.as_str())
        );
        assert_eq!(ev.get("topic").and_then(Json::as_str), Some("t"));
        assert_eq!(ev.get("exception").and_then(Json::as_str), Some("Broker"));
        assert!(
            !ev.get("message")
                .and_then(Json::as_str)
                .unwrap_or("")
                .is_empty(),
            "error message must stay actionable: {ev:?}"
        );
    }
    assert!(named(&events, "producer_send_success").is_empty());
    let tool = events.last().unwrap();
    assert_eq!(tool.get("sent").and_then(Json::as_i64), Some(3));
    assert_eq!(tool.get("acked").and_then(Json::as_i64), Some(0));
}

/// Java `--producer.config` quirk: the file wins over CLI for `acks`, and
/// unknown keys warn on stderr (Java logs the same warning).
#[tokio::test]
async fn verifiable_producer_config_file_wins_over_cli() {
    let bin = example_bin("verifiable_producer").unwrap();
    let mock = common::Mock::start().await;
    let path = std::env::temp_dir().join(format!(
        "verifiable-producer-{}.properties",
        std::process::id()
    ));
    tokio::fs::write(&path, "acks=0\nbogus.key=1\ndelivery.timeout.ms=5000\n")
        .await
        .unwrap();
    let path_str = path.to_string_lossy().into_owned();
    let run = run_example(
        &bin,
        &[
            "--topic",
            "t",
            "--bootstrap-server",
            &mock.addr,
            "--acks",
            "-1",
            "--producer.config",
            &path_str,
            "--max-messages",
            "2",
        ],
        TIMEOUT,
    )
    .await
    .unwrap();
    drop(tokio::fs::remove_file(&path).await);
    assert_eq!(run.status, 0, "stderr: {}", run.stderr);
    assert!(
        run.stderr
            .contains("ignoring unsupported producer property: 'bogus.key'"),
        "unknown key must warn, got: {}",
        run.stderr
    );
    let events = parse_events("producer-config", &run.stdout).unwrap();
    let successes = named(&events, "producer_send_success");
    assert_eq!(successes.len(), 2);
    for ev in &successes {
        assert_eq!(
            ev.get("offset").and_then(Json::as_i64),
            Some(-1),
            "acks=0 from the file must win over CLI --acks -1: {ev:?}"
        );
    }
}

/// Streaming child for rebalance barriers; stdio is drained on owned threads.
/// Every child has a bounded message count and is killed/reaped if a test fails.
struct StreamingExample {
    child: std::process::Child,
    lines: std::sync::mpsc::Receiver<String>,
    stdout: Option<std::thread::JoinHandle<Result<String, String>>>,
    stderr: Option<std::thread::JoinHandle<Result<String, String>>>,
    events: Vec<Json>,
}

impl StreamingExample {
    fn start(bin: &Path, args: &[&str]) -> Result<Self, String> {
        use std::io::{BufRead, Read};
        let mut child = std::process::Command::new(bin)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| format!("spawn {}: {error}", bin.display()))?;
        let pipes = child.stdout.take().zip(child.stderr.take());
        let Some((stdout, mut stderr)) = pipes else {
            drop(child.kill());
            drop(child.wait());
            return Err("child lacks piped stdio".to_string());
        };
        let (tx, lines) = std::sync::mpsc::channel();
        let stdout = std::thread::spawn(move || {
            let mut output = String::new();
            for line in std::io::BufReader::new(stdout).lines() {
                let line = line.map_err(|error| format!("read stdout: {error}"))?;
                output.push_str(&line);
                output.push('\n');
                if tx.send(line).is_err() {
                    break;
                }
            }
            Ok(output)
        });
        let stderr = std::thread::spawn(move || {
            let mut output = String::new();
            let _bytes = stderr
                .read_to_string(&mut output)
                .map_err(|error| format!("read stderr: {error}"))?;
            Ok(output)
        });
        Ok(Self {
            child,
            lines,
            stdout: Some(stdout),
            stderr: Some(stderr),
            events: Vec::new(),
        })
    }

    async fn partition_event_after(
        &mut self,
        after: usize,
        name: &str,
        expected: &[i64],
    ) -> Result<(), String> {
        let deadline = Instant::now() + Duration::from_secs(10);
        loop {
            for line in self.lines.try_iter() {
                self.events.push(parse_json(&line)?);
            }
            for event in self.events.iter().skip(after) {
                if event.name() == Some(name) && event_partitions(event)? == expected {
                    return Ok(());
                }
            }
            if Instant::now() >= deadline {
                return Err(format!("missing {name} {expected:?}: {:?}", self.events));
            }
            if self
                .child
                .try_wait()
                .map_err(|error| error.to_string())?
                .is_some()
            {
                return Err(format!("child exited before {name}: {:?}", self.events));
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn finish(mut self) -> Result<Run, String> {
        let deadline = Instant::now() + Duration::from_secs(10);
        let status = loop {
            if let Some(status) = self.child.try_wait().map_err(|error| error.to_string())? {
                break status
                    .code()
                    .ok_or_else(|| "child died by signal".to_string())?;
            }
            if Instant::now() >= deadline {
                return Err("child did not stop after bounded messages".to_string());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        };
        let stdout = self
            .stdout
            .take()
            .ok_or_else(|| "missing stdout reader".to_string())?
            .join()
            .map_err(|_| "stdout reader panicked".to_string())??;
        let stderr = self
            .stderr
            .take()
            .ok_or_else(|| "missing stderr reader".to_string())?
            .join()
            .map_err(|_| "stderr reader panicked".to_string())??;
        Ok(Run {
            status,
            stdout,
            stderr,
        })
    }
}

impl Drop for StreamingExample {
    fn drop(&mut self) {
        drop(self.child.kill());
        drop(self.child.wait());
        if let Some(reader) = self.stdout.take() {
            drop(reader.join());
        }
        if let Some(reader) = self.stderr.take() {
            drop(reader.join());
        }
    }
}

fn event_partitions(event: &Json) -> Result<Vec<i64>, String> {
    let entries = event
        .get("partitions")
        .and_then(Json::as_arr)
        .ok_or_else(|| format!("missing partitions in {event:?}"))?;
    let mut partitions = Vec::new();
    for partition in entries {
        if partition.get("topic").and_then(Json::as_str) != Some("t") {
            return Err(format!("unexpected topic in {event:?}"));
        }
        partitions.push(
            partition
                .get("partition")
                .and_then(Json::as_i64)
                .ok_or_else(|| format!("missing partition id in {event:?}"))?,
        );
    }
    partitions.sort_unstable();
    if partitions
        .iter()
        .zip(partitions.iter().skip(1))
        .any(|(a, b)| a == b)
    {
        return Err(format!("duplicate partition in {event:?}"));
    }
    Ok(partitions)
}

#[tokio::test]
async fn verifiable_consumer_two_member_rebalance_events() {
    use partitionline::{ProduceRecord, Producer, ProducerConfig};
    let bin = example_bin("verifiable_consumer").unwrap();
    let mock = common::Mock::start().await;
    mock.set_topic_partitions("t", 2);
    let mut first = StreamingExample::start(
        &bin,
        &[
            "--topic",
            "t",
            "--group-id",
            "verifiable-rebalance",
            "--bootstrap-server",
            &mock.addr,
            "--max-messages",
            "2",
            "--verbose",
        ],
    )
    .unwrap();
    first
        .partition_event_after(0, "partitions_assigned", &[0, 1])
        .await
        .unwrap();
    let mut second = StreamingExample::start(
        &bin,
        &[
            "--topic",
            "t",
            "--group-id",
            "verifiable-rebalance",
            "--bootstrap-server",
            &mock.addr,
            "--max-messages",
            "1",
            "--verbose",
        ],
    )
    .unwrap();
    second
        .partition_event_after(0, "partitions_assigned", &[1])
        .await
        .unwrap();
    first
        .partition_event_after(0, "partitions_revoked", &[0, 1])
        .await
        .unwrap();
    first
        .partition_event_after(0, "partitions_assigned", &[0])
        .await
        .unwrap();
    let after_join = first.events.len();
    let producer =
        Producer::new(ProducerConfig::bootstrap([mock.addr.clone()]).linger(Duration::ZERO))
            .await
            .unwrap();
    let _metadata = producer
        .send(
            ProduceRecord::to("t")
                .partition(1)
                .value(&b"second-member"[..]),
        )
        .await
        .unwrap();
    let second = second.finish().await.unwrap();
    first
        .partition_event_after(after_join, "partitions_revoked", &[0])
        .await
        .unwrap();
    first
        .partition_event_after(after_join, "partitions_assigned", &[0, 1])
        .await
        .unwrap();
    let _metadata = producer
        .send_all([
            ProduceRecord::to("t").partition(0).value(&b"first-0"[..]),
            ProduceRecord::to("t").partition(0).value(&b"first-1"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();
    let first = first.finish().await.unwrap();
    for (label, run, expected_count) in [("first", &first, 2), ("second", &second, 1)] {
        assert_eq!(run.status, 0, "{label}: {}", run.stderr);
        assert!(run.stderr.is_empty(), "{label}: {}", run.stderr);
        let events = parse_events(label, &run.stdout).unwrap();
        assert_timestamps(&events, label);
        assert_eq!(
            events.first().and_then(Json::name),
            Some("startup_complete")
        );
        assert_eq!(
            events.last().and_then(Json::name),
            Some("shutdown_complete")
        );
        assert_eq!(named(&events, "record_data").len(), expected_count);
        assert!(named(&events, "offsets_committed")
            .iter()
            .all(|event| event.get("success").and_then(Json::as_bool) == Some(true)));
    }
    let events = parse_events("first", &first.stdout).unwrap();
    assert_eq!(
        named(&events, "partitions_assigned")
            .iter()
            .map(|event| event_partitions(event).unwrap())
            .collect::<Vec<_>>(),
        [vec![0, 1], vec![0], vec![0, 1]]
    );
    assert_eq!(
        named(&events, "partitions_revoked")
            .iter()
            .map(|event| event_partitions(event).unwrap())
            .collect::<Vec<_>>(),
        [vec![0, 1], vec![0], vec![0, 1]]
    );
    let events = parse_events("second", &second.stdout).unwrap();
    assert_eq!(
        named(&events, "partitions_assigned")
            .iter()
            .map(|event| event_partitions(event).unwrap())
            .collect::<Vec<_>>(),
        [vec![1]]
    );
    assert_eq!(
        named(&events, "partitions_revoked")
            .iter()
            .map(|event| event_partitions(event).unwrap())
            .collect::<Vec<_>>(),
        [vec![1]]
    );
    assert_eq!(
        mock.committed_offset("verifiable-rebalance", "t", 0),
        Some(2)
    );
    assert_eq!(
        mock.committed_offset("verifiable-rebalance", "t", 1),
        Some(1)
    );
}

#[tokio::test]
async fn verifiable_consumer_failed_commit_preserves_attempted_offsets() {
    use partitionline::{error, ProduceRecord, Producer};
    let bin = example_bin("verifiable_consumer").unwrap();
    let mock = common::Mock::start().await;
    let producer = Producer::connect(&mock.addr).await.unwrap();
    let _metadata = producer
        .send_all([
            ProduceRecord::to("t").value(&b"zero"[..]),
            ProduceRecord::to("t").value(&b"one"[..]),
            ProduceRecord::to("t").value(&b"two"[..]),
        ])
        .await
        .unwrap();
    producer.close().await.unwrap();
    mock.set_offset_commit_error(error::GROUP_AUTHORIZATION_FAILED);
    let run = run_example(
        &bin,
        &[
            "--topic",
            "t",
            "--group-id",
            "verifiable-commit-failure",
            "--bootstrap-server",
            &mock.addr,
            "--max-messages",
            "3",
            "--verbose",
        ],
        TIMEOUT,
    )
    .await
    .unwrap();
    assert_eq!(run.status, 0, "{}", run.stderr);
    assert!(run.stderr.is_empty(), "{}", run.stderr);
    let events = parse_events("failed-commit", &run.stdout).unwrap();
    assert_timestamps(&events, "failed-commit");
    assert_eq!(named(&events, "record_data").len(), 3);
    let commits = named(&events, "offsets_committed");
    assert!(!commits.is_empty());
    for event in &commits {
        assert_eq!(event.get("success").and_then(Json::as_bool), Some(false));
        assert!(event
            .get("error")
            .and_then(Json::as_str)
            .is_some_and(|error| error.contains("GROUP_AUTHORIZATION_FAILED")));
        let offsets = event.get("offsets").and_then(Json::as_arr).unwrap();
        assert_eq!(offsets.len(), 1);
        assert_eq!(offsets[0].get("topic").and_then(Json::as_str), Some("t"));
        assert_eq!(offsets[0].get("partition").and_then(Json::as_i64), Some(0));
    }
    let last = commits
        .last()
        .unwrap()
        .get("offsets")
        .and_then(Json::as_arr)
        .unwrap();
    assert_eq!(last[0].get("offset").and_then(Json::as_i64), Some(3));
    assert_eq!(
        mock.committed_offset("verifiable-commit-failure", "t", 0),
        None
    );
    assert_eq!(
        events.last().and_then(Json::name),
        Some("shutdown_complete")
    );
}
