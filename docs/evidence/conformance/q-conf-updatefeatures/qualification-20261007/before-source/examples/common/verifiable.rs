//! Shared CLI/JSON helpers for the verifiable producer/consumer adapters.
//!
//! Contract pin: Apache Kafka **3.9.1** system-test tools, matching the
//! primary pin in `tests/conformance/java/pins.json`
//! (`upstream_tag 3.9.1`, `upstream_sha ce24d9b6aedca74f53c26f1ad2b2cc8ad950a03c`):
//!
//! - `tools/.../VerifiableProducer.java`: `--topic`, `--bootstrap-server` |
//!   `--broker-list`, `--max-messages`, `--throughput`, `--acks`,
//!   `--producer.config`, `--message-create-time`, `--value-prefix`,
//!   `--repeating-keys`; events `startup_complete`, `producer_send_success`,
//!   `producer_send_error`, `shutdown_complete`, `tool_data`.
//! - `tools/.../VerifiableConsumer.java`: connection group plus `--topic`,
//!   `--group-protocol`, `--group-remote-assignor`, `--group-id`,
//!   `--group-instance-id`, `--max-messages`, `--session-timeout`,
//!   `--verbose`, `--enable-autocommit`, `--reset-policy`,
//!   `--assignment-strategy`, `--consumer.config`; events `startup_complete`,
//!   `partitions_revoked`, `partitions_assigned`, `records_consumed`,
//!   `record_data`, `offsets_committed`, `shutdown_complete`.
//!
//! Every JSON event carries `timestamp` (ms since epoch) first, then `name`,
//! mirroring the Java `@JsonPropertyOrder({"timestamp", "name"})`. Events are
//! flushed to stdout line-by-line: ducktape-style harnesses read stdout as a
//! pipe, and block buffering would delay events past harness timeouts.
//! Diagnostics go to stderr; stdout stays JSON-parseable.
//!
//! [`exit_usage`] is the single CLI-failure path (exit 1, like Java
//! `parser.handleError` + `System.exit(1)`); bare invocation prints help and
//! exits 0, like the Java `main` methods.

use std::collections::HashMap;
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch (0 before the epoch or on clock error).
#[must_use]
pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis().try_into().unwrap_or(u64::MAX))
        .unwrap_or(0)
}

/// Append `s` as a JSON string literal (Jackson-compatible escaping).
pub(crate) fn push_json_str(out: &mut String, s: &str) {
    out.push('"');
    for c in s.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\u{08}' => out.push_str("\\b"),
            '\u{0C}' => out.push_str("\\f"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if u32::from(c) < 0x20 => {
                out.push_str(&format!("\\u{:04x}", u32::from(c)));
            }
            c => out.push(c),
        }
    }
    out.push('"');
}

/// One JSON event line, emitted flushed to stdout.
pub(crate) struct Event {
    buf: String,
}

impl Event {
    /// Start an event with `timestamp` and `name` (Java field order).
    #[must_use]
    pub(crate) fn new(name: &str) -> Self {
        let mut buf = String::from("{\"timestamp\":");
        buf.push_str(&now_ms().to_string());
        buf.push_str(",\"name\":");
        push_json_str(&mut buf, name);
        Self { buf }
    }

    /// Append a string field.
    pub(crate) fn str_field(&mut self, key: &str, value: &str) {
        self.buf.push(',');
        push_json_str(&mut self.buf, key);
        self.buf.push(':');
        push_json_str(&mut self.buf, value);
    }

    /// Append a nullable string field (`null` when `None`, like Jackson).
    pub(crate) fn opt_str_field(&mut self, key: &str, value: Option<&str>) {
        self.buf.push(',');
        push_json_str(&mut self.buf, key);
        self.buf.push(':');
        match value {
            Some(v) => push_json_str(&mut self.buf, v),
            None => self.buf.push_str("null"),
        }
    }

    /// Append an integer field.
    pub(crate) fn int_field(&mut self, key: &str, value: i64) {
        self.buf.push(',');
        push_json_str(&mut self.buf, key);
        self.buf.push(':');
        self.buf.push_str(&value.to_string());
    }

    /// Append a pre-rendered JSON fragment (nested object/array/float).
    pub(crate) fn raw_field(&mut self, key: &str, raw_json: &str) {
        self.buf.push(',');
        push_json_str(&mut self.buf, key);
        self.buf.push(':');
        self.buf.push_str(raw_json);
    }

    /// Print the event line and flush stdout (pipe-safe for harnesses).
    ///
    /// Stdout I/O errors are ignored: a dead stdout means the harness went
    /// away, and there is nowhere else to report it.
    pub(crate) fn emit(mut self) {
        self.buf.push('}');
        let stdout = std::io::stdout();
        let mut lock = stdout.lock();
        drop(lock.write_all(self.buf.as_bytes()));
        drop(lock.write_all(b"\n"));
        drop(lock.flush());
    }
}

/// Parsed `--opt value` / `--opt=value` / `--flag` command line.
///
/// Store-true flags are recorded as the value `"true"` so presence is a
/// [`RawArgs::get`] check; both example binaries share this one accessor.
#[derive(Debug, Default)]
pub(crate) struct RawArgs {
    values: HashMap<String, String>,
}

impl RawArgs {
    /// Value of an option, if present (last occurrence wins, argparse4j).
    ///
    /// Flags read back as `Some("true")` when passed.
    #[must_use]
    pub(crate) fn get(&self, name: &str) -> Option<&str> {
        self.values.get(name).map(String::as_str)
    }
}

/// Parse Java-tool-style CLI args.
///
/// `value_opts` take `--opt value` or `--opt=value`; `flag_opts` are bare
/// `--flag`s. `-h`/`--help` are rejected here so the caller can print help
/// (callers pre-scan for them, like argparse4j `defaultHelp`). Anything else
/// (unknown `--opt`, missing value, positional) is an error string.
pub(crate) fn parse_raw(
    args: &[String],
    value_opts: &[&str],
    flag_opts: &[&str],
) -> Result<RawArgs, String> {
    let mut out = RawArgs::default();
    let mut i = 0;
    while let Some(arg) = args.get(i) {
        if arg == "-h" || arg == "--help" {
            return Err("--help".to_string());
        }
        let Some(body) = arg.strip_prefix("--") else {
            return Err(format!("unexpected argument: {arg}"));
        };
        if body.is_empty() {
            return Err("unexpected argument: --".to_string());
        }
        let (name, inline) = match body.split_once('=') {
            Some((n, v)) => (n, Some(v)),
            None => (body, None),
        };
        if value_opts.contains(&name) {
            let value = match inline {
                Some(v) => v.to_string(),
                None => {
                    i += 1;
                    args.get(i)
                        .cloned()
                        .ok_or_else(|| format!("argument --{name}: expected one argument"))?
                }
            };
            let _inserted = out.values.insert(name.to_string(), value);
        } else if flag_opts.contains(&name) {
            if inline.is_some() {
                return Err(format!("argument --{name}: ignored explicit argument"));
            }
            let _inserted = out.values.insert(name.to_string(), "true".to_string());
        } else {
            return Err(format!("unrecognized arguments: --{name}"));
        }
        i += 1;
    }
    Ok(out)
}

/// Print a CLI error plus help to stderr and exit 1 (Java `handleError`).
pub(crate) fn exit_usage(tool: &str, help: &str, err: &str) -> ! {
    eprintln!("{tool}: error: {err}");
    eprintln!("{help}");
    std::process::exit(1);
}

/// True when `-h`/`--help` is present (argparse4j `defaultHelp`).
#[must_use]
pub(crate) fn wants_help(args: &[String]) -> bool {
    args.iter().any(|a| a == "-h" || a == "--help")
}

/// Parse an integer CLI/config value with a Java-style error.
pub(crate) fn parse_i64(value: &str, what: &str) -> Result<i64, String> {
    value
        .trim()
        .parse::<i64>()
        .map_err(|_| format!("argument {what}: invalid integer value: '{value}'"))
}

/// Parse a `*.config` Java-properties file body.
///
/// Supports `#`/`!` comments, blank lines, `\` continuations, `=`/`:`/bare
/// separators, and `\t \n \r \f \\ \" \' space \uXXXX` escapes. Duplicate
/// keys: last wins.
#[must_use]
pub(crate) fn parse_properties(text: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    let mut logical = String::new();
    for raw_line in text.lines() {
        let line = raw_line.strip_suffix('\r').unwrap_or(raw_line);
        // A trailing backslash continues the line when it is not itself
        // escaped (odd run of trailing backslashes).
        let trailing = line.len() - line.trim_end_matches('\\').len();
        if trailing % 2 == 1 {
            logical.push_str(line.get(..line.len() - 1).unwrap_or(""));
            continue;
        }
        logical.push_str(line);
        let trimmed = logical.trim_start_matches([' ', '\t', '\u{000C}']);
        if !trimmed.is_empty() && !trimmed.starts_with(['#', '!']) {
            let (key, value) = split_property(trimmed);
            out.push((unescape_property(&key), unescape_property(&value)));
        }
        logical.clear();
    }
    out
}

/// Split one logical properties line into key and value.
fn split_property(line: &str) -> (String, String) {
    let bytes = line.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        match bytes.get(i) {
            Some(b'\\') => i += 2,
            Some(b'=' | b':') => {
                let value = line
                    .get(i + 1..)
                    .unwrap_or("")
                    .trim_start_matches([' ', '\t', '\u{000C}']);
                let key = line.get(..i).unwrap_or("").trim_end();
                return (key.to_string(), value.to_string());
            }
            Some(b' ' | b'\t' | b'\x0C') => {
                let mut j = i + 1;
                while j < bytes.len() && matches!(bytes.get(j), Some(b' ' | b'\t' | b'\x0C')) {
                    j += 1;
                }
                let k = if matches!(bytes.get(j), Some(b'=' | b':')) {
                    j + 1
                } else {
                    j
                };
                let value = line
                    .get(k..)
                    .unwrap_or("")
                    .trim_start_matches([' ', '\t', '\u{000C}']);
                let key = line.get(..i).unwrap_or("");
                return (key.to_string(), value.to_string());
            }
            _ => i += 1,
        }
    }
    (line.to_string(), String::new())
}

/// Unescape Java-properties backslash sequences.
fn unescape_property(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    let mut chars = s.chars();
    while let Some(c) = chars.next() {
        if c != '\\' {
            out.push(c);
            continue;
        }
        match chars.next() {
            Some('t') => out.push('\t'),
            Some('n') => out.push('\n'),
            Some('r') => out.push('\r'),
            Some('f') => out.push('\u{000C}'),
            Some('u') => {
                let hex: String = chars.by_ref().take(4).collect();
                if hex.len() == 4 {
                    if let Ok(v) = u32::from_str_radix(&hex, 16) {
                        if let Some(u) = char::from_u32(v) {
                            out.push(u);
                            continue;
                        }
                    }
                }
                out.push_str("\\u");
                out.push_str(&hex);
            }
            Some(o) => out.push(o),
            None => out.push('\\'),
        }
    }
    out
}

/// Split `HOST1:PORT1,HOST2:PORT2,...` (Java bootstrap parsing shape).
pub(crate) fn split_bootstrap(list: &str) -> Result<Vec<String>, String> {
    let addrs: Vec<String> = list
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect();
    if addrs.is_empty() {
        return Err("no bootstrap servers (empty host list)".to_string());
    }
    Ok(addrs)
}

/// Resolve the `--bootstrap-server` | `--broker-list` mutual-exclusion group.
///
/// Both present is an error; `--broker-list` is the deprecated alias;
/// neither is an error (argparse4j `required(true)` group).
pub(crate) fn resolve_bootstrap(args: &RawArgs) -> Result<Vec<String>, String> {
    let server = args.get("bootstrap-server");
    let list = args.get("broker-list");
    match (server, list) {
        (Some(_), Some(_)) => {
            Err("argument --broker-list: not allowed with argument --bootstrap-server".to_string())
        }
        (Some(s), None) => split_bootstrap(s),
        (None, Some(l)) => split_bootstrap(l),
        (None, None) => {
            Err("one of the arguments --bootstrap-server --broker-list is required".to_string())
        }
    }
}
