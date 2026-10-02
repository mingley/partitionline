//! Deterministic benchmark record envelopes and append-only history journals.

use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};

use bytes::Bytes;
use partitionline::Error;
use sha2::{Digest, Sha256};

pub(crate) const MAGIC: &[u8; 8] = b"PLBENCH1";
pub(crate) const MIN_PAYLOAD: usize = 24;

pub(crate) fn setting<T: std::str::FromStr>(name: &str, default: T) -> partitionline::Result<T> {
    optional_setting(name).map(|value| value.unwrap_or(default))
}

pub(crate) fn optional_setting<T: std::str::FromStr>(
    name: &str,
) -> partitionline::Result<Option<T>> {
    match std::env::var(name) {
        Ok(value) => value
            .parse()
            .map(Some)
            .map_err(|_| Error::protocol(format!("invalid {name}"))),
        Err(std::env::VarError::NotPresent) => Ok(None),
        Err(_) => Err(Error::protocol(format!("invalid {name}"))),
    }
}

pub(crate) fn flag(name: &str, default: bool) -> partitionline::Result<bool> {
    match std::env::var(name) {
        Ok(value) if value == "1" => Ok(true),
        Ok(value) if value == "0" => Ok(false),
        Ok(_) | Err(std::env::VarError::NotUnicode(_)) => {
            Err(Error::protocol(format!("{name} must be 0 or 1")))
        }
        Err(std::env::VarError::NotPresent) => Ok(default),
    }
}

pub(crate) fn positive(name: &str, value: u64) -> partitionline::Result<()> {
    if value == 0 {
        return Err(Error::protocol(format!("{name} must be positive")));
    }
    Ok(())
}

fn mix(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9e3779b97f4a7c15);
    x = (x ^ (x >> 30)).wrapping_mul(0xbf58476d1ce4e5b9);
    x = (x ^ (x >> 27)).wrapping_mul(0x94d049bb133111eb);
    x ^ (x >> 31)
}

pub(crate) fn payload(seed: u64, id: u64, size: usize) -> partitionline::Result<Bytes> {
    if size < MIN_PAYLOAD {
        return Err(Error::protocol("history PAYLOAD_BYTES must be at least 24"));
    }
    let mut out = Vec::with_capacity(size);
    out.extend_from_slice(MAGIC);
    out.extend_from_slice(&seed.to_be_bytes());
    out.extend_from_slice(&id.to_be_bytes());
    let mut word = mix(seed ^ id);
    while out.len() < size {
        let take = (size - out.len()).min(8);
        out.extend_from_slice(word.to_be_bytes().get(..take).unwrap_or(&[]));
        word = mix(word);
    }
    Ok(Bytes::from(out))
}

pub(crate) fn identity(value: &[u8]) -> Option<(u64, u64)> {
    if value.get(..8)? != MAGIC {
        return None;
    }
    let seed = u64::from_be_bytes(value.get(8..16)?.try_into().ok()?);
    let id = u64::from_be_bytes(value.get(16..24)?.try_into().ok()?);
    Some((seed, id))
}

pub(crate) fn key(seed: u64, partition: i32) -> Bytes {
    Bytes::from(format!("plbench-{seed:016x}-{partition}"))
}

pub(crate) fn hex(value: &[u8]) -> String {
    value.iter().map(|byte| format!("{byte:02x}")).collect()
}

pub(crate) fn hash(value: &[u8]) -> String {
    hex(&Sha256::digest(value))
}

pub(crate) fn quote(value: &str) -> String {
    let mut out = String::from("\"");
    for c in value.chars() {
        match c {
            '"' => out.push_str("\\\""),
            '\\' => out.push_str("\\\\"),
            '\n' => out.push_str("\\n"),
            '\r' => out.push_str("\\r"),
            '\t' => out.push_str("\\t"),
            c if c.is_control() => out.push_str(&format!("\\u{:04x}", u32::from(c))),
            c => out.push(c),
        }
    }
    out.push('"');
    out
}

pub(crate) struct Journal {
    file: BufWriter<File>,
}

impl Journal {
    pub(crate) fn create(path: &str) -> partitionline::Result<Self> {
        let file = OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(path)
            .map_err(|error| {
                Error::protocol(format!(
                    "create history {path}: {error}; existing artifacts are never overwritten"
                ))
            })?;
        Ok(Self {
            file: BufWriter::new(file),
        })
    }
    pub(crate) fn line(&mut self, line: &str) -> partitionline::Result<()> {
        writeln!(self.file, "{line}").map_err(Error::Io)
    }
    pub(crate) fn checkpoint(&mut self) -> partitionline::Result<()> {
        self.file.flush().map_err(Error::Io)
    }
    #[expect(
        clippy::too_many_arguments,
        reason = "journal event records the complete application identity and actual record metadata"
    )]
    pub(crate) fn record(
        &mut self,
        id: &str,
        topic: &str,
        partition: i32,
        offset: Option<i64>,
        key: Option<&[u8]>,
        value: &[u8],
        phase: &str,
        status: &str,
    ) -> partitionline::Result<()> {
        self.line(&format!("{{\"kind\":\"record\",\"id\":{},\"topic\":{},\"partition\":{partition},\"offset\":{},\"key\":{},\"payload_hash\":{},\"payload_bytes\":{},\"phase\":{},\"status\":{}}}",
            quote(id), quote(topic), offset.map(|o| o.to_string()).unwrap_or_else(|| "null".into()),
            key.map(|k| quote(&hex(k))).unwrap_or_else(|| "null".into()), quote(&hash(value)), value.len(), quote(phase), quote(status)))
    }
}
