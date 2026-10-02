//! The only direct FFI is read-only configuration inspection on a live client.
use rdkafka::{client::Client, client::ClientContext};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, ffi::CString, fs};

pub const WRAPPER_VERSION: &str = "0.39.0";
pub const BINDING_VERSION: &str = "4.10.0+2.12.1";
pub const NATIVE_VERSION: &str = "2.15.0";

pub fn runtime(expected_hash: &str) -> Result<serde_json::Value, String> {
    let (number, version) = rdkafka::util::get_rdkafka_version();
    if version != NATIVE_VERSION {
        return Err(format!("native version pin mismatch: {version}"));
    }
    if expected_hash.len() != 64 || !expected_hash.bytes().all(|b| b.is_ascii_hexdigit()) {
        return Err("expected native SHA256 must be supplied by the checked build manifest".into());
    }
    let maps = fs::read_to_string("/proc/self/maps")
        .map_err(|e| format!("loaded library verification requires Linux procfs: {e}"))?;
    let paths: std::collections::BTreeSet<_> = maps
        .lines()
        .filter_map(|line| line.split_whitespace().last())
        .filter(|path| path.ends_with("/librdkafka.so.1"))
        .collect();
    if paths.len() != 1 {
        return Err("expected exactly one mapped librdkafka.so.1; deleted/renamed/shadowed library unsupported".into());
    }
    let path = paths.first().expect("length checked");
    let bytes = fs::read(path).map_err(|e| format!("read mapped library: {e}"))?;
    let hash = format!("{:x}", Sha256::digest(bytes));
    if hash != expected_hash.to_ascii_lowercase() {
        return Err("loaded native library SHA256 differs from checked build manifest".into());
    }
    Ok(
        serde_json::json!({"wrapper_version":WRAPPER_VERSION,"binding_version":BINDING_VERSION,
        "binding_header_version":"2.12.1","native_version":version,"native_version_number":number,
        "loaded_library_sha256":hash,"loaded_library_path":path}),
    )
}

pub fn effective<C: ClientContext>(client: &Client<C>) -> Result<BTreeMap<String, String>, String> {
    let mut output = BTreeMap::new();
    // SAFETY: client owns the live RDKafka; rd_kafka_conf returns a borrowed immutable
    // configuration valid until client destruction. This function never mutates or frees it.
    let config = unsafe { rdkafka_sys::rd_kafka_conf(client.native_ptr()) };
    if config.is_null() {
        return Err("native client has no configuration".into());
    }
    for key in [
        "request.required.acks",
        "queue.buffering.max.ms",
        "batch.size",
        "batch.num.messages",
        "max.in.flight.requests.per.connection",
        "queue.buffering.max.messages",
        "queue.buffering.max.kbytes",
        "message.timeout.ms",
        "enable.idempotence",
        "compression.codec",
        "security.protocol",
        "socket.nagle.disable",
        "sticky.partitioning.linger.ms",
        "partitioner",
        "retries",
        "client.id",
    ] {
        let name = CString::new(key).expect("constant contains no NUL");
        let mut size = 0;
        // SAFETY: borrowed config and name are valid; null destination requests its size.
        let status = unsafe {
            rdkafka_sys::rd_kafka_conf_get(config, name.as_ptr(), std::ptr::null_mut(), &mut size)
        };
        if status != rdkafka_sys::rd_kafka_conf_res_t::RD_KAFKA_CONF_OK || size == 0 || size > 65536
        {
            return Err(format!(
                "native configuration unavailable or oversized: {key}"
            ));
        }
        let mut value = vec![0_u8; size];
        // SAFETY: allocated destination has exactly the returned bounded capacity. All
        // pointers remain valid during the read; configuration is not mutated concurrently.
        let status = unsafe {
            rdkafka_sys::rd_kafka_conf_get(
                config,
                name.as_ptr(),
                value.as_mut_ptr().cast(),
                &mut size,
            )
        };
        if status != rdkafka_sys::rd_kafka_conf_res_t::RD_KAFKA_CONF_OK || size > value.len() {
            return Err(format!("native configuration read failed: {key}"));
        }
        let text = std::ffi::CStr::from_bytes_with_nul(&value[..size])
            .map_err(|_| "invalid native config terminator")?
            .to_str()
            .map_err(|_| "non-UTF8 native config")?
            .to_owned();
        output.insert(key.to_owned(), text);
    }
    Ok(output)
}
