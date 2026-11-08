//! Matched release-mode journal workload measurement.

use partitionline_broker::journal::{Journal, Limits};
use std::{env, error::Error, fs, hint::black_box, path::PathBuf, time::Instant};

fn main() -> Result<(), Box<dyn Error>> {
    let arguments: Vec<_> = env::args().collect();
    if arguments.len() != 6 {
        return Err("expected: mode arm trial operations directory".into());
    }
    let mode = &arguments[1];
    let arm = &arguments[2];
    let trial: usize = arguments[3].parse()?;
    let operations: usize = arguments[4].parse()?;
    let root = PathBuf::from(&arguments[5]);
    let seed = root.join("seed.journal");
    let work = root.join("work.journal");
    let limits = Limits::default();
    let seed_payload = [0xa5_u8; 256];
    let payload = [0x5a_u8; 256];

    if mode == "seed" {
        let (mut journal, _) = Journal::open(&seed, 0, limits)?;
        journal.append(1, &seed_payload)?;
        drop(journal);
        println!("PL_SEED_READY");
        return Ok(());
    }

    if mode == "layout" {
        let (ticket, lease) = Journal::__qualification_layout();
        println!("PL_LAYOUT {{\"arm\":\"{arm}\",\"ticket_bytes\":{ticket},\"lease_bytes\":{lease}}}");
        return Ok(());
    }

    if !matches!(mode.as_str(), "open" | "append" | "staged" | "group_stage")
        || operations == 0
        || (mode == "group_stage" && operations % 8 != 0)
    {
        return Err("invalid mode or operation count".into());
    }
    fs::copy(&seed, &work)?;
    if fs::read(&seed)? != fs::read(&work)? {
        return Err("seed copy differs".into());
    }
    let checksum = crc32c::crc32c(&payload);
    let elapsed = if mode == "open" {
        let start = Instant::now();
        for _ in 0..operations {
            let (journal, recovery) = Journal::open(&work, 0, limits)?;
            if recovery.recovered_entries != 1 {
                return Err("open workload recovered unexpected seed".into());
            }
            drop(journal);
        }
        start.elapsed().as_nanos()
    } else {
        let (mut journal, recovery) = Journal::open(&work, 0, limits)?;
        if recovery.recovered_entries != 1 {
            return Err("append workload recovered unexpected seed".into());
        }
        let duration = if mode == "group_stage" {
            journal.__qualification_group_stage(operations / 8, &payload, checksum)?
        } else {
            let start = Instant::now();
            if mode == "append" {
                for _ in 0..operations {
                    let _ = black_box(journal.append(1, &payload)?);
                }
            } else {
                for _ in 0..operations {
                    let _ = black_box(journal.__qualification_staged_sync(1, &payload, checksum)?);
                }
            }
            start.elapsed().as_nanos()
        };
        drop(journal);
        duration
    };

    let expected = if mode == "open" { 1 } else { operations + 1 };
    let (mut reopened, recovery) = Journal::open(&work, 0, limits)?;
    if recovery.recovered_entries != expected || reopened.next_offset() != expected as u64 {
        return Err("recovered record count differs".into());
    }
    let entries = reopened.fetch(0, expected, limits.max_fetch_bytes())?;
    if entries.len() != expected {
        return Err("fetched record count differs".into());
    }
    for (index, entry) in entries.iter().enumerate() {
        let expected_payload = if index == 0 { &seed_payload } else { &payload };
        if entry.first_offset != index as u64
            || entry.record_count != 1
            || entry.payload.as_slice() != expected_payload
        {
            return Err(format!("recovered record {index} differs").into());
        }
    }
    drop(reopened);
    fs::remove_file(&work)?;
    println!(
        "PL_MEASUREMENT {{\"arm\":\"{arm}\",\"mode\":\"{mode}\",\"trial\":{trial},\"operations\":{operations},\"elapsed_ns\":{},\"verified_records\":{expected}}}",
        elapsed
    );
    Ok(())
}
