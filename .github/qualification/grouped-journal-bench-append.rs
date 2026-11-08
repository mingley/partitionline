
/// Runs a temporary, local qualification workload.
#[doc(hidden)]
pub fn grouped_reservation_qualification(
    path: &std::path::Path,
    width: usize,
) -> Result<u128, Error> {
    const GROUPS: usize = 192;
    if width != 1 && width != 8 {
        return Err(Error::InvalidLimits);
    }
    let (mut log, _) = Journal::open(path, 0, Limits::default())?;
    let start = std::time::Instant::now();
    for group in 0..GROUPS {
        let mut tickets = Vec::new();
        tickets.try_reserve_exact(width).map_err(|_| Error::AllocationFailed)?;
        for lane in 0..width {
            let offset = (group * width + lane) as u64;
            let bytes = offset.to_le_bytes();
            tickets.push(log.append_staged_grouped(1, &bytes, crc32c::crc32c(&bytes))?);
        }
        tickets.last().ok_or(Error::InvalidLimits)?.sync_data()?;
        for lane in 0..width {
            let expected = (group * width + lane) as u64;
            let actual = log.commit_staged()?;
            if actual.first_offset != expected || actual.next_offset != expected + 1 {
                return Err(Error::ChangedFile);
            }
        }
    }
    let elapsed = start.elapsed().as_nanos();
    drop(log);
    let (mut log, recovered) = Journal::open(path, 0, Limits::default())?;
    let count = GROUPS * width;
    if recovered.next_offset != count as u64 {
        return Err(Error::ChangedFile);
    }
    let entries = log.fetch(0, count, 1024 * 1024)?;
    if entries.len() != count {
        return Err(Error::ChangedFile);
    }
    for (offset, entry) in entries.iter().enumerate() {
        if entry.first_offset != offset as u64
            || entry.record_count != 1
            || entry.payload != (offset as u64).to_le_bytes()
        {
            return Err(Error::ChangedFile);
        }
    }
    Ok(elapsed)
}
