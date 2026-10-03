fn validate_list_transactions_body(mut bytes: &[u8], remaining_listings: usize) -> Result<()> {
    use crate::protocol::buf;
    let _ = buf::get_i32(&mut bytes)?;
    let _ = buf::get_i16(&mut bytes)?;
    let unknown = buf::get_array_len(&mut bytes, true)?
        .ok_or_else(|| Error::protocol("null ListTransactions unknown-filter array"))?;
    if unknown > 8_192 {
        return Err(Error::protocol(
            "ListTransactions unknown-filter count exceeds local limit",
        ));
    }
    for _ in 0..unknown {
        list_transactions_skip_string(&mut bytes, true, false)?;
    }
    let count = buf::get_array_len(&mut bytes, true)?
        .ok_or_else(|| Error::protocol("null ListTransactions listing array"))?;
    // Every listing includes two compact strings, an i64 and a tag count.
    if count > remaining_listings || count > bytes.len() / 11 {
        return Err(Error::protocol(
            "ListTransactions listing count exceeds local limit or body",
        ));
    }
    for _ in 0..count {
        list_transactions_skip_string(&mut bytes, true, false)?;
        let _ = buf::get_i64(&mut bytes)?;
        list_transactions_skip_string(&mut bytes, true, false)?;
        buf::skip_tagged_fields(&mut bytes)?;
    }
    buf::skip_tagged_fields(&mut bytes)?;
    if !bytes.is_empty() {
        return Err(Error::protocol("trailing ListTransactions response bytes"));
    }
    Ok(())
}

fn list_transactions_skip_string(bytes: &mut &[u8], flexible: bool, nullable: bool) -> Result<()> {
    let count = if flexible {
        let encoded = crate::protocol::buf::get_unsigned_varint(bytes)?;
        if encoded == 0 {
            if nullable {
                return Ok(());
            }
            return Err(Error::protocol("null required ListTransactions string"));
        }
        usize::try_from(encoded - 1)
            .map_err(|_| Error::protocol("ListTransactions string length overflow"))?
    } else {
        let length = crate::protocol::buf::get_i16(bytes)?;
        if length < 0 {
            if nullable && length == -1 {
                return Ok(());
            }
            return Err(Error::protocol(
                "null or invalid ListTransactions Metadata string",
            ));
        }
        usize::try_from(length)
            .map_err(|_| Error::protocol("ListTransactions Metadata string length overflow"))?
    };
    if count > 64 * 1024 || count > bytes.len() {
        return Err(Error::protocol(
            "ListTransactions string exceeds local limit or body",
        ));
    }
    let (text, remaining) = bytes.split_at(count);
    let _ =
        std::str::from_utf8(text).map_err(|_| Error::protocol("invalid ListTransactions UTF-8"))?;
    *bytes = remaining;
    Ok(())
}

fn validate_list_transactions_metadata(mut bytes: &[u8], version: i16) -> Result<()> {
    use crate::protocol::buf;
    let flexible = version >= 9;
    if version >= 3 {
        let _ = buf::get_i32(&mut bytes)?;
    }
    let brokers = buf::get_array_len(&mut bytes, flexible)?
        .ok_or_else(|| Error::protocol("null ListTransactions Metadata broker array"))?;
    if brokers == 0 || brokers > 256 {
        return Err(Error::protocol(
            "ListTransactions Metadata broker count outside 1..=256",
        ));
    }
    for _ in 0..brokers {
        let _ = buf::get_i32(&mut bytes)?;
        list_transactions_skip_string(&mut bytes, flexible, false)?;
        let port = buf::get_i32(&mut bytes)?;
        if !(1..=65_535).contains(&port) {
            return Err(Error::protocol(
                "invalid ListTransactions Metadata broker port",
            ));
        }
        if version >= 1 {
            list_transactions_skip_string(&mut bytes, flexible, true)?;
        }
        if flexible {
            buf::skip_tagged_fields(&mut bytes)?;
        }
    }
    if version >= 2 {
        list_transactions_skip_string(&mut bytes, flexible, true)?;
    }
    if version >= 1 {
        let _ = buf::get_i32(&mut bytes)?;
    }
    // This operation requests an empty topic selection. Reject a nonzero/null
    // topic count before decode_metadata_response allocates any topic vector.
    if buf::get_array_len(&mut bytes, flexible)? != Some(0) {
        return Err(Error::protocol(
            "unexpected ListTransactions Metadata topics",
        ));
    }
    if (8..=10).contains(&version) {
        let _ = buf::get_i32(&mut bytes)?;
    }
    if version >= 13 {
        let _ = buf::get_i16(&mut bytes)?;
    }
    if flexible {
        buf::skip_tagged_fields(&mut bytes)?;
    }
    if !bytes.is_empty() {
        return Err(Error::protocol("trailing ListTransactions Metadata bytes"));
    }
    Ok(())
}

