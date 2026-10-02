use std::{env, fs, io::Read};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = env::args().collect();
    let mode = args.get(1).ok_or("missing mode")?.as_str();
    let input = fs::read(args.get(2).ok_or("missing input")?)?;
    let target = args.get(3).ok_or("missing output")?;
    let parameter: usize = args.get(4).ok_or("missing cap or level")?.parse()?;
    let mut output = Vec::new();
    match mode {
        "ruzstd-decode" => {
            // Bound the retained output. This alone does not bound the decoder's
            // history allocation; the corpus itself has a <= 512 KiB window.
            let mut decoder = ruzstd::decoding::StreamingDecoder::new(&input[..])?;
            (&mut decoder)
                .take(parameter as u64 + 1)
                .read_to_end(&mut output)?;
            if output.len() > parameter {
                return Err("output cap exceeded".into());
            }
            // StreamingDecoder exposes stored and calculated checksums but does
            // not compare them. The future adapter must make this explicit.
            let stored = decoder.decoder.get_checksum_from_data();
            let computed = decoder.decoder.get_calculated_checksum();
            if stored.is_some() && stored != computed {
                return Err("checksum mismatch".into());
            }
            if !decoder.get_ref().is_empty() {
                return Err("trailing frame bytes".into());
            }
        }
        "zstd-rs-decode" => {
            zstd_rs::Decompressor::new().decompress(&input, None, parameter, &mut output)?;
        }
        "ruzstd-encode" => {
            output = ruzstd::encoding::compress_to_vec(
                &input[..],
                ruzstd::encoding::CompressionLevel::Fastest,
            );
        }
        "zstd-rs-encode" => {
            let config = zstd_rs::CompressionConfig {
                level: parameter as i32,
                checksum: true,
                content_size: true,
                window_log: 19,
                ..zstd_rs::CompressionConfig::DEFAULT
            };
            zstd_rs::Compressor::new(config)?.compress(&input, None, &mut output)?;
        }
        _ => return Err("unknown mode".into()),
    }
    fs::write(target, output)?;
    Ok(())
}
