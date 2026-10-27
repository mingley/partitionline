use std::fmt::Debug;
use std::io::Write;
use std::path::Path;

pub fn failed(operation: &str, error: impl Debug) -> ! {
    eprintln!("fuzz infrastructure: {operation}: {error:?}");
    std::process::exit(86);
}

pub fn with_file(data: &[u8], kind: &str, exercise: impl FnOnce(&Path)) {
    let bytes = data.get(..data.len().min(64 * 1024)).unwrap_or(data);
    let path = std::env::temp_dir().join(format!("plb-fuzz-{kind}-{}", std::process::id()));
    let mut file = std::fs::File::create(&path)
        .unwrap_or_else(|error| failed("create recovery fixture", error));
    file.write_all(bytes)
        .unwrap_or_else(|error| failed("write recovery fixture", error));
    drop(file);
    exercise(&path);
    std::fs::remove_file(&path).unwrap_or_else(|error| failed("remove recovery fixture", error));
}
