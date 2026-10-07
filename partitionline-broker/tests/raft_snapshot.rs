//! Canonical snapshot bytes, hostile complete images and real interrupted publication.

use partitionline_broker::raft::{
    election::LogPosition,
    snapshot::{Descriptor, Entry, Error, Identity, Limits, Store},
};
use std::{
    fs,
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
};

static NEXT: AtomicU64 = AtomicU64::new(0);
struct Directory(PathBuf);
impl Directory {
    fn new() -> std::io::Result<Self> {
        let path = std::env::temp_dir().join(format!(
            "partitionline-snapshot-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path)?;
        Ok(Self(path))
    }
    fn store(&self, name: &str) -> Result<Store, Error> {
        Store::open(self.0.join(name), identity()?, limits()?)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn identity() -> Result<Identity, Error> {
    Identity::new(
        "controller-fixture".into(),
        "__cluster_metadata".into(),
        0,
        vec![0, 1, 2],
    )
}
fn limits() -> Result<Limits, Error> {
    Limits::new(8192, 8, 4096, 2048, 17, 4, 16)
}
fn entries() -> Vec<Entry> {
    vec![
        Entry {
            term: 5,
            index: 1,
            barrier: false,
            voters: false,
            payload: b"alpha".to_vec(),
        },
        Entry {
            term: 5,
            index: 2,
            barrier: true,
            voters: false,
            payload: vec![],
        },
        Entry {
            term: 6,
            index: 3,
            barrier: false,
            voters: false,
            payload: b"beta".to_vec(),
        },
    ]
}
fn base() -> LogPosition {
    LogPosition { term: 6, index: 3 }
}
fn wire(store: &mut Store, generation: [u8; 16]) -> Result<(Descriptor, Vec<u8>), Error> {
    let descriptor = store.start_read(generation)?;
    let mut bytes = Vec::new();
    loop {
        let chunk = store.next_chunk()?;
        assert_eq!(chunk.offset, bytes.len() as u64);
        assert!(chunk.bytes.len() <= limits()?.chunk_bytes());
        bytes.extend_from_slice(&chunk.bytes);
        if chunk.done {
            break;
        }
    }
    assert_eq!(descriptor.bytes, bytes.len() as u64);
    assert_eq!(descriptor.checksum, crc32c::crc32c(&bytes));
    Ok((descriptor, bytes))
}
fn receive(store: &mut Store, descriptor: Descriptor, bytes: &[u8]) -> Result<(), Error> {
    store.begin_receive(descriptor)?;
    for (ordinal, chunk) in bytes.chunks(limits()?.chunk_bytes()).enumerate() {
        store.receive_chunk(
            descriptor.generation,
            (ordinal * limits()?.chunk_bytes()) as u64,
            chunk,
        )?;
    }
    store.finish_receive(descriptor.generation)?;
    Ok(())
}
fn rehash(bytes: &mut [u8]) -> Result<(), std::array::TryFromSliceError> {
    let header = u32::from_be_bytes(bytes[12..16].try_into()?) as usize;
    let crc = crc32c::crc32c(&bytes[..header - 4]);
    bytes[header - 4..header].copy_from_slice(&crc.to_be_bytes());
    let seal = bytes.len() - 24;
    let crc = crc32c::crc32c(&bytes[..seal]);
    bytes[seal + 16..seal + 20].copy_from_slice(&crc.to_be_bytes());
    let crc = crc32c::crc32c(&bytes[seal..seal + 20]);
    bytes[seal + 20..].copy_from_slice(&crc.to_be_bytes());
    Ok(())
}

fn read_file(path: impl AsRef<std::path::Path>) -> std::io::Result<Vec<u8>> {
    use std::io::Read;
    let mut bytes = Vec::new();
    fs::File::open(path)?.take(8193).read_to_end(&mut bytes)?;
    if bytes.len() > 8192 {
        return Err(std::io::Error::other("bounded fixture read exceeded"));
    }
    Ok(bytes)
}
fn write_file(path: impl AsRef<std::path::Path>, bytes: impl AsRef<[u8]>) -> std::io::Result<()> {
    use std::io::Write;
    let mut file = fs::File::create(path)?;
    file.write_all(bytes.as_ref())?;
    file.sync_all()
}
#[test]
fn canonical_prefix_round_trip_and_sequential_transfer() {
    let directory = Directory::new().unwrap();
    let mut source = directory.store("source").unwrap();
    let published = source.create([1; 16], base(), &entries()).unwrap();
    let image = source.load([1; 16]).unwrap();
    assert_eq!(image.entries, entries());
    assert_eq!(image.descriptor, published.descriptor());
    let (descriptor, bytes) = wire(&mut source, [1; 16]).unwrap();
    let mut destination = directory.store("destination").unwrap();
    destination.begin_receive(descriptor).unwrap();
    assert!(matches!(
        destination.begin_receive(descriptor),
        Err(Error::Busy)
    ));
    assert!(matches!(
        destination.receive_chunk([1; 16], 1, &bytes[..17]),
        Err(Error::InvalidChunk)
    ));
    assert!(matches!(
        destination.receive_chunk([2; 16], 0, &bytes[..17]),
        Err(Error::InvalidChunk)
    ));
    assert!(matches!(
        destination.receive_chunk([1; 16], 0, &bytes[..18]),
        Err(Error::InvalidChunk)
    ));
    assert!(matches!(
        destination.finish_receive([1; 16]),
        Err(Error::Incomplete)
    ));
    for (ordinal, chunk) in bytes.chunks(17).enumerate() {
        destination
            .receive_chunk([1; 16], (ordinal * 17) as u64, chunk)
            .unwrap();
    }
    destination.finish_receive([1; 16]).unwrap();
    assert_eq!(destination.load([1; 16]).unwrap().entries, entries());
    assert!(matches!(
        destination.begin_receive(descriptor),
        Err(Error::DuplicateGeneration)
    ));
    assert!(matches!(destination.next_chunk(), Err(Error::InvalidChunk)));
}

#[test]
fn fully_rehashed_hostile_images_cannot_bless_malformed_content() {
    let directory = Directory::new().unwrap();
    let mut source = directory.store("source").unwrap();
    source.create([1; 16], base(), &entries()).unwrap();
    let (descriptor, bytes) = wire(&mut source, [1; 16]).unwrap();
    let header = u32::from_be_bytes(bytes[12..16].try_into().unwrap()) as usize;
    let second = header + 32 + b"alpha".len();
    for variant in 0..8 {
        let mut hostile = bytes.clone();
        match variant {
            0 => hostile[68] ^= 1,
            1 => hostile[48..56].copy_from_slice(&u64::MAX.to_be_bytes()),
            2 => hostile[second..second + 8].copy_from_slice(&4u64.to_be_bytes()),
            3 => hostile[second + 8..second + 16].copy_from_slice(&4u64.to_be_bytes()),
            4 => hostile[header + 24..header + 28].copy_from_slice(&u32::MAX.to_be_bytes()),
            5 => hostile[header + 16] = 2,
            6 => hostile[header + 17] = 1,
            7 => hostile[10] = 1,
            _ => unreachable!(),
        }
        rehash(&mut hostile).unwrap();
        let mut forged = descriptor;
        forged.checksum = crc32c::crc32c(&hostile);
        let mut destination = directory.store(&format!("variant-{variant}")).unwrap();
        let result = receive(&mut destination, forged, &hostile);
        assert!(matches!(
            result,
            Err(Error::ForeignIdentity | Error::InvalidDescriptor | Error::Corrupt | Error::Bounds)
        ));
        assert_eq!(destination.generation_count(), 0);
        destination.abort_receive().unwrap();
    }
}

#[test]
fn exact_completion_seal_and_crc_are_required() {
    let directory = Directory::new().unwrap();
    let mut source = directory.store("source").unwrap();
    source.create([1; 16], base(), &entries()).unwrap();
    let (descriptor, bytes) = wire(&mut source, [1; 16]).unwrap();
    for variant in 0..4 {
        let mut hostile = bytes.clone();
        match variant {
            0 => {
                hostile.truncate(hostile.len() - 24);
            }
            1 => hostile.push(0),
            2 => {
                let position = hostile.len() - 24;
                hostile[position] ^= 1;
            }
            3 => hostile[68] ^= 1,
            _ => unreachable!(),
        }
        let mut forged = descriptor;
        forged.bytes = hostile.len() as u64;
        forged.checksum = crc32c::crc32c(&hostile);
        let mut destination = directory.store(&format!("seal-{variant}")).unwrap();
        assert!(matches!(
            receive(&mut destination, forged, &hostile),
            Err(Error::InvalidDescriptor | Error::Incomplete | Error::Checksum)
        ));
        assert_eq!(destination.generation_count(), 0);
        destination.abort_receive().unwrap();
    }
}

#[test]
fn identity_and_generation_budgets_are_preflighted() {
    assert!(Identity::new("x".into(), "__cluster_metadata".into(), 0, vec![1, 1]).is_err());
    assert!(Identity::new("x".into(), "__cluster_metadata".into(), 0, vec![2, 1]).is_err());
    assert!(Identity::new("x".into(), String::new(), 0, vec![1]).is_err());
    assert!(Limits::new(1024, 4096, 4096, 2048, 17, 4, 16).is_err());
    let directory = Directory::new().unwrap();
    let mut store = directory.store("store").unwrap();
    let mut invalid = entries();
    invalid[1].index = 9;
    assert!(store.create([1; 16], base(), &invalid).is_err());
    assert_eq!(fs::read_dir(directory.0.join("store")).unwrap().count(), 0);
    let impossible = Descriptor {
        generation: [1; 16],
        base: base(),
        records: 3,
        payload_bytes: 9,
        bytes: 24,
        checksum: 0,
    };
    assert!(matches!(
        store.begin_receive(impossible),
        Err(Error::InvalidDescriptor)
    ));
    assert_eq!(fs::read_dir(directory.0.join("store")).unwrap().count(), 0);
    assert!(matches!(
        Store::open(
            directory.0.join("missing-parent").join("image-store"),
            identity().unwrap(),
            limits().unwrap()
        ),
        Err(Error::Storage(_))
    ));
    assert!(!directory.0.join("missing-parent").exists());
    for value in 1..=4 {
        store.create([value; 16], base(), &entries()).unwrap();
    }
    assert!(matches!(
        store.create([5; 16], base(), &entries()),
        Err(Error::Bounds)
    ));
    let loaded = store.load([1; 16]).unwrap();
    assert_eq!(loaded.entries, entries());
    assert!(limits().unwrap().decoded_bytes() >= limits().unwrap().payload_bytes());
    assert_eq!(
        limits().unwrap().disk_bytes(),
        limits().unwrap().image_bytes() * 5
    );
}

#[test]
fn foreign_fixed_group_and_namespace_are_rejected() {
    let directory = Directory::new().unwrap();
    let mut source = directory.store("source").unwrap();
    source.create([1; 16], base(), &entries()).unwrap();
    let (descriptor, bytes) = wire(&mut source, [1; 16]).unwrap();
    let different = Identity::new(
        "controller-fixture".into(),
        "__cluster_metadata".into(),
        0,
        vec![0, 1, 3],
    )
    .unwrap();
    let mut destination =
        Store::open(directory.0.join("foreign"), different, limits().unwrap()).unwrap();
    assert!(matches!(
        receive(&mut destination, descriptor, &bytes),
        Err(Error::ForeignIdentity)
    ));
    destination.abort_receive().unwrap();
    let hostile = directory.0.join("hostile");
    fs::create_dir(&hostile).unwrap();
    write_file(hostile.join("unknown-file"), b"x").unwrap();
    assert!(matches!(
        Store::open(&hostile, identity().unwrap(), limits().unwrap()),
        Err(Error::ForeignIdentity)
    ));
    let textual = Identity::new(
        "../../escape".into(),
        "__cluster_metadata".into(),
        0,
        vec![0, 1, 2],
    )
    .unwrap();
    let mut data_only =
        Store::open(directory.0.join("data-only"), textual, limits().unwrap()).unwrap();
    data_only.create([1; 16], base(), &entries()).unwrap();
    assert_eq!(
        fs::read_dir(directory.0.join("data-only")).unwrap().count(),
        1
    );
}

#[test]
fn ambiguous_rename_error_poison_preserves_previous_image() {
    let directory = Directory::new().unwrap();
    let mut source = directory.store("source").unwrap();
    source.create([2; 16], base(), &entries()).unwrap();
    let (descriptor, bytes) = wire(&mut source, [2; 16]).unwrap();
    let target = directory.0.join("target");
    let mut store = Store::open(&target, identity().unwrap(), limits().unwrap()).unwrap();
    store.create([1; 16], base(), &entries()).unwrap();
    store.begin_receive(descriptor).unwrap();
    for (ordinal, chunk) in bytes.chunks(17).enumerate() {
        store
            .receive_chunk([2; 16], (ordinal * 17) as u64, chunk)
            .unwrap();
    }
    let blocker = target.join("snapshot-02020202020202020202020202020202.image");
    fs::create_dir(&blocker).unwrap();
    assert!(matches!(
        store.finish_receive([2; 16]),
        Err(Error::Storage(_))
    ));
    assert!(store.poisoned());
    assert!(matches!(store.load([1; 16]), Err(Error::Poisoned)));
    drop(store);
    fs::remove_dir(blocker).unwrap();
    let mut reopened = Store::open(&target, identity().unwrap(), limits().unwrap()).unwrap();
    assert_eq!(reopened.generation_count(), 1);
    assert_eq!(reopened.load([1; 16]).unwrap().entries, entries());
}

#[test]
fn snapshot_crash_helper() {
    let Ok(target) = std::env::var("PL_SNAPSHOT_CRASH_TARGET") else {
        return;
    };
    let wire_path = std::env::var("PL_SNAPSHOT_CRASH_WIRE").unwrap();
    let bytes = read_file(wire_path).unwrap();
    let descriptor = Descriptor {
        generation: [2; 16],
        base: base(),
        records: 3,
        payload_bytes: 9,
        bytes: bytes.len() as u64,
        checksum: crc32c::crc32c(&bytes),
    };
    let mut store = Store::open(target, identity().unwrap(), limits().unwrap()).unwrap();
    store.begin_receive(descriptor).unwrap();
    store.receive_chunk([2; 16], 0, &bytes[..17]).unwrap();
    std::process::exit(44);
}

#[test]
fn actual_process_exit_during_transfer_keeps_old_image_recoverable() {
    let directory = Directory::new().unwrap();
    let mut source = directory.store("source").unwrap();
    source.create([2; 16], base(), &entries()).unwrap();
    let (_, bytes) = wire(&mut source, [2; 16]).unwrap();
    let target = directory.0.join("target");
    let mut old = Store::open(&target, identity().unwrap(), limits().unwrap()).unwrap();
    old.create([1; 16], base(), &entries()).unwrap();
    drop(old);
    let wire_path = directory.0.join("wire.bin");
    write_file(&wire_path, bytes).unwrap();
    let result = Command::new(std::env::current_exe().unwrap())
        .args(["--exact", "snapshot_crash_helper", "--nocapture"])
        .env("PL_SNAPSHOT_CRASH_TARGET", &target)
        .env("PL_SNAPSHOT_CRASH_WIRE", &wire_path)
        .status()
        .unwrap();
    assert_eq!(result.code(), Some(44));
    let mut reopened = Store::open(&target, identity().unwrap(), limits().unwrap()).unwrap();
    assert_eq!(reopened.generation_count(), 1);
    assert_eq!(reopened.load([1; 16]).unwrap().entries, entries());
    assert_eq!(fs::read_dir(&target).unwrap().count(), 1);
}

#[test]
fn empty_prefix_and_canceled_reader_have_explicit_boundaries() {
    let directory = Directory::new().unwrap();
    let mut store = directory.store("store").unwrap();
    store.create([1; 16], LogPosition::default(), &[]).unwrap();
    let image = store.load([1; 16]).unwrap();
    assert!(image.entries.is_empty());
    assert_eq!(image.descriptor.base, LogPosition::default());
    store.start_read([1; 16]).unwrap();
    assert!(matches!(store.start_read([1; 16]), Err(Error::Busy)));
    store.cancel_read();
    store.start_read([1; 16]).unwrap();
    while !store.next_chunk().unwrap().done {}
    assert!(matches!(store.next_chunk(), Err(Error::InvalidChunk)));
}

#[test]
fn actual_apache_opaque_observation_preserves_order_and_boundary_mapping() {
    let observation = include_str!("fixtures/raft-snapshot/apache-opaque-state.tsv");
    let mut exclusive = None;
    let mut epoch = None;
    let mut included = None;
    let mut opaque = Vec::new();
    for line in observation.lines() {
        let (field, value) = line.split_once('\t').unwrap();
        match field {
            "exclusive_offset" => exclusive = Some(value.parse::<u64>().unwrap()),
            "epoch" => epoch = Some(value.parse::<u64>().unwrap()),
            "last_contained_offset" => included = Some(value.parse::<u64>().unwrap()),
            "record_hex" => {
                assert_eq!(value.len() % 2, 0);
                let bytes = value
                    .as_bytes()
                    .as_chunks::<2>()
                    .0
                    .iter()
                    .map(|pair| u8::from_str_radix(std::str::from_utf8(pair).unwrap(), 16).unwrap())
                    .collect::<Vec<_>>();
                opaque.push(bytes);
            }
            _ => panic!("unreviewed Apache observation field"),
        }
    }
    let index = exclusive.unwrap();
    let term = epoch.unwrap() + 1;
    assert_eq!(included.unwrap() + 1, index);
    assert_eq!(opaque.len() as u64 + 1, index);
    let mut prefix = opaque
        .iter()
        .enumerate()
        .map(|(ordinal, bytes)| Entry {
            index: ordinal as u64 + 1,
            term,
            barrier: false,
            voters: false,
            payload: bytes.clone(),
        })
        .collect::<Vec<_>>();
    prefix.push(Entry {
        index,
        term,
        barrier: true,
        voters: false,
        payload: vec![],
    });
    let directory = Directory::new().unwrap();
    let mut source = directory.store("source").unwrap();
    source
        .create([1; 16], LogPosition { index, term }, &prefix)
        .unwrap();
    let (descriptor, bytes) = wire(&mut source, [1; 16]).unwrap();
    let mut destination = directory.store("destination").unwrap();
    receive(&mut destination, descriptor, &bytes).unwrap();
    drop(destination);
    let mut restarted = directory.store("destination").unwrap();
    let image = restarted.load([1; 16]).unwrap();
    assert_eq!(image.descriptor.base, LogPosition { index, term });
    assert_eq!(image.entries, prefix);
    assert_eq!(
        image
            .entries
            .into_iter()
            .filter(|entry| !entry.barrier)
            .map(|entry| entry.payload)
            .collect::<Vec<_>>(),
        opaque
    );
    // This checks opaque state/boundary correspondence. The image format and
    // internal barrier layout deliberately make no Apache serialization claim.
}
