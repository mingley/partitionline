use bytes::Bytes;
use std::collections::HashMap;
use std::hint::black_box;
use std::time::Instant;

struct Rec {
    topic: String,
    key: Option<Bytes>,
    value: Option<Bytes>,
}

fn fetched_bytes(rec: &Rec) -> u64 {
    let k = rec.key.as_ref().map(Bytes::len).unwrap_or(0);
    let v = rec.value.as_ref().map(Bytes::len).unwrap_or(0);
    u64::try_from(k.saturating_add(v)).unwrap_or(u64::MAX)
}

fn current(recs: &[Rec]) -> (u64, u64) {
    let n = u64::try_from(recs.len()).unwrap_or(u64::MAX);
    let bytes: u64 = recs.iter().map(fetched_bytes).fold(0, u64::saturating_add);
    let mut acc: HashMap<&str, (u64, u64)> = HashMap::new();
    for r in recs {
        let b = fetched_bytes(r);
        let e = acc.entry(r.topic.as_str()).or_insert((0, 0));
        e.0 = e.0.saturating_add(1);
        e.1 = e.1.saturating_add(b);
    }
    let mut topic_n = 0u64;
    let mut topic_b = 0u64;
    for (n, b) in acc.values() {
        topic_n = topic_n.saturating_add(*n);
        topic_b = topic_b.saturating_add(*b);
    }
    (n.saturating_add(bytes), topic_n.saturating_add(topic_b))
}

fn fused(recs: &[Rec]) -> (u64, u64) {
    let mut n = 0u64;
    let mut bytes = 0u64;
    let mut acc: HashMap<&str, (u64, u64)> = HashMap::new();
    for r in recs {
        let b = fetched_bytes(r);
        n = n.saturating_add(1);
        bytes = bytes.saturating_add(b);
        let e = acc.entry(r.topic.as_str()).or_insert((0, 0));
        e.0 = e.0.saturating_add(1);
        e.1 = e.1.saturating_add(b);
    }
    (n, bytes)
}

fn positions_scan(assigned: &[(String, i32, i64)], pending_front: &[Option<i64>]) -> u64 {
    let mut sink = 0u64;
    let mut owned: Vec<(String, i32, i64)> = Vec::with_capacity(assigned.len());
    for (i, (t, p, o)) in assigned.iter().enumerate() {
        let pos = pending_front[i].unwrap_or(*o);
        owned.push((t.clone(), *p, pos));
        sink = sink.wrapping_add(pos as u64);
    }
    let mut ids: HashMap<(String, i32), u64> = HashMap::with_capacity(owned.len());
    for (t, p, pos) in &owned {
        ids.insert((t.clone(), *p), *pos as u64);
        sink = sink.wrapping_add(*pos as u64);
    }
    black_box(ids);
    sink
}

fn main() {
    let payload = Bytes::from(vec![b'x'; 100]);
    let topic = "nb-fetch".to_string();
    let recs: Vec<Rec> = (0..135_000)
        .map(|_| Rec {
            topic: topic.clone(),
            key: None,
            value: Some(payload.clone()),
        })
        .collect();
    // warmup
    for _ in 0..3 {
        black_box(current(&recs));
        black_box(fused(&recs));
    }
    let rounds = 20;
    let t0 = Instant::now();
    for _ in 0..rounds {
        black_box(current(black_box(&recs)));
    }
    let cur = t0.elapsed().as_nanos() / rounds;
    let t1 = Instant::now();
    for _ in 0..rounds {
        black_box(fused(black_box(&recs)));
    }
    let fus = t1.elapsed().as_nanos() / rounds;
    println!("bulk_like current_ns={cur} fused_ns={fus} saved_ns={}", cur.saturating_sub(fus));
    println!(
        "fraction_of_65710000_ns current_pct={:.4} saved_pct={:.4}",
        cur as f64 / 65710000.0 * 100.0,
        cur.saturating_sub(fus) as f64 / 65710000.0 * 100.0
    );

    let one = &recs[..1];
    let t2 = Instant::now();
    for _ in 0..2000 {
        black_box(current(black_box(one)));
    }
    let cap = t2.elapsed().as_nanos();
    println!(
        "capped_2000_polls current_total_ns={cap} pct_of_2000*24812={:.4}",
        cap as f64 / (2000.0 * 24812.0) * 100.0
    );

    let assigned: Vec<(String, i32, i64)> = (0..6).map(|p| (topic.clone(), p, 20_000)).collect();
    let fronts = vec![None; 6];
    let t3 = Instant::now();
    let mut sink = 0u64;
    for _ in 0..2000 {
        sink = sink.wrapping_add(positions_scan(black_box(&assigned), black_box(&fronts)));
    }
    black_box(sink);
    let pos = t3.elapsed().as_nanos();
    println!(
        "group_positions_2000_polls_6p_ns={pos} pct_of_capped={:.4}",
        pos as f64 / (2000.0 * 24812.0) * 100.0
    );
    let t4 = Instant::now();
    let mut sink = 0u64;
    for _ in 0..1 {
        sink = sink.wrapping_add(positions_scan(black_box(&assigned), black_box(&fronts)));
    }
    black_box(sink);
    println!(
        "group_positions_1_poll_6p_ns={} pct_of_bulk={:.4}",
        t4.elapsed().as_nanos(),
        t4.elapsed().as_nanos() as f64 / 65710000.0 * 100.0
    );
}
