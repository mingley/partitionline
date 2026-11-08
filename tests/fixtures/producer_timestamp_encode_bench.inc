    #[tokio::test(flavor = "current_thread")]
    #[ignore]
    async fn timestamp_encode_sample() {
        let mode = std::env::var("PL_BENCH_MODE").expect("PL_BENCH_MODE");
        assert!(mode == "implicit" || mode == "explicit");
        let rounds: usize = std::env::var("PL_BENCH_ROUNDS")
            .expect("PL_BENCH_ROUNDS")
            .parse()
            .expect("integer rounds");
        assert!((1..=4096).contains(&rounds));
        let topic = Arc::<str>::from("t");
        let records = (0..4096).map(|_| {
            let mut record = queued("t", Some(0));
            record.rec.value = Some(Bytes::from_static(b"12345678"));
            if mode == "explicit" {
                record.rec.timestamp = Some(1234567);
            }
            record
        }).collect();
        let mut groups = vec![(topic, 0, records)];
        let mut buf = BytesMut::new();
        let mut scratch = CompressScratch::default();
        let mut group_scratch = ProduceGroupScratch::default();
        let mut measured = Duration::ZERO;
        let mut expected = None;
        for _ in 0..rounds {
            if mode == "implicit" {
                for pending in &mut groups.first_mut().unwrap().2 {
                    pending.rec.timestamp = None;
                }
            }
            buf.clear();
            let start = Instant::now();
            encode_produce_body_async(
                &mut buf, 9, 1, 1000, &mut groups, &[], Compression::None,
                123456789, -1, -1, None, &mut scratch, &mut group_scratch,
            ).await.unwrap();
            measured += start.elapsed();
            let digest = (buf.len(), crc32c::crc32c(&buf));
            if let Some(expected) = expected {
                assert_eq!(digest, expected);
            } else {
                expected = Some(digest);
            }
        }
        let (bytes, crc) = expected.unwrap();
        println!("TIMESTAMP_BENCH_ENCODE mode={mode} rounds={rounds} records={} elapsed_ns={} bytes={bytes} crc={crc}", rounds * 4096, measured.as_nanos());
    }

