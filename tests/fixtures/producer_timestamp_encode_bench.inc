    #[tokio::test(flavor = "current_thread")]
    #[ignore]
    async fn timestamp_encode_sample() {
        let mode = std::env::var("PL_BENCH_MODE").expect("PL_BENCH_MODE");
        assert!(mode == "implicit" || mode == "explicit");
        let rounds: usize = std::env::var("PL_BENCH_ROUNDS")
            .expect("PL_BENCH_ROUNDS")
            .parse()
            .expect("integer rounds");
        let batch: usize = std::env::var("PL_BENCH_BATCH")
            .expect("PL_BENCH_BATCH")
            .parse()
            .expect("integer batch");
        assert!((1..=4_194_304).contains(&rounds));
        assert!(batch == 1 || batch == 4096);
        let topic = Arc::<str>::from("t");
        let records: Vec<Pending> = (0..batch).map(|_| {
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
        let mut decoded = buf.clone().freeze();
        let (_, _, _, topics) = crate::protocol::api::decode_produce_request(&mut decoded, 9).unwrap();
        let records = &topics.first().unwrap().partitions.first().unwrap().records;
        let expected_time = if mode == "explicit" { 1234567 } else { 123456789 };
        assert_eq!(records.records.len(), batch);
        assert_eq!(records.base_timestamp, expected_time);
        for record in &records.records {
            assert_eq!(record.timestamp, expected_time);
            assert_eq!(record.value.as_deref(), Some(&b"12345678"[..]));
        }
        let reset = if mode == "implicit" { 1 } else { 0 };
        println!("TIMESTAMP_BENCH_ENCODE mode={mode} batch={batch} rounds={rounds} records={} elapsed_ns={} bytes={bytes} crc={crc} reset_implicit={reset} timestamps_verified=1", rounds * batch, measured.as_nanos());
    }

