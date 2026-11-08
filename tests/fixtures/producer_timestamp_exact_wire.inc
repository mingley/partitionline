    #[tokio::test(flavor = "current_thread")]
    #[ignore]
    async fn timestamp_exact_wire_bodies() {
        let output = std::path::PathBuf::from(
            std::env::var_os("PL_TIMESTAMP_WIRE_OUT").expect("PL_TIMESTAMP_WIRE_OUT"),
        );
        std::fs::create_dir_all(&output).unwrap();
        let topic = Arc::<str>::from("wire-check");
        let codecs = [
            ("none", Compression::None),
            ("gzip", Compression::Gzip),
            ("snappy", Compression::Snappy),
            ("lz4", Compression::Lz4),
            ("zstd", Compression::Zstd),
        ];
        let mut cases = 0;
        for version in [3_i16, 9, 13] {
            for (name, compression) in codecs {
                if version == 3 && name == "zstd" {
                    continue;
                }
                let mut first = queued("wire-check", Some(0));
                first.rec.key = Some(Bytes::from_static(b"key-first"));
                first.rec.value = Some(Bytes::from_static(b"value-first"));
                first.seq = Some(7);
                let mut explicit = queued("wire-check", Some(0));
                explicit.rec.timestamp = Some(123_456_700);
                explicit.rec.value = Some(Bytes::from_static(b"value-explicit"));
                let mut last = queued("wire-check", Some(0));
                last.rec.value = Some(Bytes::from_static(b"value-last"));
                let mut groups = vec![(Arc::clone(&topic), 0, vec![first, explicit, last])];
                let topic_ids = [(Arc::clone(&topic), [1_u8; 16])];
                let mut buffer = BytesMut::new();
                let mut scratch = CompressScratch::default();
                let mut group_scratch = ProduceGroupScratch::default();
                encode_produce_body_async(
                    &mut buffer, version, 1, 1000, &mut groups, &topic_ids, compression,
                    123_456_789, 17, 2, Some("txn"), &mut scratch, &mut group_scratch,
                )
                .await
                .unwrap();

                let mut decoded = buffer.clone().freeze();
                let topics = if version == 13 {
                    let (_, acks, timeout, topics, ids) =
                        crate::protocol::api::decode_produce_request_with_topic_ids(&mut decoded)
                            .unwrap();
                    assert_eq!((acks, timeout), (1, 1000));
                    assert_eq!(ids, vec![[1_u8; 16]]);
                    topics
                } else {
                    let (_, acks, timeout, topics) =
                        crate::protocol::api::decode_produce_request(&mut decoded, version).unwrap();
                    assert_eq!((acks, timeout), (1, 1000));
                    topics
                };
                let partition = topics.first().unwrap().partitions.first().unwrap();
                assert_eq!(partition.index, 0);
                let records = &partition.records.records;
                assert_eq!(records.len(), 3);
                assert_eq!(records[0].timestamp, 123_456_789);
                assert_eq!(records[1].timestamp, 123_456_700);
                assert_eq!(records[2].timestamp, 123_456_789);
                assert_eq!(records[0].key.as_deref(), Some(&b"key-first"[..]));
                assert_eq!(records[0].value.as_deref(), Some(&b"value-first"[..]));
                assert_eq!(records[1].value.as_deref(), Some(&b"value-explicit"[..]));
                assert_eq!(records[2].value.as_deref(), Some(&b"value-last"[..]));
                let path = output.join(format!("v{version}-{name}.bin"));
                std::fs::write(&path, &buffer).unwrap();
                println!(
                    "EXACT_WIRE version={version} compression={name} bytes={} decoded=ok",
                    buffer.len()
                );
                cases += 1;
            }
        }
        assert_eq!(cases, 14);
    }

