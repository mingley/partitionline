
#[cfg(test)]
mod parse_resource_measurement {
    use super::{
        parse_registered_schema, parse_versioned_schema, RegisteredSchema, SchemaReference,
        VersionedSchema, MAX_BODY_LIMIT,
    };
    use std::fs::File;
    use std::hint::black_box;
    use std::io::{BufReader, Read};
    use std::path::Path;
    use std::time::Instant;

    enum Response {
        Id(RegisteredSchema),
        Version(VersionedSchema),
    }

    fn parse(by_id: bool, id: u32, body: &[u8]) -> Response {
        if by_id {
            Response::Id(parse_registered_schema(id, body).unwrap())
        } else {
            Response::Version(parse_versioned_schema(body).unwrap())
        }
    }

    fn verify(actual: &Response, expected: &serde_json::Value, schema_file: &Path) -> usize {
        let (id, schema, schema_type, refs): (u32, &str, Option<&str>, &[SchemaReference]) =
            match actual {
                Response::Id(value) => (
                    value.id,
                    &value.schema,
                    value.schema_type.as_deref(),
                    &value.references,
                ),
                Response::Version(value) => {
                    assert_eq!(Some(value.subject.as_str()), expected["subject"].as_str());
                    assert_eq!(Some(u64::from(value.version)), expected["version"].as_u64());
                    (
                        value.id,
                        &value.schema,
                        value.schema_type.as_deref(),
                        &value.references,
                    )
                }
            };
        assert_eq!(Some(u64::from(id)), expected["id"].as_u64());
        assert_eq!(schema_type, expected["schemaType"].as_str());
        let expected_refs = expected["references"].as_array().unwrap();
        assert_eq!(refs.len(), expected_refs.len());
        for (actual_ref, expected_ref) in refs.iter().zip(expected_refs) {
            assert_eq!(Some(actual_ref.name.as_str()), expected_ref["name"].as_str());
            assert_eq!(Some(actual_ref.subject.as_str()), expected_ref["subject"].as_str());
            assert_eq!(
                Some(u64::from(actual_ref.version)),
                expected_ref["version"].as_u64()
            );
        }

        // Verify every byte without keeping another full-sized schema in memory.
        let mut reader = BufReader::new(File::open(schema_file).unwrap());
        let mut block = [0; 8192];
        for chunk in schema.as_bytes().chunks(block.len()) {
            reader.read_exact(&mut block[..chunk.len()]).unwrap();
            assert_eq!(&block[..chunk.len()], chunk);
        }
        assert_eq!(reader.read(&mut block[..1]).unwrap(), 0);
        schema.len()
    }

    #[test]
    #[ignore = "bounded release-mode Registry parser measurement"]
    fn measure() {
        let fixture = std::path::PathBuf::from(std::env::var_os("PL_PARSE_FIXTURE").unwrap());
        let repetitions: usize = std::env::var("PL_PARSE_REPETITIONS").unwrap().parse().unwrap();
        let body = std::fs::read(fixture.with_extension("json")).unwrap();
        assert!(body.len() <= MAX_BODY_LIMIT);
        let expected: serde_json::Value = serde_json::from_reader(
            File::open(fixture.with_extension("expected")).unwrap(),
        )
        .unwrap();
        let id = u32::try_from(expected["id"].as_u64().unwrap()).unwrap();
        let by_id = expected["mode"].as_str() == Some("id");
        let schema_file = fixture.with_extension("schema");

        let schema_bytes = verify(&parse(by_id, id, &body), &expected, &schema_file);
        let mut durations = Vec::with_capacity(repetitions);
        for _ in 0..repetitions {
            let started = Instant::now();
            let result = black_box(parse(by_id, id, black_box(&body)));
            durations.push(started.elapsed().as_nanos());
            drop(result);
        }
        assert_eq!(verify(&parse(by_id, id, &body), &expected, &schema_file), schema_bytes);
        durations.sort_unstable();
        println!(
            "REGISTRY_PARSE body={} schema={} iterations={} median_ns={} max_ns={} total_ns={}",
            body.len(),
            schema_bytes,
            durations.len(),
            durations[durations.len() / 2],
            durations.last().unwrap(),
            durations.iter().sum::<u128>(),
        );
    }
}
