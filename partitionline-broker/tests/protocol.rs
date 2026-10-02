//! Independent Apache header/ApiVersions goldens and bounded failure behavior.
#![allow(clippy::unwrap_used)]
use partitionline_broker::protocol::{
    ApiVersionsHandler, Error, Limits, RequestHeader, IMPLEMENTED_API_VERSIONS,
};
use partitionline_broker::transport::{Config, Handler, Transport};
use std::{error::Error as StdError, sync::Arc, time::Duration};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
type Golden = (&'static str, &'static [u8], Option<&'static [u8]>);
macro_rules! goldens {
    ($release:literal) => {
        [
            (
                "v0-named",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/v0-named.request.bin"
                )) as &[u8],
                Some(include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/v0-named.response.bin"
                )) as &[u8]),
            ),
            (
                "v1-null-client",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/v1-null-client.request.bin"
                )) as &[u8],
                Some(include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/v1-null-client.response.bin"
                )) as &[u8]),
            ),
            (
                "v2-empty-client",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/v2-empty-client.request.bin"
                )) as &[u8],
                Some(include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/v2-empty-client.response.bin"
                )) as &[u8]),
            ),
            (
                "v3-flexible",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/v3-flexible.request.bin"
                )) as &[u8],
                Some(include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/v3-flexible.response.bin"
                )) as &[u8]),
            ),
            (
                "v4-unknown-tags",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/v4-unknown-tags.request.bin"
                )) as &[u8],
                Some(include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/v4-unknown-tags.response.bin"
                )) as &[u8]),
            ),
            (
                "v3-empty-software",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/v3-empty-software.request.bin"
                )) as &[u8],
                Some(include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/v3-empty-software.response.bin"
                )) as &[u8]),
            ),
            (
                "v4-empty-software-version",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/v4-empty-software-version.request.bin"
                )) as &[u8],
                Some(include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/v4-empty-software-version.response.bin"
                )) as &[u8]),
            ),
            (
                "v3-underscore-software",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/v3-underscore-software.request.bin"
                )) as &[u8],
                Some(include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/v3-underscore-software.response.bin"
                )) as &[u8]),
            ),
            (
                "v4-trailing-dash-version",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/v4-trailing-dash-version.request.bin"
                )) as &[u8],
                Some(include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/v4-trailing-dash-version.response.bin"
                )) as &[u8]),
            ),
            (
                "unsupported-v5",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/unsupported-v5.request.bin"
                )) as &[u8],
                Some(include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/unsupported-v5.response.bin"
                )) as &[u8]),
            ),
            (
                "unsupported-negative-version",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/unsupported-negative-version.request.bin"
                )) as &[u8],
                Some(include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/unsupported-negative-version.response.bin"
                )) as &[u8]),
            ),
            (
                "header-truncated-0",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/header-truncated-0.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "header-truncated-1",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/header-truncated-1.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "header-truncated-2",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/header-truncated-2.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "header-truncated-3",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/header-truncated-3.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "header-truncated-4",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/header-truncated-4.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "header-truncated-5",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/header-truncated-5.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "header-truncated-6",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/header-truncated-6.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "header-truncated-7",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/header-truncated-7.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "header-truncated-8",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/header-truncated-8.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "header-truncated-9",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/header-truncated-9.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "header-truncated-10",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/header-truncated-10.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "client-invalid-utf8",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/client-invalid-utf8.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "client-length-minus2",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/client-length-minus2.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "header-duplicate-tags",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/header-duplicate-tags.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "header-descending-tags",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/header-descending-tags.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "header-tag-size-truncated",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/header-tag-size-truncated.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "header-tag-varint-overflow",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/header-tag-varint-overflow.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "header-tag-varint-sixbytes",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/header-tag-varint-sixbytes.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "unknown-api-key",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/unknown-api-key.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "unimplemented-metadata",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/unimplemented-metadata.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "supported-v0-trailing-body",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/supported-v0-trailing-body.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
            (
                "v3-body-null-software",
                include_bytes!(concat!(
                    "fixtures/protocol/",
                    $release,
                    "/v3-body-null-software.request.bin"
                )) as &[u8],
                None::<&[u8]>,
            ),
        ]
    };
}
const CLASSIC: &[u8] = include_bytes!("fixtures/protocol/4.3.1/v0-named.request.bin");
const FLEXIBLE: &[u8] = include_bytes!("fixtures/protocol/4.3.1/v3-flexible.request.bin");
const REGISTRY: &str = include_str!("../../tests/conformance/broker/implemented-api-versions.json");

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
fn registry_string(key: &str) -> &str {
    let suffix = REGISTRY.split_once(&format!("\"{key}\": \"")).unwrap().1;
    suffix.split('"').next().unwrap()
}

#[tokio::test]
async fn compiled_registry_and_all_apache_goldens() -> Result<(), Box<dyn StdError>> {
    let registry = REGISTRY
        .split_once("\"implemented_api_versions\"")
        .unwrap()
        .1;
    let array = registry
        .split_once('[')
        .unwrap()
        .1
        .split(']')
        .next()
        .unwrap();
    let numbers: Vec<i16> = array
        .split(|c: char| !c.is_ascii_digit() && c != '-')
        .filter(|s| !s.is_empty())
        .map(str::parse)
        .collect::<Result<_, _>>()?;
    let compiled: Vec<i16> = IMPLEMENTED_API_VERSIONS
        .iter()
        .flat_map(|v| [v.api_key, v.min_version, v.max_version])
        .collect();
    assert_eq!(compiled, numbers);
    assert_eq!(compiled, [18, 0, 4]);
    let handler = ApiVersionsHandler::default();
    let mut results = Vec::new();
    for (release, cases) in [
        ("4.1.2", goldens!("4.1.2")),
        ("4.2.1", goldens!("4.2.1")),
        ("4.3.1", goldens!("4.3.1")),
    ] {
        let cases: [Golden; 33] = cases;
        for (name, request, expected) in cases {
            let response = handler.handle(request.to_vec()).await;
            match (response, expected) {
                (Ok(Some(actual)), Some(expected)) => {
                    assert_eq!(actual, expected, "{release}/{name}");
                    if name.starts_with("v0-")
                        || name.starts_with("v1-")
                        || name.starts_with("v2-")
                        || name == "v3-flexible"
                        || name == "v4-unknown-tags"
                    {
                        let offset = if request[3] >= 3 { 7 } else { 10 };
                        assert_eq!(
                            &actual[offset..offset + 6],
                            &[0, 18, 0, 0, 0, 4],
                            "honest advertisement"
                        );
                    }
                    results.push(format!(
                        "{{\"release\":\"{release}\",\"case\":\"{name}\",\"response_hex\":\"{}\"}}",
                        hex(&actual)
                    ));
                }
                (Err(_), None) => results.push(format!(
                    "{{\"release\":\"{release}\",\"case\":\"{name}\",\"response_hex\":null}}"
                )),
                (actual, expected) => {
                    return Err(
                        format!("{release}/{name}: {actual:?} expected {expected:?}").into(),
                    )
                }
            }
        }
    }
    assert_eq!(results.len(), 99);
    if let Some(path) = std::env::var_os("PARTITIONLINE_WIRE_REPORT") {
        let data = format!("{{\"schema_version\":1,\"protocol_source_sha256\":\"{}\",\"test_source_sha256\":\"{}\",\"implemented_api_versions\":[{{\"api_key\":18,\"min_version\":0,\"max_version\":4}}],\"case_results\":[{}]}}\n", registry_string("protocol_source_sha256"), registry_string("test_source_sha256"), results.join(","));
        tokio::task::spawn_blocking(move || {
            // Optional test evidence is written on the blocking pool, never on
            // the executor or in the production handler.
            #[allow(clippy::disallowed_methods)]
            std::fs::write(path, data)
        })
        .await??;
    }
    Ok(())
}

#[test]
fn classic_and_flexible_headers_borrow_classic_client_id() -> Result<(), Error> {
    let (classic, body) = RequestHeader::parse(CLASSIC, 1, Limits::default())?;
    assert_eq!(
        (classic.api_key, classic.api_version, classic.correlation_id),
        (18, 0, 7)
    );
    assert_eq!(classic.client_id, Some("partitionline"));
    assert!(body.is_empty());
    let (flex, body) = RequestHeader::parse(FLEXIBLE, 2, Limits::default())?;
    assert_eq!(flex.client_id, Some("π-client"));
    assert_eq!(flex.correlation_id, i32::MAX);
    assert!(body.len() > 2);
    assert!(std::ptr::eq(
        flex.client_id.unwrap().as_ptr(),
        FLEXIBLE[10..].as_ptr()
    ));
    assert_eq!(
        RequestHeader::parse(CLASSIC, 0, Limits::default()),
        Err(Error::UnsupportedHeaderVersion)
    );
    Ok(())
}

#[test]
fn validated_limits_bound_frame_and_tag_work() -> Result<(), Error> {
    for (bytes, tags) in [(0, 1), (64 * 1024 * 1024 + 1, 1), (1, 65537)] {
        assert_eq!(Limits::new(bytes, tags), Err(Error::InvalidLimits));
    }
    let limits = Limits::new(CLASSIC.len() - 1, 0)?;
    assert_eq!(
        ApiVersionsHandler::new(limits).respond(CLASSIC),
        Err(Error::RequestTooLarge)
    );
    assert_eq!(
        RequestHeader::parse(CLASSIC, 1, limits),
        Err(Error::RequestTooLarge)
    );
    let limits = Limits::new(64 * 1024 * 1024, 65536)?;
    assert_eq!(limits.max_request_bytes(), 64 * 1024 * 1024);
    assert_eq!(limits.max_tagged_fields(), 65536);
    let tagged = include_bytes!("fixtures/protocol/4.3.1/v4-unknown-tags.request.bin");
    assert_eq!(
        ApiVersionsHandler::new(Limits::new(1024, 1)?).respond(tagged),
        Err(Error::TooManyTags)
    );
    let tagless = ApiVersionsHandler::new(Limits::new(1024, 0)?);
    assert!(tagless.respond(FLEXIBLE).is_ok());
    Ok(())
}

fn flexible(body: &[u8]) -> Vec<u8> {
    let mut request = vec![0, 18, 0, 3, 0, 0, 0, 7, 0, 0, 0];
    request.extend_from_slice(body);
    request
}
#[test]
fn all_supported_request_truncations_fail_without_panicking() {
    let handler = ApiVersionsHandler::default();
    for request in [
        CLASSIC,
        FLEXIBLE,
        include_bytes!("fixtures/protocol/4.3.1/v4-unknown-tags.request.bin"),
    ] {
        for end in 0..request.len() {
            assert!(handler.respond(&request[..end]).is_err(), "length{end}");
        }
    }
}

#[test]
fn software_utf8_null_string_length_and_tag_faults_fail_closed() {
    let handler = ApiVersionsHandler::default();
    for (body, error) in [
        (vec![2, 255, 2, b'1', 0], Error::InvalidUtf8),
        (vec![0, 2, b'1', 0], Error::InvalidLength),
        (vec![0x81, 0x80, 2], Error::InvalidLength), //32768-byte string exceeds schema before borrow
        (
            vec![2, b'a', 2, b'1', 2, 7, 0, 7, 0],
            Error::InvalidTagOrder,
        ),
        (
            vec![2, b'a', 2, b'1', 2, 9, 0, 7, 0],
            Error::InvalidTagOrder,
        ),
        (
            vec![2, b'a', 2, b'1', 1, 7, 255, 255, 255, 255, 15],
            Error::Truncated,
        ),
        (
            vec![2, b'a', 2, b'1', 255, 255, 255, 255, 31],
            Error::InvalidVarint,
        ),
        (
            vec![2, b'a', 2, b'1', 128, 128, 128, 128, 128, 0],
            Error::InvalidVarint,
        ),
        (vec![2, b'a', 2, b'1', 0, 1], Error::TrailingBytes),
        (vec![2, b'a', 2, b'1', 129, 8], Error::TooManyTags),
    ] {
        assert_eq!(handler.respond(&flexible(&body)), Err(error));
    }
    // Last valid u32 tagID, zero-byte unknown field, no tag payload allocation.
    assert!(handler
        .respond(&flexible(&[2, b'a', 2, b'1', 1, 255, 255, 255, 255, 15, 0]))
        .is_ok());
}

#[test]
fn semantic_software_validation_matches_apache_pattern() {
    let handler = ApiVersionsHandler::default();
    for name in ["a", "9", "a.b", "a-b", "A-0.9"] {
        let mut body = vec![(name.len() + 1) as u8];
        body.extend_from_slice(name.as_bytes());
        body.extend_from_slice(&[2, b'1', 0]);
        assert_eq!(&handler.respond(&flexible(&body)).unwrap()[4..6], &[0, 0]);
    }
    for name in ["", "-a", "a-", ".a", "a.", "a_b", "a b", "π"] {
        let mut body = vec![(name.len() + 1) as u8];
        body.extend_from_slice(name.as_bytes());
        body.extend_from_slice(&[2, b'1', 0]);
        let response = handler.respond(&flexible(&body)).unwrap();
        assert_eq!(&response[4..7], &[0, 42, 1]);
    }
}

#[test]
fn unsupported_api_versions_ignore_unknown_body_and_echo_signed_correlation() {
    let handler = ApiVersionsHandler::default();
    for version in [i16::MIN, -1, 5, i16::MAX] {
        let mut request = vec![0, 18];
        request.extend_from_slice(&version.to_be_bytes());
        request.extend_from_slice(&i32::MIN.to_be_bytes());
        request.extend_from_slice(&[255, 255]);
        if version >= 3 {
            request.push(0);
        }
        request.extend_from_slice(&[255, 128, 42]);
        assert_eq!(
            handler.respond(&request).unwrap(),
            [128, 0, 0, 0, 0, 35, 0, 0, 0, 1, 0, 18, 0, 0, 0, 4]
        );
    }
    for key in [i16::MIN, -1, 0, 3, 4, 92, 93, i16::MAX] {
        let mut request = CLASSIC.to_vec();
        request[..2].copy_from_slice(&key.to_be_bytes());
        assert_eq!(handler.respond(&request), Err(Error::UnimplementedApi(key)));
    }
}

async fn send(socket: &mut TcpStream, request: &[u8]) -> Result<(), Box<dyn StdError>> {
    socket
        .write_all(&i32::try_from(request.len())?.to_be_bytes())
        .await?;
    socket.write_all(request).await?;
    Ok(())
}
async fn read(socket: &mut TcpStream) -> Result<Vec<u8>, Box<dyn StdError>> {
    let length = socket.read_i32().await?;
    let mut response = vec![0; usize::try_from(length)?];
    socket.read_exact(&mut response).await?;
    Ok(response)
}
fn config() -> Result<Config, partitionline_broker::transport::Error> {
    Config::new(
        4,
        2,
        1024,
        1024,
        Duration::from_secs(1),
        Duration::from_secs(1),
        Duration::from_secs(1),
    )
}

#[tokio::test]
async fn real_tcp_pipeline_success_semantic_error_and_unsupported_fallback(
) -> Result<(), Box<dyn StdError>> {
    let mut server = Transport::bind(
        "127.0.0.1:0".parse()?,
        config()?,
        Arc::new(ApiVersionsHandler::default()),
    )
    .await?;
    let mut socket = TcpStream::connect(server.local_addr()).await?;
    for name in [
        "v0-named",
        "v4-unknown-tags",
        "v3-empty-software",
        "v3-flexible",
        "unsupported-v5",
        "v0-named",
    ] {
        let cases: [Golden; 33] = goldens!("4.3.1");
        let (_, request, expected) = cases.iter().find(|(n, _, _)| *n == name).unwrap();
        send(&mut socket, request).await?;
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), read(&mut socket)).await??,
            expected.unwrap()
        );
    }
    let report = server.shutdown().await?;
    assert_eq!(report.accepted_connections, report.joined_connections);
    assert_eq!(report.handler_errors, 0);
    Ok(())
}

#[tokio::test]
async fn malformed_or_unimplemented_connection_closes_while_peer_continues(
) -> Result<(), Box<dyn StdError>> {
    let mut server = Transport::bind(
        "127.0.0.1:0".parse()?,
        config()?,
        Arc::new(ApiVersionsHandler::default()),
    )
    .await?;
    let mut healthy = TcpStream::connect(server.local_addr()).await?;
    for request in [
        include_bytes!("fixtures/protocol/4.3.1/client-invalid-utf8.request.bin") as &[u8],
        include_bytes!("fixtures/protocol/4.3.1/unimplemented-metadata.request.bin"),
    ] {
        let mut bad = TcpStream::connect(server.local_addr()).await?;
        send(&mut bad, request).await?;
        let mut byte = [0];
        assert_eq!(
            tokio::time::timeout(Duration::from_secs(2), bad.read(&mut byte)).await??,
            0
        );
        send(&mut healthy, CLASSIC).await?;
        assert_eq!(
            read(&mut healthy).await?,
            include_bytes!("fixtures/protocol/4.3.1/v0-named.response.bin")
        );
    }
    let report = server.shutdown().await?;
    assert_eq!(report.handler_errors, 2);
    assert_eq!(report.accepted_connections, report.joined_connections);
    assert_eq!(report.worker_failures, 0);
    Ok(())
}
