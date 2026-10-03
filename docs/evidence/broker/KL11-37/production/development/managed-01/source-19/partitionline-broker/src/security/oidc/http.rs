use super::{Error, Limits, Work};
use ::http::{header, Method, Request, Uri};
use bytes::Bytes;
use http_body_util::{BodyExt as _, Full};
use hyper_util::rt::TokioIo;
use rustls::{
    pki_types::{CertificateDer, ServerName},
    ClientConfig, RootCertStore,
};
use std::{
    fmt,
    net::{SocketAddr, ToSocketAddrs as _},
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::{
    net::TcpStream,
    sync::{watch, Mutex, Notify, OwnedSemaphorePermit, Semaphore},
    time::timeout,
};
use tokio_rustls::TlsConnector;
use zeroize::Zeroizing;

/// Bounds shared by every managed-authority HTTPS operation.
#[derive(Clone, Copy, Debug)]
pub struct HttpLimits {
    /// Maximum response JSON bytes, 1 KiB through 256 KiB.
    pub body_bytes: usize,
    /// Maximum buffered response headers, 8 through 64 KiB.
    pub header_bytes: usize,
    /// Maximum response header count, 1 through 64.
    pub headers: usize,
    /// Maximum concurrent DNS/connect/TLS/request/body operations, 1 through 32.
    pub jobs: usize,
    /// Absolute whole operation deadline, positive through 30 seconds.
    pub timeout: Duration,
}

#[cfg(test)]
mod tests {
    use super::*;

    fn client() -> Result<Client, Error> {
        Client::new(HttpsTrust {
            roots: vec![include_bytes!("../../../tests/fixtures/tls/ca1.cert.der").to_vec()],
            limits: HttpLimits {
                jobs: 1,
                ..HttpLimits::default()
            },
        })
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_resolution_keeps_http_admission_and_is_joined() {
        let client = client().unwrap();
        let permit = client.jobs.clone().try_acquire_owned().unwrap();
        let (started, entered) = tokio::sync::oneshot::channel();
        let (release, stalled) = std::sync::mpsc::channel();
        let other = client.clone();
        let caller = tokio::spawn(async move {
            other
                .resolve(permit, move || {
                    started.send(()).unwrap();
                    stalled.recv().unwrap();
                    Ok(vec![SocketAddr::from(([127, 0, 0, 1], 443))])
                })
                .await
        });
        entered.await.unwrap();
        caller.abort();
        assert!(caller.await.unwrap_err().is_cancelled());
        assert_eq!(client.jobs.available_permits(), 0);
        assert_eq!(client.resolver.outstanding.load(Ordering::Acquire), 1);
        let endpoint = Endpoint::parse("https://localhost/keys").unwrap();
        assert_eq!(client.get(&endpoint).await.unwrap_err(), Error::Busy);
        let other = client.clone();
        let shutdown = tokio::spawn(async move { other.shutdown().await });
        while !client.activity.stopping.load(Ordering::Acquire) {
            tokio::task::yield_now().await;
        }
        assert!(!shutdown.is_finished());
        shutdown.abort();
        assert!(shutdown.await.unwrap_err().is_cancelled());
        assert_eq!(client.jobs.available_permits(), 0);
        release.send(()).unwrap();
        client.shutdown().await.unwrap();
        assert_eq!(client.resolver.outstanding.load(Ordering::Acquire), 0);
        assert_eq!(client.jobs.available_permits(), 1);
        assert!(client.jobs.is_closed());
    }

    #[test]
    fn authority_urls_and_transport_limits_are_bounded() {
        for invalid in [
            "http://localhost/keys",
            "https://user@localhost/keys",
            "https://localhost:0/keys",
            "https://localhost:65536/keys",
            "https://localhost:bad/keys",
            "https://localhost/keys#fragment",
        ] {
            assert!(Endpoint::parse(invalid).is_err(), "accepted invalid URL");
        }
        assert_eq!(
            Endpoint::parse("https://LOCALHOST/keys").unwrap().origin(),
            "https://localhost:443"
        );
        assert!(HttpLimits {
            jobs: 33,
            ..HttpLimits::default()
        }
        .validate()
        .is_err());
        assert!(HttpLimits {
            timeout: Duration::ZERO,
            ..HttpLimits::default()
        }
        .validate()
        .is_err());
    }
}
impl Default for HttpLimits {
    fn default() -> Self {
        Self {
            body_bytes: 65536,
            header_bytes: 16384,
            headers: 32,
            jobs: 8,
            timeout: Duration::from_secs(3),
        }
    }
}
impl HttpLimits {
    fn validate(self) -> Result<(), Error> {
        if !(1024..=262144).contains(&self.body_bytes)
            || !(8192..=65536).contains(&self.header_bytes)
            || !(1..=64).contains(&self.headers)
            || !(1..=32).contains(&self.jobs)
            || self.timeout.is_zero()
            || self.timeout > Duration::from_secs(30)
        {
            return Err(Error::InvalidConfiguration);
        }
        Ok(())
    }
}
/// Explicit bounded DER trust anchors; no ambient trust, insecure verifier,
/// redirects, plaintext downgrade or token-selected origin is used.
pub struct HttpsTrust {
    /// One through 64 DER trust anchors, each at most 16 KiB, combined at most 1 MiB.
    pub roots: Vec<Vec<u8>>,
    /// Whole HTTPS operation ceilings.
    pub limits: HttpLimits,
}
impl fmt::Debug for HttpsTrust {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("HttpsTrust { [REDACTED] }")
    }
}

#[derive(Clone)]
pub(super) struct Endpoint {
    uri: Uri,
    host: String,
    port: u16,
    origin: String,
}
impl Endpoint {
    pub(super) fn parse(value: &str) -> Result<Self, Error> {
        let uri: Uri = value.parse().map_err(|_| Error::InvalidConfiguration)?;
        let authority = uri.authority().ok_or(Error::InvalidConfiguration)?;
        let host = uri.host().ok_or(Error::InvalidConfiguration)?;
        let suffix = authority
            .as_str()
            .get(host.len()..)
            .ok_or(Error::InvalidConfiguration)?;
        if value.len() > 2048
            || uri.scheme_str() != Some("https")
            || authority.as_str().contains('@')
            || value.contains('#')
            || uri.host().is_none_or(|h| h.is_empty() || h.len() > 256)
            || (!suffix.is_empty() && (!suffix.starts_with(':') || uri.port_u16().is_none()))
        {
            return Err(Error::InvalidConfiguration);
        }
        let host = uri.host().ok_or(Error::InvalidConfiguration)?.to_owned();
        let port = uri.port_u16().unwrap_or(443);
        if port == 0 {
            return Err(Error::InvalidConfiguration);
        }
        let origin = format!("https://{}:{}", host.to_ascii_lowercase(), port);
        Ok(Self {
            uri,
            host,
            port,
            origin,
        })
    }
    pub(super) fn origin(&self) -> &str {
        &self.origin
    }
    pub(super) fn uri(&self) -> &Uri {
        &self.uri
    }
}

#[derive(Clone)]
pub(super) struct Client {
    tls: Arc<ClientConfig>,
    jobs: Arc<Semaphore>,
    limits: HttpLimits,
    resolver: Arc<Work>,
    activity: Arc<Activity>,
}
struct Activity {
    registration: Mutex<()>,
    stopping: AtomicBool,
    active: AtomicUsize,
    changed: Notify,
    stop: watch::Sender<bool>,
}
struct RequestGuard(Arc<Activity>);
impl Drop for RequestGuard {
    fn drop(&mut self) {
        self.0.active.fetch_sub(1, Ordering::AcqRel);
        self.0.changed.notify_waiters();
    }
}
struct BodyOwner(Zeroizing<Vec<u8>>);
impl AsRef<[u8]> for BodyOwner {
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}
impl Client {
    pub(super) fn new(trust: HttpsTrust) -> Result<Self, Error> {
        trust.limits.validate()?;
        if !(1..=64).contains(&trust.roots.len())
            || trust.roots.iter().any(|r| r.is_empty() || r.len() > 16384)
            || trust.roots.iter().map(Vec::len).sum::<usize>() > 1024 * 1024
        {
            return Err(Error::InvalidConfiguration);
        }
        let mut roots = RootCertStore::empty();
        for root in trust.roots {
            roots
                .add(CertificateDer::from(root))
                .map_err(|_| Error::InvalidConfiguration)?;
        }
        let provider = Arc::new(rustls::crypto::ring::default_provider());
        let mut tls = ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|_| Error::InvalidConfiguration)?
            .with_root_certificates(roots)
            .with_no_client_auth();
        tls.alpn_protocols = vec![b"http/1.1".to_vec()];
        tls.resumption = rustls::client::Resumption::disabled();
        tls.enable_early_data = false;
        let (stop, _) = watch::channel(false);
        Ok(Self {
            tls: Arc::new(tls),
            jobs: Arc::new(Semaphore::new(trust.limits.jobs)),
            limits: trust.limits,
            resolver: Arc::new(Work::new(Limits {
                admissions: trust.limits.jobs,
                workers: trust.limits.jobs.min(16),
                ..Limits::default()
            })),
            activity: Arc::new(Activity {
                registration: Mutex::new(()),
                stopping: AtomicBool::new(false),
                active: AtomicUsize::new(0),
                changed: Notify::new(),
                stop,
            }),
        })
    }
    pub(super) async fn get(&self, endpoint: &Endpoint) -> Result<Zeroizing<Vec<u8>>, Error> {
        self.request(endpoint, Method::GET, Zeroizing::new(Vec::new()), None)
            .await
    }
    pub(super) async fn post(
        &self,
        endpoint: &Endpoint,
        body: Zeroizing<Vec<u8>>,
        authorization: Zeroizing<String>,
    ) -> Result<Zeroizing<Vec<u8>>, Error> {
        // Hyper/rustls own immutable transport copies; these libraries do not
        // promise zeroization. Our source bearer/client-secret buffers remain
        // zeroizing, and no body/header/error text is logged or persisted.
        self.request(endpoint, Method::POST, body, Some(authorization.as_str()))
            .await
    }
    async fn request(
        &self,
        endpoint: &Endpoint,
        method: Method,
        body: Zeroizing<Vec<u8>>,
        authorization: Option<&str>,
    ) -> Result<Zeroizing<Vec<u8>>, Error> {
        if body.len() > self.limits.body_bytes || authorization.is_some_and(|a| a.len() > 8192) {
            return Err(Error::Malformed);
        }
        let permit = self
            .jobs
            .clone()
            .try_acquire_owned()
            .map_err(|_| Error::Busy)?;
        {
            let _registration = self.activity.registration.lock().await;
            if self.activity.stopping.load(Ordering::Acquire) {
                return Err(Error::Unavailable);
            }
            self.activity.active.fetch_add(1, Ordering::AcqRel);
        }
        let _request = RequestGuard(self.activity.clone());
        let mut stopped = self.activity.stop.subscribe();
        if *stopped.borrow() {
            return Err(Error::Unavailable);
        }
        let operation = timeout(self.limits.timeout, async {
            let host = endpoint.host.trim_start_matches('[').trim_end_matches(']');
            let name =
                ServerName::try_from(host.to_owned()).map_err(|_| Error::InvalidConfiguration)?;
            let resolve_host = host.to_owned();
            let port = endpoint.port;
            let (addresses, _permit) = self
                .resolve(permit, move || {
                    // HTTP admission stays with the actual resolver closure even
                    // when the caller is cancelled. Returned addresses are capped.
                    let addresses: Vec<_> = (resolve_host.as_str(), port)
                        .to_socket_addrs()
                        .map_err(|_| Error::Unavailable)?
                        .take(16)
                        .collect();
                    if addresses.is_empty() {
                        return Err(Error::Unavailable);
                    }
                    Ok(addresses)
                })
                .await?;
            let mut socket = None;
            for address in addresses {
                if let Ok(connected) = TcpStream::connect(address).await {
                    socket = Some(connected);
                    break;
                }
            }
            let socket = socket.ok_or(Error::Unavailable)?;
            let socket = TlsConnector::from(self.tls.clone())
                .connect(name, socket)
                .await
                .map_err(|_| Error::Unavailable)?;
            let (mut sender, connection) = hyper::client::conn::http1::Builder::new()
                .max_headers(self.limits.headers)
                .max_buf_size(self.limits.header_bytes)
                .handshake(TokioIo::new(socket))
                .await
                .map_err(|_| Error::Unavailable)?;
            let mut request = Request::builder()
                .method(method)
                .uri(endpoint.uri.path_and_query().map_or("/", |p| p.as_str()))
                .header(
                    header::HOST,
                    endpoint
                        .uri
                        .authority()
                        .ok_or(Error::InvalidConfiguration)?
                        .as_str(),
                )
                .header(header::ACCEPT, "application/json")
                .header(header::CONNECTION, "close");
            if let Some(authorization) = authorization {
                request = request
                    .header(header::AUTHORIZATION, authorization)
                    .header(header::CONTENT_TYPE, "application/x-www-form-urlencoded");
            }
            let request = request
                .body(Full::new(Bytes::from_owner(BodyOwner(body))))
                .map_err(|_| Error::Malformed)?;
            tokio::pin!(connection);
            // Drive the HTTP connection in this future. No detached connection
            // task can survive a caller cancellation, deadline or shutdown.
            let receive = async {
                let response = sender
                    .send_request(request)
                    .await
                    .map_err(|_| Error::Unavailable)?;
                if response.status() != 200
                    || response.headers().contains_key(header::LOCATION)
                    || response
                        .headers()
                        .get(header::CONTENT_ENCODING)
                        .is_some_and(|v| v != "identity")
                    || response
                        .headers()
                        .get(header::CONTENT_TYPE)
                        .is_none_or(|v| {
                            v.to_str().ok().is_none_or(|s| {
                                s.split(';').next().is_none_or(|m| {
                                    !m.trim().eq_ignore_ascii_case("application/json")
                                })
                            })
                        })
                {
                    return Err(Error::Unavailable);
                }
                if let Some(length) = response.headers().get(header::CONTENT_LENGTH) {
                    let length = length
                        .to_str()
                        .ok()
                        .and_then(|s| s.parse::<usize>().ok())
                        .ok_or(Error::Malformed)?;
                    if length > self.limits.body_bytes {
                        return Err(Error::Malformed);
                    }
                }
                let mut body = response.into_body();
                let mut result = Zeroizing::new(Vec::new());
                while let Some(frame) = body.frame().await {
                    let frame = frame.map_err(|_| Error::Unavailable)?;
                    let data = frame.into_data().map_err(|_| Error::Malformed)?;
                    if result
                        .len()
                        .checked_add(data.len())
                        .is_none_or(|n| n > self.limits.body_bytes)
                    {
                        return Err(Error::Malformed);
                    }
                    result.extend_from_slice(&data);
                }
                if result.is_empty() {
                    return Err(Error::Malformed);
                }
                Ok(result)
            };
            tokio::pin!(receive);
            tokio::select! {
                biased;
                result = &mut receive => result,
                // A Connection: close response may finish the connection while
                // the response/body waiter has buffered bytes left to consume.
                // It still must validate the complete body under the outer
                // deadline; premature EOF makes that waiter return an error.
                _ = &mut connection => receive.await,
            }
        });
        tokio::select! { biased; _=stopped.changed()=>Err(Error::Unavailable),result=operation=>result.map_err(|_|Error::Unavailable)? }
    }
    async fn resolve(
        &self,
        permit: OwnedSemaphorePermit,
        lookup: impl FnOnce() -> Result<Vec<SocketAddr>, Error> + Send + 'static,
    ) -> Result<(Vec<SocketAddr>, OwnedSemaphorePermit), Error> {
        self.resolver.run(move || Ok((lookup()?, permit))).await
    }
    pub(super) async fn shutdown(&self) -> Result<(), Error> {
        {
            let _registration = self.activity.registration.lock().await;
            self.activity.stopping.store(true, Ordering::Release);
            self.jobs.close();
            self.activity.stop.send_replace(true);
        }
        loop {
            let changed = self.activity.changed.notified();
            tokio::pin!(changed);
            changed.as_mut().enable();
            if self.activity.active.load(Ordering::Acquire) == 0 {
                break;
            }
            changed.await;
        }
        self.resolver.shutdown().await
    }
}
