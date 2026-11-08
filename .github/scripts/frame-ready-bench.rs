
#[cfg(test)]
mod ready_frame_microbench {
    use super::{frame_mut, new_frame, read_frame, Config, ReadAhead};
    use std::future::Future;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::io::{AsyncRead, ReadBuf};
    use tokio::time::Instant;

    struct Source<'a> { bytes: &'a [u8], at: usize, chunk: usize }
    impl AsyncRead for Source<'_> {
        fn poll_read(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, out: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            let n = self.bytes.len().saturating_sub(self.at).min(self.chunk).min(out.remaining());
            let at = self.at;
            out.put_slice(&self.bytes[at..at + n]);
            self.at += n;
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test(flavor = "current_thread")]
    #[ignore = "manual controlled frame-loop microbenchmark"]
    async fn measure_ready_frame_cost() {
        let variant = std::env::var("PL_FRAME_BENCH_VARIANT").unwrap_or_default();

        for (label, buffered, length, chunk) in [
            ("direct-64k", false, 64 * 1024usize, 8192usize),
            ("buffered-64k", true, 64 * 1024, 8192),
            ("direct-128-one", false, 128, 1),
            ("buffered-128-one", true, 128, 1),
            ("direct-129-one", false, 129, 1),
            ("buffered-129-one", true, 129, 1),
        ] {
            let config = Config::new(
                1, 1, length, 1024, Duration::from_secs(30), Duration::from_secs(30), Duration::from_secs(30),
            ).unwrap();
            let prefix = (length as i32).to_be_bytes();
            let mut data = Vec::new();
            let mut ahead = ReadAhead::new();
            if buffered { frame_mut(&mut ahead.bytes).extend_from_slice(&prefix); }
            else { data.extend_from_slice(&prefix); }
            data.extend(std::iter::repeat_n(0x5a, length));
            let mut source = Source { bytes: &data, at: 0, chunk };
            let mut read = Box::pin(read_frame(&mut source, config, Instant::now() + Duration::from_secs(30), &mut ahead, new_frame()));
            let first = std::future::poll_fn(|cx| Poll::Ready(read.as_mut().poll(cx))).await;
            let ready = matches!(&first, Poll::Ready(Ok(_)));
            drop(read);
            println!("frame-boundary variant={variant} case={label} first_ready={ready} consumed={}", source.at);
        }
        let cases = [
            ("direct-32", false, 32usize, 8192usize, 50_000usize),
            ("buffered-32", true, 32, 8192, 50_000),
            ("direct-128-one", false, 128, 1, 5_000),
            ("buffered-128-one", true, 128, 1, 5_000),
            ("direct-64k", false, 64 * 1024, 8192, 1_024),
            ("buffered-64k", true, 64 * 1024, 8192, 1_024),
            ("direct-256k", false, 256 * 1024, 8192, 256),
            ("buffered-256k", true, 256 * 1024, 8192, 256),
            ("direct-4096-one", false, 4096, 1, 256),
            ("buffered-4096-one", true, 4096, 1, 256),
        ];
        for (label, buffered, length, chunk, iterations) in cases {
            let config = Config::new(
                1, 1, length, 1024, Duration::from_secs(30), Duration::from_secs(30), Duration::from_secs(30),
            ).unwrap();
            let prefix = (length as i32).to_be_bytes();
            let mut data = Vec::new();
            if !buffered { data.extend_from_slice(&prefix); }
            data.extend(std::iter::repeat_n(0x5a, length));
            let mut source = Source { bytes: &data, at: 0, chunk };
            let mut ahead = ReadAhead::new();
            let mut spare = new_frame();
            let mut started = std::time::Instant::now();
            for i in 0..iterations + 8 {
                if i == 8 { started = std::time::Instant::now(); }
                source.at = 0;
                if buffered { frame_mut(&mut ahead.bytes).extend_from_slice(&prefix); }
                spare = read_frame(&mut source, config, Instant::now() + Duration::from_secs(30), &mut ahead, spare)
                    .await.unwrap_or_else(|_| panic!("benchmark read failed"));
                std::hint::black_box(spare.as_slice());
            }
            let elapsed = started.elapsed().as_nanos();
            println!("frame-bench variant={variant} case={label} frames={iterations} bytes={length} chunk={chunk} elapsed_ns={elapsed}");
        }
    }
}
