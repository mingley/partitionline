
#[cfg(test)]
mod frame_classify_count {
    use super::{frame_mut, new_frame, read_frame, Config, ReadAhead, CLASSIFY_CONTROL};
    use std::pin::Pin;
    use std::sync::atomic::Ordering;
    use std::task::{Context, Poll};
    use std::time::Duration;
    use tokio::io::{AsyncRead, ReadBuf};
    use tokio::time::Instant;
    struct Ready { bytes: Vec<u8>, offset: usize, chunk: usize, calls: usize }
    impl AsyncRead for Ready {
        fn poll_read(mut self: Pin<&mut Self>, _: &mut Context<'_>, dest: &mut ReadBuf<'_>) -> Poll<std::io::Result<()>> {
            self.calls += 1;
            let n = self.bytes.len().saturating_sub(self.offset).min(self.chunk).min(dest.remaining());
            dest.put_slice(&self.bytes[self.offset..self.offset+n]);
            self.offset += n;
            Poll::Ready(Ok(()))
        }
    }
    #[tokio::test(flavor = "current_thread")]
    async fn count_buffered_frame_classifications() {
        for (length,chunk) in [(32usize,8192usize),(64*1024,8192),(4096,1)] {
            let config=Config::new(1,1,length,1024,Duration::from_secs(10),Duration::from_secs(10),Duration::from_secs(10)).unwrap();
            let mut ahead=ReadAhead::new();
            frame_mut(&mut ahead.bytes).extend_from_slice(&(length as i32).to_be_bytes());
            let mut reader=Ready {bytes:vec![0x5a;length],offset:0,chunk,calls:0};
            CLASSIFY_CONTROL.store(0,Ordering::Relaxed);
            let body=read_frame(&mut reader,config,Instant::now()+Duration::from_secs(10),&mut ahead,new_frame()).await.unwrap_or_else(|_|panic!("read failed"));
            let count=CLASSIFY_CONTROL.load(Ordering::Relaxed);
            assert_eq!(body.len(),length);
            assert!(body.iter().all(|&b|b==0x5a));
            println!("classify-count variant={} length={length} chunk={chunk} reads={} calls={count}",std::env::var("PL_CLASSIFY_VARIANT").unwrap(),reader.calls);
        }
    }
}
