use partitionline::net::BrokerConn;
use std::time::{Duration, Instant};
use tokio::net::TcpListener;

fn main() {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let live = listener.local_addr().unwrap().to_string();
        let live_only = vec![live.clone()];
        let refused_first = vec!["127.0.0.1:1".to_string(), live.clone()];
        assert!(BrokerConn::connect("127.0.0.1:1", "probe", Duration::from_secs(1))
            .await
            .is_err(), "the fixture port must refuse connections");
        let accepting = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                drop(socket);
            }
        });
        for _ in 0..10 {
            for addrs in [&live_only, &refused_first] {
                let conn = BrokerConn::connect_tls_any(addrs, "probe", Duration::from_secs(1), None)
                    .await.unwrap();
                assert_eq!(conn.addr(), live);
                drop(conn);
            }
        }
        for repetition in 0..5 {
            let order = if repetition % 2 == 0 { [false, true] } else { [true, false] };
            for dead_first in order {
                let addrs = if dead_first { &refused_first } else { &live_only };
                let start = Instant::now();
                for _ in 0..100 {
                    let conn = BrokerConn::connect_tls_any(addrs, "probe", Duration::from_secs(1), None)
                        .await.unwrap();
                    assert_eq!(conn.addr(), live);
                    drop(conn);
                }
                println!("repetition={repetition} refused_first={dead_first} dials=100 total_ns={}",
                         start.elapsed().as_nanos());
            }
        }
        accepting.abort();
        let _ = accepting.await;
    });
}
