use gateway::campaign_steps;
use gateway::cells::{interleave, AckMode, ClientKind, Load, CELLS, LOADS};
use gateway::schedule::FixedRateSchedule;

#[test]
fn registered_cells_match_gateway_profile() {
    let ids: Vec<_> = CELLS.iter().map(|c| c.id).collect();
    assert_eq!(
        ids,
        [
            "lb-gateway-produce-gzip",
            "lb-gateway-produce-lz4",
            "lb-gateway-produce-ack"
        ]
    );
    for cell in CELLS {
        assert_eq!(cell.acks, -1);
        assert_eq!(cell.max_in_flight, 1);
        assert_eq!(cell.linger_ms, 100);
        assert_eq!(cell.batch_size_bytes, 1_048_576);
        assert_eq!(cell.partitions, 8);
    }
    assert_eq!(CELLS[0].codec, "gzip");
    assert_eq!(CELLS[0].ack_mode, AckMode::FireAndForgetTimedFlush);
    assert_eq!(CELLS[1].codec, "lz4");
    assert_eq!(CELLS[1].level, 9);
    assert_eq!(CELLS[2].id, "lb-gateway-produce-ack");
    assert_eq!(CELLS[2].ack_mode, AckMode::PerRecordCompletion);
    assert_eq!(LOADS, [Load::OpenLoop25k, Load::Unpaced]);
    assert_eq!(Load::OpenLoop25k.rate_per_second(), Some(25_000));
}

#[test]
fn ack_mode_labels_are_stable() {
    assert_eq!(
        AckMode::FireAndForgetTimedFlush.label(),
        "fire-and-forget confirmed by a timed flush"
    );
    assert_eq!(
        AckMode::PerRecordCompletion.label(),
        "per-record completion"
    );
    assert_eq!(
        AckMode::ManualAssignment.label(),
        "manual assignment from an explicit offset"
    );
}

#[test]
fn section4_interleave_is_abba() {
    assert_eq!(
        interleave(5),
        [
            ClientKind::Partitionline,
            ClientKind::RustRdkafka,
            ClientKind::RustRdkafka,
            ClientKind::Partitionline,
            ClientKind::Partitionline,
            ClientKind::RustRdkafka,
            ClientKind::RustRdkafka,
            ClientKind::Partitionline,
            ClientKind::Partitionline,
            ClientKind::RustRdkafka,
        ]
    );
    let steps = campaign_steps(5);
    assert_eq!(steps.len(), 3 * 2 * 10); // 3 cells × 2 loads × 10 interleaved runs
}

#[test]
fn open_loop_schedule_is_absolute() {
    let schedule = FixedRateSchedule::new(25_000).unwrap();
    assert_eq!(schedule.arrival(0).unwrap().as_nanos(), 0);
    assert_eq!(schedule.arrival(25_000).unwrap().as_secs(), 1);
    assert!(FixedRateSchedule::new(0).is_err());
}
