pub const PRODUCE: i16 = 0;
pub const FETCH: i16 = 1;
pub const METADATA: i16 = 3;
pub const OFFSET_COMMIT: i16 = 8;
pub const OFFSET_FETCH: i16 = 9;
pub const FIND_COORDINATOR: i16 = 10;
pub const JOIN_GROUP: i16 = 11;
pub const HEARTBEAT: i16 = 12;
pub const SYNC_GROUP: i16 = 14;
pub const SASL_HANDSHAKE: i16 = 17;
pub const API_VERSIONS: i16 = 18;
pub const SASL_AUTHENTICATE: i16 = 36;

pub fn pick_version(
    broker_min: i16,
    broker_max: i16,
    client_min: i16,
    client_max: i16,
) -> Option<i16> {
    let lo = broker_min.max(client_min);
    let hi = broker_max.min(client_max);
    (lo <= hi).then_some(hi)
}
