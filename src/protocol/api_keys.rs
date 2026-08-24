pub const PRODUCE: i16 = 0;
pub const FETCH: i16 = 1;
pub const METADATA: i16 = 3;
pub const API_VERSIONS: i16 = 18;

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
