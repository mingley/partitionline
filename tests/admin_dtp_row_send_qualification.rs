use partitionline::Admin;
use std::time::Duration;

#[test]
fn public_describe_topics_future_is_send() {
    fn require_send<T: Send>(_: &T) {}

    fn check(admin: &mut Admin) {
        let names = ["orders"];
        let future = admin.describe_topics_with_partition_limit_timeout(
            names.iter(),
            false,
            2,
            Duration::from_secs(5),
        );
        require_send(&future);
    }

    let _typecheck: fn(&mut Admin) = check;
}
