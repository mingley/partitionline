//! Old-main executable control: install this target at tests/list_transactions_routing.rs.
#![expect(
    clippy::unwrap_used,
    clippy::panic,
    clippy::indexing_slicing,
    reason = "finite old-main regression assertions"
)]
#[path = "fixtures/list-transactions-routing/socket_peer.rs"]
mod socket_peer;
use socket_peer::{filters, finish, listing, response, Peer, Reply, BUDGET};
include!("fixtures/list-transactions-routing/all_brokers_regression.rs");
