//! Run the same serial reservation census without test-only dependencies.
#[path = "../../tests/reserve_reuse.rs"]
mod reserve_reuse;

fn main() {
    reserve_reuse::run();
}
