//! Prints an initial census or a fresh comparison sample; never rewrites budgets.
#[global_allocator]
static ALLOC: codec::CountingAlloc = codec::CountingAlloc;
fn main() {
    println!(
        "{}",
        serde_json::to_string_pretty(&codec::json1k::census_report()).unwrap()
    );
}
