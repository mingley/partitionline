# Shared integration/library test body

The integration wrapper now holds its crate-level documentation and includes
common/raft_runtime.rs. The library unit wrapper retains its crate alias and
includes exactly that shared body. This avoids inner crate docs in an include!
expansion without renaming or changing any test or helper behavior. The shared
31258-byte body is exact SHA84a24fc9b8b3e45a9d67baa62c810a4632868ad7577f122d43426f3ed3a5d047
before and after formatting. Original23-path correction and earlier failed4e13
source/receipt/binaries remain preserved; source preservation records full modes.

Only stable and1.85 formatting/parsing checks ran after this split. No type check,
Cargo test, TCP history or library owner capture has run yet. The next prepared
WORK runner selects the5-case public integration harness and9-case library runtime
filter, with separate capture roots. Count expectations are source-derived;
actual outcomes and filtered-library counts must come from executed logs. Original
4e13 compile failure remains the only compiled result at this stage.
