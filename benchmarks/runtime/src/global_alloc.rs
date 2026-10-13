//! Optional global allocators for benchmark binaries only.
//!
//! The default is the census wrapper around the system allocator. `jemalloc`
//! and `mimalloc` replace that wrapper; allocation counts are then zero because
//! the census hooks are not installed. These features never apply to the
//! `partitionline` library.

#[cfg(all(feature = "jemalloc", feature = "mimalloc"))]
compile_error!("enable only one of the jemalloc or mimalloc features");

#[cfg(not(any(feature = "jemalloc", feature = "mimalloc")))]
#[global_allocator]
static ALLOC: codec::CountingAlloc = codec::CountingAlloc;

#[cfg(all(feature = "jemalloc", not(feature = "mimalloc")))]
#[global_allocator]
static ALLOC: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "mimalloc", not(feature = "jemalloc")))]
#[global_allocator]
static ALLOC: mimalloc::MiMalloc = mimalloc::MiMalloc;
