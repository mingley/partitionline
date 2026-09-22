//! Confluent-compatible schema wire framing for `partitionline` adopters.
//!
//! This crate is a **scaffold** (workspace-excluded, `publish = false`).
//! It always offers the Confluent wire header encode/decode: magic byte `0`,
//! big-endian schema id, payload. Behind the default `registry` feature it
//! also offers a bounded read-only Schema Registry lookup client
//! ([`registry::RegistryClient`]). There is intentionally no schema
//! registration or mutation API here.
//!
//! Avro/Protobuf/JSON codecs wait on adopter demand
//! (survey [#85](https://github.com/mingley/partitionline/issues/85)); see
//! `docs/schema-companion.md`.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

#[cfg(feature = "registry")]
pub mod registry;
mod wire;

pub use wire::{decode, encode, DecodeError, WireMessage, MAGIC};
