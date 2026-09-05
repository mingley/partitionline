//! Confluent-compatible schema wire framing for `partitionline` adopters.
//!
//! This crate is a **scaffold** (workspace-excluded, `publish = false`). It does
//! not talk to Schema Registry yet. Encode/decode only the Confluent wire header:
//! magic byte `0` + big-endian schema id + payload.
//!
//! Full registry HTTP + Avro/Protobuf/JSON codecs wait on adopter demand
//! (survey [#85](https://github.com/mingley/partitionline/issues/85)); see
//! `docs/schema-companion.md`.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

mod wire;

pub use wire::{decode, encode, DecodeError, WireMessage, MAGIC};
