//! Optional transport and authentication security primitives.

#[cfg(feature = "tls")]
pub mod tls;
#[cfg(feature = "sasl")]
pub mod sasl;
