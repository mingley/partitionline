//! Optional transport and authentication security primitives.

#[cfg(feature = "sasl")]
pub mod credentials;
#[cfg(feature = "oidc")]
pub mod oidc;
#[cfg(feature = "sasl")]
pub mod sasl;
#[cfg(feature = "sasl")]
pub mod session;
#[cfg(feature = "tls")]
pub mod tls;
