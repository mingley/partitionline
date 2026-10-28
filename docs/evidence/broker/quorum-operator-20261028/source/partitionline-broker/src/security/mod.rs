//! Transport authentication and authorization primitives.
//!
//! [`acl`] is the persistent authorizer. The SASL, OIDC, and TLS modules stay
//! behind their features.

pub mod acl;
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
pub mod tokens;
