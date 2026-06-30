//! A peer-to-peer game networking plugin using iroh, for the Bevy game engine.

/// Client features
#[cfg(feature = "client")]
pub mod client;
/// Server features
#[cfg(feature = "server")]
pub mod server;
/// Shared features between client & server
pub mod shared;
/// Configuration resources
pub mod config;
