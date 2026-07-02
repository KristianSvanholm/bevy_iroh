use bevy::prelude::Resource;
use iroh::{
    endpoint::{Builder, RelayMode},
    tls::CaTlsConfig,
    SecretKey,
};

/// Shared configuration for an iroh endpoint.
///
/// Used by both [`IrohClientConfig`] and [`IrohServerConfig`].
///
/// Defaults to iroh's N0 preset (n0 production relays + DNS-based discovery),
/// which works out of the box with no external infrastructure.
#[derive(Resource)]
pub struct IrohEndpointConfig {
    /// Relay servers to use. Default: `RelayMode::Default` (n0 production relays).
    pub relay_mode: RelayMode,
    /// ALPN protocols to accept on incoming connections (required for servers).
    pub alpns: Vec<Vec<u8>>,
    /// Optional secret key. Auto-generated if `None` (recommended for ephemeral sessions).
    pub secret_key: Option<SecretKey>,
    /// Optional TLS CA configuration for verifying relay/HTTPS connections.
    /// Defaults to embedded webpki roots if `None`.
    pub ca_tls_config: Option<CaTlsConfig>,
}

impl Default for IrohEndpointConfig {
    fn default() -> Self {
        Self {
            relay_mode: RelayMode::Default,
            alpns: Vec::new(),
            secret_key: None,
            ca_tls_config: None,
        }
    }
}

impl IrohEndpointConfig {
    /// Apply this config to an iroh [`Builder`], returning it ready to bind.
    pub fn apply_to_builder(&self, builder: Builder) -> Builder {
        let builder = builder
            .secret_key(self.resolve_secret_key())
            .alpns(self.alpns.clone())
            .relay_mode(self.relay_mode.clone());
        match &self.ca_tls_config {
            Some(config) => builder.ca_tls_config(config.clone()),
            None => builder,
        }
    }

    /// Resolve the secret key (use provided or generate random).
    pub fn resolve_secret_key(&self) -> SecretKey {
        self.secret_key.clone().unwrap_or_else(SecretKey::generate)
    }
}

/// Bevy resource for client endpoint configuration.
///
/// Modify before initializing the client to switch communities/relays.
#[derive(Resource, Default)]
pub struct IrohClientConfig {
    /// Endpoint configuration.
    pub endpoint: IrohEndpointConfig,
}

/// Configuration for a client's connection to a host.
#[derive(Debug, Clone)]
pub struct IrohClientConnectionConfig {
    /// The host's EndpointId (PublicKey) to connect to.
    pub server_id: iroh::EndpointId,
    /// ALPN protocol for this connection.
    pub alpn: Vec<u8>,
    /// Optional direct EndpointAddr. If known, bypasses AddressLookup resolution.
    pub endpoint_addr: Option<iroh::EndpointAddr>,
}

/// Bevy resource for server endpoint configuration.
///
/// Modify at runtime, then call `server.start_endpoint()` to apply changes.
#[derive(Resource, Default)]
pub struct IrohServerConfig {
    /// Endpoint configuration.
    pub endpoint: IrohEndpointConfig,
}
