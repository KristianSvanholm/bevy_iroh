use std::collections::{
    hash_map::{Iter, IterMut},
    HashMap,
};

use bevy::prelude::*;
use iroh::endpoint::Endpoint;
use tokio::runtime;

use crate::{
    client::connection::{create_client_connection_async_channels, ClientConnection},
    config::IrohClientConnectionConfig,
    shared::{
        channels::{ChannelAsyncMessage, SendChannelsConfiguration},
        error::AsyncChannelError,
        AsyncRuntime, ClientId, InternalConnectionRef, IrohSyncPreUpdate,
    },
};

use self::connection::{
    async_connection_task, ClientSideConnection, ConnectionEvent, ConnectionFailedEvent,
    ConnectionLocalId, ConnectionLostEvent, ConnectionState, InternalConnectionState,
};

pub mod connection;

mod error;
pub use error::*;

/// Errors that can occur while connecting.
#[derive(thiserror::Error, Debug, Clone)]
pub enum IrohConnectionError {
    /// An iroh connection error.
    #[error("Connection error: {0}")]
    ConnectionError(String),
    /// Client received an invalid client id.
    #[error("Client received an invalid client id")]
    InvalidClientId,
    /// Client did not receive its client id.
    #[error("Client did not receive its client id")]
    ClientIdNotReceived,
}

impl From<iroh::endpoint::ConnectionError> for IrohConnectionError {
    fn from(e: iroh::endpoint::ConnectionError) -> Self {
        IrohConnectionError::ConnectionError(e.to_string())
    }
}

#[derive(Debug)]
pub(crate) enum ClientAsyncMessage {
    Connected(InternalConnectionRef, Option<ClientId>),
    ConnectionFailed(IrohConnectionError),
    ConnectionClosed,
}

/// Main iroh client. Can open multiple [`ClientSideConnection`]s with multiple iroh servers.
///
/// Created by the [`IrohClientPlugin`] or inserted manually via
/// [`bevy::prelude::World::insert_resource`].
#[derive(Resource)]
pub struct IrohClient {
    pub(crate) endpoint: Option<Endpoint>,
    runtime: runtime::Handle,
    connections: HashMap<ConnectionLocalId, ClientSideConnection>,
    connection_local_id_gen: ConnectionLocalId,
    default_connection_id: Option<ConnectionLocalId>,
}

impl FromWorld for IrohClient {
    fn from_world(world: &mut World) -> Self {
        if world.get_resource::<AsyncRuntime>().is_none() {
            let async_runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            world.insert_resource(AsyncRuntime(async_runtime));
        };

        let runtime = world.resource::<AsyncRuntime>();
        IrohClient::new(runtime.handle().clone())
    }
}

impl IrohClient {
    fn new(runtime_handle: tokio::runtime::Handle) -> Self {
        Self {
            endpoint: None,
            connections: HashMap::new(),
            runtime: runtime_handle,
            connection_local_id_gen: 0,
            default_connection_id: None,
        }
    }

    /// Returns true if the default connection exists and is connecting.
    pub fn is_connecting(&self) -> bool {
        match self.get_connection() {
            Some(connection) => connection.state() == ConnectionState::Connecting,
            None => false,
        }
    }

    /// Returns true if the default connection exists and is connected.
    pub fn is_connected(&self) -> bool {
        match self.get_connection() {
            Some(connection) => connection.state() == ConnectionState::Connected,
            None => false,
        }
    }

    /// Returns true if the default connection does not exist or is disconnected.
    pub fn is_disconnected(&self) -> bool {
        match self.get_connection() {
            Some(connection) => connection.state() == ConnectionState::Disconnected,
            None => true,
        }
    }

    /// Returns the default connection or None.
    pub fn get_connection(&self) -> Option<&ClientSideConnection> {
        match self.default_connection_id {
            Some(id) => self.connections.get(&id),
            None => None,
        }
    }

    /// Returns the default connection as mut or None.
    pub fn get_connection_mut(&mut self) -> Option<&mut ClientSideConnection> {
        match self.default_connection_id {
            Some(id) => self.connections.get_mut(&id),
            None => None,
        }
    }

    /// Returns the default connection. **Panics** if there is no default connection.
    pub fn connection(&self) -> &ClientSideConnection {
        self.connections
            .get(&self.default_connection_id.unwrap())
            .unwrap()
    }

    /// Returns the default connection as mut. **Panics** if there is no default connection.
    pub fn connection_mut(&mut self) -> &mut ClientSideConnection {
        self.connections
            .get_mut(&self.default_connection_id.unwrap())
            .unwrap()
    }

    /// Returns the requested connection.
    pub fn get_connection_by_id(&self, id: ConnectionLocalId) -> Option<&ClientSideConnection> {
        self.connections.get(&id)
    }

    /// Returns the requested connection as mut.
    pub fn get_connection_mut_by_id(
        &mut self,
        id: ConnectionLocalId,
    ) -> Option<&mut ClientSideConnection> {
        self.connections.get_mut(&id)
    }

    /// Returns an iterator over all connections.
    pub fn connections(&'_ self) -> Iter<'_, ConnectionLocalId, ClientSideConnection> {
        self.connections.iter()
    }

    /// Returns an iterator over all connections as muts.
    pub fn connections_mut(&'_ mut self) -> IterMut<'_, ConnectionLocalId, ClientSideConnection> {
        self.connections.iter_mut()
    }

    /// Set the shared iroh [`Endpoint`] for this client.
    ///
    /// Must be called before [`open_connection`]. The endpoint should be created
    /// via [`Endpoint::builder`] / [`Endpoint::bind`].
    pub fn set_endpoint(&mut self, endpoint: Endpoint) {
        self.endpoint = Some(endpoint);
    }

    /// Access the raw iroh [`Endpoint`] for advanced use.
    pub fn raw_endpoint(&self) -> Option<&Endpoint> {
        self.endpoint.as_ref()
    }

    /// Access the raw iroh [`Endpoint`] mutably.
    pub fn raw_endpoint_mut(&mut self) -> Option<&mut Endpoint> {
        self.endpoint.as_mut()
    }

    /// Opens a connection to a server.
    ///
    /// The connection will raise an event when fully connected, see [`ConnectionEvent`].
    ///
    /// Returns the [`ConnectionLocalId`].
    ///
    /// # Panics
    ///
    /// Panics if no endpoint has been set via [`set_endpoint`].
    pub fn open_connection(
        &mut self,
        config: IrohClientConnectionConfig,
        send_channels_cfg: SendChannelsConfiguration,
        #[cfg(feature = "recv_channels")]
        recv_channels_cfg: crate::shared::peer_connection::RecvChannelsConfiguration,
    ) -> Result<ConnectionLocalId, AsyncChannelError> {
        let local_id = self.connection_local_id_gen;
        self.connection_local_id_gen += 1;

        let (
            bytes_from_server_send,
            bytes_from_server_recv,
            to_sync_client_send,
            to_sync_client_recv,
            from_channels_send,
            from_channels_recv,
            to_channels_send,
            to_channels_recv,
            close_send,
            close_recv,
        ) = create_client_connection_async_channels();

        let endpoint = self
            .endpoint
            .clone()
            .expect("IrohClient endpoint must be set before opening connections. Call set_endpoint().");
        let mut connection = ClientSideConnection::new(
            ClientConnection::new(
                local_id,
                self.runtime.clone(),
                endpoint.clone(),
                config.clone(),
                send_channels_cfg.clone(),
                to_sync_client_recv,
            ),
            bytes_from_server_recv,
            close_send,
            from_channels_recv,
            to_channels_send,
            #[cfg(feature = "recv_channels")]
            recv_channels_cfg,
        );

        connection.open_configured_channels(send_channels_cfg)?;

        self.connections.insert(local_id, connection);
        if self.default_connection_id.is_none() {
            self.default_connection_id = Some(local_id);
        }

        self.runtime.spawn(async move {
            async_connection_task(
                endpoint,
                local_id,
                config,
                to_sync_client_send,
                bytes_from_server_send,
                to_channels_recv,
                from_channels_send,
                close_recv,
            )
            .await
        });

        Ok(local_id)
    }

    /// Set the default connection.
    pub fn set_default_connection(&mut self, connection_id: ConnectionLocalId) {
        self.default_connection_id = Some(connection_id);
    }

    /// Get the default Connection Id.
    pub fn get_default_connection(&self) -> Option<ConnectionLocalId> {
        self.default_connection_id
    }

    /// Closes a specific connection. Removes it from the client.
    ///
    /// This may fail if no [`ClientSideConnection`] is found for `connection_id`,
    /// or if the connection is already closed.
    pub fn close_connection(
        &mut self,
        connection_id: ConnectionLocalId,
    ) -> Result<(), ClientConnectionCloseError> {
        match self.connections.remove(&connection_id) {
            Some(mut connection) => {
                if Some(connection_id) == self.default_connection_id {
                    self.default_connection_id = None;
                }
                connection.disconnect()
            }
            None => Err(ClientConnectionCloseError::InvalidConnectionId(
                connection_id,
            )),
        }
    }

    /// Calls [`close_connection`] on all open connections.
    pub fn close_all_connections(&mut self) {
        for connection_id in self
            .connections
            .keys()
            .cloned()
            .collect::<Vec<ConnectionLocalId>>()
        {
            let _ = self.close_connection(connection_id);
        }
    }
}

/// Receive messages from the async client tasks and update the sync client.
///
/// This system generates client's bevy events.
pub fn handle_client_events(
    mut connection_events: MessageWriter<ConnectionEvent>,
    mut connection_failed_events: MessageWriter<ConnectionFailedEvent>,
    mut connection_lost_events: MessageWriter<ConnectionLostEvent>,
    mut client: ResMut<IrohClient>,
) {
    for (connection_id, connection) in &mut client.connections {
        while let Ok(message) = connection.try_recv_from_async() {
            match message {
                ClientAsyncMessage::Connected(internal_connection, client_id) => {
                    connection.set_state(InternalConnectionState::Connected(
                        internal_connection,
                        client_id,
                    ));
                    connection_events.write(ConnectionEvent {
                        id: *connection_id,
                        client_id,
                    });
                }
                ClientAsyncMessage::ConnectionFailed(err) => {
                    connection.set_state(InternalConnectionState::Disconnected);
                    connection_failed_events.write(ConnectionFailedEvent {
                        id: *connection_id,
                        err,
                    });
                }
                ClientAsyncMessage::ConnectionClosed => match connection.internal_state() {
                    InternalConnectionState::Disconnected => (),
                    _ => {
                        connection.try_disconnect_closed_connection();
                        connection_lost_events
                            .write(ConnectionLostEvent { id: *connection_id });
                    }
                },
            }
        }
        while let Ok(message) = connection.try_recv_from_channels() {
            match message {
                ChannelAsyncMessage::LostConnection => match connection.internal_state() {
                    InternalConnectionState::Disconnected => (),
                    _ => {
                        connection.try_disconnect_closed_connection();
                        connection_lost_events
                            .write(ConnectionLostEvent { id: *connection_id });
                    }
                },
            }
        }
    }
}

#[cfg(feature = "recv_channels")]
/// Type alias for the recv channel error event for the client.
pub type ClientRecvChannelError = crate::shared::error::RecvChannelErrorEvent<ConnectionLocalId>;

#[cfg(feature = "recv_channels")]
/// Dispatches received payloads to their respective channel buffers.
///
/// This system generates client's bevy events.
pub fn dispatch_received_payloads(
    mut recv_error_events: MessageWriter<ClientRecvChannelError>,
    mut client: ResMut<IrohClient>,
) {
    for (connection_id, connection) in &mut client.connections {
        match connection.internal_state() {
            InternalConnectionState::Disconnected => (),
            _ => {
                if let Err(recv_errors) =
                    connection.dispatch_received_payloads_to_channel_buffers()
                {
                    for error in recv_errors {
                        error!(
                            "Error while dispatching received payloads to channel buffers: {}",
                            error
                        );
                        recv_error_events.write(ClientRecvChannelError {
                            id: *connection_id,
                            error,
                        });
                    }
                }
            }
        }
    }
}

#[cfg(feature = "recv_channels")]
/// Clears stale payloads on all receive channels.
pub fn clear_stale_received_payloads(mut client: ResMut<IrohClient>) {
    for connection in client.connections.values_mut() {
        connection.clear_stale_received_payloads();
    }
}

/// Iroh Client's plugin.
///
/// It is possible to add both this plugin and the [`crate::server::IrohServerPlugin`].
#[derive(Default)]
pub struct IrohClientPlugin {
    /// If `true`, prevents the plugin from initializing the [`IrohClient`] Resource.
    /// Use this if you want to create the client resource manually later.
    pub initialize_later: bool,
}

impl Plugin for IrohClientPlugin {
    fn build(&self, app: &mut App) {
        app.add_message::<ConnectionEvent>()
            .add_message::<ConnectionFailedEvent>()
            .add_message::<ConnectionLostEvent>();

        if !self.initialize_later {
            app.init_resource::<IrohClient>();
        }

        app.add_systems(
            PreUpdate,
            handle_client_events
                .in_set(IrohSyncPreUpdate)
                .run_if(resource_exists::<IrohClient>),
        );
        #[cfg(feature = "recv_channels")]
        {
            app.add_message::<ClientRecvChannelError>();
            app.add_systems(
                PreUpdate,
                dispatch_received_payloads
                    .in_set(IrohSyncPreUpdate)
                    .run_if(resource_exists::<IrohClient>),
            );
            app.add_systems(
                Last,
                clear_stale_received_payloads
                    .in_set(crate::shared::IrohSyncLast)
                    .run_if(resource_exists::<IrohClient>),
            );
        }
    }
}

/// Returns true if the following conditions are all true:
/// - the client Resource exists
/// - its default connection is connecting.
pub fn client_connecting(client: Option<Res<IrohClient>>) -> bool {
    match client {
        Some(client) => client.is_connecting(),
        None => false,
    }
}

/// Returns true if the following conditions are all true:
/// - the client Resource exists
/// - its default connection is connected.
pub fn client_connected(client: Option<Res<IrohClient>>) -> bool {
    match client {
        Some(client) => client.is_connected(),
        None => false,
    }
}

/// Returns true if the following conditions are all true:
/// - the client Resource exists and its default connection is connected
/// - the previous condition was false during the previous update
pub fn client_just_connected(
    mut last_connected: Local<bool>,
    client: Option<Res<IrohClient>>,
) -> bool {
    let connected = client.map(|client| client.is_connected()).unwrap_or(false);

    let just_connected = !*last_connected && connected;
    *last_connected = connected;
    just_connected
}

/// Returns true if the following conditions are all true:
/// - the client Resource does not exist or its default connection is disconnected
/// - the previous condition was false during the previous update
pub fn client_just_disconnected(
    mut last_connected: Local<bool>,
    client: Option<Res<IrohClient>>,
) -> bool {
    let disconnected = client
        .map(|client| client.is_disconnected())
        .unwrap_or(true);

    let just_disconnected = *last_connected && disconnected;
    *last_connected = !disconnected;
    just_disconnected
}
