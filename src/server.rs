use std::collections::HashSet;

use bevy::prelude::*;
use bytes::Bytes;
use iroh::endpoint::Endpoint;
use tokio::{
    runtime,
    sync::{
        broadcast,
        mpsc,
    },
};

use crate::{
    config::IrohEndpointConfig,
    server::{
        connection::ServerConnection,
        endpoint::ServerEndpoint,
    },
    shared::{
        channels::{
            tasks::{spawn_recv_channels_tasks, spawn_send_channels_tasks_spawner},
            ChannelAsyncMessage, ChannelId, ChannelSyncMessage, SendChannelsConfiguration,
        },
        peer_connection::PeerConnection,
        AsyncRuntime, ClientId, IrohSyncPreUpdate, DEFAULT_INTERNAL_MESSAGES_CHANNEL_SIZE,
        DEFAULT_KILL_MESSAGE_QUEUE_SIZE, DEFAULT_MESSAGE_QUEUE_SIZE,
        DEFAULT_QCHANNEL_MESSAGES_CHANNEL_SIZE,
    },
};

#[cfg(feature = "shared-client-id")]
mod client_id;

#[cfg(feature = "bincode-messages")]
pub mod messages;

pub mod connection;
pub mod endpoint;
pub mod error;

pub use error::*;

/// Connection event raised when a client just connected to the server.
#[derive(bevy::ecs::message::Message, Debug, Copy, Clone)]
pub struct ConnectionEvent {
    /// Id of the client who connected.
    pub id: ClientId,
}

/// ConnectionLost event raised when a client is considered disconnected from the server.
#[derive(bevy::ecs::message::Message, Debug, Copy, Clone)]
pub struct ConnectionLostEvent {
    /// Id of the client who lost connection.
    pub id: ClientId,
}

pub(crate) enum ServerAsyncMessage {
    ClientConnected(PeerConnection<ServerConnection>),
    ClientConnectionClosed(ClientId),
}

#[derive(Debug, Clone)]
pub(crate) enum ServerSyncMessage {
    ClientConnectedAck(ClientId),
}

/// Main iroh server. Can manage multiple [`ServerSideConnection`]s from multiple iroh clients.
///
/// Created by the [`IrohServerPlugin`] or inserted manually.
#[derive(Resource)]
pub struct IrohServer {
    pub(crate) runtime: runtime::Handle,
    pub(crate) endpoint: Option<ServerEndpoint>,
}

impl FromWorld for IrohServer {
    fn from_world(world: &mut World) -> Self {
        if world.get_resource::<AsyncRuntime>().is_none() {
            let async_runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()
                .unwrap();
            world.insert_resource(AsyncRuntime(async_runtime));
        };

        let runtime = world.resource::<AsyncRuntime>();
        IrohServer::new(runtime.handle().clone())
    }
}

impl IrohServer {
    fn new(runtime: tokio::runtime::Handle) -> Self {
        Self {
            endpoint: None,
            runtime,
        }
    }

    /// Returns a reference to the server's endpoint.
    ///
    /// **Panics** if the endpoint is not opened.
    pub fn endpoint(&self) -> &ServerEndpoint {
        self.endpoint.as_ref().unwrap()
    }

    /// Returns a mutable reference to the server's endpoint.
    ///
    /// **Panics** if the endpoint is not opened.
    pub fn endpoint_mut(&mut self) -> &mut ServerEndpoint {
        self.endpoint.as_mut().unwrap()
    }

    /// Returns an optional reference to the server's endpoint.
    pub fn get_endpoint(&self) -> Option<&ServerEndpoint> {
        self.endpoint.as_ref()
    }

    /// Returns an optional mutable reference to the server's endpoint.
    pub fn get_endpoint_mut(&mut self) -> Option<&mut ServerEndpoint> {
        self.endpoint.as_mut()
    }

    /// Starts a new endpoint, which will listen for incoming connections from clients.
    ///
    /// The endpoint is created from the provided [`IrohEndpointConfig`] (ALPN must be set).
    ///
    /// Returns the [`EndpointId`] of this server (its public key).
    pub fn start_endpoint(
        &mut self,
        config: IrohEndpointConfig,
        send_channels_cfg: SendChannelsConfiguration,
        #[cfg(feature = "recv_channels")]
        recv_channels_cfg: crate::shared::peer_connection::RecvChannelsConfiguration,
    ) -> Result<iroh::EndpointId, EndpointStartError> {
        let sk = config.resolve_secret_key();
        let builder = Endpoint::builder(iroh::endpoint::presets::N0)
            .secret_key(sk)
            .alpns(config.alpns.clone())
            .relay_mode(config.relay_mode.clone());

        let endpoint = self.runtime.block_on(async move {
            builder.bind().await
        })?;

        let endpoint_id = endpoint.id();

        let (to_sync_endpoint_send, from_async_endpoint_recv) =
            mpsc::channel::<ServerAsyncMessage>(DEFAULT_INTERNAL_MESSAGES_CHANNEL_SIZE);
        let (endpoint_close_send, endpoint_close_recv) =
            broadcast::channel(DEFAULT_KILL_MESSAGE_QUEUE_SIZE);

        info!("Starting endpoint with id: {} ...", endpoint_id.fmt_short());

        #[cfg(feature = "recv_channels")]
        let recv_cfg = recv_channels_cfg.clone();
        let ep_clone = endpoint.clone();
        self.runtime.spawn(async move {
            endpoint_task(
                ep_clone,
                to_sync_endpoint_send.clone(),
                endpoint_close_recv,
                #[cfg(feature = "recv_channels")]
                recv_cfg,
            )
            .await;
        });

        let mut server_ep = ServerEndpoint::new(
            endpoint,
            endpoint_close_send,
            from_async_endpoint_recv,
            #[cfg(feature = "recv_channels")]
            recv_channels_cfg,
        );

        for channel_type in send_channels_cfg.configs() {
            server_ep.unchecked_open_channel(*channel_type)?;
        }

        self.endpoint = Some(server_ep);

        Ok(endpoint_id)
    }

    /// Closes the endpoint and all the connections associated with it.
    ///
    /// Returns [`EndpointAlreadyClosed`] if the endpoint is already closed.
    pub fn stop_endpoint(&mut self) -> Result<(), EndpointAlreadyClosed> {
        match self.endpoint.take() {
            Some(mut endpoint) => {
                endpoint.disconnect_all_clients();
                match endpoint.close_incoming_connections_handler() {
                    Ok(_) => Ok(()),
                    Err(_) => Err(EndpointAlreadyClosed),
                }
            }
            None => Err(EndpointAlreadyClosed),
        }
    }

    /// Returns true if the server is currently listening for messages and connections.
    pub fn is_listening(&self) -> bool {
        self.endpoint.is_some()
    }
}

async fn endpoint_task(
    endpoint: Endpoint,
    to_sync_endpoint_send: mpsc::Sender<ServerAsyncMessage>,
    mut endpoint_close_recv: broadcast::Receiver<()>,
    #[cfg(feature = "recv_channels")]
    recv_channels_cfg: crate::shared::peer_connection::RecvChannelsConfiguration,
) {
    tokio::select! {
        _ = endpoint_close_recv.recv() => {
            trace!("Endpoint incoming connection handler received a request to close")
        }
        _ = async {
            while let Some(connecting) = endpoint.accept().await {
                match connecting.await {
                    Err(err) => error!("An incoming connection failed: {}", err),
                    Ok(connection) => {
                        let to_sync_endpoint_send = to_sync_endpoint_send.clone();
                        #[cfg(feature = "recv_channels")]
                        let recv_channels_cfg = recv_channels_cfg.clone();
                        tokio::spawn(async move {
                            client_connection_task(
                                connection,
                                to_sync_endpoint_send,
                                #[cfg(feature = "recv_channels")]
                                recv_channels_cfg,
                            )
                            .await
                        });
                    },
                }
            }
        } => {}
    }
}

async fn client_connection_task(
    connection_handle: iroh::endpoint::Connection,
    to_sync_endpoint_send: mpsc::Sender<ServerAsyncMessage>,
    #[cfg(feature = "recv_channels")]
    recv_channels_cfg: crate::shared::peer_connection::RecvChannelsConfiguration,
) {
    let (client_close_send, client_close_recv) =
        broadcast::channel(DEFAULT_KILL_MESSAGE_QUEUE_SIZE);
    let (bytes_from_client_send, bytes_from_client_recv) =
        mpsc::channel::<(ChannelId, Bytes)>(DEFAULT_MESSAGE_QUEUE_SIZE);
    let (to_connection_send, mut from_sync_server_recv) =
        mpsc::channel::<ServerSyncMessage>(DEFAULT_INTERNAL_MESSAGES_CHANNEL_SIZE);
    let (from_channels_send, from_channels_recv) =
        mpsc::channel::<ChannelAsyncMessage>(DEFAULT_INTERNAL_MESSAGES_CHANNEL_SIZE);
    let (to_channels_send, to_channels_recv) =
        mpsc::channel::<ChannelSyncMessage>(DEFAULT_QCHANNEL_MESSAGES_CHANNEL_SIZE);

    // Signal the sync server of this new connection
    to_sync_endpoint_send
        .send(ServerAsyncMessage::ClientConnected(PeerConnection::new(
            ServerConnection::new(connection_handle.clone(), to_connection_send),
            bytes_from_client_recv,
            client_close_send.clone(),
            from_channels_recv,
            to_channels_send,
            #[cfg(feature = "recv_channels")]
            recv_channels_cfg,
        )))
        .await
        .expect("Failed to signal connection to sync server");

    // Wait for the sync server response before spawning connection tasks.
    match from_sync_server_recv.recv().await {
        Some(ServerSyncMessage::ClientConnectedAck(client_id)) => {
            info!(
                "New connection from {}, client_id: {}",
                connection_handle.remote_id(),
                client_id
            );

            #[cfg(feature = "shared-client-id")]
            client_id::spawn_client_id_sender(
                connection_handle.clone(),
                client_id,
                from_channels_send.clone(),
            );

            // Spawn a task to listen for the underlying connection being closed
            {
                let conn = connection_handle.clone();
                let to_sync_server = to_sync_endpoint_send.clone();
                tokio::spawn(async move {
                    let _conn_err = conn.closed().await;
                    info!("Connection {} closed: {}", client_id, _conn_err);
                    if !to_sync_server.is_closed() {
                        to_sync_server
                            .send(ServerAsyncMessage::ClientConnectionClosed(client_id))
                            .await
                            .expect("Failed to signal connection lost in async connection");
                    }
                });
            };

            spawn_recv_channels_tasks(
                connection_handle.clone(),
                client_id,
                client_close_recv.resubscribe(),
                bytes_from_client_send,
            );

            spawn_send_channels_tasks_spawner(
                connection_handle,
                client_close_recv,
                to_channels_recv,
                from_channels_send,
            );
        }
        _ => info!(
            "Connection from {} refused",
            connection_handle.remote_id()
        ),
    }
}

/// - Receives events from the async server tasks
/// - Updates the sync server state
///
/// This system generates server's bevy events.
pub fn handle_server_events(
    mut server: ResMut<IrohServer>,
    mut connection_events: MessageWriter<ConnectionEvent>,
    mut connection_lost_events: MessageWriter<ConnectionLostEvent>,
    mut lost_clients: Local<HashSet<ClientId>>,
) {
    let Some(endpoint) = server.get_endpoint_mut() else {
        return;
    };

    while let Ok(endpoint_message) = endpoint.try_recv_from_async() {
        match endpoint_message {
            ServerAsyncMessage::ClientConnected(new_connection) => {
                match endpoint.handle_new_connection(new_connection) {
                    Ok(client_id) => {
                        connection_events.write(ConnectionEvent { id: client_id });
                    }
                    Err(_) => {
                        error!("Failed to handle connection of a client, already disconnected");
                    }
                };
            }
            ServerAsyncMessage::ClientConnectionClosed(client_id) => {
                if endpoint.clients.contains_key(&client_id) {
                    endpoint.try_disconnect_closed_client(client_id);
                    connection_lost_events.write(ConnectionLostEvent { id: client_id });
                }
            }
        }
    }

    for (client_id, connection) in endpoint.clients.iter_mut() {
        while let Ok(message) = connection.try_recv_from_channels() {
            match message {
                ChannelAsyncMessage::LostConnection => {
                    if !lost_clients.contains(client_id) {
                        lost_clients.insert(*client_id);
                        connection_lost_events
                            .write(ConnectionLostEvent { id: *client_id });
                    }
                }
            }
        }
    }

    for client_id in lost_clients.drain() {
        endpoint.try_disconnect_client(client_id);
    }
}

#[cfg(feature = "recv_channels")]
/// Type alias for the server's recv channel error event.
pub type ServerRecvChannelError = crate::shared::error::RecvChannelErrorEvent<ClientId>;

#[cfg(feature = "recv_channels")]
/// Dispatches received payloads to their respective channel buffers for all clients.
pub fn dispatch_received_payloads(
    mut server: ResMut<IrohServer>,
    mut recv_error_events: MessageWriter<ServerRecvChannelError>,
) {
    let Some(endpoint) = server.get_endpoint_mut() else {
        return;
    };

    endpoint.dispatch_received_payloads(&mut recv_error_events);
}

#[cfg(feature = "recv_channels")]
/// Clears stale payloads on all receive channels.
pub fn clear_stale_received_payloads(mut server: ResMut<IrohServer>) {
    let Some(endpoint) = server.get_endpoint_mut() else {
        return;
    };

    if endpoint.recv_channels_cfg().clear_stale_received_payloads {
        endpoint.clear_payloads_from_clients();
    }
}

/// Iroh Server's plugin.
///
/// It is possible to add both this plugin and the [`crate::client::IrohClientPlugin`].
#[derive(Default)]
pub struct IrohServerPlugin {
    /// If `true`, prevents the plugin from initializing the [`IrohServer`] Resource.
    pub initialize_later: bool,
}

impl Plugin for IrohServerPlugin {
    fn build(&self, app: &mut App) {
        app.add_message::<ConnectionEvent>()
            .add_message::<ConnectionLostEvent>();

        if !self.initialize_later {
            app.init_resource::<IrohServer>();
        }

        app.add_systems(
            PreUpdate,
            handle_server_events
                .in_set(IrohSyncPreUpdate)
                .run_if(resource_exists::<IrohServer>),
        );
        #[cfg(feature = "recv_channels")]
        {
            app.add_message::<ServerRecvChannelError>();
            app.add_systems(
                PreUpdate,
                dispatch_received_payloads
                    .in_set(IrohSyncPreUpdate)
                    .run_if(resource_exists::<IrohServer>),
            );
            app.add_systems(
                Last,
                clear_stale_received_payloads
                    .in_set(crate::shared::IrohSyncLast)
                    .run_if(resource_exists::<IrohServer>),
            );
        }
    }
}

/// Returns true if the server Resource exists and its endpoint is opened.
pub fn server_listening(server: Option<Res<IrohServer>>) -> bool {
    match server {
        Some(server) => server.is_listening(),
        None => false,
    }
}

/// Returns true if the server was not listening last frame, but is now.
pub fn server_just_opened(
    mut was_listening: Local<bool>,
    server: Option<Res<IrohServer>>,
) -> bool {
    let listening = server.map(|server| server.is_listening()).unwrap_or(false);

    let just_opened = !*was_listening && listening;
    *was_listening = listening;
    just_opened
}

/// Returns true if the server was listening last frame, but is not now.
pub fn server_just_closed(
    mut was_listening: Local<bool>,
    server: Option<Res<IrohServer>>,
) -> bool {
    let closed = server.map(|server| !server.is_listening()).unwrap_or(true);

    let just_closed = *was_listening && closed;
    *was_listening = !closed;
    just_closed
}
