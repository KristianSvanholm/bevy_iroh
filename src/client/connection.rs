use bevy::log::{error, info, trace, warn};
use bytes::Bytes;
use iroh::endpoint::{Connection, Endpoint};
use tokio::{
    runtime,
    sync::{
        broadcast,
        mpsc::{self, error::TryRecvError},
    },
};

#[cfg(feature = "shared-client-id")]
mod client_id;

#[cfg(feature = "bincode-messages")]
pub mod messages;

use crate::{
    config::IrohClientConnectionConfig,
    shared::{
        channels::{
            tasks::{spawn_recv_channels_tasks, spawn_send_channels_tasks_spawner},
            ChannelAsyncMessage, ChannelConfig, ChannelId, ChannelSyncMessage, CloseReason, CloseRecv,
            CloseSend, SendChannelsConfiguration,
        },
        error::{AsyncChannelError, ChannelCloseError, ChannelCreationError},
        peer_connection::{
            ChannelAsyncMsgRecv, ChannelAsyncMsgSend, ChannelSyncMsgRecv, ChannelSyncMsgSend,
            ChannelsIdsPool, PayloadRecv, PayloadSend, PeerConnection,
        },
        ClientId, InternalConnectionRef, DEFAULT_INTERNAL_MESSAGES_CHANNEL_SIZE,
        DEFAULT_KILL_MESSAGE_QUEUE_SIZE, DEFAULT_MESSAGE_QUEUE_SIZE,
        DEFAULT_QCHANNEL_MESSAGES_CHANNEL_SIZE,
    },
};

use super::{
    error::{ClientPayloadSendError, ClientSendError},
    ClientAsyncMessage, ClientConnectionCloseError, IrohConnectionError,
};

pub type ConnectionLocalId = u64;

#[derive(bevy::ecs::message::Message, Debug, Copy, Clone)]
pub struct ConnectionEvent {
    pub id: ConnectionLocalId,
    pub client_id: Option<ClientId>,
}

#[derive(bevy::ecs::message::Message, Debug, Clone)]
pub struct ConnectionFailedEvent {
    pub id: ConnectionLocalId,
    pub err: IrohConnectionError,
}

#[derive(bevy::ecs::message::Message, Debug, Copy, Clone)]
pub struct ConnectionLostEvent {
    pub id: ConnectionLocalId,
}

#[derive(Debug, PartialEq, Eq, Copy, Clone)]
pub enum ConnectionState {
    Connecting,
    Connected,
    Disconnected,
}
impl From<&InternalConnectionState> for ConnectionState {
    fn from(internal_conn: &InternalConnectionState) -> Self {
        match internal_conn {
            InternalConnectionState::Connecting => ConnectionState::Connecting,
            InternalConnectionState::Connected(_, _) => ConnectionState::Connected,
            InternalConnectionState::Disconnected => ConnectionState::Disconnected,
        }
    }
}

#[derive(Debug)]
pub(crate) enum InternalConnectionState {
    Connecting,
    Connected(InternalConnectionRef, Option<ClientId>),
    Disconnected,
}

pub(crate) type ClientAsyncMsgSend = mpsc::Sender<ClientAsyncMessage>;
pub(crate) type ClientAsyncMsgRecv = mpsc::Receiver<ClientAsyncMessage>;

pub(crate) fn create_client_connection_async_channels() -> (
    PayloadSend,
    PayloadRecv,
    ClientAsyncMsgSend,
    ClientAsyncMsgRecv,
    ChannelAsyncMsgSend,
    ChannelAsyncMsgRecv,
    ChannelSyncMsgSend,
    ChannelSyncMsgRecv,
    CloseSend,
    CloseRecv,
) {
    let (bytes_from_server_send, bytes_from_server_recv) =
        mpsc::channel::<(ChannelId, Bytes)>(DEFAULT_MESSAGE_QUEUE_SIZE);
    let (to_sync_client_send, to_sync_client_recv) =
        mpsc::channel::<ClientAsyncMessage>(DEFAULT_INTERNAL_MESSAGES_CHANNEL_SIZE);
    let (from_channels_send, from_channels_recv) =
        mpsc::channel::<ChannelAsyncMessage>(DEFAULT_INTERNAL_MESSAGES_CHANNEL_SIZE);
    let (to_channels_send, to_channels_recv) =
        mpsc::channel::<ChannelSyncMessage>(DEFAULT_QCHANNEL_MESSAGES_CHANNEL_SIZE);
    let (close_send, close_recv) = broadcast::channel(DEFAULT_KILL_MESSAGE_QUEUE_SIZE);
    (
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
    )
}

pub type ClientSideConnection = PeerConnection<ClientConnection>;

pub struct ClientConnection {
    local_id: ConnectionLocalId,
    runtime: runtime::Handle,
    config: IrohClientConnectionConfig,
    channels_config: SendChannelsConfiguration,
    state: InternalConnectionState,
    send_channel_ids: ChannelsIdsPool,
    from_async_client_recv: mpsc::Receiver<ClientAsyncMessage>,
    endpoint: Endpoint,
}
impl ClientConnection {
    pub(crate) fn new(
        local_id: ConnectionLocalId,
        runtime: runtime::Handle,
        endpoint: Endpoint,
        config: IrohClientConnectionConfig,
        channels_config: SendChannelsConfiguration,
        from_async_client_recv: mpsc::Receiver<ClientAsyncMessage>,
    ) -> Self {
        Self {
            local_id,
            runtime,
            endpoint,
            config,
            channels_config,
            state: InternalConnectionState::Connecting,
            send_channel_ids: ChannelsIdsPool::new(),
            from_async_client_recv,
        }
    }
}
impl ClientSideConnection {
    pub fn send_payload_on<T: Into<Bytes>, C: Into<ChannelId>>(
        &mut self,
        channel_id: C,
        payload: T,
    ) -> Result<(), ClientSendError> {
        let channel_id = channel_id.into();
        match &self.specific.state {
            InternalConnectionState::Disconnected => Err(ClientSendError::ConnectionClosed),
            _ => Ok(self.internal_send_payload(channel_id, payload.into())?),
        }
    }

    pub fn try_send_payload<T: Into<Bytes>>(&mut self, payload: T) {
        if let Err(err) = self.send_payload(payload) {
            error!("try_send_payload: {}", err);
        }
    }

    pub fn try_send_payload_on<T: Into<Bytes>, C: Into<ChannelId>>(
        &mut self,
        channel_id: C,
        payload: T,
    ) {
        if let Err(err) = self.send_payload_on(channel_id, payload) {
            error!("try_send_payload_on: {}", err);
        }
    }

    pub fn send_payload<T: Into<Bytes>>(
        &mut self,
        payload: T,
    ) -> Result<(), ClientPayloadSendError> {
        match self.specific.send_channel_ids.default_channel() {
            Some(channel) => Ok(self.send_payload_on(channel, payload.into())?),
            None => Err(ClientPayloadSendError::NoDefaultChannel),
        }
    }

    pub fn open_channel(
        &mut self,
        channel_config: ChannelConfig,
    ) -> Result<ChannelId, ChannelCreationError> {
        let channel_id = self.specific.send_channel_ids.take_id()?;
        self.create_connection_channel(channel_id, channel_config)?;
        Ok(channel_id)
    }

    pub fn close_channel(&mut self, channel_id: ChannelId) -> Result<(), ChannelCloseError> {
        self.internal_close_channel(channel_id)?;
        self.specific.send_channel_ids.release_id(channel_id);
        Ok(())
    }

    pub(crate) fn open_configured_channels(
        &mut self,
        channel_configs: SendChannelsConfiguration,
    ) -> Result<(), AsyncChannelError> {
        for channel_config in channel_configs.configs() {
            let channel_id = self.specific.send_channel_ids.take_id().unwrap();
            match self.create_unregistered_connection_channel(channel_id, *channel_config) {
                Ok(channel) => self.register_connection_channel(channel),
                Err(e) => {
                    self.specific.send_channel_ids.release_id(channel_id);
                    return Err(e);
                }
            };
        }
        Ok(())
    }

    fn internal_disconnect(
        &mut self,
        reason: CloseReason,
    ) -> Result<(), ClientConnectionCloseError> {
        match &self.specific.state {
            InternalConnectionState::Disconnected => Ok(()),
            _ => {
                self.specific.state = InternalConnectionState::Disconnected;
                Ok(self.close(reason)?)
            }
        }
    }

    pub fn reconnect(&mut self) -> Result<(), AsyncChannelError> {
        if let InternalConnectionState::Disconnected = &self.internal_state() {
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

            self.set_state(InternalConnectionState::Connecting);
            self.specific.send_channel_ids = ChannelsIdsPool::new();
            self.specific.from_async_client_recv = to_sync_client_recv;
            self.internal_reset(
                close_send,
                to_channels_send,
                from_channels_recv,
                bytes_from_server_recv,
                self.specific.channels_config.configs().len(),
            );

            self.open_configured_channels(self.specific.channels_config.clone())?;

            let local_id = self.specific.local_id;
            let endpoint = self.specific.endpoint.clone();
            let config = self.specific.config.clone();
            self.specific.runtime.spawn(async move {
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
        }
        Ok(())
    }

    #[inline(always)]
    pub fn disconnect(&mut self) -> Result<(), ClientConnectionCloseError> {
        self.internal_disconnect(CloseReason::LocalOrder)
    }

    pub fn try_disconnect(&mut self) {
        if let Err(err) = &self.disconnect() {
            error!("Failed to properly close connection: {}", err);
        }
    }

    pub(crate) fn try_disconnect_closed_connection(&mut self) {
        if let Err(err) = self.internal_disconnect(CloseReason::PeerClosed) {
            error!("Failed to properly close connection: {}", err);
        }
    }

    #[inline(always)]
    pub(crate) fn try_recv_from_async(&mut self) -> Result<ClientAsyncMessage, TryRecvError> {
        self.specific.from_async_client_recv.try_recv()
    }

    #[inline(always)]
    pub(crate) fn set_state(&mut self, state: InternalConnectionState) {
        self.specific.state = state;
    }

    #[inline(always)]
    pub(crate) fn internal_state(&self) -> &InternalConnectionState {
        &self.specific.state
    }

    #[inline(always)]
    pub fn state(&self) -> ConnectionState {
        (&self.specific.state).into()
    }

    pub fn client_id(&self) -> Option<ClientId> {
        match &self.internal_state() {
            InternalConnectionState::Connected(_, client_id) => *client_id,
            _ => None,
        }
    }

    pub fn connection_stats(&self) -> Option<iroh::endpoint::ConnectionStats> {
        match &self.internal_state() {
            InternalConnectionState::Connected(connection, _) => Some(connection.stats()),
            _ => None,
        }
    }

    pub fn max_datagram_size(&self) -> Option<usize> {
        match &self.internal_state() {
            InternalConnectionState::Connected(connection, _) => connection.max_datagram_size(),
            _ => None,
        }
    }

    #[inline(always)]
    pub fn set_default_channel(&mut self, channel_id: ChannelId) {
        self.specific
            .send_channel_ids
            .set_default_channel(channel_id);
    }

    #[inline(always)]
    pub fn default_channel(&self) -> Option<ChannelId> {
        self.specific.send_channel_ids.default_channel()
    }

    pub fn channels_config(&self) -> &SendChannelsConfiguration {
        &self.specific.channels_config
    }

    pub fn client_config(&self) -> &IrohClientConnectionConfig {
        &self.specific.config
    }
}

#[cfg(feature = "recv_channels")]
impl ClientSideConnection {
    pub fn receive_payload<C: Into<ChannelId>>(
        &mut self,
        channel_id: C,
    ) -> Result<Option<Bytes>, crate::client::error::ConnectionClosed> {
        match &self.internal_state() {
            InternalConnectionState::Disconnected => Err(crate::client::error::ConnectionClosed),
            _ => Ok(self.internal_receive_payload(channel_id.into())),
        }
    }

    pub fn try_receive_payload<C: Into<ChannelId>>(&mut self, channel_id: C) -> Option<Bytes> {
        match self.receive_payload(channel_id) {
            Ok(payload) => payload,
            Err(err) => {
                error!("try_receive_payload: {}", err);
                None
            }
        }
    }
}

pub(crate) async fn async_connection_task(
    endpoint: Endpoint,
    local_id: ConnectionLocalId,
    config: IrohClientConnectionConfig,
    to_sync_client_send: ClientAsyncMsgSend,
    bytes_from_server_send: PayloadSend,
    to_channels_recv: ChannelSyncMsgRecv,
    from_channels_send: ChannelAsyncMsgSend,
    close_recv: CloseRecv,
) {
    info!("Connection {} trying to connect...", local_id);

    let endpoint_addr: iroh::EndpointAddr = if let Some(addr) = config.endpoint_addr.clone() {
        addr
    } else {
        let mut addr: iroh::EndpointAddr = config.server_id.into();
        if endpoint.addr().relay_urls().next().is_none() {
            info!("Waiting for home relay to become available...");
        }
        let relay_start = std::time::Instant::now();
        let relay_url = loop {
            if let Some(url) = endpoint.addr().relay_urls().next().cloned() {
                break Some(url);
            }
            if relay_start.elapsed() > std::time::Duration::from_secs(10) {
                warn!("Timed out waiting for home relay (10s)");
                break None;
            }
            tokio::time::sleep(std::time::Duration::from_millis(100)).await;
        };
        if let Some(url) = relay_url {
            addr = addr.with_relay_url(url);
        }
        addr
    };

    let connection = match endpoint.connect(endpoint_addr, &config.alpn).await {
        Ok(conn) => conn,
        Err(e) => {
            error!("Connection {}, error while connecting: {}", local_id, e);
            let _ = to_sync_client_send
                .send(ClientAsyncMessage::ConnectionFailed(
                    IrohConnectionError::ConnectionError(e.to_string()),
                ))
                .await;
            return;
        }
    };

    {
        let conn = connection.clone();
        let to_sync_client = to_sync_client_send.clone();
        tokio::spawn(async move {
            let _conn_err = conn.closed().await;
            info!("Connection {} closed: {}", local_id, _conn_err);
            if !to_sync_client.is_closed() {
                let _ = to_sync_client
                    .send(ClientAsyncMessage::ConnectionClosed)
                    .await;
            }
        })
    };

    spawn_recv_channels_tasks(
        connection.clone(),
        local_id,
        close_recv.resubscribe(),
        bytes_from_server_send,
    );

    spawn_send_channels_tasks_spawner(
        connection.clone(),
        close_recv.resubscribe(),
        to_channels_recv,
        from_channels_send,
    );

    #[cfg(not(feature = "shared-client-id"))]
    signal_connected(connection, local_id, None, to_sync_client_send).await;

    #[cfg(feature = "shared-client-id")]
    match client_id::receive_client_id(connection.clone(), close_recv).await {
        client_id::ClientIdReception::Retrieved(client_id) => {
            signal_connected(connection, local_id, Some(client_id), to_sync_client_send).await
        }
        client_id::ClientIdReception::Failed(e) => {
            error!(
                "Connection {}, error while retrieving client_id: {}",
                local_id, e
            );
            let _ = to_sync_client_send
                .send(ClientAsyncMessage::ConnectionFailed(e))
                .await;
        }
        client_id::ClientIdReception::Interrupted => trace!(
            "Connection {}, reception of client_id was interrupted",
            local_id
        ),
    }
}

async fn signal_connected(
    connection_handle: Connection,
    connection_id: ConnectionLocalId,
    client_id: Option<ClientId>,
    to_sync_client_send: mpsc::Sender<ClientAsyncMessage>,
) {
    let _ = to_sync_client_send
        .send(ClientAsyncMessage::Connected(
            connection_handle.clone(),
            client_id,
        ))
        .await;

    info!(
        "Connection {} connected with client_id {:?}",
        connection_id, client_id
    );
}
