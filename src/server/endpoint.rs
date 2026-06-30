use bevy::{log::error, platform::collections::HashMap};
use bytes::Bytes;
use iroh::endpoint::Endpoint;
use tokio::sync::{
    broadcast,
    mpsc::{self, error::TryRecvError},
};

use crate::{
    server::{
        connection::ServerConnection, ServerAsyncMessage, ServerDisconnectError,
        ServerGroupPayloadSendError, ServerGroupSendError, ServerPayloadSendError,
        ServerSendError, ServerSyncMessage,
    },
    shared::{
        channels::{Channel, ChannelConfig, ChannelId, CloseReason},
        error::{AsyncChannelError, ChannelCloseError, ChannelCreationError},
        peer_connection::{ChannelsIdsPool, PeerConnection},
        ClientId,
    },
};

/// Server-side endpoint that manages client connections.
pub struct ServerEndpoint {
    pub(crate) clients: HashMap<ClientId, PeerConnection<ServerConnection>>,
    /// The raw iroh endpoint.
    pub(crate) raw: Endpoint,
    client_id_gen: ClientId,
    /// Opened send channels configs on this endpoint.
    opened_channels: HashMap<ChannelId, ChannelConfig>,
    /// Internal ordered pool of available channel ids.
    send_channel_ids: ChannelsIdsPool,
    close_sender: broadcast::Sender<()>,
    /// Receiver for internal messages sent by the async endpoint task.
    from_async_endpoint_recv: mpsc::Receiver<ServerAsyncMessage>,
    stats: EndpointStats,

    #[cfg(feature = "recv_channels")]
    recv_channels_cfg: crate::shared::peer_connection::RecvChannelsConfiguration,
}

impl ServerEndpoint {
    pub(crate) fn new(
        raw: Endpoint,
        endpoint_close_send: broadcast::Sender<()>,
        from_async_endpoint_recv: mpsc::Receiver<ServerAsyncMessage>,
        #[cfg(feature = "recv_channels")]
        recv_channels_cfg: crate::shared::peer_connection::RecvChannelsConfiguration,
    ) -> Self {
        Self {
            clients: HashMap::new(),
            raw,
            client_id_gen: 0,
            opened_channels: HashMap::new(),
            send_channel_ids: ChannelsIdsPool::new(),
            close_sender: endpoint_close_send,
            from_async_endpoint_recv,
            stats: EndpointStats::default(),
            #[cfg(feature = "recv_channels")]
            recv_channels_cfg,
        }
    }

    /// Access the raw iroh [`Endpoint`] for advanced use.
    pub fn raw_endpoint(&self) -> &Endpoint {
        &self.raw
    }

    /// Access the raw iroh [`Endpoint`] mutably.
    pub fn raw_endpoint_mut(&mut self) -> &mut Endpoint {
        &mut self.raw
    }

    /// Returns an allocated vector of all the currently connected client ids.
    pub fn clients(&self) -> Vec<ClientId> {
        self.clients.keys().cloned().collect()
    }

    pub fn send_group_payload<'a, I: Iterator<Item = &'a ClientId>, T: Into<Bytes>>(
        &mut self,
        client_ids: I,
        payload: T,
    ) -> Result<(), ServerGroupPayloadSendError> {
        match self.send_channel_ids.default_channel() {
            Some(channel) => self.send_group_payload_on(client_ids, channel, payload),
            None => Err(ServerGroupPayloadSendError::NoDefaultChannel),
        }
    }

    pub fn try_send_group_payload<'a, I: Iterator<Item = &'a ClientId>, T: Into<Bytes>>(
        &mut self,
        client_ids: I,
        payload: T,
    ) {
        if let Err(err) = self.send_group_payload(client_ids, payload) {
            error!("try_send_group_payload: {}", err);
        }
    }

    pub fn send_group_payload_on<
        'a,
        I: Iterator<Item = &'a ClientId>,
        T: Into<Bytes>,
        C: Into<ChannelId>,
    >(
        &mut self,
        client_ids: I,
        channel_id: C,
        payload: T,
    ) -> Result<(), ServerGroupPayloadSendError> {
        let channel_id = channel_id.into();
        let bytes = payload.into();
        let mut errs = vec![];

        for &client_id in client_ids {
            if let Err(e) = self.send_payload_on(client_id, channel_id, bytes.clone()) {
                errs.push((client_id, e));
            }
        }

        match errs.is_empty() {
            true => Ok(()),
            false => Err(ServerGroupSendError(errs).into()),
        }
    }

    pub fn try_send_group_payload_on<
        'a,
        I: Iterator<Item = &'a ClientId>,
        T: Into<Bytes>,
        C: Into<ChannelId>,
    >(
        &mut self,
        client_ids: I,
        channel_id: C,
        payload: T,
    ) {
        if let Err(err) = self.send_group_payload_on(client_ids, channel_id, payload) {
            error!("try_send_group_payload_on: {}", err);
        }
    }

    pub fn broadcast_payload<T: Into<Bytes>>(
        &mut self,
        payload: T,
    ) -> Result<(), ServerGroupPayloadSendError> {
        match self.send_channel_ids.default_channel() {
            Some(channel) => Ok(self.broadcast_payload_on(channel, payload)?),
            None => Err(ServerGroupPayloadSendError::NoDefaultChannel),
        }
    }

    pub fn broadcast_payload_on<T: Into<Bytes>, C: Into<ChannelId>>(
        &mut self,
        channel_id: C,
        payload: T,
    ) -> Result<(), ServerGroupSendError> {
        let payload: Bytes = payload.into();
        let channel_id = channel_id.into();

        let mut errs = vec![];
        for (&client_id, connection) in self.clients.iter_mut() {
            if let Err(e) = connection.internal_send_payload(channel_id, payload.clone()) {
                errs.push((client_id, e.into()));
            }
        }
        match errs.is_empty() {
            true => Ok(()),
            false => Err(ServerGroupSendError(errs)),
        }
    }

    pub fn try_broadcast_payload<T: Into<Bytes>>(&mut self, payload: T) {
        if let Err(err) = self.broadcast_payload(payload) {
            error!("try_broadcast_payload: {}", err);
        }
    }

    pub fn try_broadcast_payload_on<T: Into<Bytes>, C: Into<ChannelId>>(
        &mut self,
        channel_id: C,
        payload: T,
    ) {
        if let Err(err) = self.broadcast_payload_on(channel_id, payload) {
            error!("try_broadcast_payload_on: {}", err);
        }
    }

    pub fn send_payload<T: Into<Bytes>>(
        &mut self,
        client_id: ClientId,
        payload: T,
    ) -> Result<(), ServerPayloadSendError> {
        match self.send_channel_ids.default_channel() {
            Some(channel) => Ok(self.send_payload_on(client_id, channel, payload)?),
            None => Err(ServerPayloadSendError::NoDefaultChannel),
        }
    }

    pub fn send_payload_on<T: Into<Bytes>, C: Into<ChannelId>>(
        &mut self,
        client_id: ClientId,
        channel_id: C,
        payload: T,
    ) -> Result<(), ServerSendError> {
        if let Some(client_connection) = self.clients.get_mut(&client_id) {
            Ok(client_connection.internal_send_payload(channel_id.into(), payload.into())?)
        } else {
            Err(ServerSendError::UnknownClient(client_id))
        }
    }

    pub fn try_send_payload<T: Into<Bytes>>(&mut self, client_id: ClientId, payload: T) {
        match self.send_payload(client_id, payload) {
            Ok(_) => {}
            Err(err) => error!("try_send_payload: {}", err),
        }
    }

    pub fn try_send_payload_on<T: Into<Bytes>, C: Into<ChannelId>>(
        &mut self,
        client_id: ClientId,
        channel_id: C,
        payload: T,
    ) {
        match self.send_payload_on(client_id, channel_id, payload) {
            Ok(_) => {}
            Err(err) => error!("try_send_payload_on: {}", err),
        }
    }

    fn internal_disconnect_client(
        &mut self,
        client_id: ClientId,
        reason: CloseReason,
    ) -> Result<(), ServerDisconnectError> {
        match self.clients.remove(&client_id) {
            Some(mut client_connection) => {
                self.stats.disconnect_count += 1;
                client_connection
                    .close(reason)
                    .map_err(|_| ServerDisconnectError::ClientAlreadyDisconnected(client_id))
            }
            None => Err(ServerDisconnectError::UnknownClient(client_id)),
        }
    }

    pub(crate) fn try_disconnect_closed_client(&mut self, client_id: ClientId) {
        if let Err(err) = self.internal_disconnect_client(client_id, CloseReason::PeerClosed) {
            error!(
                "Failed to properly disconnect client {}: {}",
                client_id, err
            );
        }
    }

    pub fn disconnect_client(
        &mut self,
        client_id: ClientId,
    ) -> Result<(), ServerDisconnectError> {
        self.internal_disconnect_client(client_id, CloseReason::LocalOrder)
    }

    pub fn try_disconnect_client(&mut self, client_id: ClientId) {
        if let Err(err) = self.disconnect_client(client_id) {
            error!(
                "Failed to properly disconnect client {}: {}",
                client_id, err
            );
        }
    }

    pub fn disconnect_all_clients(&mut self) {
        for (_, mut client_connection) in self.clients.drain() {
            let _ = client_connection.close(CloseReason::LocalOrder);
        }
    }

    pub fn connection_mut(
        &mut self,
        client_id: ClientId,
    ) -> Option<&mut PeerConnection<ServerConnection>> {
        self.clients.get_mut(&client_id)
    }

    pub fn connection(
        &self,
        client_id: ClientId,
    ) -> Option<&PeerConnection<ServerConnection>> {
        self.clients.get(&client_id)
    }

    pub fn endpoint_stats(&self) -> &EndpointStats {
        &self.stats
    }

    pub fn open_channel(
        &mut self,
        channel_type: ChannelConfig,
    ) -> Result<ChannelId, ChannelCreationError> {
        let channel_id = self.send_channel_ids.take_id()?;
        Ok(self.create_endpoint_channel(channel_id, channel_type)?)
    }

    pub(crate) fn unchecked_open_channel(
        &mut self,
        channel_type: ChannelConfig,
    ) -> Result<ChannelId, AsyncChannelError> {
        let channel_id = self.send_channel_ids.take_id().unwrap();
        self.create_endpoint_channel(channel_id, channel_type)
    }

    fn create_endpoint_channel(
        &mut self,
        channel_id: ChannelId,
        channel_type: ChannelConfig,
    ) -> Result<ChannelId, AsyncChannelError> {
        let unregistered_channels =
            match self.create_unregistered_endpoint_channels(channel_id, channel_type) {
                Ok(channels) => channels,
                Err(err) => {
                    self.send_channel_ids.release_id(channel_id);
                    return Err(err);
                }
            };
        for (client_id, channel) in unregistered_channels {
            self.clients
                .get_mut(&client_id)
                .unwrap()
                .register_connection_channel(channel);
        }
        self.opened_channels.insert(channel_id, channel_type);
        Ok(channel_id)
    }

    fn create_unregistered_endpoint_channels(
        &mut self,
        channel_id: ChannelId,
        channel_type: ChannelConfig,
    ) -> Result<HashMap<ClientId, Channel>, AsyncChannelError> {
        let mut unregistered_channels = HashMap::new();
        for (&client_id, client_connection) in self.clients.iter_mut() {
            let channel = client_connection
                .create_unregistered_connection_channel(channel_id, channel_type)?;
            unregistered_channels.insert(client_id, channel);
        }
        Ok(unregistered_channels)
    }

    pub fn close_channel(&mut self, channel_id: ChannelId) -> Result<(), ChannelCloseError> {
        match self.opened_channels.remove(&channel_id) {
            Some(_) => {
                for (_, connection) in self.clients.iter_mut() {
                    connection.internal_close_channel(channel_id)?;
                }
                self.send_channel_ids.release_id(channel_id);
                Ok(())
            }
            None => Err(ChannelCloseError::InvalidChannelId(channel_id)),
        }
    }

    #[inline(always)]
    pub fn set_default_channel(&mut self, channel_id: ChannelId) {
        self.send_channel_ids.set_default_channel(channel_id);
    }

    #[inline(always)]
    pub fn default_channel(&self) -> Option<ChannelId> {
        self.send_channel_ids.default_channel()
    }

    pub(crate) fn close_incoming_connections_handler(&mut self) -> Result<(), AsyncChannelError> {
        match self.close_sender.send(()) {
            Ok(_) => Ok(()),
            Err(_) => Err(AsyncChannelError::InternalChannelClosed),
        }
    }

    pub(crate) fn handle_new_connection(
        &mut self,
        mut connection: PeerConnection<ServerConnection>,
    ) -> Result<ClientId, AsyncChannelError> {
        for (channel_id, channel_type) in self.opened_channels.iter() {
            if let Err(err) = connection.create_connection_channel(*channel_id, *channel_type) {
                connection.try_close(CloseReason::LocalOrder);
                return Err(err);
            };
        }

        self.client_id_gen += 1;
        let client_id = self.client_id_gen;

        match connection
            .try_send_to_async_connection(ServerSyncMessage::ClientConnectedAck(client_id))
        {
            Ok(_) => {
                self.clients.insert(client_id, connection);
                self.stats.connect_count += 1;
                Ok(client_id)
            }
            Err(_) => {
                connection.try_close(CloseReason::LocalOrder);
                Err(AsyncChannelError::InternalChannelClosed)
            }
        }
    }

    #[inline(always)]
    pub(crate) fn try_recv_from_async(&mut self) -> Result<ServerAsyncMessage, TryRecvError> {
        self.from_async_endpoint_recv.try_recv()
    }
}

#[cfg(feature = "recv_channels")]
use crate::server::ServerRecvChannelError;

#[cfg(feature = "recv_channels")]
impl ServerEndpoint {
    pub fn receive_payload<C: Into<ChannelId>>(
        &mut self,
        client_id: ClientId,
        channel_id: C,
    ) -> Result<Option<Bytes>, crate::server::ServerReceiveError> {
        match self.clients.get_mut(&client_id) {
            Some(connection) => Ok(connection.internal_receive_payload(channel_id.into())),
            None => Err(crate::server::ServerReceiveError::UnknownClient(client_id)),
        }
    }

    pub fn try_receive_payload<C: Into<ChannelId>>(
        &mut self,
        client_id: ClientId,
        channel_id: C,
    ) -> Option<Bytes> {
        match self.receive_payload(client_id, channel_id.into()) {
            Ok(payload) => payload,
            Err(err) => {
                error!("try_receive_payload: {}", err);
                None
            }
        }
    }

    pub(crate) fn dispatch_received_payloads(
        &mut self,
        recv_error_events: &mut bevy::ecs::message::MessageWriter<ServerRecvChannelError>,
    ) {
        for (client_id, connection) in self.clients.iter_mut() {
            if let Err(recv_errors) = connection.dispatch_received_payloads_to_channel_buffers() {
                for error in recv_errors {
                    error!(
                        "Error while dispatching received payloads to channel buffers: {}",
                        error
                    );
                    recv_error_events.write(ServerRecvChannelError {
                        id: *client_id,
                        error,
                    });
                }
            }
        }
    }

    pub fn clear_payloads_from_clients(&mut self) {
        for connection in self.clients.values_mut() {
            connection.clear_received_payloads();
        }
    }

    #[inline(always)]
    pub fn set_clear_stale_client_payloads(&mut self, enable: bool) {
        self.recv_channels_cfg.clear_stale_received_payloads = enable;
    }

    #[inline(always)]
    pub fn recv_channels_cfg(&self) -> &crate::shared::peer_connection::RecvChannelsConfiguration {
        &self.recv_channels_cfg
    }
}

/// Basic stats about this server endpoint.
#[derive(Default)]
pub struct EndpointStats {
    connect_count: u32,
    disconnect_count: u32,
}
impl EndpointStats {
    /// Returns how many connection events occurred on this endpoint.
    pub fn connect_count(&self) -> u32 {
        self.connect_count
    }
    /// Returns how many disconnection events occurred on this endpoint.
    pub fn disconnect_count(&self) -> u32 {
        self.disconnect_count
    }
}
