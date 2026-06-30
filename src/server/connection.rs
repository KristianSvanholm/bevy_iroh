use iroh::EndpointId;
use tokio::sync::mpsc::{self, error::TrySendError};

use crate::{
    server::ServerSyncMessage,
    shared::{peer_connection::PeerConnection, InternalConnectionRef},
};

/// A connection to a client from the server's perspective.
pub type ServerSideConnection = PeerConnection<ServerConnection>;

/// Specific data for a server-side connection.
pub struct ServerConnection {
    pub(crate) connection_handle: InternalConnectionRef,
    to_connection_send: mpsc::Sender<ServerSyncMessage>,
}
impl ServerConnection {
    pub(crate) fn new(
        connection_handle: InternalConnectionRef,
        to_connection_send: mpsc::Sender<ServerSyncMessage>,
    ) -> Self {
        Self {
            connection_handle,
            to_connection_send,
        }
    }
}

impl ServerSideConnection {
    #[inline(always)]
    pub fn max_datagram_size(&self) -> Option<usize> {
        self.specific.connection_handle.max_datagram_size()
    }

    #[inline(always)]
    pub fn connection_stats(&self) -> iroh::endpoint::ConnectionStats {
        self.specific.connection_handle.stats()
    }

    #[inline(always)]
    pub(crate) fn try_send_to_async_connection(
        &self,
        msg: ServerSyncMessage,
    ) -> Result<(), TrySendError<ServerSyncMessage>> {
        self.specific.to_connection_send.try_send(msg)
    }

    /// Returns the remote endpoint id of the client.
    #[inline(always)]
    pub fn remote_id(&self) -> EndpointId {
        self.specific.connection_handle.remote_id()
    }
}
