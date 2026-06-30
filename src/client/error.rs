use crate::shared::error::{ConnectionAlreadyClosed, ConnectionSendError};

use super::connection::ConnectionLocalId;

#[derive(thiserror::Error, Debug)]
pub enum ClientSendError {
    #[error("Connection is 'disconnected'")]
    ConnectionClosed,
    #[error("Error when sending data on the connection")]
    ConnectionSendError(#[from] ConnectionSendError),
}

#[derive(thiserror::Error, Debug)]
pub enum ClientPayloadSendError {
    #[error("There is no default channel")]
    NoDefaultChannel,
    #[error("Error when sending")]
    SendError(#[from] ClientSendError),
}

/// The client connection is closed
#[derive(thiserror::Error, Debug)]
#[error("The client connection is closed")]
pub struct ConnectionClosed;

#[derive(thiserror::Error, Debug)]
pub enum ClientConnectionCloseError {
    #[error("Connection is already closed")]
    ConnectionAlreadyClosed(#[from] ConnectionAlreadyClosed),
    #[error("Connection id `{0}` is invalid")]
    InvalidConnectionId(ConnectionLocalId),
}
