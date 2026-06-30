use crate::shared::{
    error::{AsyncChannelError, ConnectionSendError},
    ClientId,
};

#[derive(thiserror::Error, Debug)]
pub enum ServerSendError {
    #[error("Client with id `{0}` is unknown")]
    UnknownClient(ClientId),
    #[error("Error when sending data on the connection")]
    ConnectionSendError(#[from] ConnectionSendError),
}

#[derive(thiserror::Error, Debug)]
pub enum ServerPayloadSendError {
    #[error("There is no default channel")]
    NoDefaultChannel,
    #[error("Error when sending data")]
    SendError(#[from] ServerSendError),
}

#[derive(thiserror::Error, Debug)]
#[error("Error while sending to multiple recipients")]
pub struct ServerGroupSendError(pub Vec<(ClientId, ServerSendError)>);

#[derive(thiserror::Error, Debug)]
pub enum ServerGroupPayloadSendError {
    #[error("There is no default channel")]
    NoDefaultChannel,
    #[error("Error while sending data to a group of clients")]
    GroupSendError(#[from] ServerGroupSendError),
}

#[derive(thiserror::Error, Debug)]
pub enum ServerReceiveError {
    #[error("Client with id `{0}` is unknown")]
    UnknownClient(ClientId),
}

#[derive(thiserror::Error, Debug)]
pub enum ServerDisconnectError {
    #[error("Client with id `{0}` is unknown")]
    UnknownClient(ClientId),
    #[error("Client with id `{0}` is already disconnected")]
    ClientAlreadyDisconnected(ClientId),
}

#[derive(thiserror::Error, Debug)]
#[error("Endpoint is already closed")]
pub struct EndpointAlreadyClosed;

#[derive(thiserror::Error, Debug)]
pub enum EndpointStartError {
    #[error("I/O error")]
    IoError(#[from] std::io::Error),
    #[error("Bind error")]
    BindError(String),
    #[error("Async channel error")]
    AsyncChannelError(#[from] AsyncChannelError),
}

impl From<iroh::endpoint::BindError> for EndpointStartError {
    fn from(e: iroh::endpoint::BindError) -> Self {
        EndpointStartError::BindError(e.to_string())
    }
}
