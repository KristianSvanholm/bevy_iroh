use bevy::log::error;
use bytes::Bytes;

use crate::{
    server::{
        endpoint::ServerEndpoint, ServerGroupSendError, ServerReceiveError, ServerSendError,
    },
    shared::{channels::ChannelId, ClientId},
};

#[derive(thiserror::Error, Debug)]
pub enum ServerMessageSendError {
    #[error("Failed serialization")]
    Serialization,
    #[error("There is no default channel")]
    NoDefaultChannel,
    #[error("Error when sending data")]
    SendError(#[from] ServerSendError),
}

#[derive(thiserror::Error, Debug)]
pub enum ServerGroupMessageSendError {
    #[error("Failed serialization")]
    Serialization,
    #[error("There is no default channel")]
    NoDefaultChannel,
    #[error("Error while sending data to a group of clients")]
    GroupSendError(#[from] ServerGroupSendError),
}

#[derive(thiserror::Error, Debug)]
pub enum ServerMessageReceiveError {
    #[error("Failed deserialization")]
    Deserialization,
    #[error("There is no default channel")]
    NoDefaultChannel,
    #[error("Error while receiving data")]
    ReceiveError(#[from] ServerReceiveError),
}

impl ServerEndpoint {
    /// Same as [Self::receive_message_from] but on the default channel
    pub fn receive_message<T: serde::de::DeserializeOwned>(
        &mut self,
        client_id: ClientId,
    ) -> Result<Option<T>, ServerMessageReceiveError> {
        match self.default_channel() {
            Some(channel) => self.receive_message_from(client_id, channel),
            None => Err(ServerMessageReceiveError::NoDefaultChannel),
        }
    }

    /// Same as [Self::receive_message_from] but will log the error instead of returning it
    pub fn try_receive_message<T: serde::de::DeserializeOwned>(
        &mut self,
        client_id: ClientId,
    ) -> Option<T> {
        match self.receive_message(client_id) {
            Ok(message) => message,
            Err(err) => {
                error!("try_receive_message: {}", err);
                None
            }
        }
    }

    /// Attempt to deserialise a message into type `T`.
    pub fn receive_message_from<T: serde::de::DeserializeOwned, C: Into<ChannelId>>(
        &mut self,
        client_id: ClientId,
        channel_id: C,
    ) -> Result<Option<T>, ServerMessageReceiveError> {
        match self.receive_payload(client_id, channel_id)? {
            Some(payload) => {
                match bincode::serde::decode_from_slice(&payload, bincode::config::standard()) {
                    Ok((msg, _size)) => Ok(Some(msg)),
                    Err(_) => Err(ServerMessageReceiveError::Deserialization),
                }
            }
            None => Ok(None),
        }
    }

    /// [`ServerEndpoint::receive_message_from`] that logs the error instead of returning a result.
    pub fn try_receive_message_from<T: serde::de::DeserializeOwned, C: Into<ChannelId>>(
        &mut self,
        client_id: ClientId,
        channel_id: C,
    ) -> Option<T> {
        match self.receive_message_from(client_id, channel_id) {
            Ok(message) => message,
            Err(err) => {
                error!("try_receive_message: {}", err);
                None
            }
        }
    }

    /// Same as [ServerEndpoint::send_message_on] but on the default channel
    pub fn send_message<T: serde::Serialize>(
        &mut self,
        client_id: ClientId,
        message: T,
    ) -> Result<(), ServerMessageSendError> {
        match self.default_channel() {
            Some(channel) => self.send_message_on(client_id, channel, message),
            None => Err(ServerMessageSendError::NoDefaultChannel),
        }
    }

    /// Sends a message to the specified client on the specified channel
    pub fn send_message_on<T: serde::Serialize, C: Into<ChannelId>>(
        &mut self,
        client_id: ClientId,
        channel_id: C,
        message: T,
    ) -> Result<(), ServerMessageSendError> {
        match bincode::serde::encode_to_vec(&message, bincode::config::standard()) {
            Ok(payload) => Ok(self.send_payload_on(client_id, channel_id, payload)?),
            Err(_) => Err(ServerMessageSendError::Serialization),
        }
    }

    /// [`ServerEndpoint::send_message`] that logs the error instead of returning a result.
    pub fn try_send_message<T: serde::Serialize>(&mut self, client_id: ClientId, message: T) {
        match self.send_message(client_id, message) {
            Ok(_) => {}
            Err(err) => error!("try_send_message: {}", err),
        }
    }

    /// [`ServerEndpoint::send_message_on`] that logs the error instead of returning a result.
    pub fn try_send_message_on<T: serde::Serialize, C: Into<ChannelId>>(
        &mut self,
        client_id: ClientId,
        channel_id: C,
        message: T,
    ) {
        match self.send_message_on(client_id, channel_id, message) {
            Ok(_) => {}
            Err(err) => error!("try_send_message_on: {}", err),
        }
    }

    /// Same as [ServerEndpoint::send_group_message_on] but on the default channel
    pub fn send_group_message<'a, I: Iterator<Item = &'a ClientId>, T: serde::Serialize>(
        &mut self,
        client_ids: I,
        message: T,
    ) -> Result<(), ServerGroupMessageSendError> {
        match self.default_channel() {
            Some(channel) => self.send_group_message_on(client_ids, channel, message),
            None => Err(ServerGroupMessageSendError::NoDefaultChannel),
        }
    }

    /// Sends the message to the specified clients on the specified channel.
    pub fn send_group_message_on<
        'a,
        I: Iterator<Item = &'a ClientId>,
        T: serde::Serialize,
        C: Into<ChannelId>,
    >(
        &mut self,
        client_ids: I,
        channel_id: C,
        message: T,
    ) -> Result<(), ServerGroupMessageSendError> {
        let channel_id = channel_id.into();
        let Ok(payload) = bincode::serde::encode_to_vec(&message, bincode::config::standard())
        else {
            return Err(ServerGroupMessageSendError::Serialization);
        };
        let bytes = Bytes::from(payload);
        let mut errs = vec![];
        for &client_id in client_ids {
            if let Err(e) = self.send_payload_on(client_id, channel_id, bytes.clone()) {
                errs.push((client_id, e.into()));
            }
        }
        match errs.is_empty() {
            true => Ok(()),
            false => Err(ServerGroupSendError(errs).into()),
        }
    }

    /// Same as [ServerEndpoint::send_group_message] but will log the error instead of returning it
    pub fn try_send_group_message<'a, I: Iterator<Item = &'a ClientId>, T: serde::Serialize>(
        &mut self,
        client_ids: I,
        message: T,
    ) {
        if let Err(err) = self.send_group_message(client_ids, message) {
            error!("try_send_group_message: {}", err);
        }
    }

    /// Same as [ServerEndpoint::send_group_message_on] but will log the error instead of returning it
    pub fn try_send_group_message_on<
        'a,
        I: Iterator<Item = &'a ClientId>,
        T: serde::Serialize,
        C: Into<ChannelId>,
    >(
        &mut self,
        client_ids: I,
        channel_id: C,
        message: T,
    ) {
        if let Err(err) = self.send_group_message_on(client_ids, channel_id, message) {
            error!("try_send_group_message_on: {}", err);
        }
    }

    /// Same as [ServerEndpoint::broadcast_message_on] but on the default channel
    pub fn broadcast_message<T: serde::Serialize>(
        &mut self,
        message: T,
    ) -> Result<(), ServerGroupMessageSendError> {
        match self.default_channel() {
            Some(channel) => self.broadcast_message_on(channel, message),
            None => Err(ServerGroupMessageSendError::NoDefaultChannel),
        }
    }

    /// Same as [ServerEndpoint::broadcast_payload_on] but will serialize the message to a payload before
    pub fn broadcast_message_on<T: serde::Serialize, C: Into<ChannelId>>(
        &mut self,
        channel_id: C,
        message: T,
    ) -> Result<(), ServerGroupMessageSendError> {
        match bincode::serde::encode_to_vec(&message, bincode::config::standard()) {
            Ok(payload) => Ok(self.broadcast_payload_on(channel_id, payload)?),
            Err(_) => Err(ServerGroupMessageSendError::Serialization),
        }
    }

    /// Same as [ServerEndpoint::broadcast_message] but will log the error instead of returning it
    pub fn try_broadcast_message<T: serde::Serialize>(&mut self, message: T) {
        if let Err(err) = self.broadcast_message(message) {
            error!("try_broadcast_message: {}", err);
        }
    }

    /// Same as [ServerEndpoint::broadcast_message_on] but will log the error instead of returning it
    pub fn try_broadcast_message_on<T: serde::Serialize, C: Into<ChannelId>>(
        &mut self,
        channel_id: C,
        message: T,
    ) {
        if let Err(err) = self.broadcast_message_on(channel_id, message) {
            error!("try_broadcast_message_on: {}", err);
        }
    }
}
