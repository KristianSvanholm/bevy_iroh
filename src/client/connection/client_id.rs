use bevy::prelude::*;
use bytes::Buf;
use futures_util::StreamExt;
use iroh::endpoint::Connection;
use tokio_util::codec::{FramedRead, LengthDelimitedCodec};

use crate::{
    client::IrohConnectionError,
    shared::{ClientId, CLIENT_ID_LEN},
};

use super::CloseRecv;

pub(crate) enum ClientIdReception {
    Interrupted,
    Retrieved(ClientId),
    Failed(IrohConnectionError),
}

pub(crate) async fn receive_client_id(
    connection_handle: Connection,
    mut close_recv: CloseRecv,
) -> ClientIdReception {
    tokio::select! {
        _ = close_recv.recv() => {
            trace!("Client id receiver received a close signal");
            ClientIdReception::Interrupted
        }
        client_id_res = async {
            let (_, recv) = connection_handle.accept_bi().await?;
            let mut frame_recv = FramedRead::new(recv, LengthDelimitedCodec::new());
            let mut msg_bytes = frame_recv.next().await.ok_or(IrohConnectionError::ClientIdNotReceived)?.map_err(|_| IrohConnectionError::ClientIdNotReceived)?;
            if msg_bytes.len() >= CLIENT_ID_LEN {
                Ok(msg_bytes.get_uint(CLIENT_ID_LEN))
            } else {
                Err(IrohConnectionError::InvalidClientId)
            }
        } => {
            trace!("Client id receiver ended");
            match client_id_res{
                Ok(client_id) => ClientIdReception::Retrieved(client_id),
                Err(err) => ClientIdReception::Failed(err),
            }
        }
    }
}
