use std::net::SocketAddr;
use tokio::prelude::*;

mod active_packetizer;
mod error;
mod packetizer;
mod request;
mod response;
mod watch;

pub(crate) use self::error::ZkError;
pub(crate) use self::packetizer::{Enqueuer, Packetizer};
pub(crate) use self::request::Request;
pub(crate) use self::response::Response;
pub(crate) use self::watch::Watch;

pub trait ZooKeeperTransport: AsyncRead + AsyncWrite + Sized + Send {
    type Addr: Send;
    type ConnectError: Into<failure::Error>;
    type ConnectFut: Future<Item = Self, Error = Self::ConnectError> + Send + 'static;
    fn connect(addr: &Self::Addr) -> Self::ConnectFut;
}

impl ZooKeeperTransport for tokio::net::TcpStream {
    type Addr = SocketAddr;
    type ConnectError = tokio::io::Error;
    type ConnectFut = tokio::net::tcp::ConnectFuture;
    fn connect(addr: &Self::Addr) -> Self::ConnectFut {
        tokio::net::TcpStream::connect(addr)
    }
}
