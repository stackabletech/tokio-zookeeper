use async_trait::async_trait;
use std::net::SocketAddr;
use tokio::io::{AsyncRead, AsyncWrite};

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

#[async_trait]
pub trait ZooKeeperTransport: AsyncRead + AsyncWrite + Sized + Send + 'static {
    type Addr: Send + Clone;
    type ConnectError: Into<failure::Error> + 'static;
    async fn connect(addr: Self::Addr) -> Result<Self, Self::ConnectError>;
}

#[async_trait]
impl ZooKeeperTransport for tokio::net::TcpStream {
    type Addr = SocketAddr;
    type ConnectError = tokio::io::Error;
    async fn connect(addr: Self::Addr) -> Result<Self, Self::ConnectError> {
        tokio::net::TcpStream::connect(addr).await
    }
}
