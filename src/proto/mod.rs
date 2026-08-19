use std::error::Error;
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

pub trait ZooKeeperTransport: AsyncRead + AsyncWrite + Sized + Send + 'static {
    type Addr: Send + Clone;
    type ConnectError: Error + Send + Sync + 'static;
    fn connect(
        addr: Self::Addr,
    ) -> impl std::future::Future<Output = Result<Self, Self::ConnectError>> + std::marker::Send;
}

impl ZooKeeperTransport for tokio::net::TcpStream {
    type Addr = SocketAddr;
    type ConnectError = tokio::io::Error;
    async fn connect(addr: Self::Addr) -> Result<Self, Self::ConnectError> {
        tokio::net::TcpStream::connect(addr).await
    }
}

/// mTLS transport (spike). The `ClientConfig` and `ServerName` live in [`Self::Addr`] because the
/// [`Packetizer`] re-invokes [`ZooKeeperTransport::connect`] with the stored `Addr` on every
/// reconnect — there is nowhere else to thread them through.
impl ZooKeeperTransport for tokio_rustls::client::TlsStream<tokio::net::TcpStream> {
    type Addr = (
        SocketAddr,
        std::sync::Arc<tokio_rustls::rustls::ClientConfig>,
        tokio_rustls::rustls::pki_types::ServerName<'static>,
    );
    type ConnectError = tokio::io::Error;
    async fn connect(addr: Self::Addr) -> Result<Self, Self::ConnectError> {
        let (socket_addr, config, server_name) = addr;
        let tcp = tokio::net::TcpStream::connect(socket_addr).await?;
        tokio_rustls::TlsConnector::from(config)
            .connect(server_name, tcp)
            .await
    }
}
