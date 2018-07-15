extern crate byteorder;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate futures;
extern crate tokio;
#[macro_use]
extern crate lazy_static;

use std::borrow::Cow;
use std::net::SocketAddr;
use tokio::prelude::*;

pub mod error;
mod proto;
mod types;

use proto::ZkError;
pub use types::{Acl, CreateMode, Stat};

#[derive(Clone)]
pub struct ZooKeeper {
    #[allow(dead_code)]
    connection: proto::Enqueuer,
}

impl ZooKeeper {
    pub fn connect(addr: &SocketAddr) -> impl Future<Item = Self, Error = failure::Error> {
        tokio::net::TcpStream::connect(addr)
            .map_err(failure::Error::from)
            .and_then(|stream| Self::handshake(stream))
    }

    fn handshake<S>(stream: S) -> impl Future<Item = Self, Error = failure::Error>
    where
        S: Send + 'static + AsyncRead + AsyncWrite,
    {
        let request = proto::Request::Connect {
            protocol_version: 0,
            last_zxid_seen: 0,
            timeout: 0,
            session_id: 0,
            passwd: vec![],
            read_only: false,
        };
        eprintln!("about to handshake");

        let enqueuer = proto::Packetizer::new(stream);
        enqueuer.enqueue(request).map(move |response| {
            eprintln!("{:?}", response);
            ZooKeeper {
                connection: enqueuer,
            }
        })
    }

    pub fn create<D, A>(
        self,
        path: &str,
        data: D,
        acl: A,
        mode: CreateMode,
    ) -> impl Future<Item = (Self, Result<String, error::Create>), Error = failure::Error>
    where
        D: Into<Cow<'static, [u8]>>,
        A: Into<Cow<'static, [Acl]>>,
    {
        self.connection
            .enqueue(proto::Request::Create {
                path: path.to_string(),
                data: data.into(),
                acl: acl.into(),
                mode,
            })
            .and_then(move |r| match r {
                Ok(proto::Response::String(s)) => Ok(Ok(s)),
                Ok(_) => unreachable!("got non-string response to create"),
                Err(ZkError::NoNode) => Ok(Err(error::Create::NoNode)),
                Err(ZkError::NodeExists) => Ok(Err(error::Create::NodeExists)),
                Err(ZkError::InvalidACL) => Ok(Err(error::Create::InvalidAcl)),
                Err(ZkError::NoChildrenForEphemerals) => {
                    Ok(Err(error::Create::NoChildrenForEphemerals))
                }
                Err(e) => Err(format_err!("create call failed: {:?}", e)),
            })
            .map(move |r| (self, r))
    }

    pub fn exists(
        self,
        path: &str,
    ) -> impl Future<Item = (Self, Option<Stat>), Error = failure::Error> {
        self.connection
            .enqueue(proto::Request::Exists {
                path: path.to_string(),
                watch: 0,
            })
            .and_then(|r| match r {
                Ok(proto::Response::Exists { stat }) => Ok(Some(stat)),
                Err(ZkError::NoNode) => Ok(None),
                Err(e) => bail!("exists call failed: {:?}", e),
                _ => {
                    unreachable!("got a non-create response to a create request: {:?}", r);
                }
            })
            .map(move |r| (self, r))
    }

    pub fn delete(
        self,
        path: &str,
        version: Option<i32>,
    ) -> impl Future<Item = (Self, Result<(), error::Delete>), Error = failure::Error> {
        let version = version.unwrap_or(-1);
        self.connection
            .enqueue(proto::Request::Delete {
                path: path.to_string(),
                version: version,
            })
            .and_then(move |r| match r {
                Ok(proto::Response::Empty) => Ok(Ok(())),
                Ok(_) => unreachable!("got non-empty response to delete"),
                Err(ZkError::NoNode) => Ok(Err(error::Delete::NoNode)),
                Err(ZkError::NotEmpty) => Ok(Err(error::Delete::NotEmpty)),
                Err(ZkError::BadVersion) => {
                    Ok(Err(error::Delete::BadVersion { expected: version }))
                }
                Err(e) => Err(format_err!("delete call failed: {:?}", e)),
            })
            .map(move |r| (self, r))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        let mut rt = tokio::runtime::Runtime::new().unwrap();
        let zk: ZooKeeper =
            rt.block_on(
                ZooKeeper::connect(&"127.0.0.1:2181".parse().unwrap()).and_then(|zk| {
                    zk.create(
                        "/foo",
                        &b"Hello world"[..],
                        Acl::open_unsafe(),
                        CreateMode::Persistent,
                    ).inspect(|(_, ref path)| {
                            assert_eq!(path.as_ref().map(String::as_str), Ok("/foo"))
                        })
                        .and_then(|(zk, _)| zk.exists("/foo"))
                        .inspect(|(_, stat)| {
                            assert_eq!(stat.unwrap().data_length as usize, b"Hello world".len())
                        })
                        .and_then(|(zk, _)| zk.delete("/foo", None))
                        .inspect(|(_, res)| assert_eq!(res, &Ok(())))
                        .and_then(|(zk, _)| zk.exists("/foo"))
                        .inspect(|(_, stat)| assert_eq!(stat, &None))
                        .map(|(zk, _)| zk)
                }),
            ).unwrap();
        drop(zk);
        rt.shutdown_on_idle();
    }
}
