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
pub use types::{Acl, CreateMode, KeeperState, Stat, WatchedEvent, WatchedEventType};

#[derive(Clone)]
pub struct ZooKeeper {
    #[allow(dead_code)]
    connection: proto::Enqueuer,
}

impl ZooKeeper {
    pub fn connect(
        addr: &SocketAddr,
    ) -> impl Future<Item = (Self, impl Stream<Item = WatchedEvent, Error = ()>), Error = failure::Error>
    {
        let (tx, rx) = futures::sync::mpsc::unbounded();
        let addr = addr.clone();
        tokio::net::TcpStream::connect(&addr)
            .map_err(failure::Error::from)
            .and_then(move |stream| Self::handshake(addr, stream, tx))
            .map(move |zk| (zk, rx))
    }

    fn handshake(
        addr: SocketAddr,
        stream: tokio::net::TcpStream,
        default_watcher: futures::sync::mpsc::UnboundedSender<WatchedEvent>,
    ) -> impl Future<Item = Self, Error = failure::Error> {
        let request = proto::Request::Connect {
            protocol_version: 0,
            last_zxid_seen: 0,
            timeout: 0,
            session_id: 0,
            passwd: vec![],
            read_only: false,
        };
        eprintln!("about to handshake");

        let enqueuer = proto::Packetizer::new(addr, stream, default_watcher);
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
                Ok(r) => bail!("got non-string response to create: {:?}", r),
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
        watch: bool,
    ) -> impl Future<Item = (Self, Option<Stat>), Error = failure::Error> {
        self.connection
            .enqueue(proto::Request::Exists {
                path: path.to_string(),
                watch: if watch { 1 } else { 0 },
            })
            .and_then(|r| match r {
                Ok(proto::Response::Exists { stat }) => Ok(Some(stat)),
                Ok(r) => bail!("got a non-create response to a create request: {:?}", r),
                Err(ZkError::NoNode) => Ok(None),
                Err(e) => bail!("exists call failed: {:?}", e),
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
                Ok(r) => bail!("got non-empty response to delete: {:?}", r),
                Err(ZkError::NoNode) => Ok(Err(error::Delete::NoNode)),
                Err(ZkError::NotEmpty) => Ok(Err(error::Delete::NotEmpty)),
                Err(ZkError::BadVersion) => {
                    Ok(Err(error::Delete::BadVersion { expected: version }))
                }
                Err(e) => Err(format_err!("delete call failed: {:?}", e)),
            })
            .map(move |r| (self, r))
    }

    pub fn get_children(
        self,
        path: &str,
    ) -> impl Future<Item = (Self, Option<Vec<String>>), Error = failure::Error> {
        self.connection
            .enqueue(proto::Request::GetChildren {
                path: path.to_string(),
                watch: 0,
            })
            .and_then(move |r| match r {
                Ok(proto::Response::Strings(children)) => Ok(Some(children)),
                Ok(r) => bail!("got non-strings response to get-children: {:?}", r),
                Err(ZkError::NoNode) => Ok(None),
                Err(e) => Err(format_err!("get-children call failed: {:?}", e)),
            })
            .map(move |r| (self, r))
    }

    pub fn get_data(
        self,
        path: &str,
    ) -> impl Future<Item = (Self, Option<(Vec<u8>, Stat)>), Error = failure::Error> {
        self.connection
            .enqueue(proto::Request::GetData {
                path: path.to_string(),
                watch: 0,
            })
            .and_then(move |r| match r {
                Ok(proto::Response::GetData { bytes, stat }) => Ok(Some((bytes, stat))),
                Ok(r) => bail!("got non-data response to get-data: {:?}", r),
                Err(ZkError::NoNode) => Ok(None),
                Err(e) => Err(format_err!("get-data call failed: {:?}", e)),
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
        let (zk, w): (ZooKeeper, _) =
            rt.block_on(
                ZooKeeper::connect(&"127.0.0.1:2181".parse().unwrap()).and_then(|(zk, w)| {
                    zk.exists("/foo", true)
                        .inspect(|(_, stat)| assert_eq!(stat, &None))
                        .and_then(|(zk, _)| {
                            zk.create(
                                "/foo",
                                &b"Hello world"[..],
                                Acl::open_unsafe(),
                                CreateMode::Persistent,
                            )
                        })
                        .inspect(|(_, ref path)| {
                            assert_eq!(path.as_ref().map(String::as_str), Ok("/foo"))
                        })
                        .and_then(|(zk, _)| zk.exists("/foo", true))
                        .inspect(|(_, stat)| {
                            assert_eq!(stat.unwrap().data_length as usize, b"Hello world".len())
                        })
                        .and_then(|(zk, _)| zk.get_data("/foo"))
                        .inspect(|(_, res)| {
                            let data = b"Hello world";
                            let res = res.as_ref().unwrap();
                            assert_eq!(res.0, data);
                            assert_eq!(res.1.data_length as usize, data.len());
                        })
                        .and_then(|(zk, _)| {
                            zk.create(
                                "/foo/bar",
                                &b"Hello bar"[..],
                                Acl::open_unsafe(),
                                CreateMode::Persistent,
                            )
                        })
                        .inspect(|(_, ref path)| {
                            assert_eq!(path.as_ref().map(String::as_str), Ok("/foo/bar"))
                        })
                        .and_then(|(zk, _)| zk.get_children("/foo"))
                        .inspect(|(_, children)| {
                            assert_eq!(children, &Some(vec!["bar".to_string()]));
                        })
                        .and_then(|(zk, _)| zk.get_data("/foo/bar"))
                        .inspect(|(_, res)| {
                            let data = b"Hello bar";
                            let res = res.as_ref().unwrap();
                            assert_eq!(res.0, data);
                            assert_eq!(res.1.data_length as usize, data.len());
                        })
                        .and_then(|(zk, _)| zk.delete("/foo", None))
                        .inspect(|(_, res)| assert_eq!(res, &Err(error::Delete::NotEmpty)))
                        .and_then(|(zk, _)| zk.delete("/foo/bar", None))
                        .inspect(|(_, res)| assert_eq!(res, &Ok(())))
                        .and_then(|(zk, _)| zk.delete("/foo", None))
                        .inspect(|(_, res)| assert_eq!(res, &Ok(())))
                        .and_then(|(zk, _)| zk.exists("/foo", true))
                        .inspect(|(_, stat)| assert_eq!(stat, &None))
                        .and_then(move |(zk, _)| {
                            w.into_future()
                                .map(move |x| (zk, x))
                                .map_err(|e| format_err!("stream error: {:?}", e.0))
                        })
                        .inspect(|(_, (event, _))| {
                            assert_eq!(
                                event,
                                &Some(WatchedEvent {
                                    event_type: WatchedEventType::NodeCreated,
                                    keeper_state: KeeperState::SyncConnected,
                                    path: String::from("/foo"),
                                })
                            );
                        })
                        .and_then(|(zk, (_, w))| {
                            w.into_future()
                                .map(move |x| (zk, x))
                                .map_err(|e| format_err!("stream error: {:?}", e.0))
                        })
                        .inspect(|(_, (event, _))| {
                            assert_eq!(
                                event,
                                &Some(WatchedEvent {
                                    event_type: WatchedEventType::NodeDeleted,
                                    keeper_state: KeeperState::SyncConnected,
                                    path: String::from("/foo"),
                                })
                            );
                        })
                        .map(|(zk, (_, w))| (zk, w))
                }),
            ).unwrap();

        eprintln!("got through all futures");
        drop(zk); // make Packetizer idle
        rt.shutdown_on_idle().wait().unwrap();
        assert_eq!(w.wait().count(), 0);
    }
}
