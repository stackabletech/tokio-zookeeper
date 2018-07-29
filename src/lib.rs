//! See [`ZooKeeper`].
//!
//! # Limitations
//!
//!  - Multi-server connections are not supported
//!  - Client does not recover from errors during reconnects (e.g., session expiry)

#![deny(missing_docs)]
#![deny(missing_debug_implementations)]
#![deny(missing_copy_implementations)]

extern crate byteorder;
#[macro_use]
extern crate failure;
#[macro_use]
extern crate futures;
extern crate tokio;
#[macro_use]
extern crate lazy_static;

use futures::sync::oneshot;
use std::borrow::Cow;
use std::net::SocketAddr;
use tokio::prelude::*;

/// Per-operation ZooKeeper error types.
pub mod error;
mod proto;
mod types;

use proto::{Watch, ZkError};
pub use types::{Acl, CreateMode, KeeperState, Stat, WatchedEvent, WatchedEventType};

/// A connection to ZooKeeper.
///
/// All interactions with ZooKeeper are performed by calling the methods of a `ZooKeeper` instance.
/// All clones of the same `ZooKeeper` instance use the same underlying connection. Once a
/// connection to a server is established, a session ID is assigned to the client. The client will
/// send heart beats to the server periodically to keep the session valid.
///
/// The application can call ZooKeeper APIs through a client as long as the session ID of the
/// client remains valid. If for some reason, the client fails to send heart beats to the server
/// for a prolonged period of time (exceeding the session timeout value, for instance), the server
/// will expire the session, and the session ID will become invalid. The `ZooKeeper` instance will
/// then no longer be usable, and all futures will resolve with a protocol-level error. To make
/// further ZooKeeper API calls, the application must create a new `ZooKeeper` instance.
///
/// If the ZooKeeper server the client currently connects to fails or otherwise does not respond,
/// the client will automatically try to connect to another server before its session ID expires.
/// If successful, the application can continue to use the client.
///
/// Some successful ZooKeeper API calls can leave watches on the "data nodes" in the ZooKeeper
/// server. Other successful ZooKeeper API calls can trigger those watches. Once a watch is
/// triggered, an event will be delivered to the client which left the watch at the first place.
/// Each watch can be triggered only once. Thus, up to one event will be delivered to a client for
/// every watch it leaves.
// TODO: When a client drops the current connection and re-connects to a server, all the existing
// watches are considered as being triggered but the undelivered events are lost. To emulate this,
// the client will generate a special event to tell the event handler a connection has been
// dropped. This special event has EventType None and KeeperState Disconnected.
#[derive(Debug, Clone)]
pub struct ZooKeeper {
    #[allow(dead_code)]
    connection: proto::Enqueuer,
}

impl ZooKeeper {
    /// Connect to a ZooKeeper server instance at the given address.
    ///
    /// Session establishment is asynchronous. This constructor will initiate connection to the
    /// server and return immediately - potentially (usually) before the session is fully
    /// established. When the session is established, a `ZooKeeper` instance is returned, along
    /// with a "watcher" that will provide notifications of any changes in state.
    ///
    /// If the connection to the server fails, the client will automatically try to re-connect.
    /// Only if re-connection fails is an error returned to the client. Requests that are in-flight
    /// during a disconnect may fail and have to be retried.
    // TODO: To create a ZooKeeper client object, the application needs to pass a connection string
    // containing a comma separated list of host:port pairs, each corresponding to a ZooKeeper
    // server. The instantiated ZooKeeper client object will pick an arbitrary server from the
    // connectString and attempt to connect to it. If establishment of the connection fails,
    // another server in the connect string will be tried (the order is non-deterministic, as we
    // random shuffle the list), until a connection is established. The client will continue
    // attempts until the session is explicitly closed.
    //
    // TODO: An optional "chroot" suffix may also be appended to the connection string. This will
    // run the client commands while interpreting all paths relative to this root (similar to the
    // unix chroot command).
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

    /// Create a node with the given `path` with `data` as its contents.
    ///
    /// The `mode` argument specifies additional options for the newly created node.
    ///
    /// If `mode` is set to [`CreateMode::Ephemeral`] (or [`CreateMode::EphemeralSequential`]), the
    /// node will be removed by the ZooKeeper automatically when the session associated with the
    /// creation of the node expires.
    ///
    /// If `mode` is set to [`CreateMode::PersistentSequential`] or
    /// [`CreateMode::EphemeralSequential`], the actual path name of a sequential node will be the
    /// given `path` plus a suffix `i` where `i` is the current sequential number of the node. The
    /// sequence number is always fixed length of 10 digits, 0 padded. Once such a node is created,
    /// the sequential number will be incremented by one. The newly created node's full name is
    /// returned when the future is resolved.
    ///
    /// If a node with the same actual path already exists in the ZooKeeper, the returned future
    /// resolves with an error of [`error::Create::NodeExists`]. Note that since a different actual
    /// path is used for each invocation of creating sequential nodes with the same `path`
    /// argument, calls with sequential modes will never return `NodeExists`.
    ///
    /// Ephemeral nodes cannot have children in ZooKeeper. Therefore, if the parent node of the
    /// given `path` is ephemeral, the return future resolves to
    /// [`error::Create::NoChildrenForEphemerals`].
    ///
    /// If a node is created successfully, the ZooKeeper server will trigger the watches on the
    /// `path` left by `exists` calls, and the watches on the parent of the node by `get_children`
    /// calls.
    ///
    /// The maximum allowable size of the data array is 1 MB (1,048,576 bytes).
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

    /// Delete the node at the given `path`. The call will succeed if such a node exists, and the
    /// given `version` matches the node's version (if the given `version` is `None`, it matches
    /// any node's versions).
    ///
    /// This operation, if successful, will trigger all the watches on the node of the given `path`
    /// left by `exists` API calls, and the watches on the parent node left by `get_children` API
    /// calls.
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
}

impl ZooKeeper {
    /// Add a global watch for the next chained operation.
    pub fn watch(self) -> WatchGlobally {
        WatchGlobally(self)
    }

    /// Add a watch for the next chained operation, and return a future for any received event
    /// along with the operation's (successful) result.
    pub fn with_watcher(self) -> WithWatcher {
        WithWatcher(self)
    }

    fn exists_w(
        self,
        path: &str,
        watch: Watch,
    ) -> impl Future<Item = (Self, Option<Stat>), Error = failure::Error> {
        self.connection
            .enqueue(proto::Request::Exists {
                path: path.to_string(),
                watch,
            })
            .and_then(|r| match r {
                Ok(proto::Response::Exists { stat }) => Ok(Some(stat)),
                Ok(r) => bail!("got a non-create response to a create request: {:?}", r),
                Err(ZkError::NoNode) => Ok(None),
                Err(e) => bail!("exists call failed: {:?}", e),
            })
            .map(move |r| (self, r))
    }

    /// Return the [`Stat`] of the node of the given `path`, or `None` if the node does not exist.
    pub fn exists(
        self,
        path: &str,
    ) -> impl Future<Item = (Self, Option<Stat>), Error = failure::Error> {
        self.exists_w(path, Watch::None)
    }

    fn get_children_w(
        self,
        path: &str,
        watch: Watch,
    ) -> impl Future<Item = (Self, Option<Vec<String>>), Error = failure::Error> {
        self.connection
            .enqueue(proto::Request::GetChildren {
                path: path.to_string(),
                watch,
            })
            .and_then(|r| match r {
                Ok(proto::Response::Strings(children)) => Ok(Some(children)),
                Ok(r) => bail!("got non-strings response to get-children: {:?}", r),
                Err(ZkError::NoNode) => Ok(None),
                Err(e) => Err(format_err!("get-children call failed: {:?}", e)),
            })
            .map(move |r| (self, r))
    }

    /// Return the names of the children of the node at the given `path`, or `None` if the node
    /// does not exist.
    ///
    /// The returned list of children is not sorted and no guarantee is provided as to its natural
    /// or lexical order.
    pub fn get_children(
        self,
        path: &str,
    ) -> impl Future<Item = (Self, Option<Vec<String>>), Error = failure::Error> {
        self.get_children_w(path, Watch::None)
    }

    fn get_data_w(
        self,
        path: &str,
        watch: Watch,
    ) -> impl Future<Item = (Self, Option<(Vec<u8>, Stat)>), Error = failure::Error> {
        self.connection
            .enqueue(proto::Request::GetData {
                path: path.to_string(),
                watch,
            })
            .and_then(|r| match r {
                Ok(proto::Response::GetData { bytes, stat }) => Ok(Some((bytes, stat))),
                Ok(r) => bail!("got non-data response to get-data: {:?}", r),
                Err(ZkError::NoNode) => Ok(None),
                Err(e) => Err(format_err!("get-data call failed: {:?}", e)),
            })
            .map(move |r| (self, r))
    }

    /// Return the data and the [`Stat`] of the node at the given `path`, or `None` if it does not
    /// exist.
    pub fn get_data(
        self,
        path: &str,
    ) -> impl Future<Item = (Self, Option<(Vec<u8>, Stat)>), Error = failure::Error> {
        self.get_data_w(path, Watch::None)
    }
}

/// Proxy for [`ZooKeeper`] that adds watches for initiated operations.
///
/// Triggered watches produce events on the global watcher stream.
#[derive(Debug, Clone)]
pub struct WatchGlobally(ZooKeeper);

impl WatchGlobally {
    /// Return the [`Stat`] of the node of the given `path`, or `None` if the node does not exist.
    ///
    /// If no errors occur, a watch is left on the node at the given `path`. The watch is triggered
    /// by any successful operation that creates or deletes the node, or sets the node's data. When
    /// the watch triggers, an event is sent to the global watcher stream.
    pub fn exists(
        self,
        path: &str,
    ) -> impl Future<Item = (ZooKeeper, Option<Stat>), Error = failure::Error> {
        self.0.exists_w(path, Watch::Global)
    }

    /// Return the names of the children of the node at the given `path`, or `None` if the node
    /// does not exist.
    ///
    /// The returned list of children is not sorted and no guarantee is provided as to its natural
    /// or lexical order.
    ///
    /// If no errors occur, a watch is left on the node at the given `path`. The watch is triggered
    /// by any successful operation that deletes the node at the given `path`, or creates or
    /// deletes a child of that node. When the watch triggers, an event is sent to the global
    /// watcher stream.
    pub fn get_children(
        self,
        path: &str,
    ) -> impl Future<Item = (ZooKeeper, Option<Vec<String>>), Error = failure::Error> {
        self.0.get_children_w(path, Watch::Global)
    }

    /// Return the data and the [`Stat`] of the node at the given `path`, or `None` if it does not
    /// exist.
    ///
    /// If no errors occur, a watch is left on the node at the given `path`. The watch is triggered
    /// by any successful operation that sets the node's data, or deletes it. When the watch
    /// triggers, an event is sent to the global watcher stream.
    pub fn get_data(
        self,
        path: &str,
    ) -> impl Future<Item = (ZooKeeper, Option<(Vec<u8>, Stat)>), Error = failure::Error> {
        self.0.get_data_w(path, Watch::Global)
    }
}

/// Proxy for [`ZooKeeper`] that adds non-global watches for initiated operations.
///
/// Events from triggered watches are yielded through returned `oneshot` channels. All events are
/// also produced on the global watcher stream.
#[derive(Debug, Clone)]
pub struct WithWatcher(ZooKeeper);

impl WithWatcher {
    /// Return the [`Stat`] of the node of the given `path`, or `None` if the node does not exist.
    ///
    /// If no errors occur, a watch will be left on the node at the given `path`. The watch is
    /// triggered by any successful operation that creates or deletes the node, or sets the data on
    /// the node, and in turn causes the included `oneshot::Receiver` to resolve.
    pub fn exists(
        self,
        path: &str,
    ) -> impl Future<
        Item = (ZooKeeper, oneshot::Receiver<WatchedEvent>, Option<Stat>),
        Error = failure::Error,
    > {
        let (tx, rx) = oneshot::channel();
        self.0
            .exists_w(path, Watch::Custom(tx))
            .map(|r| (r.0, rx, r.1))
    }

    /// Return the names of the children of the node at the given `path`, or `None` if the node
    /// does not exist.
    ///
    /// The returned list of children is not sorted and no guarantee is provided as to its natural
    /// or lexical order.
    ///
    /// If no errors occur, a watch is left on the node at the given `path`. The watch is triggered
    /// by any successful operation that deletes the node at the given `path`, or creates or
    /// deletes a child of that node, and in turn causes the included `oneshot::Receiver` to
    /// resolve.
    pub fn get_children(
        self,
        path: &str,
    ) -> impl Future<
        Item = (
            ZooKeeper,
            Option<(oneshot::Receiver<WatchedEvent>, Vec<String>)>,
        ),
        Error = failure::Error,
    > {
        let (tx, rx) = oneshot::channel();
        self.0
            .get_children_w(path, Watch::Custom(tx))
            .map(|r| (r.0, r.1.map(move |c| (rx, c))))
    }

    /// Return the data and the [`Stat`] of the node at the given `path`, or `None` if it does not
    /// exist.
    ///
    /// If no errors occur, a watch is left on the node at the given `path`. The watch is triggered
    /// by any successful operation that sets the node's data, or deletes it, and in turn causes
    /// the included `oneshot::Receiver` to resolve.
    pub fn get_data(
        self,
        path: &str,
    ) -> impl Future<
        Item = (
            ZooKeeper,
            Option<(oneshot::Receiver<WatchedEvent>, Vec<u8>, Stat)>,
        ),
        Error = failure::Error,
    > {
        let (tx, rx) = oneshot::channel();
        self.0
            .get_data_w(path, Watch::Custom(tx))
            .map(|r| (r.0, r.1.map(move |(b, s)| (rx, b, s))))
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
                    zk.with_watcher()
                        .exists("/foo")
                        .inspect(|(_, _, stat)| assert_eq!(stat, &None))
                        .and_then(|(zk, exists_w, _)| {
                            zk.watch()
                                .exists("/foo")
                                .map(move |(zk, x)| (zk, x, exists_w))
                        })
                        .inspect(|(_, stat, _)| assert_eq!(stat, &None))
                        .and_then(|(zk, _, exists_w)| {
                            zk.create(
                                "/foo",
                                &b"Hello world"[..],
                                Acl::open_unsafe(),
                                CreateMode::Persistent,
                            ).map(move |(zk, x)| (zk, x, exists_w))
                        })
                        .inspect(|(_, ref path, _)| {
                            assert_eq!(path.as_ref().map(String::as_str), Ok("/foo"))
                        })
                        .and_then(move |(zk, _, exists_w)| {
                            exists_w
                                .map(move |w| (zk, w))
                                .map_err(|e| format_err!("exists_w failed: {:?}", e))
                        })
                        .inspect(|(_, event)| {
                            assert_eq!(
                                event,
                                &WatchedEvent {
                                    event_type: WatchedEventType::NodeCreated,
                                    keeper_state: KeeperState::SyncConnected,
                                    path: String::from("/foo"),
                                }
                            );
                        })
                        .and_then(|(zk, _)| zk.watch().exists("/foo"))
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
                        .and_then(|(zk, _)| zk.watch().exists("/foo"))
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
