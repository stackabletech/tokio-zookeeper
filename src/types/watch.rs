/// Represents a change on the ZooKeeper that a `Watcher` is able to respond to.
///
/// The `WatchedEvent` includes exactly what happened, the current state of the ZooKeeper, and the
/// path of the znode that was involved in the event.
#[derive(Clone, Debug)]
pub struct WatchedEvent {
    /// The trigger that caused the watch to hit.
    pub event_type: WatchedEventType,
    /// The current state of ZooKeeper (and the client's connection to it).
    pub keeper_state: KeeperState,
    /// The path of the znode that was involved.
    // This will be `None` for session-related triggers.
    pub path: String,
}

/// Enumeration of states the client may be at a Watcher Event. It represents the state of the
/// server at the time the event was generated.
#[repr(i32)]
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum KeeperState {
    /// The client is in the disconnected state - it is not connected to any server in the ensemble.
    Disconnected = 0,
    /// The client is in the connected state - it is connected to a server in the ensemble (one of
    /// the servers specified in the host connection parameter during ZooKeeper client creation).
    SyncConnected = 3,
    /// Authentication has failed -- connection requires a new `ZooKeeper` instance.
    AuthFailed = 4,
    /// The client is connected to a read-only server, that is the server which is not currently
    /// connected to the majority. The only operations allowed after receiving this state is read
    /// operations. This state is generated for read-only clients only since read/write clients
    /// aren't allowed to connect to read-only servers.
    ConnectedReadOnly = 5,
    /// Used to notify clients that they are SASL-authenticated, so that they can perform ZooKeeper
    /// actions with their SASL-authorized permissions.
    SaslAuthenticated = 6,
    /// The serving cluster has expired this session. The ZooKeeper client connection (the session)
    /// is no longer valid. You must create a new client connection (instantiate a new `ZooKeeper`
    /// instance) if you with to access the ensemble.
    Expired = -112,
}

impl From<i32> for KeeperState {
    fn from(code: i32) -> Self {
        match code {
            0 => KeeperState::Disconnected,
            3 => KeeperState::SyncConnected,
            4 => KeeperState::AuthFailed,
            5 => KeeperState::ConnectedReadOnly,
            6 => KeeperState::SaslAuthenticated,
            -112 => KeeperState::Expired,
            _ => unreachable!("unknown keeper state {:x}", code),
        }
    }
}

/// Enumeration of types of events that may occur on the znode.
#[repr(i32)]
#[derive(Clone, Copy, Debug)]
pub enum WatchedEventType {
    /// Nothing known has occurred on the znode. This value is issued as part of a `WatchedEvent`
    /// when the `KeeperState` changes.
    None = -1,
    /// Issued when a znode at a given path is created.
    NodeCreated = 1,
    /// Issued when a znode at a given path is deleted.
    NodeDeleted = 2,
    /// Issued when the data of a watched znode are altered. This event value is issued whenever a
    /// *set* operation occurs without an actual contents check, so there is no guarantee the data
    /// actually changed.
    NodeDataChanged = 3,
    /// Issued when the children of a watched znode are created or deleted. This event is not issued
    /// when the data within children is altered.
    NodeChildrenChanged = 4,
    /// Issued when the client removes a data watcher.
    DataWatchRemoved = 5,
    /// Issued when the client removes a child watcher.
    ChildWatchRemoved = 6,
}

impl From<i32> for WatchedEventType {
    fn from(code: i32) -> Self {
        match code {
            -1 => WatchedEventType::None,
            1 => WatchedEventType::NodeCreated,
            2 => WatchedEventType::NodeDeleted,
            3 => WatchedEventType::NodeDataChanged,
            4 => WatchedEventType::NodeChildrenChanged,
            5 => WatchedEventType::DataWatchRemoved,
            6 => WatchedEventType::ChildWatchRemoved,
            _ => unreachable!("unknown event type {:x}", code),
        }
    }
}
