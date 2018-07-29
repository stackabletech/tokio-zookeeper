#[derive(Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum ZkError {
    /// This code is never returned from the server. It should not be used other than to indicate a
    /// range. Specifically error codes greater than this value are API errors (while values less
    /// than this indicate a system error.
    APIError = -100,
    /// Client authentication failed.
    AuthFailed = -115,
    /// Invalid arguments.
    BadArguments = -8,
    /// Version conflict in `set` operation. In case of reconfiguration: reconfig requested from
    /// config version X but last seen config has a different version Y.
    BadVersion = -103,
    /// Connection to the server has been lost.
    ConnectionLoss = -4,
    /// A data inconsistency was found.
    DataInconsistency = -3,
    /// Attempt to create ephemeral node on a local session.
    EphemeralOnLocalSession = -120,
    /// Invalid `Acl` specified.
    InvalidACL = -114,
    /// Invalid callback specified.
    InvalidCallback = -113,
    /// Error while marshalling or unmarshalling data.
    MarshallingError = -5,
    /// Not authenticated.
    NoAuth = -102,
    /// Ephemeral nodes may not have children.
    NoChildrenForEphemerals = -108,
    /// Request to create node that already exists.
    NodeExists = -110,
    /// Attempted to read a node that does not exist.
    NoNode = -101,
    /// The node has children.
    NotEmpty = -111,
    /// State-changing request is passed to read-only server.
    NotReadOnly = -119,
    /// Attempt to remove a non-existing watcher.
    NoWatcher = -121,
    /// Operation timeout.
    OperationTimeout = -7,
    /// A runtime inconsistency was found.
    RuntimeInconsistency = -2,
    /// The session has been expired by the server.
    SessionExpired = -112,
    /// Session moved to another server, so operation is ignored.
    SessionMoved = -118,
    /// System and server-side errors. This is never thrown by the server, it shouldn't be used
    /// other than to indicate a range. Specifically error codes greater than this value, but lesser
    /// than `APIError`, are system errors.
    SystemError = -1,
    /// Operation is unimplemented.
    Unimplemented = -6,
}

impl From<i32> for ZkError {
    fn from(code: i32) -> Self {
        match code {
            -100 => ZkError::APIError,
            -115 => ZkError::AuthFailed,
            -8 => ZkError::BadArguments,
            -103 => ZkError::BadVersion,
            -4 => ZkError::ConnectionLoss,
            -3 => ZkError::DataInconsistency,
            -120 => ZkError::EphemeralOnLocalSession,
            -114 => ZkError::InvalidACL,
            -113 => ZkError::InvalidCallback,
            -5 => ZkError::MarshallingError,
            -102 => ZkError::NoAuth,
            -108 => ZkError::NoChildrenForEphemerals,
            -110 => ZkError::NodeExists,
            -101 => ZkError::NoNode,
            -111 => ZkError::NotEmpty,
            -119 => ZkError::NotReadOnly,
            -121 => ZkError::NoWatcher,
            -7 => ZkError::OperationTimeout,
            -2 => ZkError::RuntimeInconsistency,
            -112 => ZkError::SessionExpired,
            -118 => ZkError::SessionMoved,
            -1 => ZkError::SystemError,
            -6 => ZkError::Unimplemented,
            _ => unimplemented!(),
        }
    }
}
