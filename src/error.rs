/// Errors that may cause a delete request to fail.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Fail)]
pub enum Delete {
    /// No node exists with the given `path`.
    #[fail(display = "target node does not exist")]
    NoNode,

    /// The target node has a different version than was specified by the call to delete.
    #[fail(
        display = "target node has different version than expected ({})",
        expected
    )]
    BadVersion {
        /// The expected node version.
        expected: i32,
    },

    /// The target node has child nodes, and therefore cannot be deleted.
    #[fail(display = "target node has children, and cannot be deleted")]
    NotEmpty,
}

/// Errors that may cause a `set_data` request to fail.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Fail)]
pub enum SetData {
    /// No node exists with the given `path`.
    #[fail(display = "target node does not exist")]
    NoNode,

    /// The target node has a different version than was specified by the call to `set_data`.
    #[fail(
        display = "target node has different version than expected ({})",
        expected
    )]
    BadVersion {
        /// The expected node version.
        expected: i32,
    },

    /// The target node's permission does not accept data modification or requires different
    /// authentication to be altered.
    #[fail(display = "insuficient authentication")]
    NoAuth,
}

/// Errors that may cause a create request to fail.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Fail)]
pub enum Create {
    /// A node with the given `path` already exists.
    #[fail(display = "target node already exists")]
    NodeExists,

    /// The parent node of the given `path` does not exist.
    #[fail(display = "parent node of target does not exist")]
    NoNode,

    /// The parent node of the given `path` is ephemeral, and cannot have children.
    #[fail(display = "parent node is ephemeral, and cannot have children")]
    NoChildrenForEphemerals,

    /// The given ACL is invalid.
    #[fail(display = "the given ACL is invalid")]
    InvalidAcl,
}

/// Errors that may cause a `get_acl` request to fail.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Fail)]
pub enum GetAcl {
    /// No node exists with the given `path`.
    #[fail(display = "target node does not exist")]
    NoNode,
}

/// Errors that may cause a `set_acl` request to fail.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Fail)]
pub enum SetAcl {
    /// No node exists with the given `path`.
    #[fail(display = "target node does not exist")]
    NoNode,

    /// The target node has a different version than was specified by the call to `set_acl`.
    #[fail(
        display = "target node has different version than expected ({})",
        expected
    )]
    BadVersion {
        /// The expected node version.
        expected: i32,
    },

    /// The given ACL is invalid.
    #[fail(display = "the given ACL is invalid")]
    InvalidAcl,

    /// The target node's permission does not accept acl modification or requires different
    /// authentication to be altered.
    #[fail(display = "insufficient authentication")]
    NoAuth,
}

/// Errors that may cause a `check` request to fail.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Fail)]
pub enum Check {
    /// No node exists with the given `path`.
    #[fail(display = "target node does not exist")]
    NoNode,

    /// The target node has a different version than was specified by the call to `check`.
    #[fail(
        display = "target node has different version than expected ({})",
        expected
    )]
    BadVersion {
        /// The expected node version.
        expected: i32,
    },
}

/// The result of a failed `multi` request.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Fail)]
pub enum Multi {
    /// A failed `delete` request.
    #[fail(display = "delete failed: {}", 0)]
    Delete(Delete),

    /// A failed `set_data` request.
    #[fail(display = "set_data failed: {}", 0)]
    SetData(SetData),

    /// A failed `create` request.
    #[fail(display = "create failed: {}", 0)]
    Create(Create),

    /// A failed `check` request.
    #[fail(display = "check failed")]
    Check(Check),

    /// The request would have succeeded, but a later request in the `multi`
    /// batch failed and caused this request to get rolled back.
    #[fail(display = "request rolled back due to later failed request")]
    RolledBack,

    /// The request was skipped because an earlier request in the `multi` batch
    /// failed. It is unknown whether this request would have succeeded.
    #[fail(display = "request failed due to earlier failed request")]
    Skipped,
}

impl From<Delete> for Multi {
    fn from(err: Delete) -> Self {
        Multi::Delete(err)
    }
}

impl From<SetData> for Multi {
    fn from(err: SetData) -> Self {
        Multi::SetData(err)
    }
}

impl From<Create> for Multi {
    fn from(err: Create) -> Self {
        Multi::Create(err)
    }
}

impl From<Check> for Multi {
    fn from(err: Check) -> Self {
        Multi::Check(err)
    }
}
