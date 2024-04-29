use snafu::Snafu;

/// Errors that may cause a delete request to fail.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Snafu)]
#[snafu(module)]
pub enum Delete {
    /// No node exists with the given `path`.
    #[snafu(display("target node does not exist"))]
    NoNode,

    /// The target node has a different version than was specified by the call to delete.
    #[snafu(display("target node has different version than expected ({expected})"))]
    BadVersion {
        /// The expected node version.
        expected: i32,
    },

    /// The target node has child nodes, and therefore cannot be deleted.
    #[snafu(display("target node has children, and cannot be deleted"))]
    NotEmpty,
}

/// Errors that may cause a `set_data` request to fail.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Snafu)]
#[snafu(module)]
pub enum SetData {
    /// No node exists with the given `path`.
    #[snafu(display("target node does not exist"))]
    NoNode,

    /// The target node has a different version than was specified by the call to `set_data`.
    #[snafu(display("target node has different version than expected ({expected})"))]
    BadVersion {
        /// The expected node version.
        expected: i32,
    },

    /// The target node's permission does not accept data modification or requires different
    /// authentication to be altered.
    #[snafu(display("insuficient authentication"))]
    NoAuth,
}

/// Errors that may cause a create request to fail.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Snafu)]
#[snafu(module)]
pub enum Create {
    /// A node with the given `path` already exists.
    #[snafu(display("target node already exists"))]
    NodeExists,

    /// The parent node of the given `path` does not exist.
    #[snafu(display("parent node of target does not exist"))]
    NoNode,

    /// The parent node of the given `path` is ephemeral, and cannot have children.
    #[snafu(display("parent node is ephemeral, and cannot have children"))]
    NoChildrenForEphemerals,

    /// The given ACL is invalid.
    #[snafu(display("the given ACL is invalid"))]
    InvalidAcl,
}

/// Errors that may cause a `get_acl` request to fail.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Snafu)]
#[snafu(module)]
pub enum GetAcl {
    /// No node exists with the given `path`.
    #[snafu(display("target node does not exist"))]
    NoNode,
}

/// Errors that may cause a `set_acl` request to fail.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Snafu)]
#[snafu(module)]
pub enum SetAcl {
    /// No node exists with the given `path`.
    #[snafu(display("target node does not exist"))]
    NoNode,

    /// The target node has a different version than was specified by the call to `set_acl`.
    #[snafu(display("target node has different version than expected ({expected})"))]
    BadVersion {
        /// The expected node version.
        expected: i32,
    },

    /// The given ACL is invalid.
    #[snafu(display("the given ACL is invalid"))]
    InvalidAcl,

    /// The target node's permission does not accept acl modification or requires different
    /// authentication to be altered.
    #[snafu(display("insufficient authentication"))]
    NoAuth,
}

/// Errors that may cause a `check` request to fail.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Snafu)]
#[snafu(module)]
pub enum Check {
    /// No node exists with the given `path`.
    #[snafu(display("target node does not exist"))]
    NoNode,

    /// The target node has a different version than was specified by the call to `check`.
    #[snafu(display("target node has different version than expected ({expected})"))]
    BadVersion {
        /// The expected node version.
        expected: i32,
    },
}

/// The result of a failed `multi` request.
#[derive(Clone, Copy, PartialEq, Eq, Debug, Snafu)]
pub enum Multi {
    /// A failed `delete` request.
    #[snafu(display("delete failed"), context(false))]
    Delete {
        /// The source error.
        source: Delete,
    },

    /// A failed `set_data` request.
    #[snafu(display("set_data failed"), context(false))]
    SetData {
        /// The source error.
        source: SetData,
    },

    /// A failed `create` request.
    #[snafu(display("create failed"), context(false))]
    Create {
        /// The source error.
        source: Create,
    },

    /// A failed `check` request.
    #[snafu(display("check failed"), context(false))]
    Check {
        /// The source error.
        source: Check,
    },

    /// The request would have succeeded, but a later request in the `multi`
    /// batch failed and caused this request to get rolled back.
    #[snafu(display("request rolled back due to later failed request"))]
    RolledBack,

    /// The request was skipped because an earlier request in the `multi` batch
    /// failed. It is unknown whether this request would have succeeded.
    #[snafu(display("request failed due to earlier failed request"))]
    Skipped,
}
