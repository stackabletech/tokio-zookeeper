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
