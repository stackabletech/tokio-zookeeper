#[derive(Clone, Copy, PartialEq, Eq, Debug, Fail)]
pub enum Delete {
    #[fail(display = "target node does not exist")]
    NoNode,
    #[fail(
        display = "target node has different version than expected ({})",
        expected
    )]
    BadVersion { expected: i32 },
    #[fail(display = "target node has children, and cannot be deleted")]
    NotEmpty,
}

#[derive(Clone, Copy, PartialEq, Eq, Debug, Fail)]
pub enum Create {
    #[fail(display = "target node already exists")]
    NodeExists,
    #[fail(display = "parent node of target does not exist")]
    NoNode,
    #[fail(display = "parent node is ephemeral, and cannot have children")]
    NoChildrenForEphemerals,
    #[fail(display = "the given ACL is invalid")]
    InvalidAcl,
}
