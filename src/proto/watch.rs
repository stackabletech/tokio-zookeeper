use crate::WatchedEvent;
use futures::channel::oneshot;

#[derive(Debug)]
pub(crate) enum Watch {
    None,
    Global,
    Custom(oneshot::Sender<WatchedEvent>),
}

impl Watch {
    pub(crate) fn to_u8(&self) -> u8 {
        if let Watch::None = *self { 0 } else { 1 }
    }
}

/// Describes what a `Watch` is looking for.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash)]
pub(crate) enum WatchType {
    /// Watching for changes to children.
    Child,
    /// Watching for changes to data.
    Data,
    /// Watching for the creation of a node at the given path.
    Exist,
}
