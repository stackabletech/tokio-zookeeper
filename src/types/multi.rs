use super::Stat;

/// An individual response in a `multi` request.
#[derive(Debug, PartialEq)]
pub enum MultiResponse {
    /// The response to a `create` request within a `multi` batch.
    Create(String),
    /// The response to a `set_data` request within a `multi` batch.
    SetData(Stat),
    /// The response to a `delete` request within a `multi` batch.
    Delete,
    /// The response to a `check` request within a `multi` batch.
    Check,
}
