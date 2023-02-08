use failure::{bail, format_err};

use crate::proto::{Request, Response, ZkError};
use crate::{error, Acl, MultiResponse, Stat};

pub(crate) fn create(
    res: Result<Response, ZkError>,
) -> Result<Result<String, error::Create>, failure::Error> {
    match res {
        Ok(Response::String(s)) => Ok(Ok(s)),
        Ok(r) => bail!("got non-string response to create: {:?}", r),
        Err(ZkError::NoNode) => Ok(Err(error::Create::NoNode)),
        Err(ZkError::NodeExists) => Ok(Err(error::Create::NodeExists)),
        Err(ZkError::InvalidACL) => Ok(Err(error::Create::InvalidAcl)),
        Err(ZkError::NoChildrenForEphemerals) => Ok(Err(error::Create::NoChildrenForEphemerals)),
        Err(e) => Err(format_err!("create call failed: {:?}", e)),
    }
}

pub(crate) fn set_data(
    version: i32,
    res: Result<Response, ZkError>,
) -> Result<Result<Stat, error::SetData>, failure::Error> {
    match res {
        Ok(Response::Stat(stat)) => Ok(Ok(stat)),
        Ok(r) => bail!("got a non-stat response to a set_data request: {:?}", r),
        Err(ZkError::NoNode) => Ok(Err(error::SetData::NoNode)),
        Err(ZkError::BadVersion) => Ok(Err(error::SetData::BadVersion { expected: version })),
        Err(ZkError::NoAuth) => Ok(Err(error::SetData::NoAuth)),
        Err(e) => bail!("set_data call failed: {:?}", e),
    }
}

pub(crate) fn delete(
    version: i32,
    res: Result<Response, ZkError>,
) -> Result<Result<(), error::Delete>, failure::Error> {
    match res {
        Ok(Response::Empty) => Ok(Ok(())),
        Ok(r) => bail!("got non-empty response to delete: {:?}", r),
        Err(ZkError::NoNode) => Ok(Err(error::Delete::NoNode)),
        Err(ZkError::NotEmpty) => Ok(Err(error::Delete::NotEmpty)),
        Err(ZkError::BadVersion) => Ok(Err(error::Delete::BadVersion { expected: version })),
        Err(e) => Err(format_err!("delete call failed: {:?}", e)),
    }
}

pub(crate) fn get_acl(
    res: Result<Response, ZkError>,
) -> Result<Result<(Vec<Acl>, Stat), error::GetAcl>, failure::Error> {
    match res {
        Ok(Response::GetAcl { acl, stat }) => Ok(Ok((acl, stat))),
        Ok(r) => bail!("got non-acl response to a get_acl request: {:?}", r),
        Err(ZkError::NoNode) => Ok(Err(error::GetAcl::NoNode)),
        Err(e) => Err(format_err!("get_acl call failed: {:?}", e)),
    }
}

pub(crate) fn set_acl(
    version: i32,
    res: Result<Response, ZkError>,
) -> Result<Result<Stat, error::SetAcl>, failure::Error> {
    match res {
        Ok(Response::Stat(stat)) => Ok(Ok(stat)),
        Ok(r) => bail!("got non-stat response to a set_acl request: {:?}", r),
        Err(ZkError::NoNode) => Ok(Err(error::SetAcl::NoNode)),
        Err(ZkError::BadVersion) => Ok(Err(error::SetAcl::BadVersion { expected: version })),
        Err(ZkError::InvalidACL) => Ok(Err(error::SetAcl::InvalidAcl)),
        Err(ZkError::NoAuth) => Ok(Err(error::SetAcl::NoAuth)),
        Err(e) => Err(format_err!("set_acl call failed: {:?}", e)),
    }
}

pub(crate) fn exists(res: Result<Response, ZkError>) -> Result<Option<Stat>, failure::Error> {
    match res {
        Ok(Response::Stat(stat)) => Ok(Some(stat)),
        Ok(r) => bail!("got a non-create response to a create request: {:?}", r),
        Err(ZkError::NoNode) => Ok(None),
        Err(e) => bail!("exists call failed: {:?}", e),
    }
}

pub(crate) fn get_children(
    res: Result<Response, ZkError>,
) -> Result<Option<Vec<String>>, failure::Error> {
    match res {
        Ok(Response::Strings(children)) => Ok(Some(children)),
        Ok(r) => bail!("got non-strings response to get-children: {:?}", r),
        Err(ZkError::NoNode) => Ok(None),
        Err(e) => Err(format_err!("get-children call failed: {:?}", e)),
    }
}

pub(crate) fn get_data(
    res: Result<Response, ZkError>,
) -> Result<Option<(Vec<u8>, Stat)>, failure::Error> {
    match res {
        Ok(Response::GetData { bytes, stat }) => Ok(Some((bytes, stat))),
        Ok(r) => bail!("got non-data response to get-data: {:?}", r),
        Err(ZkError::NoNode) => Ok(None),
        Err(e) => Err(format_err!("get-data call failed: {:?}", e)),
    }
}

pub(crate) fn check(
    version: i32,
    res: Result<Response, ZkError>,
) -> Result<Result<(), error::Check>, failure::Error> {
    match res {
        Ok(Response::Empty) => Ok(Ok(())),
        Ok(r) => bail!("got a non-check response to a check request: {:?}", r),
        Err(ZkError::NoNode) => Ok(Err(error::Check::NoNode)),
        Err(ZkError::BadVersion) => Ok(Err(error::Check::BadVersion { expected: version })),
        Err(e) => bail!("check call failed: {:?}", e),
    }
}

/// The subset of [`proto::Request`] that a multi request needs to retain.
///
/// In order to properly handle errors, a multi request needs to retain the
/// expected version for each constituent set data, delete, or check operation.
/// Unfortunately, executing a multi request requires transferring ownership of
/// the `proto::Request`, which contains this information, to the future. A
/// `RequestMarker` is used to avoid cloning the whole `proto::Request`, which
/// can be rather large, when only the version information is necessary.
#[derive(Debug)]
pub(crate) enum RequestMarker {
    Create,
    SetData { version: i32 },
    Delete { version: i32 },
    Check { version: i32 },
}

impl From<&Request> for RequestMarker {
    fn from(r: &Request) -> RequestMarker {
        match r {
            Request::Create { .. } => RequestMarker::Create,
            Request::SetData { version, .. } => RequestMarker::SetData { version: *version },
            Request::Delete { version, .. } => RequestMarker::Delete { version: *version },
            Request::Check { version, .. } => RequestMarker::Check { version: *version },
            _ => unimplemented!(),
        }
    }
}

pub(crate) fn multi(
    req: &RequestMarker,
    res: Result<Response, ZkError>,
) -> Result<Result<MultiResponse, error::Multi>, failure::Error> {
    // Handle multi-specific errors.
    match res {
        Err(ZkError::Ok) => return Ok(Err(error::Multi::RolledBack)),
        // Confusingly, the ZooKeeper server uses RuntimeInconsistency to
        // indicate that a request in a multi batch was skipped because an
        // earlier request in the batch failed.
        // Source: https://github.com/apache/zookeeper/blob/372e713a9/zookeeper-server/src/main/java/org/apache/zookeeper/server/DataTree.java#L945-L946
        Err(ZkError::RuntimeInconsistency) => return Ok(Err(error::Multi::Skipped)),
        _ => (),
    };

    Ok(match req {
        RequestMarker::Create => create(res)?
            .map(MultiResponse::Create)
            .map_err(|err| err.into()),
        RequestMarker::SetData { version } => set_data(*version, res)?
            .map(MultiResponse::SetData)
            .map_err(|err| err.into()),
        RequestMarker::Delete { version } => delete(*version, res)?
            .map(|_| MultiResponse::Delete)
            .map_err(|err| err.into()),
        RequestMarker::Check { version } => check(*version, res)?
            .map(|_| MultiResponse::Check)
            .map_err(|err| err.into()),
    })
}
