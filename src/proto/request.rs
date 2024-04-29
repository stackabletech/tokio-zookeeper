use super::Watch;
use super::ZkError;
use crate::{Acl, CreateMode};
use byteorder::{BigEndian, WriteBytesExt};
use std::borrow::Cow;
use std::io::{self, Write};

#[derive(Debug)]
pub(crate) enum Request {
    Connect {
        protocol_version: i32,
        last_zxid_seen: i64,
        timeout: i32,
        session_id: i64,
        passwd: Vec<u8>,
        read_only: bool,
    },
    Exists {
        path: String,
        watch: Watch,
    },
    Delete {
        path: String,
        version: i32,
    },
    SetData {
        path: String,
        data: Cow<'static, [u8]>,
        version: i32,
    },
    Create {
        path: String,
        data: Cow<'static, [u8]>,
        acl: Cow<'static, [Acl]>,
        mode: CreateMode,
    },
    GetChildren {
        path: String,
        watch: Watch,
    },
    GetData {
        path: String,
        watch: Watch,
    },
    GetAcl {
        path: String,
    },
    SetAcl {
        path: String,
        acl: Cow<'static, [Acl]>,
        version: i32,
    },
    Check {
        path: String,
        version: i32,
    },
    Multi(Vec<Request>),
}

#[derive(Clone, Copy, Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
#[repr(i32)]
#[allow(dead_code)]
pub(super) enum OpCode {
    Notification = 0,
    Create = 1,
    Delete = 2,
    Exists = 3,
    GetData = 4,
    SetData = 5,
    GetACL = 6,
    SetACL = 7,
    GetChildren = 8,
    Synchronize = 9,
    Ping = 11,
    GetChildren2 = 12,
    Check = 13,
    Multi = 14,
    Auth = 100,
    SetWatches = 101,
    Sasl = 102,
    CreateSession = -10,
    CloseSession = -11,
    Error = -1,
}

impl From<i32> for OpCode {
    fn from(code: i32) -> Self {
        match code {
            0 => OpCode::Notification,
            1 => OpCode::Create,
            2 => OpCode::Delete,
            3 => OpCode::Exists,
            4 => OpCode::GetData,
            5 => OpCode::SetData,
            6 => OpCode::GetACL,
            7 => OpCode::SetACL,
            8 => OpCode::GetChildren,
            9 => OpCode::Synchronize,
            11 => OpCode::Ping,
            12 => OpCode::GetChildren2,
            13 => OpCode::Check,
            14 => OpCode::Multi,
            100 => OpCode::Auth,
            101 => OpCode::SetWatches,
            102 => OpCode::Sasl,
            -10 => OpCode::CreateSession,
            -11 => OpCode::CloseSession,
            -1 => OpCode::Error,
            _ => unimplemented!(),
        }
    }
}

pub(super) enum MultiHeader {
    NextOk(OpCode),
    NextErr(ZkError),
    Done,
}

pub trait WriteTo {
    fn write_to<W: Write>(&self, writer: W) -> io::Result<()>;
}

impl WriteTo for Acl {
    fn write_to<W: Write>(&self, mut writer: W) -> io::Result<()> {
        writer.write_u32::<BigEndian>(self.perms.code())?;
        self.scheme.write_to(&mut writer)?;
        self.id.write_to(writer)
    }
}

impl WriteTo for MultiHeader {
    fn write_to<W: Write>(&self, mut writer: W) -> io::Result<()> {
        match *self {
            MultiHeader::NextOk(opcode) => {
                writer.write_i32::<BigEndian>(opcode as i32)?;
                writer.write_u8(false as u8)?;
                writer.write_i32::<BigEndian>(-1)
            }
            MultiHeader::NextErr(_) => {
                panic!("client should not serialize MultiHeader::NextErr");
            }
            MultiHeader::Done => {
                writer.write_i32::<BigEndian>(-1)?;
                writer.write_u8(true as u8)?;
                writer.write_i32::<BigEndian>(-1)
            }
        }
    }
}

impl WriteTo for u8 {
    fn write_to<W: Write>(&self, mut writer: W) -> io::Result<()> {
        writer.write_u8(*self)?;
        Ok(())
    }
}

impl WriteTo for str {
    fn write_to<W: Write>(&self, mut writer: W) -> io::Result<()> {
        writer.write_i32::<BigEndian>(self.len() as i32)?;
        writer.write_all(self.as_ref())
    }
}

impl WriteTo for [u8] {
    fn write_to<W: Write>(&self, mut writer: W) -> io::Result<()> {
        writer.write_i32::<BigEndian>(self.len() as i32)?;
        writer.write_all(self.as_ref())
    }
}

fn write_list<W, T>(mut writer: W, ts: &[T]) -> io::Result<()>
where
    T: WriteTo,
    W: Write,
{
    writer.write_i32::<BigEndian>(ts.len() as i32)?;
    for elem in ts {
        elem.write_to(&mut writer)?;
    }
    Ok(())
}

impl Request {
    pub(super) fn serialize_into(&self, buffer: &mut Vec<u8>) -> Result<(), io::Error> {
        match *self {
            Request::Connect {
                protocol_version,
                last_zxid_seen,
                timeout,
                session_id,
                ref passwd,
                read_only,
            } => {
                buffer.write_i32::<BigEndian>(protocol_version)?;
                buffer.write_i64::<BigEndian>(last_zxid_seen)?;
                buffer.write_i32::<BigEndian>(timeout)?;
                buffer.write_i64::<BigEndian>(session_id)?;
                buffer.write_i32::<BigEndian>(passwd.len() as i32)?;
                buffer.write_all(passwd)?;
                buffer.write_u8(read_only as u8)?;
            }
            Request::GetData {
                ref path,
                ref watch,
            }
            | Request::GetChildren {
                ref path,
                ref watch,
            }
            | Request::Exists {
                ref path,
                ref watch,
            } => {
                path.write_to(&mut *buffer)?;
                buffer.write_u8(watch.to_u8())?;
            }
            Request::Delete { ref path, version } => {
                path.write_to(&mut *buffer)?;
                buffer.write_i32::<BigEndian>(version)?;
            }
            Request::SetData {
                ref path,
                ref data,
                version,
            } => {
                path.write_to(&mut *buffer)?;
                data.write_to(&mut *buffer)?;
                buffer.write_i32::<BigEndian>(version)?;
            }
            Request::Create {
                ref path,
                ref data,
                mode,
                ref acl,
            } => {
                path.write_to(&mut *buffer)?;
                data.write_to(&mut *buffer)?;
                write_list(&mut *buffer, acl)?;
                buffer.write_i32::<BigEndian>(mode as i32)?;
            }
            Request::GetAcl { ref path } => {
                path.write_to(&mut *buffer)?;
            }
            Request::SetAcl {
                ref path,
                ref acl,
                version,
            } => {
                path.write_to(&mut *buffer)?;
                write_list(&mut *buffer, acl)?;
                buffer.write_i32::<BigEndian>(version)?;
            }
            Request::Check { ref path, version } => {
                path.write_to(&mut *buffer)?;
                buffer.write_i32::<BigEndian>(version)?;
            }
            Request::Multi(ref requests) => {
                for r in requests {
                    MultiHeader::NextOk(r.opcode()).write_to(&mut *buffer)?;
                    r.serialize_into(&mut *buffer)?;
                }
                MultiHeader::Done.write_to(&mut *buffer)?;
            }
        }
        Ok(())
    }

    pub(super) fn opcode(&self) -> OpCode {
        match *self {
            Request::Connect { .. } => OpCode::CreateSession,
            Request::Exists { .. } => OpCode::Exists,
            Request::Delete { .. } => OpCode::Delete,
            Request::Create { .. } => OpCode::Create,
            Request::GetChildren { .. } => OpCode::GetChildren,
            Request::SetData { .. } => OpCode::SetData,
            Request::GetData { .. } => OpCode::GetData,
            Request::GetAcl { .. } => OpCode::GetACL,
            Request::SetAcl { .. } => OpCode::SetACL,
            Request::Multi { .. } => OpCode::Multi,
            Request::Check { .. } => OpCode::Check,
        }
    }
}
