use byteorder::{BigEndian, WriteBytesExt};
use std::borrow::Cow;
use std::io::{self, Write};
use {Acl, CreateMode};

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
        watch: u8,
    },
    Delete {
        path: String,
        version: i32,
    },
    Create {
        path: String,
        data: Cow<'static, [u8]>,
        acl: Cow<'static, [Acl]>,
        mode: CreateMode,
    },
}

#[derive(Debug, Hash, Eq, PartialEq, Ord, PartialOrd)]
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

impl WriteTo for u8 {
    fn write_to<W: Write>(&self, mut writer: W) -> io::Result<()> {
        try!(writer.write_u8(*self));
        Ok(())
    }
}

impl WriteTo for str {
    fn write_to<W: Write>(&self, mut writer: W) -> io::Result<()> {
        try!(writer.write_i32::<BigEndian>(self.len() as i32));
        writer.write_all(self.as_ref())
    }
}

impl WriteTo for [u8] {
    fn write_to<W: Write>(&self, mut writer: W) -> io::Result<()> {
        try!(writer.write_i32::<BigEndian>(self.len() as i32));
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
                // buffer.write_i32(OpCode::Auth);
                buffer.write_i32::<BigEndian>(protocol_version)?;
                buffer.write_i64::<BigEndian>(last_zxid_seen)?;
                buffer.write_i32::<BigEndian>(timeout)?;
                buffer.write_i64::<BigEndian>(session_id)?;
                buffer.write_i32::<BigEndian>(passwd.len() as i32)?;
                buffer.write_all(passwd)?;
                buffer.write_u8(read_only as u8)?;
            }
            Request::Exists { ref path, watch } => {
                buffer.write_i32::<BigEndian>(OpCode::Exists as i32)?;
                path.write_to(&mut *buffer)?;
                buffer.write_u8(watch)?;
            }
            Request::Delete { ref path, version } => {
                buffer.write_i32::<BigEndian>(OpCode::Delete as i32)?;
                path.write_to(&mut *buffer)?;
                buffer.write_i32::<BigEndian>(version)?;
            }
            Request::Create {
                ref path,
                ref data,
                mode,
                ref acl,
            } => {
                buffer.write_i32::<BigEndian>(OpCode::Create as i32)?;
                path.write_to(&mut *buffer)?;
                data.write_to(&mut *buffer)?;
                write_list(&mut *buffer, acl)?;
                buffer.write_i32::<BigEndian>(mode as i32)?;
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
        }
    }
}
