use byteorder::{BigEndian, WriteBytesExt};
use std::io::{self, Write};

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
    fn write_to(&self, writer: &mut Write) -> io::Result<()>;
}

impl WriteTo for u8 {
    fn write_to(&self, writer: &mut Write) -> io::Result<()> {
        try!(writer.write_u8(*self));
        Ok(())
    }
}

impl WriteTo for str {
    fn write_to(&self, writer: &mut Write) -> io::Result<()> {
        try!(writer.write_i32::<BigEndian>(self.len() as i32));
        writer.write_all(self.as_ref())
    }
}

impl<T: WriteTo> WriteTo for [T] {
    fn write_to(&self, writer: &mut Write) -> io::Result<()> {
        try!(writer.write_i32::<BigEndian>(self.len() as i32));
        let mut res = Ok(());
        for elem in self.iter() {
            res = elem.write_to(writer);
            if res.is_err() {
                return res;
            }
        }
        res
    }
}

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
                path.write_to(buffer)?;
                buffer.write_u8(watch)?;
            }
        }
        Ok(())
    }

    pub(super) fn opcode(&self) -> OpCode {
        match *self {
            Request::Connect { .. } => OpCode::CreateSession,
            Request::Exists { .. } => OpCode::Exists,
        }
    }
}
