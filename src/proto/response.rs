use byteorder::{BigEndian, ReadBytesExt};
use failure;
use std::io::{self, Read};
use {Acl, KeeperState, Permission, Stat, WatchedEvent, WatchedEventType};

#[derive(Debug)]
pub(crate) enum Response {
    Connect {
        protocol_version: i32,
        timeout: i32,
        session_id: i64,
        password: Vec<u8>,
        read_only: bool,
    },
    Stat(Stat),
    GetData {
        bytes: Vec<u8>,
        stat: Stat,
    },
    GetAcl {
        acl: Vec<Acl>,
        stat: Stat,
    },
    Empty,
    Strings(Vec<String>),
    String(String),
}

pub trait ReadFrom: Sized {
    fn read_from<R: Read>(read: &mut R) -> io::Result<Self>;
}

impl ReadFrom for Vec<String> {
    fn read_from<R: Read>(read: &mut R) -> io::Result<Self> {
        let len = try!(read.read_i32::<BigEndian>());
        let mut items = Vec::with_capacity(len as usize);
        for _ in 0..len {
            items.push(try!(read.read_string()));
        }
        Ok(items)
    }
}

impl ReadFrom for Stat {
    fn read_from<R: Read>(read: &mut R) -> io::Result<Stat> {
        Ok(Stat {
            czxid: try!(read.read_i64::<BigEndian>()),
            mzxid: try!(read.read_i64::<BigEndian>()),
            ctime: try!(read.read_i64::<BigEndian>()),
            mtime: try!(read.read_i64::<BigEndian>()),
            version: try!(read.read_i32::<BigEndian>()),
            cversion: try!(read.read_i32::<BigEndian>()),
            aversion: try!(read.read_i32::<BigEndian>()),
            ephemeral_owner: try!(read.read_i64::<BigEndian>()),
            data_length: try!(read.read_i32::<BigEndian>()),
            num_children: try!(read.read_i32::<BigEndian>()),
            pzxid: try!(read.read_i64::<BigEndian>()),
        })
    }
}

impl ReadFrom for WatchedEvent {
    fn read_from<R: Read>(read: &mut R) -> io::Result<WatchedEvent> {
        let wtype = read.read_i32::<BigEndian>()?;
        let state = read.read_i32::<BigEndian>()?;
        let path = read.read_string()?;
        Ok(WatchedEvent {
            event_type: WatchedEventType::from(wtype),
            keeper_state: KeeperState::from(state),
            path,
        })
    }
}

impl ReadFrom for Vec<Acl> {
    fn read_from<R: Read>(read: &mut R) -> io::Result<Self> {
        let len = try!(read.read_i32::<BigEndian>());
        let mut items = Vec::with_capacity(len as usize);
        for _ in 0..len {
            items.push(try!(Acl::read_from(read)));
        }
        Ok(items)
    }
}

impl ReadFrom for Acl {
    fn read_from<R: Read>(read: &mut R) -> io::Result<Self> {
        let perms = try!(Permission::read_from(read));
        let scheme = try!(read.read_string());
        let id = try!(read.read_string());
        Ok(Acl { perms, scheme, id })
    }
}

impl ReadFrom for Permission {
    fn read_from<R: Read>(read: &mut R) -> io::Result<Self> {
        Ok(Permission::from_raw(try!(read.read_u32::<BigEndian>())))
    }
}

pub trait BufferReader: Read {
    fn read_buffer(&mut self) -> io::Result<Vec<u8>>;
}

impl<R: Read> BufferReader for R {
    fn read_buffer(&mut self) -> io::Result<Vec<u8>> {
        let len = try!(self.read_i32::<BigEndian>());
        let len = if len < 0 { 0 } else { len as usize };
        let mut buf = vec![0; len];
        let read = try!(self.read(&mut buf));
        if read == len {
            Ok(buf)
        } else {
            Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "read_buffer failed",
            ))
        }
    }
}

trait StringReader: Read {
    fn read_string(&mut self) -> io::Result<String>;
}

impl<R: Read> StringReader for R {
    fn read_string(&mut self) -> io::Result<String> {
        let raw = try!(self.read_buffer());
        Ok(String::from_utf8(raw).unwrap())
    }
}

use super::request::OpCode;
impl Response {
    pub(super) fn parse(opcode: OpCode, buf: &[u8]) -> Result<Self, failure::Error> {
        let mut reader = buf;
        match opcode {
            OpCode::CreateSession => Ok(Response::Connect {
                protocol_version: reader.read_i32::<BigEndian>()?,
                timeout: reader.read_i32::<BigEndian>()?,
                session_id: reader.read_i64::<BigEndian>()?,
                password: reader.read_buffer()?,
                read_only: reader.read_u8()? != 0,
            }),
            OpCode::Exists | OpCode::SetData => Ok(Response::Stat(Stat::read_from(&mut reader)?)),
            OpCode::GetData => Ok(Response::GetData {
                bytes: reader.read_buffer()?,
                stat: Stat::read_from(&mut reader)?,
            }),
            OpCode::Delete => Ok(Response::Empty),
            OpCode::GetChildren => Ok(Response::Strings(Vec::<String>::read_from(&mut reader)?)),
            OpCode::Create => Ok(Response::String(reader.read_string()?)),
            OpCode::GetAcl => Ok(Response::GetAcl {
                acl: Vec::<Acl>::read_from(&mut reader)?,
                stat: Stat::read_from(&mut reader)?,
            }),
            _ => unimplemented!(),
        }
    }
}
