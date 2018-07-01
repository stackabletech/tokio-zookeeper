use byteorder::{BigEndian, ReadBytesExt};
use failure;
use std::io::Read;

#[derive(Debug)]
pub(crate) enum Response {
    Connect {
        protocol_version: i32,
        timeout: i32,
        session_id: i64,
        passwd: Vec<u8>,
        read_only: bool,
    },
}

pub trait BufferReader: Read {
    fn read_buffer(&mut self) -> Result<Vec<u8>, failure::Error>;
}

impl<R: Read> BufferReader for R {
    fn read_buffer(&mut self) -> Result<Vec<u8>, failure::Error> {
        let len = try!(self.read_i32::<BigEndian>());
        let len = if len < 0 { 0 } else { len as usize };
        let mut buf = vec![0; len];
        let read = try!(self.read(&mut buf));
        if read == len {
            Ok(buf)
        } else {
            bail!("read_buffer failed")
        }
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
                passwd: reader.read_buffer()?,
                read_only: reader.read_u8()? != 0,
            }),
            _ => unimplemented!(),
        }
    }
}
