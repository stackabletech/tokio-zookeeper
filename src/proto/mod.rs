use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use failure;
use futures::{
    future::Either,
    sync::{mpsc, oneshot},
};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::{mem, time};
use tokio;
use tokio::prelude::*;
use WatchedEvent;

mod error;
mod request;
mod response;

pub(crate) use self::error::ZkError;
pub(crate) use self::request::Request;
pub(crate) use self::response::Response;

pub trait ZooKeeperTransport: AsyncRead + AsyncWrite + Sized + Send {
    type Addr: Send;
    type ConnectError: Into<failure::Error>;
    type ConnectFut: Future<Item = Self, Error = Self::ConnectError> + Send + 'static;
    fn connect(&Self::Addr) -> Self::ConnectFut;
}

impl ZooKeeperTransport for tokio::net::TcpStream {
    type Addr = SocketAddr;
    type ConnectError = tokio::io::Error;
    type ConnectFut = tokio::net::ConnectFuture;
    fn connect(addr: &Self::Addr) -> Self::ConnectFut {
        tokio::net::TcpStream::connect(addr)
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Enqueuer(
    mpsc::UnboundedSender<(Request, oneshot::Sender<Result<Response, ZkError>>)>,
);

impl Enqueuer {
    // TODO: maybe:
    // fn enqueue<Req, Res>(&self, req: Req) -> impl Future<Item = Res> where Res: Returns<Req>
    pub(crate) fn enqueue(
        &self,
        req: Request,
    ) -> impl Future<Item = Result<Response, ZkError>, Error = failure::Error> {
        let (tx, rx) = oneshot::channel();
        match self.0.unbounded_send((req, tx)) {
            Ok(()) => {
                Either::A(rx.map_err(|e| format_err!("failed to enqueue new request: {:?}", e)))
            }
            Err(e) => {
                Either::B(Err(format_err!("failed to enqueue new request: {:?}", e)).into_future())
            }
        }
    }
}

pub(crate) struct Packetizer<S>
where
    S: ZooKeeperTransport,
{
    /// ZooKeeper address
    addr: S::Addr,

    /// Current state
    state: PacketizerState<S>,

    /// Watcher to send watch events to.
    default_watcher: mpsc::UnboundedSender<WatchedEvent>,

    /// Incoming requests
    rx: mpsc::UnboundedReceiver<(Request, oneshot::Sender<Result<Response, ZkError>>)>,

    /// Next xid to issue
    xid: i32,

    exiting: bool,
}

impl<S> Packetizer<S>
where
    S: ZooKeeperTransport,
{
    /// TODO: document that it calls tokio::spawn
    pub(crate) fn new(
        addr: S::Addr,
        stream: S,
        default_watcher: mpsc::UnboundedSender<WatchedEvent>,
    ) -> Enqueuer
    where
        S: Send + 'static + AsyncRead + AsyncWrite,
    {
        // TODO: do connect directly here now that we can
        let (tx, rx) = mpsc::unbounded();

        tokio::spawn(
            Packetizer {
                addr,
                state: PacketizerState::Connected(ActivePacketizer::new(stream)),
                xid: 0,
                default_watcher,
                rx: rx,
                exiting: false,
            }.map_err(|e| {
                // TODO: expose this error to the user somehow
                eprintln!("packetizer exiting: {:?}", e);
                drop(e);
            }),
        );

        Enqueuer(tx)
    }
}

struct ActivePacketizer<S> {
    stream: S,

    /// Heartbeat timer,
    timer: tokio::timer::Delay,
    timeout: time::Duration,

    /// Bytes we have not yet set.
    outbox: Vec<u8>,

    /// Prefix of outbox that has been sent.
    outstart: usize,

    /// Bytes we have not yet deserialized.
    inbox: Vec<u8>,

    /// Prefix of inbox that has been sent.
    instart: usize,

    /// What operation are we waiting for a response for?
    reply: HashMap<i32, (request::OpCode, oneshot::Sender<Result<Response, ZkError>>)>,

    first: bool,

    /// Fields for re-connection
    last_zxid_seen: i64,
    session_id: i64,
    password: Vec<u8>,
}

impl<S> ActivePacketizer<S>
where
    S: AsyncRead + AsyncWrite,
{
    fn new(stream: S) -> Self {
        ActivePacketizer {
            stream,
            timer: tokio::timer::Delay::new(
                time::Instant::now() + time::Duration::from_secs(86_400),
            ),
            timeout: time::Duration::new(0, 0),
            outbox: Vec::new(),
            outstart: 0,
            inbox: Vec::new(),
            instart: 0,
            reply: Default::default(),
            first: true,

            last_zxid_seen: 0,
            session_id: 0,
            password: Vec::new(),
        }
    }

    fn outlen(&self) -> usize {
        self.outbox.len() - self.outstart
    }

    fn inlen(&self) -> usize {
        self.inbox.len() - self.instart
    }

    fn enqueue(&mut self, xid: i32, item: Request, tx: oneshot::Sender<Result<Response, ZkError>>) {
        let lengthi = self.outbox.len();
        // dummy length
        self.outbox.push(0);
        self.outbox.push(0);
        self.outbox.push(0);
        self.outbox.push(0);

        let old = self.reply.insert(xid, (item.opcode(), tx));
        assert!(old.is_none());

        if let Request::Connect { .. } = item {
        } else {
            // xid
            self.outbox
                .write_i32::<BigEndian>(xid)
                .expect("Vec::write should never fail");
        }

        // type and payload
        item.serialize_into(&mut self.outbox)
            .expect("Vec::write should never fail");
        // set true length
        let written = self.outbox.len() - lengthi - 4;
        let mut length = &mut self.outbox[lengthi..lengthi + 4];
        length
            .write_i32::<BigEndian>(written as i32)
            .expect("Vec::write should never fail");
    }

    fn poll_write(&mut self, exiting: bool) -> Result<Async<()>, failure::Error>
    where
        S: AsyncWrite,
    {
        let mut wrote = false;
        while self.outlen() != 0 {
            let n = try_ready!(self.stream.poll_write(&self.outbox[self.outstart..]));
            wrote = true;
            self.outstart += n;
            if self.outstart == self.outbox.len() {
                self.outbox.clear();
                self.outstart = 0;
            }
        }

        if wrote {
            self.timer.reset(time::Instant::now() + self.timeout);
        }

        self.stream.poll_flush().map_err(failure::Error::from)?;

        if exiting {
            eprintln!("shutting down writer");
            try_ready!(self.stream.shutdown());
        }

        Ok(Async::Ready(()))
    }

    fn poll_read(
        &mut self,
        default_watcher: &mut mpsc::UnboundedSender<WatchedEvent>,
    ) -> Result<Async<()>, failure::Error>
    where
        S: AsyncRead,
    {
        loop {
            let mut need = if self.inlen() >= 4 {
                let length = (&mut &self.inbox[self.instart..]).read_i32::<BigEndian>()? as usize;
                length + 4
            } else {
                4
            };

            while self.inlen() < need {
                eprintln!("READ MORE BYTES, have {}", self.inlen());
                let read_from = self.inbox.len();
                self.inbox.resize(read_from + need, 0);
                match self.stream.poll_read(&mut self.inbox[read_from..])? {
                    Async::Ready(n) => {
                        if n == 0 {
                            let left = &self.inbox[self.instart..];
                            if left == &[0, 0, 0, 0][..] {
                                // server normally sends 4*0x00 at the end
                                return Ok(Async::Ready(()));
                            } else {
                                eprintln!("{:x?}", &self.inbox[..]);
                                bail!(
                                    "connection closed with {} bytes left in buffer: {:x?}",
                                    self.inlen(),
                                    &self.inbox[self.instart..]
                                );
                            }
                        }

                        self.inbox.truncate(read_from + n);
                        if self.inlen() >= 4 && need == 4 {
                            let length = (&mut &self.inbox[self.instart..]).read_i32::<BigEndian>()?
                                as usize;
                            need += length;
                        }
                    }
                    Async::NotReady => {
                        self.inbox.truncate(read_from);
                        return Ok(Async::NotReady);
                    }
                }
            }

            eprintln!("length is {}", need - 4);
            {
                let mut err = None;
                let mut buf = &self.inbox[self.instart + 4..self.instart + need];
                self.instart += need;

                let xid = if self.first {
                    0
                } else {
                    let xid = buf.read_i32::<BigEndian>()?;
                    let zxid = buf.read_i64::<BigEndian>()?;
                    if zxid > 0 {
                        eprintln!("{} {}", zxid, self.last_zxid_seen);
                        assert!(zxid >= self.last_zxid_seen);
                        self.last_zxid_seen = zxid;
                    }
                    let errcode = buf.read_i32::<BigEndian>()?;
                    if errcode != 0 {
                        err = Some(error::ZkError::from(errcode));
                    }
                    xid
                };

                if xid == 0 && !self.first {
                    // response to shutdown -- empty response
                    // XXX: in theory, server should now shut down receive end
                    eprintln!("got response to CloseSession");
                    if let Some(e) = err {
                        bail!("failed to close session: {:?}", e);
                    }
                } else if xid == -1 {
                    // watch event
                    use self::response::ReadFrom;
                    let e = WatchedEvent::read_from(&mut buf)?;
                    // TODO: maybe send to non-default watcher
                    // NOTE: ignoring error, because the user may not care about events
                    eprintln!("got watcher event {:?}", e);
                    let _ = default_watcher.unbounded_send(e);
                } else if xid == -2 {
                    // response to ping -- empty response
                    eprintln!("got response to heartbeat");
                    if let Some(e) = err {
                        bail!("bad response to ping: {:?}", e);
                    }
                } else {
                    // response to user request
                    self.first = false;
                    eprintln!("{:?}", buf);

                    // find the waiting request future
                    let (opcode, tx) = self.reply.remove(&xid).unwrap(); // TODO: return an error if xid was unknown
                    eprintln!("handling response to xid {} with opcode {:?}", xid, opcode);

                    if let Some(e) = err {
                        tx.send(Err(e)).is_ok();
                    } else {
                        let mut r = Response::parse(opcode, buf)?;
                        if let Response::Connect {
                            timeout,
                            session_id,
                            ref mut password,
                            ..
                        } = r
                        {
                            assert!(timeout >= 0);
                            eprintln!("timeout is {}ms", timeout);
                            self.timeout = time::Duration::from_millis(2 * timeout as u64 / 3);
                            self.timer.reset(time::Instant::now() + self.timeout);

                            // keep track of these for consistent re-connect
                            self.session_id = session_id;
                            mem::swap(&mut self.password, password);
                        }
                        tx.send(Ok(r)).is_ok(); // if receiver doesn't care, we don't either
                    }
                }
            }

            if self.instart == self.inbox.len() {
                self.inbox.clear();
                self.instart = 0;
            }
        }
    }

    fn poll(
        &mut self,
        exiting: bool,
        default_watcher: &mut mpsc::UnboundedSender<WatchedEvent>,
    ) -> Result<Async<()>, failure::Error> {
        eprintln!("poll_read");
        let r = self.poll_read(default_watcher)?;

        if let Async::Ready(()) = self.timer.poll()? {
            if self.outbox.is_empty() {
                // send a ping!
                // length is known for pings
                self.outbox
                    .write_i32::<BigEndian>(8)
                    .expect("Vec::write should never fail");
                // xid
                self.outbox
                    .write_i32::<BigEndian>(-2)
                    .expect("Vec::write should never fail");
                // opcode
                self.outbox
                    .write_i32::<BigEndian>(request::OpCode::Ping as i32)
                    .expect("Vec::write should never fail");
            } else {
                // already request in flight, so no need to also send heartbeat
            }

            self.timer.reset(time::Instant::now() + self.timeout);
        }

        eprintln!("poll_write");
        let w = self.poll_write(exiting)?;

        match (r, w) {
            (Async::Ready(()), Async::Ready(())) if exiting => {
                eprintln!("packetizer done");
                Ok(Async::Ready(()))
            }
            (Async::Ready(()), Async::Ready(())) => Ok(Async::NotReady),
            (Async::Ready(()), _) => bail!("outstanding requests, but response channel closed"),
            _ => Ok(Async::NotReady),
        }
    }
}

enum PacketizerState<S> {
    Connected(ActivePacketizer<S>),
    Reconnecting(Box<Future<Item = ActivePacketizer<S>, Error = failure::Error> + Send + 'static>),
}

impl<S> PacketizerState<S>
where
    S: AsyncRead + AsyncWrite,
{
    fn poll(
        &mut self,
        exiting: bool,
        default_watcher: &mut mpsc::UnboundedSender<WatchedEvent>,
    ) -> Result<Async<()>, failure::Error> {
        let ap = match *self {
            PacketizerState::Connected(ref mut ap) => return ap.poll(exiting, default_watcher),
            PacketizerState::Reconnecting(ref mut c) => try_ready!(c.poll()),
        };

        // we are now connected!
        mem::replace(self, PacketizerState::Connected(ap));
        self.poll(exiting, default_watcher)
    }
}

impl<S> Packetizer<S>
where
    S: ZooKeeperTransport,
{
    fn poll_enqueue(&mut self) -> Result<Async<()>, ()> {
        while let PacketizerState::Connected(ref mut ap) = self.state {
            let (item, tx) = match try_ready!(self.rx.poll()) {
                Some((item, tx)) => (item, tx),
                None => return Err(()),
            };
            eprintln!("got request {:?}", item);

            ap.enqueue(self.xid, item, tx);
            self.xid += 1;
        }
        Ok(Async::NotReady)
    }
}

impl<S> Future for Packetizer<S>
where
    S: ZooKeeperTransport,
{
    type Item = ();
    type Error = failure::Error;

    fn poll(&mut self) -> Result<Async<Self::Item>, Self::Error> {
        eprintln!("packetizer polled");
        if !self.exiting {
            eprintln!("poll_enqueue");
            match self.poll_enqueue() {
                Ok(_) => {}
                Err(()) => {
                    // no more requests will be enqueued
                    self.exiting = true;

                    if let PacketizerState::Connected(ref mut ap) = self.state {
                        // send CloseSession
                        // length is fixed
                        ap.outbox
                            .write_i32::<BigEndian>(8)
                            .expect("Vec::write should never fail");
                        // xid
                        ap.outbox
                            .write_i32::<BigEndian>(0)
                            .expect("Vec::write should never fail");
                        // opcode
                        ap.outbox
                            .write_i32::<BigEndian>(request::OpCode::CloseSession as i32)
                            .expect("Vec::write should never fail");
                    } else {
                        unreachable!("poll_enqueue will never return Err() if not connected");
                    }
                }
            }
        }

        match self.state.poll(self.exiting, &mut self.default_watcher) {
            Ok(v) => Ok(v),
            Err(e) => {
                // if e is disconnect, then purge state and reconnect
                // for now, assume all errors are disconnects
                // TODO: test this!

                let password = if let PacketizerState::Connected(ActivePacketizer {
                    ref mut password,
                    ..
                }) = self.state
                {
                    password.split_off(0)
                } else {
                    // XXX: error while connecting -- don't recurse (for now)
                    return Err(e);
                };

                if let PacketizerState::Connected(ActivePacketizer {
                    last_zxid_seen,
                    session_id,
                    ..
                }) = self.state
                {
                    let xid = self.xid;
                    self.xid += 1;

                    let retry = S::connect(&self.addr)
                        .map_err(|e| e.into())
                        .map(move |stream| {
                            let request = Request::Connect {
                                protocol_version: 0,
                                last_zxid_seen,
                                timeout: 0,
                                session_id,
                                passwd: password,
                                read_only: false,
                            };
                            eprintln!("about to handshake (again)");

                            let (tx, rx) = oneshot::channel();
                            tokio::spawn(rx.then(|r| {
                                eprintln!("re-connection response: {:?}", r);
                                Ok(())
                            }));

                            let mut ap = ActivePacketizer::new(stream);
                            ap.enqueue(xid, request, tx);
                            ap
                        });

                    // dropping the old state will also cancel in-flight requests
                    mem::replace(
                        &mut self.state,
                        PacketizerState::Reconnecting(Box::new(retry)),
                    );
                    self.poll()
                } else {
                    unreachable!();
                }
            }
        }
    }
}
