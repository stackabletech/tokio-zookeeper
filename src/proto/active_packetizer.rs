use super::{request, watch::WatchType, Request, Response};
use crate::{WatchedEvent, WatchedEventType, ZkError};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use futures::{
    channel::{mpsc, oneshot},
    ready,
};
use pin_project::pin_project;
use snafu::{Snafu, Whatever};
use std::collections::HashMap;
use std::{
    future::Future,
    mem,
    pin::Pin,
    task::{Context, Poll},
    time,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tracing::{debug, info, instrument, trace};

#[derive(Debug, Snafu)]
pub enum Error {
    #[snafu(transparent)]
    Io { source: std::io::Error },
    #[snafu(transparent)]
    Whatever { source: Whatever },

    #[snafu(display("connection closed with {len} bytes left in buffer: {buf:x?}", len = buf.len()))]
    ConnectionClosed { buf: Vec<u8> },

    #[snafu(display("Not exiting, but server closed connection"))]
    ServerClosedConnection,
    #[snafu(display("outstanding requests, but response channel closed"))]
    ResponseChannelClosedPrematurely,

    #[snafu(display("bad response to ping: {error_code:?}"))]
    BadPing { error_code: ZkError },
    #[snafu(display("failed to close session: {error_code:?}"))]
    CloseSession { error_code: ZkError },
}

#[pin_project]
pub(super) struct ActivePacketizer<S> {
    #[pin]
    stream: S,

    /// Heartbeat timer,
    #[pin]
    timer: tokio::time::Sleep,
    timeout: time::Duration,

    /// Bytes we have not yet set.
    pub(super) outbox: Vec<u8>,

    /// Prefix of outbox that has been sent.
    outstart: usize,

    /// Bytes we have not yet deserialized.
    inbox: Vec<u8>,

    /// Prefix of inbox that has been sent.
    instart: usize,

    /// What operation are we waiting for a response for?
    reply: HashMap<i32, (request::OpCode, oneshot::Sender<Result<Response, ZkError>>)>,

    /// Custom registered watchers (path -> watcher)
    watchers: HashMap<String, Vec<(oneshot::Sender<WatchedEvent>, WatchType)>>,

    /// Custom registered watchers (xid -> watcher to add when ok)
    pub(super) pending_watchers: HashMap<i32, (String, oneshot::Sender<WatchedEvent>, WatchType)>,

    first: bool,

    /// Fields for re-connection
    pub(super) last_zxid_seen: i64,
    pub(super) session_id: i64,
    pub(super) password: Vec<u8>,
}

type ReplySender = oneshot::Sender<Result<Response, ZkError>>;

impl<S> ActivePacketizer<S>
where
    S: AsyncRead + AsyncWrite,
{
    pub(super) fn new(stream: S) -> Self {
        ActivePacketizer {
            stream,
            timer: tokio::time::sleep(time::Duration::from_secs(86_400)),
            timeout: time::Duration::new(86_400, 0),
            outbox: Vec::new(),
            outstart: 0,
            inbox: Vec::new(),
            instart: 0,
            reply: Default::default(),
            watchers: Default::default(),
            pending_watchers: Default::default(),
            first: true,

            last_zxid_seen: 0,
            session_id: 0,
            password: Vec::new(),
        }
    }

    pub(super) fn outbox(self: Pin<&mut Self>) -> &mut Vec<u8> {
        self.project().outbox
    }
    pub(super) fn take_password(self: Pin<&mut Self>) -> Vec<u8> {
        self.project().password.split_off(0)
    }
    pub(super) fn pending_watchers(
        self: Pin<&mut Self>,
    ) -> &mut HashMap<i32, (String, oneshot::Sender<WatchedEvent>, WatchType)> {
        self.project().pending_watchers
    }

    fn outlen(&self) -> usize {
        self.outbox.len() - self.outstart
    }

    fn inlen(&self) -> usize {
        self.inbox.len() - self.instart
    }

    #[allow(clippy::type_complexity)]
    fn enqueue_impl(
        outbox: &mut Vec<u8>,
        reply: &mut HashMap<i32, (request::OpCode, ReplySender)>,
        xid: i32,
        item: Request,
        tx: oneshot::Sender<Result<Response, ZkError>>,
    ) {
        let lengthi = outbox.len();
        // dummy length
        outbox.push(0);
        outbox.push(0);
        outbox.push(0);
        outbox.push(0);

        let old = reply.insert(xid, (item.opcode(), tx));
        assert!(old.is_none());

        if let Request::Connect { .. } = item {
        } else {
            // xid
            outbox
                .write_i32::<BigEndian>(xid)
                .expect("Vec::write should never fail");
            // opcode
            outbox
                .write_i32::<BigEndian>(item.opcode() as i32)
                .expect("Vec::write should never fail");
        }

        // type and payload
        item.serialize_into(outbox)
            .expect("Vec::write should never fail");
        // set true length
        let written = outbox.len() - lengthi - 4;
        let mut length = &mut outbox[lengthi..lengthi + 4];
        length
            .write_i32::<BigEndian>(written as i32)
            .expect("Vec::write should never fail");
    }
    pub(super) fn enqueue(
        self: Pin<&mut Self>,
        xid: i32,
        item: Request,
        tx: oneshot::Sender<Result<Response, ZkError>>,
    ) {
        let this = self.project();
        Self::enqueue_impl(this.outbox, this.reply, xid, item, tx)
    }
    pub(super) fn enqueue_unpin(
        &mut self,
        xid: i32,
        item: Request,
        tx: oneshot::Sender<Result<Response, ZkError>>,
    ) {
        Self::enqueue_impl(&mut self.outbox, &mut self.reply, xid, item, tx)
    }

    #[instrument(skip(self, cx))]
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        exiting: bool,
    ) -> Poll<Result<(), Error>>
    where
        S: AsyncWrite,
    {
        let mut wrote = false;
        while self.outlen() != 0 {
            let mut this = self.as_mut().project();
            let n = ready!(this
                .stream
                .as_mut()
                .poll_write(cx, &this.outbox[*this.outstart..])?);
            wrote = true;
            *this.outstart += n;
            if *this.outstart == this.outbox.len() {
                this.outbox.clear();
                *this.outstart = 0;
            }
        }

        let mut this = self.project();
        if wrote {
            // heartbeat is since last write traffic!
            trace!("resetting heartbeat timer");
            this.timer
                .reset(tokio::time::Instant::now() + *this.timeout);
        }

        ready!(this.stream.as_mut().poll_flush(cx)?);

        if exiting {
            debug!("shutting down writer");
            ready!(this.stream.poll_shutdown(cx)?);
        }

        Poll::Ready(Ok(()))
    }

    #[instrument(skip(self, cx, default_watcher))]
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        default_watcher: &mut mpsc::UnboundedSender<WatchedEvent>,
    ) -> Poll<Result<(), Error>>
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
            trace!("need {need} bytes, have {inlen}", inlen = self.inlen());

            while self.inlen() < need {
                let this = self.as_mut().project();
                let read_from = this.inbox.len();
                this.inbox.resize(*this.instart + need, 0);
                let mut inbox_buf = ReadBuf::new(&mut this.inbox[read_from..]);
                match this.stream.poll_read(cx, &mut inbox_buf)? {
                    Poll::Ready(()) => {
                        let n = inbox_buf.filled().len();
                        this.inbox.truncate(read_from + n);
                        if n == 0 {
                            if self.inlen() != 0 {
                                return Poll::Ready(
                                    ConnectionClosedSnafu {
                                        buf: &self.inbox[self.instart..],
                                    }
                                    .fail(),
                                );
                            } else {
                                // Server closed session with no bytes left in buffer
                                debug!("server closed connection");
                                return Poll::Ready(Ok(()));
                            }
                        }

                        if self.inlen() >= 4 && need == 4 {
                            let length = (&mut &self.inbox[self.instart..])
                                .read_i32::<BigEndian>()?
                                as usize;
                            need += length;
                        }
                    }
                    Poll::Pending => {
                        this.inbox.truncate(read_from);
                        return Poll::Pending;
                    }
                }
            }

            {
                let mut this = self.as_mut().project();
                let mut err = None;
                let mut buf = &this.inbox[*this.instart + 4..*this.instart + need];
                *this.instart += need;

                let xid = if *this.first {
                    0
                } else {
                    let xid = buf.read_i32::<BigEndian>()?;
                    let zxid = buf.read_i64::<BigEndian>()?;
                    if zxid > 0 {
                        trace!(
                            "updated zxid from {last_zxid_seen} to {zxid}",
                            last_zxid_seen = *this.last_zxid_seen
                        );

                        assert!(zxid >= *this.last_zxid_seen);
                        *this.last_zxid_seen = zxid;
                    }
                    let zk_err: ZkError = buf.read_i32::<BigEndian>()?.into();
                    if zk_err != ZkError::Ok {
                        err = Some(zk_err);
                    }
                    xid
                };

                if xid == 0 && !*this.first {
                    // response to shutdown -- empty response
                    // XXX: in theory, server should now shut down receive end
                    trace!("got response to CloseSession");
                    if let Some(e) = err {
                        return Poll::Ready(CloseSessionSnafu { error_code: e }.fail());
                    }
                } else if xid == -1 {
                    // watch event
                    use super::response::ReadFrom;
                    let e = WatchedEvent::read_from(&mut buf)?;
                    trace!(event = ?e, "got watcher event");

                    let mut remove = false;
                    if let Some(watchers) = this.watchers.get_mut(&e.path) {
                        // custom watchers were set by the user -- notify them
                        let mut i = (watchers.len() - 1) as isize;
                        trace!(
                            watcher_count = watchers.len(),
                            "found potentially waiting custom watchers"
                        );

                        while i >= 0 {
                            let triggers = match (&watchers[i as usize].1, e.event_type) {
                                (WatchType::Child, WatchedEventType::NodeDeleted)
                                | (WatchType::Child, WatchedEventType::NodeChildrenChanged) => true,
                                (WatchType::Child, _) => false,
                                (WatchType::Data, WatchedEventType::NodeDeleted)
                                | (WatchType::Data, WatchedEventType::NodeDataChanged) => true,
                                (WatchType::Data, _) => false,
                                (WatchType::Exist, WatchedEventType::NodeChildrenChanged) => false,
                                (WatchType::Exist, _) => true,
                            };

                            if triggers {
                                // this watcher is no longer active
                                let w = watchers.swap_remove(i as usize);
                                // NOTE: ignore the case where the receiver has been dropped
                                let _ = w.0.send(e.clone());
                            }
                            i -= 1;
                        }

                        if watchers.is_empty() {
                            remove = true;
                        }
                    }

                    if remove {
                        this.watchers
                            .remove(&e.path)
                            .expect("tried to remove watcher that didn't exist");
                    }

                    // NOTE: ignoring error, because the user may not care about events
                    let _ = default_watcher.unbounded_send(e);
                } else if xid == -2 {
                    // response to ping -- empty response
                    trace!("got response to heartbeat");
                    if let Some(e) = err {
                        return Poll::Ready(BadPingSnafu { error_code: e }.fail());
                    }
                } else {
                    // response to user request
                    *this.first = false;

                    // find the waiting request future
                    let (opcode, tx) = this.reply.remove(&xid).unwrap(); // TODO: return an error if xid was unknown

                    if let Some(w) = this.pending_watchers.remove(&xid) {
                        // normally, watches are *only* added for successful operations
                        // the exception to this is if an exists call fails with NoNode
                        if err.is_none()
                            || (opcode == request::OpCode::Exists && err == Some(ZkError::NoNode))
                        {
                            trace!(xid, "pending watcher turned into real watcher");
                            this.watchers.entry(w.0).or_default().push((w.1, w.2));
                        } else {
                            trace!(
                                xid,
                                error = ?err,
                                "pending watcher not turned into real watcher"
                            );
                        }
                    }

                    if let Some(e) = err {
                        info!(xid, ?opcode, error = ?e, "handling server error response");

                        let _ = tx.send(Err(e));
                    } else {
                        let mut response = Response::parse(opcode, &mut buf)?;

                        debug!(?response, xid, ?opcode, "handling server response");

                        if let Response::Connect {
                            timeout,
                            session_id,
                            ref mut password,
                            ..
                        } = response
                        {
                            assert!(timeout >= 0);

                            *this.timeout = time::Duration::from_millis(2 * timeout as u64 / 3);
                            trace!(
                                timeout = ?this.timeout,
                                "negotiated session timeout",
                            );

                            this.timer
                                .as_mut()
                                .reset(tokio::time::Instant::now() + *this.timeout);

                            // keep track of these for consistent re-connect
                            *this.session_id = session_id;
                            mem::swap(this.password, password);
                        }

                        let _ = tx.send(Ok(response)); // if receiver doesn't care, we don't either
                    }
                }
            }

            if self.instart == self.inbox.len() {
                let this = self.as_mut().project();
                this.inbox.clear();
                *this.instart = 0;
            }
        }
    }

    #[instrument(name = "poll_read", skip(self, cx, default_watcher))]
    pub(super) fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        exiting: bool,
        default_watcher: &mut mpsc::UnboundedSender<WatchedEvent>,
    ) -> Poll<Result<(), Error>> {
        let r = self.as_mut().poll_read(cx, default_watcher)?;

        let mut this = self.as_mut().project();
        if let Poll::Ready(()) = this.timer.as_mut().poll(cx) {
            if this.outbox.is_empty() {
                // send a ping!
                // length is known for pings
                this.outbox
                    .write_i32::<BigEndian>(8)
                    .expect("Vec::write should never fail");
                // xid
                this.outbox
                    .write_i32::<BigEndian>(-2)
                    .expect("Vec::write should never fail");
                // opcode
                this.outbox
                    .write_i32::<BigEndian>(request::OpCode::Ping as i32)
                    .expect("Vec::write should never fail");
                trace!("sending heartbeat");
            } else {
                // already request in flight, so no need to also send heartbeat
            }

            this.timer
                .as_mut()
                .reset(tokio::time::Instant::now() + *this.timeout);
        }

        let w = self.poll_write(cx, exiting)?;

        match (r, w) {
            (Poll::Ready(()), Poll::Ready(())) if exiting => {
                debug!("packetizer done");
                Poll::Ready(Ok(()))
            }
            (Poll::Ready(()), Poll::Ready(())) => Poll::Ready(ServerClosedConnectionSnafu.fail()),
            (Poll::Ready(()), _) => Poll::Ready(ResponseChannelClosedPrematurelySnafu.fail()),
            _ => Poll::Pending,
        }
    }
}
