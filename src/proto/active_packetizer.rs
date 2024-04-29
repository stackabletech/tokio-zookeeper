use super::{request, watch::WatchType, Request, Response};
use crate::{WatchedEvent, WatchedEventType, ZkError};
use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use failure::format_err;
use futures::{
    channel::{mpsc, oneshot},
    ready,
};
use pin_project::pin_project;
use slog::{debug, info, trace};
use std::collections::HashMap;
use std::{
    future::Future,
    mem,
    pin::Pin,
    task::{Context, Poll},
    time,
};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};

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
        reply: &mut HashMap<i32, (request::OpCode, oneshot::Sender<Result<Response, ZkError>>)>,
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

    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        exiting: bool,
        logger: &mut slog::Logger,
    ) -> Poll<Result<(), failure::Error>>
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
            trace!(logger, "resetting heartbeat timer");
            this.timer
                .reset(tokio::time::Instant::now() + *this.timeout);
        }

        ready!(this
            .stream
            .as_mut()
            .poll_flush(cx)
            .map_err(failure::Error::from)?);

        if exiting {
            debug!(logger, "shutting down writer");
            ready!(this.stream.poll_shutdown(cx)?);
        }

        Poll::Ready(Ok(()))
    }

    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        default_watcher: &mut mpsc::UnboundedSender<WatchedEvent>,
        logger: &mut slog::Logger,
    ) -> Poll<Result<(), failure::Error>>
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
            trace!(logger, "need {} bytes, have {}", need, self.inlen());

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
                                return Poll::Ready(Err(format_err!(
                                    "connection closed with {} bytes left in buffer: {:x?}",
                                    self.inlen(),
                                    &self.inbox[self.instart..]
                                )));
                            } else {
                                // Server closed session with no bytes left in buffer
                                debug!(logger, "server closed connection");
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
                            logger,
                            "updated zxid from {} to {}",
                            *this.last_zxid_seen,
                            zxid
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
                    trace!(logger, "got response to CloseSession");
                    if let Some(e) = err {
                        return Poll::Ready(Err(format_err!("failed to close session: {:?}", e)));
                    }
                } else if xid == -1 {
                    // watch event
                    use super::response::ReadFrom;
                    let e = WatchedEvent::read_from(&mut buf)?;
                    trace!(logger, "got watcher event {:?}", e);

                    let mut remove = false;
                    if let Some(watchers) = this.watchers.get_mut(&e.path) {
                        // custom watchers were set by the user -- notify them
                        let mut i = (watchers.len() - 1) as isize;
                        trace!(logger,
                               "found potentially waiting custom watchers";
                               "n" => watchers.len()
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
                    trace!(logger, "got response to heartbeat");
                    if let Some(e) = err {
                        return Poll::Ready(Err(format_err!("bad response to ping: {:?}", e)));
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
                            trace!(logger, "pending watcher turned into real watcher"; "xid" => xid);
                            this.watchers.entry(w.0).or_default().push((w.1, w.2));
                        } else {
                            trace!(logger,
                                   "pending watcher not turned into real watcher: {:?}",
                                   err;
                                   "xid" => xid
                            );
                        }
                    }

                    if let Some(e) = err {
                        info!(logger,
                               "handling server error response: {:?}", e;
                               "xid" => xid, "opcode" => ?opcode);

                        let _ = tx.send(Err(e));
                    } else {
                        let mut r = Response::parse(opcode, &mut buf)?;

                        debug!(logger,
                               "handling server response: {:?}", r;
                               "xid" => xid, "opcode" => ?opcode);

                        if let Response::Connect {
                            timeout,
                            session_id,
                            ref mut password,
                            ..
                        } = r
                        {
                            assert!(timeout >= 0);
                            trace!(logger, "negotiated session timeout: {}ms", timeout);

                            *this.timeout = time::Duration::from_millis(2 * timeout as u64 / 3);
                            this.timer
                                .as_mut()
                                .reset(tokio::time::Instant::now() + *this.timeout);

                            // keep track of these for consistent re-connect
                            *this.session_id = session_id;
                            mem::swap(this.password, password);
                        }

                        let _ = tx.send(Ok(r)); // if receiver doesn't care, we don't either
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

    pub(super) fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        exiting: bool,
        logger: &mut slog::Logger,
        default_watcher: &mut mpsc::UnboundedSender<WatchedEvent>,
    ) -> Poll<Result<(), failure::Error>> {
        trace!(logger, "poll_read");
        let r = self.as_mut().poll_read(cx, default_watcher, logger)?;

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
                trace!(logger, "sending heartbeat");
            } else {
                // already request in flight, so no need to also send heartbeat
            }

            this.timer
                .as_mut()
                .reset(tokio::time::Instant::now() + *this.timeout);
        }

        trace!(logger, "poll_read");
        let w = self.poll_write(cx, exiting, logger)?;

        match (r, w) {
            (Poll::Ready(()), Poll::Ready(())) if exiting => {
                debug!(logger, "packetizer done");
                Poll::Ready(Ok(()))
            }
            (Poll::Ready(()), Poll::Ready(())) => Poll::Ready(Err(format_err!(
                "Not exiting, but server closed connection"
            ))),
            (Poll::Ready(()), _) => Poll::Ready(Err(format_err!(
                "outstanding requests, but response channel closed"
            ))),
            _ => Poll::Pending,
        }
    }
}
