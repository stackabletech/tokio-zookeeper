use super::{
    active_packetizer::ActivePacketizer, request, watch::WatchType, Request, Response,
    ZooKeeperTransport,
};
use crate::{Watch, WatchedEvent, ZkError};
use byteorder::{BigEndian, WriteBytesExt};
use failure::format_err;
use futures::{
    channel::{mpsc, oneshot},
    future::Either,
    ready, FutureExt, StreamExt, TryFutureExt,
};
use pin_project::pin_project;
use slog::{debug, error, info, trace};
use std::{
    future::{self, Future},
    mem,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite};

#[pin_project]
pub(crate) struct Packetizer<S>
where
    S: ZooKeeperTransport,
{
    /// ZooKeeper address
    addr: S::Addr,

    /// Current state
    #[pin]
    state: PacketizerState<S>,

    /// Watcher to send watch events to.
    default_watcher: mpsc::UnboundedSender<WatchedEvent>,

    /// Incoming requests
    rx: mpsc::UnboundedReceiver<(Request, oneshot::Sender<Result<Response, ZkError>>)>,

    /// Next xid to issue
    xid: i32,

    logger: slog::Logger,

    exiting: bool,
}

impl<S> Packetizer<S>
where
    S: ZooKeeperTransport,
{
    // Enqueuer is the entry point for submitting data to Packetizer
    #[allow(clippy::new_ret_no_self)]
    pub(crate) fn new(
        addr: S::Addr,
        stream: S,
        log: slog::Logger,
        default_watcher: mpsc::UnboundedSender<WatchedEvent>,
    ) -> Enqueuer
    where
        S: Send + 'static + AsyncRead + AsyncWrite,
    {
        let (tx, rx) = mpsc::unbounded();

        let exitlogger = log.clone();
        tokio::spawn(
            Packetizer {
                addr,
                state: PacketizerState::Connected(ActivePacketizer::new(stream)),
                xid: 0,
                default_watcher,
                rx,
                logger: log,
                exiting: false,
            }
            .map_err(move |e| {
                error!(exitlogger, "packetizer exiting: {:?}", e);
                drop(e);
            }),
        );

        Enqueuer(tx)
    }
}

#[pin_project(project = PacketizerStateProj)]
enum PacketizerState<S> {
    Connected(#[pin] ActivePacketizer<S>),
    Reconnecting(
        Pin<Box<dyn Future<Output = Result<ActivePacketizer<S>, failure::Error>> + Send + 'static>>,
    ),
}

impl<S> PacketizerState<S>
where
    S: AsyncRead + AsyncWrite,
{
    fn poll(
        mut self: Pin<&mut Self>,
        cx: &mut Context,
        exiting: bool,
        logger: &mut slog::Logger,
        default_watcher: &mut mpsc::UnboundedSender<WatchedEvent>,
    ) -> Poll<Result<(), failure::Error>> {
        let ap = match self.as_mut().project() {
            PacketizerStateProj::Connected(ref mut ap) => {
                return ap.as_mut().poll(cx, exiting, logger, default_watcher)
            }
            PacketizerStateProj::Reconnecting(ref mut c) => ready!(c.as_mut().poll(cx)?),
        };

        // we are now connected!
        self.set(PacketizerState::Connected(ap));
        self.poll(cx, exiting, logger, default_watcher)
    }
}

impl<S> Packetizer<S>
where
    S: ZooKeeperTransport,
{
    fn poll_enqueue(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), ()>> {
        let mut this = self.project();
        while let PacketizerStateProj::Connected(ref mut ap) = this.state.as_mut().project() {
            let (mut item, tx) = match ready!(this.rx.poll_next_unpin(cx)) {
                Some((request, response)) => (request, response),
                None => return Poll::Ready(Err(())),
            };
            debug!(this.logger, "enqueueing request {:?}", item; "xid" => *this.xid);

            match item {
                Request::GetData {
                    ref path,
                    ref mut watch,
                    ..
                }
                | Request::GetChildren {
                    ref path,
                    ref mut watch,
                    ..
                }
                | Request::Exists {
                    ref path,
                    ref mut watch,
                    ..
                } => {
                    if let Watch::Custom(_) = *watch {
                        // set to Global so that watch will be sent as 1u8
                        let w = mem::replace(watch, Watch::Global);
                        if let Watch::Custom(w) = w {
                            let wtype = match item {
                                Request::GetData { .. } => WatchType::Data,
                                Request::GetChildren { .. } => WatchType::Child,
                                Request::Exists { .. } => WatchType::Exist,
                                _ => unreachable!(),
                            };
                            trace!(
                                this.logger,
                                "adding pending watcher";
                                "xid" => *this.xid,
                                "path" => path,
                                "wtype" => ?wtype
                            );
                            ap.as_mut()
                                .pending_watchers()
                                .insert(*this.xid, (path.to_string(), w, wtype));
                        } else {
                            unreachable!();
                        }
                    }
                }
                _ => {}
            }

            ap.as_mut().enqueue(*this.xid, item, tx);
            *this.xid += 1;
        }
        Poll::Pending
    }
}

impl<S> Future for Packetizer<S>
where
    S: ZooKeeperTransport,
{
    type Output = Result<(), failure::Error>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        trace!(self.logger, "packetizer polled");
        if !self.exiting {
            trace!(self.logger, "poll_enqueue");
            match self.as_mut().poll_enqueue(cx) {
                Poll::Ready(Ok(())) | Poll::Pending => {}
                Poll::Ready(Err(())) => {
                    let this = self.as_mut().project();
                    // no more requests will be enqueued
                    *this.exiting = true;

                    if let PacketizerStateProj::Connected(ref mut ap) = this.state.project() {
                        // send CloseSession
                        // length is fixed
                        ap.as_mut()
                            .outbox()
                            .write_i32::<BigEndian>(8)
                            .expect("Vec::write should never fail");
                        // xid
                        ap.as_mut()
                            .outbox()
                            .write_i32::<BigEndian>(0)
                            .expect("Vec::write should never fail");
                        // opcode
                        ap.as_mut()
                            .outbox()
                            .write_i32::<BigEndian>(request::OpCode::CloseSession as i32)
                            .expect("Vec::write should never fail");
                    } else {
                        unreachable!("poll_enqueue will never return Err() if not connected");
                    }
                }
            }
        }

        let mut this = self.as_mut().project();
        match this
            .state
            .as_mut()
            .poll(cx, *this.exiting, this.logger, this.default_watcher)
        {
            Poll::Ready(Ok(v)) => Poll::Ready(Ok(v)),
            Poll::Pending => Poll::Pending,
            Poll::Ready(Err(e)) => {
                // if e is disconnect, then purge state and reconnect
                // for now, assume all errors are disconnects
                // TODO: test this!

                let password =
                    if let PacketizerStateProj::Connected(ap) = this.state.as_mut().project() {
                        ap.take_password()
                    } else {
                        // XXX: error while connecting -- don't recurse (for now)
                        return Poll::Ready(Err(e));
                    };

                if *this.exiting {
                    debug!(this.logger, "connection lost during exit; not reconnecting");
                    Poll::Ready(Ok(()))
                } else if let PacketizerState::Connected(ActivePacketizer {
                    last_zxid_seen,
                    session_id,
                    ..
                }) = *this.state
                {
                    info!(this.logger, "connection lost; reconnecting";
                          "session_id" => session_id,
                          "last_zxid" => last_zxid_seen
                    );

                    let xid = *this.xid;
                    *this.xid += 1;

                    let log = this.logger.clone();
                    let retry =
                        S::connect(this.addr.clone())
                            .map_err(|e| e.into())
                            .map_ok(move |stream| {
                                let request = Request::Connect {
                                    protocol_version: 0,
                                    last_zxid_seen,
                                    timeout: 0,
                                    session_id,
                                    passwd: password,
                                    read_only: false,
                                };
                                trace!(log, "about to handshake (again)");

                                let (tx, rx) = oneshot::channel();
                                tokio::spawn(rx.map(move |r| {
                                    trace!(log, "re-connection response: {:?}", r);
                                }));

                                let mut ap = ActivePacketizer::new(stream);
                                ap.enqueue_unpin(xid, request, tx);
                                ap
                            });

                    // dropping the old state will also cancel in-flight requests
                    this.state
                        .set(PacketizerState::Reconnecting(Box::pin(retry)));
                    self.poll(cx)
                } else {
                    unreachable!();
                }
            }
        }
    }
}

#[derive(Clone, Debug)]
pub(crate) struct Enqueuer(
    mpsc::UnboundedSender<(Request, oneshot::Sender<Result<Response, ZkError>>)>,
);

impl Enqueuer {
    pub(crate) fn enqueue(
        &self,
        request: Request,
    ) -> impl Future<Output = Result<Result<Response, ZkError>, failure::Error>> {
        let (tx, rx) = oneshot::channel();
        match self.0.unbounded_send((request, tx)) {
            Ok(()) => {
                Either::Left(rx.map_err(|e| format_err!("failed to enqueue new request: {:?}", e)))
            }
            Err(e) => Either::Right(future::ready(Err(format_err!(
                "failed to enqueue new request: {:?}",
                e
            )))),
        }
    }
}
