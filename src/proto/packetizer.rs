use super::{
    active_packetizer::ActivePacketizer, request, watch::WatchType, Request, Response,
    ZooKeeperTransport,
};
use crate::{error::Error, format_err, Watch, WatchedEvent, ZkError};
use byteorder::{BigEndian, WriteBytesExt};
use futures::{
    channel::{mpsc, oneshot},
    future::Either,
    ready, FutureExt, StreamExt, TryFutureExt,
};
use pin_project::pin_project;
use snafu::ResultExt;
use std::{
    future::{self, Future},
    mem,
    pin::Pin,
    task::{Context, Poll},
};
use tokio::io::{AsyncRead, AsyncWrite};
use tracing::{debug, error, info, instrument, trace};

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
        default_watcher: mpsc::UnboundedSender<WatchedEvent>,
    ) -> Enqueuer
    where
        S: Send + 'static + AsyncRead + AsyncWrite,
    {
        let (tx, rx) = mpsc::unbounded();

        tokio::spawn(
            Packetizer {
                addr,
                state: PacketizerState::Connected(ActivePacketizer::new(stream)),
                xid: 0,
                default_watcher,
                rx,
                exiting: false,
            }
            .map_err(move |error| {
                error!(%error, "packetizer exiting");
                drop(error);
            }),
        );

        Enqueuer(tx)
    }
}

#[pin_project(project = PacketizerStateProj)]
enum PacketizerState<S> {
    Connected(#[pin] ActivePacketizer<S>),
    Reconnecting(
        Pin<Box<dyn Future<Output = Result<ActivePacketizer<S>, Error>> + Send + 'static>>,
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
        default_watcher: &mut mpsc::UnboundedSender<WatchedEvent>,
    ) -> Poll<Result<(), Error>> {
        let ap = match self.as_mut().project() {
            PacketizerStateProj::Connected(ref mut ap) => {
                return ap
                    .as_mut()
                    .poll(cx, exiting, default_watcher)
                    .map(|res| res.whatever_context("active packetizer failed"))
            }
            PacketizerStateProj::Reconnecting(ref mut c) => ready!(c.as_mut().poll(cx)?),
        };

        // we are now connected!
        self.set(PacketizerState::Connected(ap));
        self.poll(cx, exiting, default_watcher)
    }
}

impl<S> Packetizer<S>
where
    S: ZooKeeperTransport,
{
    #[instrument(skip(self, cx))]
    fn poll_enqueue(self: Pin<&mut Self>, cx: &mut Context) -> Poll<Result<(), ()>> {
        let mut this = self.project();
        while let PacketizerStateProj::Connected(ref mut ap) = this.state.as_mut().project() {
            let (mut item, tx) = match ready!(this.rx.poll_next_unpin(cx)) {
                Some((request, response)) => (request, response),
                None => return Poll::Ready(Err(())),
            };
            debug!(?item, xid = this.xid, "enqueueing request");

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
                            trace!(xid = this.xid, path, ?wtype, "adding pending watcher");
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
    type Output = Result<(), Error>;

    #[instrument(skip(self, cx))]
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Self::Output> {
        if !self.exiting {
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
            .poll(cx, *this.exiting, this.default_watcher)
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
                    debug!("connection lost during exit; not reconnecting");
                    Poll::Ready(Ok(()))
                } else if let PacketizerState::Connected(ActivePacketizer {
                    last_zxid_seen,
                    session_id,
                    ..
                }) = *this.state
                {
                    info!(
                        session_id,
                        last_zxid = last_zxid_seen,
                        "connection lost; reconnecting"
                    );

                    let xid = *this.xid;
                    *this.xid += 1;

                    let retry = S::connect(this.addr.clone())
                        .map(|res| res.whatever_context("failed to connect"))
                        .map_ok(move |stream| {
                            let request = Request::Connect {
                                protocol_version: 0,
                                last_zxid_seen,
                                timeout: 0,
                                session_id,
                                passwd: password,
                                read_only: false,
                            };
                            trace!("about to handshake (again)");

                            let (tx, rx) = oneshot::channel();
                            tokio::spawn(rx.map(move |r| {
                                trace!(response = ?r, "re-connection response");
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
    ) -> impl Future<Output = Result<Result<Response, ZkError>, Error>> {
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
