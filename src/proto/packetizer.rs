use super::{
    active_packetizer::ActivePacketizer, request, watch::WatchType, Request, Response,
    ZooKeeperTransport,
};
use crate::{Watch, WatchedEvent, ZkError};
use byteorder::{BigEndian, WriteBytesExt};
use failure::format_err;
use futures::{
    future::Either,
    sync::{mpsc, oneshot},
    try_ready,
};
use slog::{debug, error, info, trace};
use std::mem;
use tokio::prelude::*;

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

    logger: slog::Logger,

    exiting: bool,
}

impl<S> Packetizer<S>
where
    S: ZooKeeperTransport,
{
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

enum PacketizerState<S> {
    Connected(ActivePacketizer<S>),
    Reconnecting(
        Box<dyn Future<Item = ActivePacketizer<S>, Error = failure::Error> + Send + 'static>,
    ),
}

impl<S> PacketizerState<S>
where
    S: AsyncRead + AsyncWrite,
{
    fn poll(
        &mut self,
        exiting: bool,
        logger: &mut slog::Logger,
        default_watcher: &mut mpsc::UnboundedSender<WatchedEvent>,
    ) -> Result<Async<()>, failure::Error> {
        let ap = match *self {
            PacketizerState::Connected(ref mut ap) => {
                return ap.poll(exiting, logger, default_watcher)
            }
            PacketizerState::Reconnecting(ref mut c) => try_ready!(c.poll()),
        };

        // we are now connected!
        *self = PacketizerState::Connected(ap);
        self.poll(exiting, logger, default_watcher)
    }
}

impl<S> Packetizer<S>
where
    S: ZooKeeperTransport,
{
    fn poll_enqueue(&mut self) -> Result<Async<()>, ()> {
        while let PacketizerState::Connected(ref mut ap) = self.state {
            let (mut item, tx) = match try_ready!(self.rx.poll()) {
                Some((request, response)) => (request, response),
                None => return Err(()),
            };
            debug!(self.logger, "enqueueing request {:?}", item; "xid" => self.xid);

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
                                self.logger,
                                "adding pending watcher";
                                "xid" => self.xid,
                                "path" => path,
                                "wtype" => ?wtype
                            );
                            ap.pending_watchers
                                .insert(self.xid, (path.to_string(), w, wtype));
                        } else {
                            unreachable!();
                        }
                    }
                }
                _ => {}
            }

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
        trace!(self.logger, "packetizer polled");
        if !self.exiting {
            trace!(self.logger, "poll_enqueue");
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

        match self
            .state
            .poll(self.exiting, &mut self.logger, &mut self.default_watcher)
        {
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
                    info!(self.logger, "connection lost; reconnecting";
                          "session_id" => session_id,
                          "last_zxid" => last_zxid_seen
                    );

                    let xid = self.xid;
                    self.xid += 1;

                    let log = self.logger.clone();
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
                            trace!(log, "about to handshake (again)");

                            let (tx, rx) = oneshot::channel();
                            tokio::spawn(rx.then(move |r| {
                                trace!(log, "re-connection response: {:?}", r);
                                Ok(())
                            }));

                            let mut ap = ActivePacketizer::new(stream);
                            ap.enqueue(xid, request, tx);
                            ap
                        });

                    // dropping the old state will also cancel in-flight requests
                    self.state = PacketizerState::Reconnecting(Box::new(retry));
                    self.poll()
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
    ) -> impl Future<Item = Result<Response, ZkError>, Error = failure::Error> {
        let (tx, rx) = oneshot::channel();
        match self.0.unbounded_send((request, tx)) {
            Ok(()) => {
                Either::A(rx.map_err(|e| format_err!("failed to enqueue new request: {:?}", e)))
            }
            Err(e) => {
                Either::B(Err(format_err!("failed to enqueue new request: {:?}", e)).into_future())
            }
        }
    }
}
