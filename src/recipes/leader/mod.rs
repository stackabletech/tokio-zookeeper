use snafu::Snafu;
use snafu::{ResultExt, whatever};
use tokio::task::AbortHandle;
use tracing::{Instrument, debug, error, info, trace_span, warn};
use uuid::Uuid;

use crate::WatchedEventType::{self, NodeDeleted};
use crate::{Acl, WatchedEvent};
use crate::{CreateMode, KeeperState};
use crate::{ZooKeeper, error};

use tokio::sync::watch;

use backon::ExponentialBuilder;
use backon::Retryable;

/// Participate in leader election through this struct
#[derive(Debug)]
pub struct LeaderElection {
    /// ZNode under which volunteers are registered
    election_prefix: String,
    zk: ZooKeeper,
    backon_builder: ExponentialBuilder,
    acl: Vec<Acl>,
}

/// Represents the current state this node is in
#[derive(Debug, Clone, Copy)]
pub enum LeadershipState {
    /// This node is a follower
    Follower,
    /// This node is the leader
    Leader,
    /// Leadership participation procedure has not yet started
    Uninitialized,
    /// An error has occurred in the leader election procedure; terminal state
    Error,
}

impl LeaderElection {
    /// Create a new leader election struct
    pub fn new(zk: ZooKeeper, election_node: &str, acl: Vec<Acl>) -> Self {
        let backon_builder = ExponentialBuilder::default()
            .with_jitter()
            .with_min_delay(core::time::Duration::from_millis(100))
            .with_max_delay(core::time::Duration::from_millis(5000))
            .with_max_times(5);

        Self {
            election_prefix: election_node.to_string(),
            zk,
            backon_builder,
            acl,
        }
    }
    /// Participate in [leader election](https://zookeeper.apache.org/doc/current/recipes.html#sc_leaderElection)
    ///
    /// # Returns
    ///
    /// - A [tokio::sync::watch::Receiver] that resolves once this node becomes
    ///   a leader. To stop participating, drop the underlying ZooKeeper
    ///   connection, so that the underlying ephemeral znodes are removed.
    /// - A [tokio::runtime::task::AbortHandle]. Call .abort() to stop
    ///   participating in leader election. If a connection to ZooKeeper is
    ///   kept alive after this call, the ephemeral nodes are not removed making
    ///   it it seem like you're still participating
    ///
    /// Upon receiving from this receiver applications may consider creating a
    /// separate znode to acknowledge that the leader has executed the leader
    /// procedure.
    ///
    /// Does not automatically re-create ephemeral nodes for participation but
    /// sends an error whenever session expires or other unexpected events or
    /// errors occur in the process. Only these transitions are possible:
    /// - Uninitialized -> Leader -> Error
    /// - Uninitialized -> Follower -> Error
    /// - Uninitialized -> Follower -> Leader -> Error
    ///
    /// Here is an example of how this might be used:
    ///
    /// ```no_run
    /// use tokio::select;
    /// use tokio_zookeeper::{Acl, ZooKeeper, recipes::leader::*};
    ///
    /// fn init_tracing_subscriber() {
    ///     let _ = tracing_subscriber::fmt()
    ///         .with_env_filter(tracing_subscriber::EnvFilter::from_default_env())
    ///         .init();
    /// }
    ///
    /// #[tokio::main]
    /// async fn main() {
    ///     init_tracing_subscriber();
    ///     let binding = "127.0.0.1:2181".parse().unwrap();
    ///     let (zk, _default_watcher) = ZooKeeper::connect(&binding).await.unwrap();
    ///
    ///     let leader_election = LeaderElection::new(zk, "/election", Acl::open_unsafe().to_vec());
    ///     let (mut leader_receiver, _abort_handle) = leader_election.volunteer().await.unwrap();
    ///     loop {
    ///         let state = *leader_receiver.borrow_and_update();
    ///         match state {
    ///             LeadershipState::Leader => {
    ///                 select! {
    ///                     _ = leader_receiver.changed() => { println!("changed");  }
    ///                     _ = async  {
    ///                         loop {
    ///                             println!("doing leader work...");
    ///                             tokio::time::sleep(tokio::time::Duration::from_secs(1_000)).await
    ///                         }
    ///                     } =>  {}
    ///                 }
    ///             }
    ///             LeadershipState::Follower => {
    ///                 select! {
    ///                     _ = leader_receiver.changed() => { println!("changed");  }
    ///                     _ = async  {
    ///                         loop {
    ///                             println!("doing follower work...");
    ///                             tokio::time::sleep(tokio::time::Duration::from_secs(1_000)).await
    ///                         }
    ///                     } =>  unreachable!("select should cancel the future")
    ///                 }
    ///             }
    ///             LeadershipState::Uninitialized => {
    ///                 println!("uninitialized");
    ///                 _ = leader_receiver.changed().await;
    ///             }
    ///             LeadershipState::Error => {
    ///                 eprintln!("error");
    ///                 return;
    ///             }
    ///         }
    ///     }
    /// }
    /// ```
    pub async fn volunteer(
        self,
    ) -> Result<(watch::Receiver<LeadershipState>, AbortHandle), VolunteeringError> {
        info!("volunteering for leader election");

        // Error handling with guids:
        // https://zookeeper.apache.org/doc/current/recipes.html#sc_recipes_GuidNote
        // "If a recoverable error occurs calling create() the client should
        // call getChildren() and check for a node containing the guid used in the path
        // name. This handles the case [...] of the create() succeeding on the
        // server but the server crashing before returning the name of the new node."
        let guid = Uuid::new_v4();
        let path = match self
            .zk
            .create(
                &format!("{}/{}-n_", self.election_prefix, guid),
                &b""[..],
                self.acl,
                CreateMode::EphemeralSequential,
            )
            .await
        {
            Ok(create_res) => {
                // if this is an err, it is "unrecoverable": no parent node, node already exists, etc.
                create_res?
            }
            Err(e) => {
                warn!(
                    "can't create ephemeral node : {e}, recoverable error, will now try to find my guid again..."
                );
                (|| async { get_children(&self.zk, &self.election_prefix).await })
                    .retry(self.backon_builder)
                    .notify(|err, dur| {
                        warn!("retrying {:?} after {:?}", err, dur);
                    })
                    .await
                    .map_err(|e| VolunteeringError::GetChildrenError { source: e })?
                    .into_iter()
                    .find(|child| child.guid == guid)
                    .map(|child| child.full_path())
                    .ok_or(VolunteeringError::NodeNotFound)?
            }
        };

        let node = ElectionChild::try_from_full_path(&path, &self.election_prefix)
            .expect("get_children should return only well-formatted paths");

        let (leader_sender, leader_receiver) = watch::channel(LeadershipState::Uninitialized);
        let candidate = Candidate::new(node, self.zk, self.backon_builder);

        let jh = tokio::spawn(
            candidate
                .observe(leader_sender)
                .instrument(trace_span!("election_observer", my_path = %path)),
        );

        Ok((leader_receiver, jh.abort_handle()))
    }
}

/// Errors that may appear in the process of volunteering for leadership
#[derive(Debug, Snafu)]
pub enum VolunteeringError {
    /// Ephemeral znode for leadership election could not be created
    #[snafu(display("ZNode creation error"), context(false))]
    NodeCreationError {
        /// The underlying tokio_zookeeper's create error
        source: error::Create,
    },
    /// Ephemeral znode with the generated GUID was not found among the children of the election znode
    #[snafu(display(
        "Ephemeral ZNode with the generated GUID was not found among the children of the election znode"
    ))]
    NodeNotFound,
    /// Could not get children of the election znode
    #[snafu(display("Could not get children of the election znode"))]
    GetChildrenError {
        /// The underlying tokio_zookeeper error
        source: crate::Error,
    },
}

#[derive(Debug)]
struct Candidate {
    zk: ZooKeeper,
    node: ElectionChild,
    backon_builder: ExponentialBuilder,
}

impl Candidate {
    fn new(node: ElectionChild, zk: ZooKeeper, backon_builder: ExponentialBuilder) -> Self {
        Self {
            zk,
            node,
            backon_builder,
        }
    }

    async fn observe(self, leader_sender: watch::Sender<LeadershipState>) {
        loop {
            match (|| async { self.observe_once(&leader_sender).await })
                .retry(self.backon_builder)
                .notify(|err, dur| {
                    warn!("retrying {:?} after {:?}", err, dur);
                })
                .when(|e| matches!(e, LeaderElectionError::ZnodeNotFound))
                .await
            {
                Ok(_) => continue,
                Err(e) => {
                    error!("{e}");
                    _ = leader_sender.send(LeadershipState::Error);
                    return;
                }
            }
        }
    }

    async fn observe_once(
        &self,
        leader_sender: &watch::Sender<LeadershipState>,
    ) -> Result<(), LeaderElectionError> {
        let mut children: Vec<ElectionChild> =
            get_children(&self.zk, &self.node.election_prefix).await?;
        children.sort_unstable();

        debug!(participants = ?children, "got leader election participants");

        match children.iter().position(|node| node == &self.node) {
            Some(0) => {
                self.observe_leader(leader_sender).await?;
            }
            Some(index) => {
                self.observe_follower(&children[index - 1], leader_sender)
                    .await?;
            }
            None => return Err(LeaderElectionError::ZnodeNotFound),
        };

        Ok(())
    }

    async fn observe_leader(
        &self,
        leader_sender: &watch::Sender<LeadershipState>,
    ) -> Result<(), LeaderElectionError> {
        // start watching my own ephemeral node
        let (rx, stat) = self
            .zk
            .with_watcher()
            .exists(&self.node.full_path())
            .await?;
        if stat.is_none() {
            return Err(LeaderElectionError::ZnodeNotFound);
        }

        info!("i am the leader");
        leader_sender
            .send(LeadershipState::Leader)
            .map_err(|e| LeaderElectionError::SendError { source: e })?;

        let event = rx
            .await
            .map_err(|e| LeaderElectionError::Canceled { source: e })?;
        match (event.event_type, event.keeper_state) {
            (NodeDeleted, _) => {
                error!("Leader's ephemeral node was deleted");
                Err(LeaderElectionError::UnexpectedEvent { change: event })
            }
            (WatchedEventType::None, KeeperState::Expired | KeeperState::AuthFailed) => {
                Err(LeaderElectionError::UnexpectedEvent { change: event })
            }
            _ => {
                // transient disconnects, SyncConnected, SaslAuthenticated, NodeDataChanged events etc.
                debug!(?event, "retryable event, will retry");
                Ok(())
            }
        }
    }

    async fn observe_follower(
        &self,
        preceding_node: &ElectionChild,
        leader_sender: &watch::Sender<LeadershipState>,
    ) -> Result<(), LeaderElectionError> {
        let path = preceding_node.full_path();
        debug!(?path, "setting the watch for the preceding node");

        let (rx, stat) = self.zk.with_watcher().exists(&path).await?;
        if stat.is_none() {
            debug!("preceding node already gone, re-evaluating");
            return Ok(());
        }

        leader_sender
            .send(LeadershipState::Follower)
            .map_err(|e| LeaderElectionError::SendError { source: e })?;

        let event = rx
            .await
            .map_err(|e| LeaderElectionError::Canceled { source: e })?;
        match (event.event_type, event.keeper_state) {
            (NodeDeleted, _) => {
                debug!(?event, "the preceding node was removed");
                Ok(())
            }
            (WatchedEventType::None, KeeperState::Expired | KeeperState::AuthFailed) => {
                Err(LeaderElectionError::UnexpectedEvent { change: event })
            }
            _ => {
                // transient disconnects, SyncConnected, SaslAuthenticated, NodeDataChanged events, etc.
                debug!(?event, "retryable event, will retry");
                Ok(())
            }
        }
    }
}

#[derive(PartialEq, Eq, Debug)]
struct ElectionChild {
    election_prefix: String,
    path: String,
    guid: Uuid,
    seq: u32,
}

impl ElectionChild {
    fn try_from_full_path(full_path: &str, election_prefix: &str) -> Result<Self, crate::Error> {
        if !full_path.starts_with(election_prefix) {
            whatever!(
                "wrong format of a child node; must start with election prefix {election_prefix}"
            );
        }

        let Some(path) = full_path.strip_prefix(&format!("{}/", election_prefix)) else {
            whatever!("wrong format of a child node; must be `/prefix/path`");
        };

        ElectionChild::try_from_parts(election_prefix, path)
    }

    fn try_from_parts(prefix: &str, path: &str) -> Result<Self, crate::Error> {
        let path_parts = path.split("-n_").collect::<Vec<&str>>();
        if path_parts.len() != 2 {
            whatever!("wrong format of a child node's path; must `<guid>-n_<number>`");
        }
        let guid = Uuid::parse_str(path_parts[0]).whatever_context("Can't parse node's guid")?;
        let seq = path_parts[1].parse::<u32>().whatever_context(format!(
            "cant parse node's sequential number as u32 from prefix `{prefix}` and path `{path}`"
        ))?;

        Ok(Self {
            election_prefix: prefix.to_string(),
            path: path.to_string(),
            guid,
            seq,
        })
    }

    fn full_path(&self) -> String {
        format!("{}/{}", self.election_prefix, self.path)
    }
}

impl PartialOrd for ElectionChild {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for ElectionChild {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.seq.cmp(&other.seq)
    }
}

#[derive(Debug, Snafu)]
enum LeaderElectionError {
    #[snafu(
        display("Watch receiver canceled, server disconnected?"),
        context(false)
    )]
    Canceled {
        source: futures::channel::oneshot::Canceled,
    },
    #[snafu(display("ZooKeeper error: {source}"), context(false))]
    ZkError { source: crate::Error },
    #[snafu(display("Error while sending leadership state update"), context(false))]
    SendError {
        source: watch::error::SendError<LeadershipState>,
    },
    #[snafu(display("Ephemeral znode for leader election not found"))]
    ZnodeNotFound,
    #[snafu(display("Unexpected event received: {change:?}"))]
    UnexpectedEvent { change: WatchedEvent },
}

async fn get_children(
    zk: &ZooKeeper,
    election_node: &str,
) -> Result<Vec<ElectionChild>, crate::Error> {
    Ok(zk
        .get_children(election_node)
        .await?
        .unwrap_or_default()
        .into_iter()
        .filter_map(|s| {
            ElectionChild::try_from_parts(election_node, &s)
                .inspect_err(|e| warn!("skipping malformed election child {s:?}: {e}"))
                .ok()
        })
        .collect())
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::time::Duration;

    use super::*;
    use crate::{ZooKeeperBuilder, error::Create};

    #[test]
    fn parse_path() {
        init_tracing_subscriber();
        let guid = Uuid::new_v4();
        let ok_path = format!("/election/{}-n_000123", guid);
        let c = ElectionChild::try_from_full_path(&ok_path, "/election").unwrap();
        assert_eq!(c.election_prefix, "/election");
        assert_eq!(c.path, format!("{}-n_000123", guid));
        assert_eq!(c.guid, guid);
        assert_eq!(c.full_path(), format!("/election/{}-n_000123", guid));

        let bad_paths = vec![
            format!("/election//{guid}-n_00123"),
            format!("/election_bad/{guid}-n_00123"),
            "/election/bad_path".to_string(),
            "/election/bad-guid-n_00123".to_string(),
            format!("/election/{guid}-n_badnumber"),
        ];

        for path in bad_paths {
            assert!(ElectionChild::try_from_full_path(&path, "/election").is_err());
        }
    }

    #[tokio::test]
    async fn election_works() {
        let builder = ZooKeeperBuilder::default();
        let connect_addr: Vec<SocketAddr> = vec!["127.0.0.1:2181".parse().unwrap()];

        init_tracing_subscriber();

        let (zk1, _w) = builder.connect(connect_addr.clone()).await.unwrap();
        create_election_node(&zk1).await;
        let leader_election1 = LeaderElection::new(zk1, "/election", Acl::open_unsafe().to_vec());
        let (mut rx1, jh1) = leader_election1.volunteer().await.unwrap();
        tokio::time::timeout(Duration::from_secs(10), wait_for_leadership(&mut rx1))
            .await
            .expect("the first participant should be the leader");

        let (zk2, _w) = builder.connect(connect_addr).await.unwrap();
        let leader_election2 = LeaderElection::new(zk2, "/election", Acl::open_unsafe().to_vec());
        let (mut rx2, _jh2) = leader_election2.volunteer().await.unwrap();

        tokio::time::timeout(Duration::from_secs(10), wait_for_follower(&mut rx2))
            .await
            .expect("the second participant should become follower");

        jh1.abort();

        tokio::time::timeout(Duration::from_secs(10), wait_for_leadership(&mut rx2))
            .await
            .expect("the second participant should now become the leader");
    }

    fn init_tracing_subscriber() {
        let _ = tracing_subscriber::fmt()
            .with_max_level(tracing::Level::DEBUG)
            .try_init();
    }

    async fn create_election_node(zk: &ZooKeeper) {
        let res = zk
            .create(
                "/election",
                &b""[..],
                Acl::open_unsafe(),
                CreateMode::Persistent,
            )
            .await
            .unwrap();

        match res {
            Ok(_) => {}
            Err(e) if e == Create::NodeExists => {}
            Err(e) => panic!("{e}"),
        };
    }

    async fn wait_for_leadership(rx: &mut tokio::sync::watch::Receiver<LeadershipState>) {
        debug!("waiting for leadership");
        loop {
            let state = *rx.borrow_and_update();
            match state {
                LeadershipState::Leader => {
                    return;
                }
                LeadershipState::Uninitialized | LeadershipState::Follower => {
                    rx.changed().await.unwrap()
                }
                _ => panic!("wait for leadership error {state:?}"),
            }
        }
    }

    async fn wait_for_follower(rx: &mut tokio::sync::watch::Receiver<LeadershipState>) {
        debug!("waiting for follower");
        loop {
            let state = *rx.borrow_and_update();
            match state {
                LeadershipState::Leader | LeadershipState::Uninitialized => {
                    rx.changed().await.unwrap()
                }
                LeadershipState::Follower => {
                    return;
                }
                _ => panic!("wait for follower error {state:?}"),
            }
        }
    }
}
