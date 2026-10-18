//! Networking for the `iroh-gossip` protocol

use std::{
    collections::{BTreeSet, HashMap, HashSet, VecDeque},
    net::SocketAddr,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

use bytes::Bytes;
use futures_concurrency::stream::{stream_group, StreamGroup};
use iroh::{
    endpoint::Connection,
    protocol::{AcceptError, ProtocolHandler},
    Endpoint, EndpointAddr, EndpointId, PublicKey, RelayUrl, Watcher,
};
use irpc::WithChannels;
use n0_error::{e, stack_error};
use n0_future::{
    task::{self, AbortOnDropHandle, JoinSet},
    time::{Duration, Instant},
    Stream, StreamExt as _,
};
use rand::{rngs::StdRng, SeedableRng};
use serde::{Deserialize, Serialize};
use tokio::sync::{broadcast, mpsc, oneshot};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, error_span, trace, warn, Instrument};

use self::{
    address_lookup::GossipAddressLookup,
    conns::{Action, Conn, ConnSet},
    util::{RecvLoop, SendLoop, Timers},
};
use crate::{
    api::{self, Command, Event, GossipApi, RpcMessage},
    metrics::Metrics,
    proto::{self, HyparviewConfig, PeerData, PlumtreeConfig, Scope, TopicId},
};

mod address_lookup;
mod conns;
mod util;

/// ALPN protocol name
pub const GOSSIP_ALPN: &[u8] = b"/iroh-gossip/1";

/// Channel capacity for the send queue (one per connection).
///
/// The actor never waits for this queue, see [`Actor::handle_in_event_inner`]. It holds a burst:
/// one anti-entropy answer of a large mesh is 64 broadcasts, and each goes to every eager peer.
const SEND_QUEUE_CAP: usize = 2048;
/// Cap of the queue of messages for a peer that we dial and are not connected to yet.
const PENDING_QUEUE_CAP: usize = SEND_QUEUE_CAP;
/// How long the send queue of a peer may refuse messages, with no message accepted in between,
/// before we disconnect the peer.
///
/// Longer than the pauses of a healthy link, such as the switch of a connection between a relay
/// and a direct path (a few seconds), and much shorter than the idle timeout of the connection,
/// after which a stuck peer is closed anyway (more than 100 s).
const SEND_REFUSED_DEADLINE: Duration = Duration::from_secs(10);
/// Channel capacity for the ToActor message queue (single)
const TO_ACTOR_CAP: usize = 64;
/// Channel capacity for the InEvent message queue (single)
const IN_EVENT_CAP: usize = 1024;
/// Channel capacity for broadcast subscriber event queue (one per topic)
const TOPIC_EVENT_CAP: usize = 256;

/// Events emitted from the gossip protocol
pub type ProtoEvent = proto::Event<PublicKey>;
/// Commands for the gossip protocol
pub type ProtoCommand = proto::Command<PublicKey>;

type InEvent = proto::InEvent<PublicKey>;
type OutEvent = proto::OutEvent<PublicKey>;
type Timer = proto::Timer<PublicKey>;
type ProtoMessage = proto::Message<PublicKey>;

/// Publish and subscribe on gossiping topics.
///
/// Each topic is a separate broadcast tree with separate memberships.
///
/// A topic has to be joined before you can publish or subscribe on the topic.
/// To join the swarm for a topic, you have to know the [`PublicKey`] of at least one peer that also joined the topic.
///
/// Messages published on the swarm will be delivered to all peers that joined the swarm for that
/// topic. You will also be relaying (gossiping) messages published by other peers.
///
/// With the default settings, the protocol will maintain up to 5 peer connections per topic.
///
/// Even though the [`Gossip`] is created from a [`Endpoint`], it does not accept connections
/// itself. You should run an accept loop on the [`Endpoint`] yourself, check the ALPN protocol of incoming
/// connections, and if the ALPN protocol equals [`GOSSIP_ALPN`], forward the connection to the
/// gossip actor through [Self::handle_connection].
///
/// The gossip actor will, however, initiate new connections to other peers by itself.
#[derive(Debug, Clone)]
pub struct Gossip {
    pub(crate) inner: Arc<Inner>,
}

impl std::ops::Deref for Gossip {
    type Target = GossipApi;
    fn deref(&self) -> &Self::Target {
        &self.inner.api
    }
}

#[derive(Debug)]
enum LocalActorMessage {
    HandleConnection(Connection),
    Shutdown { reply: oneshot::Sender<()> },
}

#[allow(missing_docs)]
#[stack_error(derive, add_meta)]
#[non_exhaustive]
pub enum Error {
    ActorDropped {},
}

impl<T> From<mpsc::error::SendError<T>> for Error {
    fn from(_value: mpsc::error::SendError<T>) -> Self {
        e!(Error::ActorDropped)
    }
}
impl From<oneshot::error::RecvError> for Error {
    fn from(_value: oneshot::error::RecvError) -> Self {
        e!(Error::ActorDropped)
    }
}

#[derive(Debug)]
pub(crate) struct Inner {
    api: GossipApi,
    local_tx: mpsc::Sender<LocalActorMessage>,
    _actor_handle: AbortOnDropHandle<()>,
    max_message_size: usize,
    metrics: Arc<Metrics>,
}

impl ProtocolHandler for Gossip {
    async fn accept(&self, connection: Connection) -> Result<(), AcceptError> {
        self.handle_connection(connection)
            .await
            .map_err(AcceptError::from_err)?;
        Ok(())
    }

    async fn shutdown(&self) {
        if let Err(err) = self.shutdown().await {
            warn!("error while shutting down gossip: {err:#}");
        }
    }
}

/// Builder to configure and construct [`Gossip`].
#[derive(Debug, Clone)]
pub struct Builder {
    config: proto::Config,
    alpn: Option<Bytes>,
}

impl Builder {
    /// Sets the maximum message size in bytes.
    /// By default this is `4096` bytes.
    pub fn max_message_size(mut self, size: usize) -> Self {
        self.config.max_message_size = size;
        self
    }

    /// Set the membership configuration.
    pub fn membership_config(mut self, config: HyparviewConfig) -> Self {
        self.config.membership = config;
        self
    }

    /// Set the broadcast configuration.
    pub fn broadcast_config(mut self, config: PlumtreeConfig) -> Self {
        self.config.broadcast = config;
        self
    }

    /// Set the ALPN this gossip instance uses.
    ///
    /// It has to be the same for all peers in the network. If you set a custom ALPN,
    /// you have to use the same ALPN when registering the [`Gossip`] in on a iroh
    /// router with [`RouterBuilder::accept`].
    ///
    /// [`RouterBuilder::accept`]: iroh::protocol::RouterBuilder::accept
    pub fn alpn(mut self, alpn: impl AsRef<[u8]>) -> Self {
        self.alpn = Some(alpn.as_ref().to_vec().into());
        self
    }

    /// Spawn a gossip actor and get a handle for it
    pub fn spawn(self, endpoint: Endpoint) -> Gossip {
        let metrics = Arc::new(Metrics::default());
        let address_lookup = GossipAddressLookup::default();

        // `Endpoint::address_lookup` returns `Err` when the endpoint is closed.
        // In that case, the gossip actor will close too very soon for other reasons,
        // so it's fine if we only add our `GossipAddressLookup` for the non-closed
        // case. The alternative would be to return a `Result` from `spawn`,
        // but as long as this is the only direct error case, it seem unwarranted.
        if let Ok(endpoint_addr_lookup) = endpoint.address_lookup().as_ref() {
            endpoint_addr_lookup.add(address_lookup.clone());
        }
        let (actor, rpc_tx, local_tx) = Actor::new(
            endpoint,
            self.config,
            metrics.clone(),
            self.alpn,
            address_lookup,
        );
        let me = actor.endpoint.id().fmt_short();
        let max_message_size = actor.state.max_message_size();

        let actor_handle = task::spawn(actor.run().instrument(error_span!("gossip", %me)));

        let api = GossipApi::local(rpc_tx);

        Gossip {
            inner: Inner {
                api,
                local_tx,
                _actor_handle: AbortOnDropHandle::new(actor_handle),
                max_message_size,
                metrics,
            }
            .into(),
        }
    }
}

impl Gossip {
    /// Creates a default `Builder`, with the endpoint set.
    pub fn builder() -> Builder {
        Builder {
            config: Default::default(),
            alpn: None,
        }
    }

    /// Listen on a noq endpoint for incoming RPC connections.
    #[cfg(feature = "rpc")]
    pub async fn listen(self, endpoint: noq::Endpoint) {
        self.inner.api.listen(endpoint).await
    }

    /// Get the maximum message size configured for this gossip actor.
    pub fn max_message_size(&self) -> usize {
        self.inner.max_message_size
    }

    /// Handle an incoming [`Connection`].
    ///
    /// Make sure to check the ALPN protocol yourself before passing the connection.
    pub async fn handle_connection(&self, conn: Connection) -> Result<(), Error> {
        self.inner
            .local_tx
            .send(LocalActorMessage::HandleConnection(conn))
            .await?;
        Ok(())
    }

    /// Shutdown the gossip instance.
    ///
    /// This leaves all topics, sending `Disconnect` messages to peers, and then
    /// stops the gossip actor loop and drops all state and connections.
    pub async fn shutdown(&self) -> Result<(), Error> {
        let (reply, reply_rx) = oneshot::channel();
        self.inner
            .local_tx
            .send(LocalActorMessage::Shutdown { reply })
            .await?;
        reply_rx.await?;
        Ok(())
    }

    /// Returns the metrics tracked for this gossip instance.
    pub fn metrics(&self) -> &Arc<Metrics> {
        &self.inner.metrics
    }
}

/// Actor that sends and handles messages between the connection and main state loops
struct Actor {
    alpn: Bytes,
    /// Protocol state
    state: proto::State<PublicKey, StdRng>,
    /// The endpoint through which we dial peers
    endpoint: Endpoint,
    /// Dial machine to connect to peers
    dialer: Dialer,
    /// Input messages to the actor
    rpc_rx: mpsc::Receiver<RpcMessage>,
    local_rx: mpsc::Receiver<LocalActorMessage>,
    /// Sender for the state input (cloned into the connection loops)
    in_event_tx: mpsc::Sender<InEvent>,
    /// Input events to the state (emitted from the connection loops)
    in_event_rx: mpsc::Receiver<InEvent>,
    /// Sender for the first message that a connection read from its peer (cloned into the
    /// connection loops).
    carried_tx: mpsc::Sender<(EndpointId, ConnId)>,
    /// See [`Actor::carried_tx`].
    carried_rx: mpsc::Receiver<(EndpointId, ConnId)>,
    /// When the connections of the peers are looked at next, for the ones that are parked too long
    /// or went silent. See [`conns::PARK`].
    next_conns_check: Instant,
    /// Queued timers
    timers: Timers<Timer>,
    /// Map of topics to their state.
    topics: HashMap<TopicId, TopicState>,
    /// Map of peers to their state.
    peers: HashMap<EndpointId, PeerState>,
    /// Stream of commands from topic handles.
    command_rx: stream_group::Keyed<TopicCommandStream>,
    /// Internal queue of topic to close because all handles were dropped.
    quit_queue: VecDeque<TopicId>,
    /// Tasks for the connection loops, to keep track of panics.
    connection_tasks: JoinSet<(EndpointId, Connection, Result<(), ConnectionLoopError>)>,
    metrics: Arc<Metrics>,
    topic_event_forwarders: JoinSet<TopicId>,
    address_lookup: GossipAddressLookup,
    /// Peers to disconnect after the current event: their send queue refused a message that
    /// must not be dropped, or refused messages for longer than `send_refused_deadline`.
    overflow_disconnects: VecDeque<EndpointId>,
    /// Connections whose send loop ended while the actor still sent on them, to drop after the
    /// current event. The result of their task is read by an arm of the event loop that sits behind
    /// the arms that carry data, so a busy actor can send to the dead connection for a long time
    /// before that arm runs.
    dead_conns: VecDeque<(EndpointId, ConnId)>,
    /// See [`SEND_REFUSED_DEADLINE`]. A field so that a test can shorten it.
    send_refused_deadline: Duration,
}

impl Actor {
    fn new(
        endpoint: Endpoint,
        config: proto::Config,
        metrics: Arc<Metrics>,
        alpn: Option<Bytes>,
        address_lookup: GossipAddressLookup,
    ) -> (
        Self,
        mpsc::Sender<RpcMessage>,
        mpsc::Sender<LocalActorMessage>,
    ) {
        let peer_id = endpoint.id();
        let dialer = Dialer::new(endpoint.clone());
        let state = proto::State::new(
            peer_id,
            Default::default(),
            config,
            rand::rngs::StdRng::from_rng(&mut rand::rng()),
        );
        let (rpc_tx, rpc_rx) = mpsc::channel(TO_ACTOR_CAP);
        let (local_tx, local_rx) = mpsc::channel(16);
        let (in_event_tx, in_event_rx) = mpsc::channel(IN_EVENT_CAP);
        let (carried_tx, carried_rx) = mpsc::channel(IN_EVENT_CAP);

        let actor = Actor {
            alpn: alpn.unwrap_or_else(|| GOSSIP_ALPN.to_vec().into()),
            endpoint,
            state,
            dialer,
            rpc_rx,
            in_event_rx,
            in_event_tx,
            carried_tx,
            carried_rx,
            next_conns_check: Instant::now() + CONNS_CHECK_INTERVAL,
            timers: Timers::new(),
            command_rx: StreamGroup::new().keyed(),
            peers: Default::default(),
            topics: Default::default(),
            quit_queue: Default::default(),
            connection_tasks: Default::default(),
            metrics,
            local_rx,
            topic_event_forwarders: Default::default(),
            address_lookup,
            overflow_disconnects: Default::default(),
            dead_conns: Default::default(),
            send_refused_deadline: SEND_REFUSED_DEADLINE,
        };

        (actor, rpc_tx, local_tx)
    }

    pub async fn run(mut self) {
        let mut addr_update_stream = self.setup().await;

        let mut i = 0;
        while self.event_loop(&mut addr_update_stream, i).await {
            i += 1;
        }
    }

    /// Performs the initial actor setup to run the [`Actor::event_loop`].
    ///
    /// This updates our current address and return it. It also returns the home relay stream and
    /// direct addr stream.
    async fn setup(&mut self) -> impl Stream<Item = EndpointAddr> + Send + Unpin + use<> {
        let addr_update_stream = self.endpoint.watch_addr().stream();
        let initial_addr = self.endpoint.addr();
        self.handle_addr_update(initial_addr).await;
        addr_update_stream
    }

    /// One event loop processing step.
    ///
    /// None is returned when no further processing should be performed.
    async fn event_loop(
        &mut self,
        addr_updates: &mut (impl Stream<Item = EndpointAddr> + Send + Unpin),
        i: usize,
    ) -> bool {
        self.metrics.actor_tick_main.inc();
        tokio::select! {
            biased;
            conn = self.local_rx.recv() => {
                match conn {
                    Some(LocalActorMessage::Shutdown { reply }) => {
                        debug!("received shutdown message, quit all topics");
                        self.quit_queue.extend(self.topics.keys().copied());
                        self.process_quit_queue().await;
                        debug!("all topics quit, stop gossip actor");
                        reply.send(()).ok();
                        return false;
                    },
                    Some(LocalActorMessage::HandleConnection(conn)) => {
                        self.handle_connection(conn.remote_id(), ConnOrigin::Accept, conn);
                    }
                    None => {
                        debug!("all gossip handles dropped, stop gossip actor");
                        return false;
                    }
                }
            }
            msg = self.rpc_rx.recv() => {
                trace!(?i, "tick: to_actor_rx");
                self.metrics.actor_tick_rx.inc();
                match msg {
                    Some(msg) => {
                        self.handle_rpc_msg(msg, Instant::now()).await;
                    }
                    None => {
                        debug!("all gossip handles dropped, stop gossip actor");
                        return false;
                    }
                }
            },
            Some((key, (topic, command))) = self.command_rx.next(), if !self.command_rx.is_empty() => {
                trace!(?i, "tick: command_rx");
                self.handle_command(topic, key, command).await;
            },
            Some(new_address) = addr_updates.next() => {
                trace!(?i, "tick: new_address");
                self.metrics.actor_tick_endpoint.inc();
                self.handle_addr_update(new_address).await;
            }
            (peer_id, res) = self.dialer.next_conn() => {
                trace!(?i, "tick: dialer");
                self.metrics.actor_tick_dialer.inc();
                match res {
                    Some(Ok(conn)) => {
                        self.dialer.no_address.forget(peer_id);
                        debug!(peer = %peer_id.fmt_short(), "dial successful");
                        self.metrics.actor_tick_dialer_success.inc();
                        self.handle_connection(peer_id, ConnOrigin::Dial, conn);
                    }
                    Some(Err(err)) => {
                        if dial_had_no_address(&err) {
                            if self.dialer.no_address.note(peer_id, Instant::now()) {
                                warn!(
                                    peer = %peer_id.fmt_short(),
                                    "dial without an address for over {NO_ADDRESS_WARN_AFTER:?}: {err}"
                                );
                            } else {
                                debug!(peer = %peer_id.fmt_short(), "dial without an address: {err}");
                            }
                        } else {
                            self.dialer.no_address.forget(peer_id);
                            if dial_was_refused(&err) {
                                debug!(peer = %peer_id.fmt_short(), "dial refused: {err}");
                            } else if dial_timed_out(&err) && !self.a_topic_is_isolated() {
                                debug!(peer = %peer_id.fmt_short(), "dial failed: {err}");
                            } else {
                                warn!(peer = %peer_id.fmt_short(), "dial failed: {err}");
                            }
                        }
                        self.metrics.actor_tick_dialer_failure.inc();
                        let peer_state = self.peers.get(&peer_id);
                        let is_active = matches!(peer_state, Some(PeerState::Active { .. }));
                        if !is_active {
                            self.handle_in_event(InEvent::PeerDisconnected(peer_id), Instant::now())
                                .await;
                        }
                    }
                    None => {
                        warn!(peer = %peer_id.fmt_short(), "dial disconnected");
                        self.metrics.actor_tick_dialer_failure.inc();
                    }
                }
            }
            event = self.in_event_rx.recv() => {
                trace!(?i, "tick: in_event_rx");
                self.metrics.actor_tick_in_event_rx.inc();
                let event = event.expect("unreachable: in_event_tx is never dropped before receiver");
                self.drain_carried();
                self.handle_in_event(event, Instant::now()).await;
            }
            _ = self.timers.wait_next() => {
                trace!(?i, "tick: timers");
                self.metrics.actor_tick_timers.inc();
                let now = Instant::now();
                while let Some((_instant, timer)) = self.timers.pop_before(now) {
                    self.handle_in_event(InEvent::TimerExpired(timer), now).await;
                }
            }
            Some(res) = self.connection_tasks.join_next(), if !self.connection_tasks.is_empty() => {
                trace!(?i, "tick: connection_tasks");
                let (peer_id, conn, result) = res.expect("connection task panicked");
                self.handle_connection_task_finished(peer_id, conn, result).await;
            }
            _ = n0_future::time::sleep(self.next_conns_check.saturating_duration_since(Instant::now())) => {
                let now = Instant::now();
                self.next_conns_check = now + CONNS_CHECK_INTERVAL;
                self.drain_carried();
                self.check_conns(now);
            }
            Some(res) = self.topic_event_forwarders.join_next(), if !self.topic_event_forwarders.is_empty() => {
                let topic_id = res.expect("topic event forwarder panicked");
                if let Some(state) = self.topics.get_mut(&topic_id) {
                    if !state.still_needed() {
                        self.quit_queue.push_back(topic_id);
                        self.process_quit_queue().await;
                    }
                }
            }
        }

        true
    }

    async fn handle_addr_update(&mut self, endpoint_addr: EndpointAddr) {
        // let peer_data = our_peer_data(&self.endpoint, current_addresses);
        let peer_data = encode_peer_data(&endpoint_addr.into());
        self.handle_in_event(InEvent::UpdatePeerData(peer_data), Instant::now())
            .await
    }

    async fn handle_command(
        &mut self,
        topic: TopicId,
        key: stream_group::Key,
        command: Option<Command>,
    ) {
        debug!(?topic, ?key, ?command, "handle command");
        let Some(state) = self.topics.get_mut(&topic) else {
            // TODO: unreachable?
            warn!("received command for unknown topic");
            return;
        };
        match command {
            Some(command) => {
                let command = match command {
                    Command::Broadcast(message) => ProtoCommand::Broadcast(message, Scope::Swarm),
                    Command::BroadcastNeighbors(message) => {
                        ProtoCommand::Broadcast(message, Scope::Neighbors)
                    }
                    Command::JoinPeers(peers) => {
                        let now = Instant::now();
                        for peer in &peers {
                            self.dialer.replace_old_dial(*peer, now);
                        }
                        ProtoCommand::Join(peers)
                    }
                    Command::LeavePeers(peers) => ProtoCommand::LeavePeers(peers),
                    Command::NeighborPeers(peers) => ProtoCommand::NeighborPeers(peers),
                };
                self.handle_in_event(proto::InEvent::Command(topic, command), Instant::now())
                    .await;
            }
            None => {
                state.command_rx_keys.remove(&key);
                if !state.still_needed() {
                    self.quit_queue.push_back(topic);
                    self.process_quit_queue().await;
                }
            }
        }
    }

    fn handle_connection(&mut self, peer_id: EndpointId, origin: ConnOrigin, conn: Connection) {
        let (send_tx, send_rx) = mpsc::channel(SEND_QUEUE_CAP);
        let rx_clock = RxClock::new();
        let dialed_by_lower =
            dialed_by_lower_id(self.endpoint.id(), peer_id, conn.side().is_client());
        let entry = Conn::new(
            conn.stable_id(),
            ConnHandle {
                send_tx: Some(send_tx),
                conn: Some(conn.clone()),
            },
            dialed_by_lower,
            rx_clock.clone(),
        );
        // A connection that is not chosen gets a sender too and keeps reading: the peer may send
        // on it. See [`conns`].
        let (queue, actions) = self
            .peers
            .entry(peer_id)
            .or_default()
            .admit(entry, Instant::now());
        self.apply_actions(peer_id, actions);

        let max_message_size = self.state.max_message_size();
        let link = ActorLink {
            in_event_tx: self.in_event_tx.clone(),
            carried_tx: self.carried_tx.clone(),
        };

        // Spawn a task for this connection
        self.connection_tasks.spawn(
            async move {
                // The sampler never ends, so it stops when the loop does.
                let res = tokio::select! {
                    res = connection_loop(
                        peer_id,
                        conn.clone(),
                        origin,
                        send_rx,
                        link,
                        max_message_size,
                        queue,
                    ) => res,
                    never = sample_rx(conn.clone(), rx_clock) => match never {},
                };
                (peer_id, conn, res)
            }
            .instrument(error_span!("conn", peer = %peer_id.fmt_short())),
        );
    }

    /// Does what the set of connections of a peer asks for after a change.
    fn apply_actions(&mut self, peer_id: EndpointId, actions: Vec<Action>) {
        let Some(PeerState::Active {
            conns,
            refused_since,
        }) = self.peers.get_mut(&peer_id)
        else {
            return;
        };
        for action in actions {
            match action {
                Action::Choose(conn_id) => {
                    debug!(conn_id, "send on this connection from now on");
                    *refused_since = None;
                }
                Action::StopSending(conn_id) => {
                    debug!(
                        conn_id,
                        "stop sending on this connection: it drains and closes"
                    );
                    if let Some(conn) = conns.get_mut(conn_id) {
                        conn.handle.send_tx = None;
                    }
                }
            }
        }
    }

    /// Takes in what the connections carried. A reader sends this signal before it sends the
    /// message, so a message that arrives finds its connection already chosen, or its answer goes to
    /// the one that the peer left. The signal has no select arm of its own: an arm would be one more
    /// step of the event loop, and the tests that drive the loop by hand count steps.
    fn drain_carried(&mut self) {
        while let Ok((peer_id, conn_id)) = self.carried_rx.try_recv() {
            self.handle_carried(peer_id, conn_id);
        }
    }

    /// A message of the peer was read on a connection.
    fn handle_carried(&mut self, peer_id: EndpointId, conn_id: ConnId) {
        let Some(PeerState::Active { conns, .. }) = self.peers.get_mut(&peer_id) else {
            return;
        };
        let actions = conns.carried(conn_id, Instant::now());
        self.apply_actions(peer_id, actions);
    }

    /// Looks at the connections of every peer: one can have gone silent, or have been parked for
    /// too long.
    fn check_conns(&mut self, now: Instant) {
        let mut changes = Vec::new();
        for (peer_id, state) in &mut self.peers {
            if let PeerState::Active { conns, .. } = state {
                let actions = conns.tick(now);
                if !actions.is_empty() {
                    changes.push((*peer_id, actions));
                }
            }
        }
        for (peer_id, actions) in changes {
            self.apply_actions(peer_id, actions);
        }
    }

    #[tracing::instrument(name = "conn", skip_all, fields(peer = %peer_id.fmt_short()))]
    async fn handle_connection_task_finished(
        &mut self,
        peer_id: EndpointId,
        conn: Connection,
        task_result: Result<(), ConnectionLoopError>,
    ) {
        if conn.close_reason().is_none() {
            conn.close(0u32.into(), b"close from disconnect");
        }
        let reason = conn.close_reason().expect("just closed");
        let error = task_result.err();
        debug!(%reason, ?error, "connection closed");
        self.drop_conn(peer_id, conn.stable_id()).await;
        self.drain_queues().await;
    }

    /// Takes the closed connection `conn_id` out of the set of `peer_id`. A peer with no connection
    /// left is removed and the protocol is told `PeerDisconnected`. A peer with another connection
    /// keeps it, and the set chooses it. The caller runs [`Self::drain_queues`] after it: this
    /// takes `handle_in_event_inner`, not `handle_in_event`, because `process_dead_conns` calls it
    /// from inside `handle_in_event`, and an async fn cannot call itself.
    async fn drop_conn(&mut self, peer_id: EndpointId, conn_id: ConnId) {
        if let Some(PeerState::Active { conns, .. }) = self.peers.get_mut(&peer_id) {
            let (actions, none_left) = conns.closed(conn_id, Instant::now());
            if none_left {
                debug!(
                    conn_id,
                    "last connection to send on closed, mark peer as disconnected"
                );
                // Remove the entry here rather than wait for `DisconnectPeer`:
                // a connection that dies before the gossip handshake never made
                // the peer a proto neighbor, so the proto never asks to
                // disconnect it, and the dead sender swallows every later send
                // instead of letting the next one re-dial.
                self.peers.remove(&peer_id);
                self.handle_in_event_inner(InEvent::PeerDisconnected(peer_id), Instant::now())
                    .await;
            } else {
                // Another connection takes over, or this one was not the one sends went to. The
                // protocol is told nothing: it sees one peer.
                debug!(conn_id, "connection closed, others left");
                self.apply_actions(peer_id, actions);
            }
        } else {
            debug!("peer already marked as disconnected");
        }
    }

    async fn handle_rpc_msg(&mut self, msg: RpcMessage, now: Instant) {
        trace!("handle to_actor  {msg:?}");
        match msg {
            RpcMessage::Join(msg) => {
                let WithChannels {
                    inner,
                    rx,
                    tx,
                    // TODO(frando): make use of span?
                    span: _,
                } = msg;
                let api::JoinRequest {
                    topic_id,
                    bootstrap,
                } = inner;
                let TopicState {
                    neighbors,
                    event_sender,
                    command_rx_keys,
                    ..
                } = self.topics.entry(topic_id).or_default();
                let mut sender_dead = false;
                if !neighbors.is_empty() {
                    for neighbor in neighbors.iter() {
                        if let Err(_err) = tx.try_send(Event::NeighborUp(*neighbor)).await {
                            sender_dead = true;
                            break;
                        }
                    }
                }

                if !sender_dead {
                    let subscriber_fut = topic_subscriber_loop(tx, event_sender.subscribe());
                    let fut = async move {
                        subscriber_fut.await;
                        topic_id
                    };
                    self.topic_event_forwarders
                        .spawn(fut.instrument(tracing::Span::current()));
                }
                let command_rx = TopicCommandStream::new(topic_id, Box::pin(rx.into_stream()));
                let key = self.command_rx.insert(command_rx);
                command_rx_keys.insert(key);

                self.handle_in_event(
                    InEvent::Command(
                        topic_id,
                        ProtoCommand::Join(bootstrap.into_iter().collect()),
                    ),
                    now,
                )
                .await;
            }
        }
    }

    /// Whether a topic has had no neighbor for longer than [`ISOLATED_WARN_AFTER`]. It answers
    /// `true` once per topic and spell. The dial does not tell its topic, so any isolated topic
    /// lets the warning through.
    fn a_topic_is_isolated(&mut self) -> bool {
        let now = Instant::now();
        self.topics
            .values_mut()
            .any(|topic| topic.isolation.warn_due(now))
    }

    async fn handle_in_event(&mut self, event: InEvent, now: Instant) {
        self.handle_in_event_inner(event, now).await;
        self.drain_queues().await;
    }

    /// What an event leaves for after it: the peers to disconnect, the connections to drop and the
    /// topics to quit.
    async fn drain_queues(&mut self) {
        self.process_overflow_disconnects().await;
        self.process_dead_conns().await;
        self.process_quit_queue().await;
    }

    /// Drops the connections whose send loop ended, as the result of their task would
    /// (`handle_connection_task_finished`). The result of the task, when it comes, finds the
    /// connection gone.
    async fn process_dead_conns(&mut self) {
        while let Some((peer_id, conn_id)) = self.dead_conns.pop_front() {
            let Some(PeerState::Active { conns, .. }) = self.peers.get_mut(&peer_id) else {
                continue;
            };
            let Some(entry) = conns.get_mut(conn_id) else {
                continue;
            };
            if let Some(conn) = entry.handle.conn.as_ref() {
                if conn.close_reason().is_none() {
                    conn.close(0u32.into(), b"send loop ended");
                }
            }
            self.drop_conn(peer_id, conn_id).await;
        }
    }

    /// Disconnect the peers whose send queue refused a message that must not be dropped, or
    /// refused messages for longer than the deadline.
    ///
    /// This takes the path of a lost connection (`handle_connection_task_finished`): the peer
    /// entry goes, the connection is closed, and the protocol is told `PeerDisconnected`, so
    /// that both sides see `NeighborDown` and `HyParView` refills the view.
    async fn process_overflow_disconnects(&mut self) {
        while let Some(peer_id) = self.overflow_disconnects.pop_front() {
            self.metrics.send_overflow_disconnects.inc();
            let disconnects = self.metrics.send_overflow_disconnects.get();
            if disconnects.is_power_of_two() {
                warn!(
                    peer = %peer_id.fmt_short(),
                    disconnects,
                    "disconnect: the send queue of the peer refused a message for too long, or one that must not be dropped",
                );
            }
            if let Some(PeerState::Active { mut conns, .. }) = self.peers.remove(&peer_id) {
                let chosen = conns.chosen().map(|conn| conn.id);
                for conn in conns.drain_all() {
                    if Some(conn.id) == chosen {
                        if let Some(conn) = conn.handle.conn {
                            conn.close(0u32.into(), b"send queue refused");
                        }
                    }
                }
            }
            self.handle_in_event_inner(InEvent::PeerDisconnected(peer_id), Instant::now())
                .await;
        }
    }

    async fn process_quit_queue(&mut self) {
        while let Some(topic_id) = self.quit_queue.pop_front() {
            self.handle_in_event_inner(
                InEvent::Command(topic_id, ProtoCommand::Quit),
                Instant::now(),
            )
            .await;
            if self.topics.remove(&topic_id).is_some() {
                tracing::debug!(%topic_id, "publishers and subscribers gone; unsubscribing");
            }
        }
    }

    // Stays `async`: it must never wait on a peer, and the test
    // `a_peer_with_a_full_send_queue_does_not_stall_the_actor` fails it with a timeout if it does.
    #[allow(clippy::unused_async)]
    async fn handle_in_event_inner(&mut self, event: InEvent, now: Instant) {
        if matches!(event, InEvent::TimerExpired(_)) {
            trace!(?event, "handle in_event");
        } else {
            debug!(?event, "handle in_event");
        };
        let out = self.state.handle(event, now, Some(&self.metrics));
        for event in out {
            if matches!(event, OutEvent::ScheduleTimer(_, _)) {
                trace!(?event, "handle out_event");
            } else {
                debug!(?event, "handle out_event");
            };
            match event {
                OutEvent::SendMessage(peer_id, message) => {
                    // The actor never waits for a peer: it is one task, and a peer that does
                    // not drain its queue would stop it from serving every other peer.
                    // A data message that does not fit is dropped (the protocol repairs it).
                    // A message that must not be lost leads to a disconnect, so that both sides
                    // see the same state. See `ProtoMessage::is_droppable`.
                    let state = self.peers.entry(peer_id).or_default();
                    match state {
                        PeerState::Active {
                            conns,
                            refused_since,
                        } => match send_on_chosen(conns, message) {
                            Ok(()) => *refused_since = None,
                            Err(mpsc::error::TrySendError::Full(message)) => {
                                let since = *refused_since.get_or_insert_with(Instant::now);
                                if message.is_droppable()
                                    && since.elapsed() < self.send_refused_deadline
                                {
                                    self.metrics.msgs_dropped_send_queue_full.inc();
                                    let dropped = self.metrics.msgs_dropped_send_queue_full.get();
                                    // Log the first drop and then each power of two.
                                    if dropped.is_power_of_two() {
                                        warn!(
                                            peer = %peer_id.fmt_short(),
                                            dropped,
                                            "dropped a data message: the send queue of the connection is full",
                                        );
                                    }
                                } else if !self.overflow_disconnects.contains(&peer_id) {
                                    self.overflow_disconnects.push_back(peer_id);
                                }
                            }
                            Err(mpsc::error::TrySendError::Closed(_message)) => {
                                // The send loop of the chosen connection ended. Waiting for
                                // [`Self::handle_connection_task_finished`] is not enough: the arm
                                // that reads the result of the task is behind the arms that carry
                                // data, and the protocol sends to this peer until that arm runs. So
                                // the connection is dropped after this event, once, and the log has
                                // one line for each connection, not one for each message.
                                match conns.chosen().map(|conn| conn.id) {
                                    Some(conn_id) => {
                                        if !self.dead_conns.contains(&(peer_id, conn_id)) {
                                            self.dead_conns.push_back((peer_id, conn_id));
                                            if ended_without_a_fault(conns.chosen()) {
                                                debug!(
                                                    peer = %peer_id.fmt_short(),
                                                    conn_id,
                                                    "send on a connection that ended without a fault",
                                                );
                                            } else {
                                                warn!(
                                                    peer = %peer_id.fmt_short(),
                                                    conn_id,
                                                    "failed to send: connection task send loop terminated",
                                                );
                                            }
                                        }
                                    }
                                    None => warn!(
                                        peer = %peer_id.fmt_short(),
                                        "failed to send: connection task send loop terminated",
                                    ),
                                }
                            }
                        },
                        PeerState::Pending { queue } => {
                            // Dial on every send, not only on an empty queue: a failed
                            // dial leaves the queue non-empty, which used to stop every
                            // later send from dialing. `Dialer::queue_dial` already
                            // returns early while a dial is in flight, so it is the
                            // guard here, not the queue.
                            if !self.dialer.is_pending(peer_id) {
                                debug!(peer = %peer_id.fmt_short(), "start to dial");
                            }
                            self.dialer.queue_dial(peer_id, self.alpn.clone());
                            if queue.len() < PENDING_QUEUE_CAP {
                                queue.push(message);
                            } else if message.is_droppable() {
                                self.metrics.msgs_dropped_send_queue_full.inc();
                            } else if !self.overflow_disconnects.contains(&peer_id) {
                                self.overflow_disconnects.push_back(peer_id);
                            }
                        }
                    }
                }
                OutEvent::EmitEvent(topic_id, event) => {
                    let Some(state) = self.topics.get_mut(&topic_id) else {
                        // TODO: unreachable?
                        warn!(?topic_id, "gossip state emitted event for unknown topic");
                        continue;
                    };
                    let TopicState {
                        neighbors,
                        event_sender,
                        isolation,
                        ..
                    } = state;
                    match &event {
                        ProtoEvent::NeighborUp(neighbor) => {
                            neighbors.insert(*neighbor);
                            isolation.neighbors_changed(true, now);
                        }
                        ProtoEvent::NeighborDown(neighbor) => {
                            neighbors.remove(neighbor);
                            isolation.neighbors_changed(!neighbors.is_empty(), now);
                        }
                        _ => {}
                    }
                    event_sender.send(event).ok();
                    if !state.still_needed() {
                        self.quit_queue.push_back(topic_id);
                    }
                }
                OutEvent::ScheduleTimer(delay, timer) => {
                    self.timers.insert(now + delay, timer);
                }
                OutEvent::DisconnectPeer(peer_id) => {
                    // signal disconnection by dropping the senders to the connection
                    debug!(peer=%peer_id.fmt_short(), "gossip state indicates disconnect: drop peer");
                    self.peers.remove(&peer_id);
                }
                OutEvent::PeerData(endpoint_id, data) => match decode_peer_data(&data) {
                    Err(err) => warn!("Failed to decode {data:?} from {endpoint_id}: {err}"),
                    Ok(info) => {
                        debug!(peer = ?endpoint_id, "add known addrs: {info:?}");
                        let mut endpoint_addr = EndpointAddr::new(endpoint_id);
                        for addr in info.direct_addresses {
                            endpoint_addr = endpoint_addr.with_ip_addr(addr);
                        }
                        if let Some(relay_url) = info.relay_url {
                            endpoint_addr = endpoint_addr.with_relay_url(relay_url);
                        }

                        self.address_lookup.add(endpoint_addr);
                    }
                },
            }
        }
    }
}

type ConnId = usize;

/// What the actor keeps of one connection besides its standing in the [`ConnSet`].
#[derive(Debug)]
struct ConnHandle {
    /// `None` once the connection was stopped: its send loop ends, and it drains and closes.
    send_tx: Option<mpsc::Sender<ProtoMessage>>,
    /// To close it when we disconnect a peer whose queue is stuck. `None` only in tests that have
    /// no connection.
    conn: Option<Connection>,
}

#[derive(Debug)]
enum PeerState {
    Pending {
        queue: Vec<ProtoMessage>,
    },
    Active {
        /// The connections to the peer: the one that sends go to, and the ones that are parked.
        conns: ConnSet<ConnHandle>,
        /// Since when the send queue refuses messages, with none accepted in between.
        refused_since: Option<Instant>,
    },
}

/// How often the connections of the peers are looked at.
const CONNS_CHECK_INTERVAL: Duration = Duration::from_secs(1);

/// Sends `message` on the chosen connection of a peer.
fn send_on_chosen(
    conns: &ConnSet<ConnHandle>,
    message: ProtoMessage,
) -> Result<(), mpsc::error::TrySendError<ProtoMessage>> {
    match conns.chosen().and_then(|conn| conn.handle.send_tx.as_ref()) {
        Some(send_tx) => send_tx.try_send(message),
        None => Err(mpsc::error::TrySendError::Closed(message)),
    }
}

/// What a connection loop uses to talk to the actor.
struct ActorLink {
    in_event_tx: mpsc::Sender<InEvent>,
    /// Gets the id of the connection when it reads the first message of the peer.
    carried_tx: mpsc::Sender<(EndpointId, ConnId)>,
}

/// How long a connection can receive nothing and still be taken as alive. A live connection ranks
/// before a dead one in the choice of the connection that sends go to (see [`conns`]): a
/// connection that went silent for this long is a connection of a peer that restarted or lost the
/// path, and a new one from the same peer must win at once. A connection that a gate holds is
/// alive at the QUIC level: the peer sends keep-alives on it.
///
/// The value is two keep-alive intervals of iroh (5 s each): a live path of an idle connection
/// receives one ping per interval, so a live connection always changes its count within it. The
/// bound cannot be read from iroh, which keeps its interval private (`HEARTBEAT_INTERVAL` in
/// `iroh/src/socket.rs`): read that constant again when iroh moves.
///
/// The bound limits the restart case, it does not remove it: a restarted peer that dials again less
/// than this time after its last datagram has a dial that ranks like a live connection until it
/// carries its first message, and then it wins.
const LIVENESS_BOUND: Duration = Duration::from_secs(10);

/// How often the task of a connection reads the count of datagrams it received.
const RX_SAMPLE_INTERVAL: Duration = Duration::from_secs(1);

/// When the count of datagrams that a connection received last changed. The task of the connection
/// writes it, and the actor reads it when a second connection arrives.
#[derive(Debug, Clone)]
struct RxClock(Arc<Mutex<Instant>>);

impl RxClock {
    /// A new connection just completed its handshake, so it counts as alive now.
    fn new() -> Self {
        Self(Arc::new(Mutex::new(Instant::now())))
    }

    fn touch(&self) {
        self.touch_at(Instant::now());
    }

    fn touch_at(&self, now: Instant) {
        *self.0.lock().expect("lock is not poisoned") = now;
    }

    fn age_at(&self, now: Instant) -> Duration {
        now.saturating_duration_since(*self.0.lock().expect("lock is not poisoned"))
    }
}

/// Reads the datagram count of `conn` every [`RX_SAMPLE_INTERVAL`] and notes each change in `clock`.
async fn sample_rx(conn: Connection, clock: RxClock) -> std::convert::Infallible {
    let mut seen = conn.stats().udp_rx.datagrams;
    loop {
        n0_future::time::sleep(RX_SAMPLE_INTERVAL).await;
        let now = conn.stats().udp_rx.datagrams;
        if now != seen {
            seen = now;
            clock.touch();
        }
    }
}

/// How long a connection whose send queue closed cleanly keeps reading before it is closed: what
/// the peer sent on it before it learned that the connection was replaced still reaches us.
///
/// The queue also closes when the peer is dropped on purpose: `OutEvent::DisconnectPeer` only
/// drops the senders. That connection then lingers for up to this long too, besides the time the
/// send loop waits for the peer to take what is queued, before its task closes it. The protocol
/// has forgotten the peer by then, so nothing waits on the connection. Passing the reason to
/// `drains` would end the linger, but a replaced connection and a dropped one look the same to
/// the task, so it is left as it is.
const DRAIN: Duration = Duration::from_secs(2);

/// Whether a connection keeps reading after its send loop ended. A write error ends the
/// connection at once, and so does a connection that is closed already.
fn drains(send_ended_cleanly: bool, conn_closed: bool) -> bool {
    send_ended_cleanly && !conn_closed
}

/// Whether the endpoint with the lower id dialed a connection: `local_dialed` says whether this
/// side did. Both sides of a pair give the same answer, which is what lets them keep the same
/// connection when both dial at once.
fn dialed_by_lower_id(local: EndpointId, peer: EndpointId, local_dialed: bool) -> bool {
    (local < peer) == local_dialed
}

impl PeerState {
    /// A new connection to the peer. Returns the messages that were queued for the peer while no
    /// connection existed (they go to this connection, the first one), and what to do.
    fn admit(&mut self, conn: Conn<ConnHandle>, now: Instant) -> (Vec<ProtoMessage>, Vec<Action>) {
        let queue = match self {
            PeerState::Pending { queue } => {
                let queue = std::mem::take(queue);
                *self = PeerState::Active {
                    conns: ConnSet::default(),
                    refused_since: None,
                };
                queue
            }
            PeerState::Active { .. } => Vec::new(),
        };
        let PeerState::Active { conns, .. } = self else {
            unreachable!("the state is active now");
        };
        (queue, conns.admit(conn, now))
    }
}

impl Default for PeerState {
    fn default() -> Self {
        PeerState::Pending { queue: Vec::new() }
    }
}

#[derive(Debug)]
struct TopicState {
    neighbors: BTreeSet<EndpointId>,
    isolation: Isolation,
    event_sender: broadcast::Sender<ProtoEvent>,
    /// Keys identifying command receivers in [`Actor::command_rx`].
    ///
    /// This represents the receiver side of gossip's publish public API.
    command_rx_keys: HashSet<stream_group::Key>,
}

impl Default for TopicState {
    fn default() -> Self {
        let (event_sender, _) = broadcast::channel(TOPIC_EVENT_CAP);
        Self {
            neighbors: Default::default(),
            isolation: Isolation::new(Instant::now()),
            command_rx_keys: Default::default(),
            event_sender,
        }
    }
}

impl TopicState {
    /// Check if the topic still has any publisher or subscriber.
    fn still_needed(&self) -> bool {
        // Keep topic alive if either senders or receivers exist.
        // Using || prevents topic closure when senders are dropped while receivers listen.
        !self.command_rx_keys.is_empty() || self.event_sender.receiver_count() > 0
    }

    #[cfg(test)]
    fn joined(&self) -> bool {
        !self.neighbors.is_empty()
    }
}

/// Whether a connection is initiated by us (Dial) or by the remote peer (Accept)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ConnOrigin {
    Accept,
    Dial,
}

#[allow(missing_docs)]
#[stack_error(derive, add_meta, from_sources, std_sources)]
#[non_exhaustive]
enum ConnectionLoopError {
    #[error(transparent)]
    Write {
        source: self::util::WriteError,
    },
    #[error(transparent)]
    Read {
        source: self::util::ReadError,
    },
    #[error(transparent)]
    Connection {
        #[error(std_err)]
        source: iroh::endpoint::ConnectionError,
    },
    ActorDropped {},
}

impl<T> From<mpsc::error::SendError<T>> for ConnectionLoopError {
    fn from(_value: mpsc::error::SendError<T>) -> Self {
        e!(ConnectionLoopError::ActorDropped)
    }
}

async fn connection_loop(
    from: PublicKey,
    conn: Connection,
    origin: ConnOrigin,
    send_rx: mpsc::Receiver<ProtoMessage>,
    link: ActorLink,
    max_message_size: usize,
    queue: Vec<ProtoMessage>,
) -> Result<(), ConnectionLoopError> {
    debug!(?origin, "connection established");

    let mut send_loop = SendLoop::new(conn.clone(), send_rx, max_message_size);
    let observed = conn.clone();
    let mut recv_loop = RecvLoop::new(
        from,
        conn,
        link.in_event_tx,
        link.carried_tx,
        max_message_size,
    );

    let send_fut = send_loop.run(queue).instrument(error_span!("send"));
    let recv_fut = recv_loop.run().instrument(error_span!("recv"));
    let mut recv_fut = std::pin::pin!(recv_fut);

    // `select!`, not `join!`: a superseded connection loses its `send_tx`, so its
    // send loop ends, but the recv loop would wait for a close the peer never
    // sends under keep-alive — one leaked connection per churned link. The active
    // connection keeps its sender in `PeerState`, so it still exits on the recv
    // half alone.
    //
    // A send loop that ends cleanly leaves the recv loop `DRAIN` more to deliver what the peer
    // sent before the connection was replaced; a write error does not.
    tokio::select! {
        send_res = send_fut => {
            send_res?;
            if drains(true, observed.close_reason().is_some()) {
                // Not `tokio::time::timeout`: in a browser there is no tokio timer and the call
                // panics. The timeout of `n0_future` runs on both targets.
                let _ = n0_future::time::timeout(DRAIN, &mut recv_fut).await;
            }
        }
        recv_res = &mut recv_fut => recv_res?,
    }
    Ok(())
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
struct AddrInfo {
    relay_url: Option<RelayUrl>,
    direct_addresses: BTreeSet<SocketAddr>,
}

impl From<EndpointAddr> for AddrInfo {
    fn from(endpoint_addr: EndpointAddr) -> Self {
        Self {
            relay_url: endpoint_addr.relay_urls().next().cloned(),
            direct_addresses: endpoint_addr.ip_addrs().cloned().collect(),
        }
    }
}

fn encode_peer_data(info: &AddrInfo) -> PeerData {
    let bytes = postcard::to_stdvec(info).expect("serializing AddrInfo may not fail");
    PeerData::new(bytes)
}

fn decode_peer_data(peer_data: &PeerData) -> Result<AddrInfo, postcard::Error> {
    let bytes = peer_data.as_bytes();
    if bytes.is_empty() {
        return Ok(AddrInfo::default());
    }
    let info = postcard::from_bytes(bytes)?;
    Ok(info)
}

async fn topic_subscriber_loop(
    sender: irpc::channel::mpsc::Sender<Event>,
    mut topic_events: broadcast::Receiver<ProtoEvent>,
) {
    loop {
        tokio::select! {
           biased;
           msg = topic_events.recv() => {
               let event = match msg {
                   Err(broadcast::error::RecvError::Closed) => break,
                   Err(broadcast::error::RecvError::Lagged(_)) => Event::Lagged,
                   Ok(event) => event.into(),
               };
               if sender.send(event).await.is_err() {
                   break;
               }
           }
           _ = sender.closed() => break,
        }
    }
}

/// A stream of commands for a gossip subscription.
type BoxedCommandReceiver =
    n0_future::stream::Boxed<Result<Command, irpc::channel::mpsc::RecvError>>;

#[derive(derive_more::Debug)]
struct TopicCommandStream {
    topic_id: TopicId,
    #[debug("CommandStream")]
    stream: BoxedCommandReceiver,
    closed: bool,
}

impl TopicCommandStream {
    fn new(topic_id: TopicId, stream: BoxedCommandReceiver) -> Self {
        Self {
            topic_id,
            stream,
            closed: false,
        }
    }
}

impl Stream for TopicCommandStream {
    type Item = (TopicId, Option<Command>);
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.closed {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Ready(Some(Ok(item))) => Poll::Ready(Some((self.topic_id, Some(item)))),
            Poll::Ready(None) | Poll::Ready(Some(Err(_))) => {
                self.closed = true;
                Poll::Ready(Some((self.topic_id, None)))
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

/// Whether the connection of a peer ended without a fault of ours or of the peer's stack: closed
/// in order, or lost with the peer. A peer that leaves closes in order, and one that crashes goes
/// silent until the idle timeout, or leaves no path (`NO_VIABLE_PATH`), or comes back reset. The
/// next send finds the send loop ended, and the protocol reports the loss as `NeighborDown`. A
/// protocol violation, a connection that is still open and a missing connection are not that.
fn ended_without_a_fault(conn: Option<&conns::Conn<ConnHandle>>) -> bool {
    conn.and_then(|conn| conn.handle.conn.as_ref())
        .and_then(Connection::close_reason)
        .is_some_and(|reason| is_no_fault(&reason))
}

fn is_no_fault(reason: &iroh::endpoint::ConnectionError) -> bool {
    use iroh::endpoint::{ConnectionError, TransportErrorCode};
    match reason {
        ConnectionError::ApplicationClosed(_)
        | ConnectionError::LocallyClosed
        | ConnectionError::TimedOut
        | ConnectionError::Reset => true,
        ConnectionError::TransportError(err) => err.code == TransportErrorCode::NO_VIABLE_PATH,
        _ => false,
    }
}

/// Whether the peer answered the dial with a refusal, as an endpoint that is closing does.
/// That is a clear answer, and the caller retries or gives up, so it is not a fault to warn about.
fn dial_was_refused(err: &iroh::endpoint::ConnectError) -> bool {
    use iroh::endpoint::{ConnectError, ConnectingError, ConnectionError, TransportErrorCode};
    let (ConnectError::Connection { source, .. }
    | ConnectError::Connecting {
        source: ConnectingError::ConnectionError { source, .. },
        ..
    }) = err
    else {
        return false;
    };
    matches!(
        source,
        ConnectionError::ConnectionClosed(close)
            if close.error_code == TransportErrorCode::CONNECTION_REFUSED
    )
}

/// Whether the dial ended because the peer never answered.
fn dial_timed_out(err: &iroh::endpoint::ConnectError) -> bool {
    use iroh::endpoint::{ConnectError, ConnectingError, ConnectionError};
    matches!(
        err,
        ConnectError::Connection {
            source: ConnectionError::TimedOut,
            ..
        } | ConnectError::Connecting {
            source: ConnectingError::ConnectionError {
                source: ConnectionError::TimedOut,
                ..
            },
            ..
        }
    )
}

/// Whether the dialer knows no address of the peer. The gossip learns a peer by id, often before
/// anyone told the endpoint where to reach it, and the actor dials again when it knows.
fn dial_had_no_address(err: &iroh::endpoint::ConnectError) -> bool {
    use iroh::endpoint::{ConnectError, ConnectWithOptsError};
    matches!(
        err,
        ConnectError::Connect {
            source: ConnectWithOptsError::NoAddress { .. },
            ..
        }
    )
}

/// A dial that is older than this is replaced when its caller asks for the same peer again. The
/// handshake backs off its retransmits, so a dial that began while nobody served the peer misses
/// the peer for up to the idle timeout after the peer is served.
const DIAL_REPLACED_AFTER: Duration = Duration::from_secs(5);

/// How long a peer may fail the dial for want of an address before the log says so at warn.
/// The dial repeats with each message to the peer, and the address of a peer that the gossip
/// learned from the overlay comes from the app a few hundred ms later, so a short gap is the
/// normal case and stays at debug. Tests use a short bound.
const NO_ADDRESS_WARN_AFTER: Duration = if cfg!(test) {
    Duration::from_secs(1)
} else {
    Duration::from_secs(30)
};

/// How long a topic may have no neighbor before a dial that times out is a warning. A peer in the
/// passive view of a topic that still has a neighbor is gone as a matter of course, and its dial
/// times out. A topic that has no neighbor at all and still fails its dials has lost the swarm,
/// and the log must say so. Tests use a short bound.
const ISOLATED_WARN_AFTER: Duration = if cfg!(test) {
    Duration::from_millis(500)
} else {
    Duration::from_secs(30)
};

/// Since when a topic has no neighbor, and whether the log said so.
#[derive(Debug)]
struct Isolation {
    since: Option<Instant>,
    warned: bool,
}

impl Isolation {
    /// A topic starts alone.
    fn new(now: Instant) -> Self {
        Self {
            since: Some(now),
            warned: false,
        }
    }

    /// Notes that the neighbors of the topic changed. A neighbor ends the spell.
    fn neighbors_changed(&mut self, has_neighbors: bool, now: Instant) {
        if has_neighbors {
            *self = Self {
                since: None,
                warned: false,
            };
        } else if self.since.is_none() {
            *self = Self::new(now);
        }
    }

    /// Returns `true` once per spell: when the topic has had no neighbor for longer than
    /// [`ISOLATED_WARN_AFTER`].
    fn warn_due(&mut self, now: Instant) -> bool {
        let Some(since) = self.since else {
            return false;
        };
        let due = !self.warned && now.saturating_duration_since(since) > ISOLATED_WARN_AFTER;
        self.warned |= due;
        due
    }
}

/// A run of failed dials for want of an address ends when the peer is not dialed for this long,
/// so the entry of a peer that is gone does not stay. The disconnect of the peer cannot end it:
/// each failed dial is followed by one. Twice the bound of the warning, so that a peer that
/// is dialed with each message is not cut short. Tests use a short time.
const NO_ADDRESS_SPELL_ENDS_AFTER: Duration = if cfg!(test) {
    Duration::from_secs(2)
} else {
    Duration::from_secs(60)
};

/// How many peers [`NoAddressWatch`] keeps. The oldest one goes out when a new one comes in.
const MAX_NO_ADDRESS_SPELLS: usize = 1024;

/// Since when the dials of a peer fail for want of an address, and whether the log said so.
#[derive(Debug)]
struct NoAddressSpell {
    since: Instant,
    /// The last failed dial.
    last: Instant,
    warned: bool,
}

/// The peers whose last dial failed for want of an address.
#[derive(Debug, Default)]
struct NoAddressWatch {
    spells: HashMap<EndpointId, NoAddressSpell>,
}

impl NoAddressWatch {
    /// Notes that a dial of the peer failed for want of an address. Returns `true` once per
    /// spell: when the peer has had no address for longer than [`NO_ADDRESS_WARN_AFTER`].
    fn note(&mut self, endpoint_id: EndpointId, now: Instant) -> bool {
        self.spells.retain(|_, spell| {
            now.saturating_duration_since(spell.last) <= NO_ADDRESS_SPELL_ENDS_AFTER
        });
        if self.spells.len() >= MAX_NO_ADDRESS_SPELLS && !self.spells.contains_key(&endpoint_id) {
            let oldest = self
                .spells
                .iter()
                .min_by_key(|(_, spell)| spell.last)
                .map(|(id, _)| *id);
            if let Some(oldest) = oldest {
                self.spells.remove(&oldest);
            }
        }
        let spell = self.spells.entry(endpoint_id).or_insert(NoAddressSpell {
            since: now,
            last: now,
            warned: false,
        });
        spell.last = now;
        let too_long = now.saturating_duration_since(spell.since) > NO_ADDRESS_WARN_AFTER;
        let warn_now = too_long && !spell.warned;
        spell.warned |= too_long;
        warn_now
    }

    /// Notes that a dial of the peer ended in another way, so the spell without an address ends.
    fn forget(&mut self, endpoint_id: EndpointId) {
        self.spells.remove(&endpoint_id);
    }
}

/// The end of a dial: the connection or the error, or nothing if the dial was dropped.
type DialOutcome = Option<Result<Connection, iroh::endpoint::ConnectError>>;

#[derive(Debug)]
struct PendingDial {
    cancel: CancellationToken,
    started: Instant,
    /// Tells the dial of the peer from a dial that replaced it, when the old one ends.
    generation: u64,
}

#[derive(Debug)]
struct Dialer {
    endpoint: Endpoint,
    pending: JoinSet<(EndpointId, u64, DialOutcome)>,
    pending_dials: HashMap<EndpointId, PendingDial>,
    next_generation: u64,
    no_address: NoAddressWatch,
}

impl Dialer {
    /// Create a new dialer for a [`Endpoint`]
    fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            pending: Default::default(),
            pending_dials: Default::default(),
            next_generation: 0,
            no_address: Default::default(),
        }
    }

    /// Starts to dial a endpoint by [`EndpointId`].
    fn queue_dial(&mut self, endpoint_id: EndpointId, alpn: Bytes) {
        if self.is_pending(endpoint_id) {
            return;
        }
        let cancel = CancellationToken::new();
        let generation = self.next_generation;
        self.next_generation += 1;
        self.pending_dials.insert(
            endpoint_id,
            PendingDial {
                cancel: cancel.clone(),
                started: Instant::now(),
                generation,
            },
        );
        let endpoint = self.endpoint.clone();
        self.pending.spawn(
            async move {
                let res = tokio::select! {
                    biased;
                    _ = cancel.cancelled() => None,
                    res = endpoint.connect(endpoint_id, &alpn) => Some(res),
                };
                (endpoint_id, generation, res)
            }
            .instrument(tracing::Span::current()),
        );
    }

    /// Drops the dial of a peer that its caller asks for again, if the dial is older than
    /// [`DIAL_REPLACED_AFTER`]. The next [`Self::queue_dial`] for the peer starts a new dial. The
    /// dropped dial ends without a result.
    fn replace_old_dial(&mut self, endpoint_id: EndpointId, now: Instant) {
        let Some(dial) = self.pending_dials.get(&endpoint_id) else {
            return;
        };
        if now.saturating_duration_since(dial.started) < DIAL_REPLACED_AFTER {
            return;
        }
        debug!(peer = %endpoint_id.fmt_short(), "replace a dial that is still pending");
        dial.cancel.cancel();
        self.pending_dials.remove(&endpoint_id);
    }

    /// Checks if a endpoint is currently being dialed.
    fn is_pending(&self, endpoint: EndpointId) -> bool {
        self.pending_dials.contains_key(&endpoint)
    }

    /// Waits for the next dial operation to complete.
    /// `None` means disconnected
    async fn next_conn(&mut self) -> (EndpointId, DialOutcome) {
        match self.pending_dials.is_empty() {
            false => {
                let (endpoint_id, res) = loop {
                    match self.pending.join_next().await {
                        Some(Ok((endpoint_id, generation, res))) => {
                            let current = self
                                .pending_dials
                                .get(&endpoint_id)
                                .is_some_and(|dial| dial.generation == generation);
                            if current {
                                self.pending_dials.remove(&endpoint_id);
                                break (endpoint_id, res);
                            }
                        }
                        Some(Err(e)) => {
                            error!("next conn error: {:?}", e);
                        }
                        None => {
                            error!("no more pending conns available");
                            std::future::pending().await
                        }
                    }
                };

                (endpoint_id, res)
            }
            true => std::future::pending().await,
        }
    }
}

#[cfg(test)]
pub(crate) mod tests {
    use std::time::Duration;

    use bytes::Bytes;
    use futures_concurrency::future::TryJoin;
    use iroh::{
        address_lookup::memory::MemoryLookup,
        endpoint::{presets, BindError, QuicTransportConfig},
        protocol::Router,
        tls::CaTlsConfig,
        RelayMap, RelayMode, RelayUrl, SecretKey,
    };
    use n0_error::{AnyError, Result, StdResultExt};
    use n0_tracing_test::traced_test;
    use rand::{CryptoRng, RngExt};
    use tokio::{spawn, time::timeout};
    use tokio_util::sync::CancellationToken;
    use tracing::{info, instrument};

    use super::*;
    use crate::api::{ApiError, GossipReceiver, GossipSender};

    struct ManualActorLoop {
        actor: Actor,
        step: usize,
    }

    impl std::ops::Deref for ManualActorLoop {
        type Target = Actor;

        fn deref(&self) -> &Self::Target {
            &self.actor
        }
    }

    impl std::ops::DerefMut for ManualActorLoop {
        fn deref_mut(&mut self) -> &mut Self::Target {
            &mut self.actor
        }
    }

    type EndpointHandle = tokio::task::JoinHandle<Result<()>>;

    impl ManualActorLoop {
        #[instrument(skip_all, fields(me = %actor.endpoint.id().fmt_short()))]
        async fn new(mut actor: Actor) -> Self {
            let _ = actor.setup().await;
            Self { actor, step: 0 }
        }

        #[instrument(skip_all, fields(me = %self.endpoint.id().fmt_short()))]
        async fn step(&mut self) -> bool {
            let ManualActorLoop { actor, step } = self;
            *step += 1;
            // ignore updates that change our published address. This gives us better control over
            // events since the endpoint it no longer emitting changes
            let addr_update_stream = &mut n0_future::stream::pending();
            actor.event_loop(addr_update_stream, *step).await
        }

        async fn steps(&mut self, n: usize) {
            for _ in 0..n {
                self.step().await;
            }
        }

        async fn finish(mut self) {
            while self.step().await {}
        }
    }

    impl Gossip {
        /// Creates a testing gossip instance and its actor without spawning it.
        ///
        /// This creates the endpoint and spawns the endpoint loop as well. The handle for the
        /// endpoing task is returned along the gossip instance and actor. Since the actor is not
        /// actually spawned as [`Builder::spawn`] would, the gossip instance will have a
        /// handle to a dummy task instead.
        async fn t_new_with_actor(
            rng: &mut rand::rngs::ChaCha12Rng,
            config: proto::Config,
            relay_map: RelayMap,
            cancel: &CancellationToken,
        ) -> Result<(Self, Actor, EndpointHandle), BindError> {
            let endpoint = create_endpoint(rng, relay_map, None).await?;
            Ok(Self::t_new_with_actor_on(endpoint, config, cancel))
        }

        /// [`Self::t_new_with_actor`] on an endpoint that the test built.
        fn t_new_with_actor_on(
            endpoint: Endpoint,
            config: proto::Config,
            cancel: &CancellationToken,
        ) -> (Self, Actor, EndpointHandle) {
            let metrics = Arc::new(Metrics::default());
            let address_lookup = GossipAddressLookup::default();
            endpoint
                .address_lookup()
                .expect("endpoint is not closed")
                .add(address_lookup.clone());

            let (actor, to_actor_tx, conn_tx) =
                Actor::new(endpoint, config, metrics.clone(), None, address_lookup);
            let max_message_size = actor.state.max_message_size();

            let _actor_handle = AbortOnDropHandle::new(task::spawn(n0_future::future::pending()));
            let gossip = Self {
                inner: Inner {
                    api: GossipApi::local(to_actor_tx),
                    local_tx: conn_tx,
                    _actor_handle,
                    max_message_size,
                    metrics,
                }
                .into(),
            };

            let endpoint_task = task::spawn(endpoint_loop(
                actor.endpoint.clone(),
                gossip.clone(),
                cancel.child_token(),
            ));

            (gossip, actor, endpoint_task)
        }

        /// Crates a new testing gossip instance with the normal actor loop.
        async fn t_new(
            rng: &mut rand::rngs::ChaCha12Rng,
            config: proto::Config,
            relay_map: RelayMap,
            cancel: &CancellationToken,
        ) -> Result<(Self, Endpoint, EndpointHandle, impl Drop + use<>), BindError> {
            let endpoint = create_endpoint(rng, relay_map, None).await?;
            Ok(Gossip::t_new_on(endpoint, config, cancel))
        }

        /// [`Self::t_new`] on an endpoint that the test built.
        fn t_new_on(
            endpoint: Endpoint,
            config: proto::Config,
            cancel: &CancellationToken,
        ) -> (Self, Endpoint, EndpointHandle, impl Drop + use<>) {
            let (g, actor, ep_handle) = Gossip::t_new_with_actor_on(endpoint, config, cancel);
            let ep = actor.endpoint.clone();
            let me = ep.id().fmt_short();
            let actor_handle =
                task::spawn(actor.run().instrument(tracing::error_span!("gossip", %me)));
            (g, ep, ep_handle, AbortOnDropHandle::new(actor_handle))
        }
    }

    pub(crate) async fn create_endpoint(
        rng: &mut rand::rngs::ChaCha12Rng,
        relay_map: RelayMap,
        memory_lookup: Option<MemoryLookup>,
    ) -> Result<Endpoint, BindError> {
        let ep = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Custom(relay_map))
            .secret_key(SecretKey::from_bytes(&rng.random()))
            .alpns(vec![GOSSIP_ALPN.to_vec()])
            .ca_tls_config(CaTlsConfig::insecure_skip_verify())
            .bind()
            .await?;

        if let Some(memory_lookup) = memory_lookup {
            ep.address_lookup()
                .expect("endpoint is not closed")
                .add(memory_lookup);
        }
        ep.online().await;
        Ok(ep)
    }

    async fn endpoint_loop(
        endpoint: Endpoint,
        gossip: Gossip,
        cancel: CancellationToken,
    ) -> Result<()> {
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                incoming = endpoint.accept() => match incoming {
                    None => break,
                    Some(incoming) => {
                        let connecting = match incoming.accept() {
                            Ok(connecting) => connecting,
                            Err(err) => {
                                warn!("incoming connection failed: {err:#}");
                                // we can carry on in these cases:
                                // this can be caused by retransmitted datagrams
                                continue;
                            }
                        };
                        let connection = connecting
                            .await
                            .std_context("await incoming connection")?;
                        gossip.handle_connection(connection).await?
                    }
                }
            }
        }
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn gossip_net_smoke() {
        let mut rng = rand::rngs::ChaCha12Rng::seed_from_u64(1);
        let (relay_map, relay_url, _guard) = iroh::test_utils::run_relay_server().await.unwrap();

        let memory_lookup = MemoryLookup::new();

        let ep1 = create_endpoint(&mut rng, relay_map.clone(), Some(memory_lookup.clone()))
            .await
            .unwrap();
        let ep2 = create_endpoint(&mut rng, relay_map.clone(), Some(memory_lookup.clone()))
            .await
            .unwrap();
        let ep3 = create_endpoint(&mut rng, relay_map.clone(), Some(memory_lookup.clone()))
            .await
            .unwrap();

        let go1 = Gossip::builder().spawn(ep1.clone());
        let go2 = Gossip::builder().spawn(ep2.clone());
        let go3 = Gossip::builder().spawn(ep3.clone());
        debug!("peer1 {:?}", ep1.id());
        debug!("peer2 {:?}", ep2.id());
        debug!("peer3 {:?}", ep3.id());
        let pi1 = ep1.id();
        let pi2 = ep2.id();

        let cancel = CancellationToken::new();
        let tasks = [
            spawn(endpoint_loop(ep1.clone(), go1.clone(), cancel.clone())),
            spawn(endpoint_loop(ep2.clone(), go2.clone(), cancel.clone())),
            spawn(endpoint_loop(ep3.clone(), go3.clone(), cancel.clone())),
        ];

        debug!("----- adding peers  ----- ");
        let topic: TopicId = blake3::hash(b"foobar").into();

        let addr1 = EndpointAddr::new(pi1).with_relay_url(relay_url.clone());
        let addr2 = EndpointAddr::new(pi2).with_relay_url(relay_url);
        memory_lookup.add_endpoint_info(addr1.clone());
        memory_lookup.add_endpoint_info(addr2.clone());

        debug!("----- joining  ----- ");
        // join the topics and wait for the connection to succeed
        let [sub1, mut sub2, mut sub3] = [
            go1.subscribe_and_join(topic, vec![]),
            go2.subscribe_and_join(topic, vec![pi1]),
            go3.subscribe_and_join(topic, vec![pi2]),
        ]
        .try_join()
        .await
        .unwrap();

        let (sink1, _stream1) = sub1.split();

        let len = 2;

        // publish messages on endpoint1
        let pub1 = spawn(async move {
            for i in 0..len {
                let message = format!("hi{i}");
                info!("go1 broadcast: {message:?}");
                sink1.broadcast(message.into_bytes().into()).await.unwrap();
                tokio::time::sleep(Duration::from_micros(1)).await;
            }
        });

        // wait for messages on endpoint2
        let sub2 = spawn(async move {
            let mut recv = vec![];
            loop {
                let ev = sub2.next().await.unwrap().unwrap();
                info!("go2 event: {ev:?}");
                if let Event::Received(msg) = ev {
                    recv.push(msg.content);
                }
                if recv.len() == len {
                    return recv;
                }
            }
        });

        // wait for messages on endpoint3
        let sub3 = spawn(async move {
            let mut recv = vec![];
            loop {
                let ev = sub3.next().await.unwrap().unwrap();
                info!("go3 event: {ev:?}");
                if let Event::Received(msg) = ev {
                    recv.push(msg.content);
                }
                if recv.len() == len {
                    return recv;
                }
            }
        });

        timeout(Duration::from_secs(10), pub1)
            .await
            .unwrap()
            .unwrap();
        let recv2 = timeout(Duration::from_secs(10), sub2)
            .await
            .unwrap()
            .unwrap();
        let recv3 = timeout(Duration::from_secs(10), sub3)
            .await
            .unwrap()
            .unwrap();

        // We assert the received messages, but not their order.
        // While commonly they will be received in-order, for go3 it may happen
        // that the second message arrives before the first one, because it managed to
        // forward-join go1 before the second message is published.
        let expected: HashSet<Bytes> = (0..len)
            .map(|i| Bytes::from(format!("hi{i}").into_bytes()))
            .collect();
        assert_eq!(HashSet::from_iter(recv2), expected);
        assert_eq!(HashSet::from_iter(recv3), expected);

        cancel.cancel();
        for t in tasks {
            timeout(Duration::from_secs(10), t)
                .await
                .unwrap()
                .unwrap()
                .unwrap();
        }
    }

    /// Test that when a gossip topic is no longer needed it's actually unsubscribed.
    ///
    /// This test will:
    /// - Create two endpoints, the first using manual event loop.
    /// - Subscribe both endpoints to the same topic. The first endpoint will subscribe twice and connect
    ///   to the second endpoint. The second endpoint will subscribe without bootstrap.
    /// - Ensure that the first endpoint removes the subscription iff all topic handles have been
    ///   dropped
    // NOTE: this is a regression test.
    #[tokio::test]
    #[traced_test]
    async fn subscription_cleanup() -> Result {
        let rng = &mut rand::rngs::ChaCha12Rng::seed_from_u64(1);
        let ct = CancellationToken::new();
        let (relay_map, relay_url, _guard) = iroh::test_utils::run_relay_server().await.unwrap();

        // create the first endpoint with a manual actor loop
        let (go1, actor, ep1_handle) =
            Gossip::t_new_with_actor(rng, Default::default(), relay_map.clone(), &ct).await?;
        let mut actor = ManualActorLoop::new(actor).await;

        // create the second endpoint with the usual actor loop
        let (go2, ep2, ep2_handle, _test_actor_handle) =
            Gossip::t_new(rng, Default::default(), relay_map, &ct).await?;

        let endpoint_id1 = actor.endpoint.id();
        let endpoint_id2 = ep2.id();
        tracing::info!(
            endpoint_1 = %endpoint_id1.fmt_short(),
            endpoint_2 = %endpoint_id2.fmt_short(),
            "endpoints ready"
        );

        let topic: TopicId = blake3::hash(b"subscription_cleanup").into();
        tracing::info!(%topic, "joining");

        // create the tasks for each gossip instance:
        // - second endpoint subscribes once without bootstrap and listens to events
        // - first endpoint subscribes twice with the second endpoint as bootstrap. This is done on command
        //   from the main task (this)

        // second endpoint
        let ct2 = ct.clone();
        let go2_task = async move {
            let (_pub_tx, mut sub_rx) = go2.subscribe_and_join(topic, vec![]).await?.split();

            let subscribe_fut = async {
                while let Some(ev) = sub_rx.try_next().await? {
                    match ev {
                        Event::Lagged => tracing::debug!("missed some messages :("),
                        Event::Received(_) => unreachable!("test does not send messages"),
                        other => tracing::debug!(?other, "gs event"),
                    }
                }

                tracing::debug!("subscribe stream ended");
                Ok::<_, AnyError>(())
            };

            tokio::select! {
                _ = ct2.cancelled() => Ok(()),
                res = subscribe_fut => res,
            }
        }
        .instrument(tracing::debug_span!("endpoint_2", %endpoint_id2));
        let go2_handle = task::spawn(go2_task);

        // first endpoint
        let addr2 = EndpointAddr::new(endpoint_id2).with_relay_url(relay_url);
        let memory_lookup = MemoryLookup::new();
        memory_lookup.add_endpoint_info(addr2);
        actor.endpoint.address_lookup()?.add(memory_lookup);
        // we use a channel to signal advancing steps to the task
        let (tx, mut rx) = mpsc::channel::<()>(1);
        let ct1 = ct.clone();
        let go1_task = async move {
            // first subscribe is done immediately
            tracing::info!("subscribing the first time");
            let sub_1a = go1.subscribe_and_join(topic, vec![endpoint_id2]).await?;

            // wait for signal to subscribe a second time
            rx.recv().await.expect("signal for second subscribe");
            tracing::info!("subscribing a second time");
            let sub_1b = go1.subscribe_and_join(topic, vec![endpoint_id2]).await?;
            drop(sub_1a);

            // wait for signal to drop the second handle as well
            rx.recv().await.expect("signal for second subscribe");
            tracing::info!("dropping all handles");
            drop(sub_1b);

            // wait for cancellation
            ct1.cancelled().await;
            drop(go1);

            Ok::<_, AnyError>(())
        }
        .instrument(tracing::debug_span!("endpoint_1", %endpoint_id1));
        let go1_handle = task::spawn(go1_task);

        // advance and check that the topic is now subscribed
        actor.steps(3).await; // handle our subscribe;
                              // get peer connection;
                              // receive the other peer's information for a NeighborUp
        let state = actor.topics.get(&topic).expect("get registered topic");
        assert!(state.joined());

        // signal the second subscribe, we should remain subscribed
        tx.send(())
            .await
            .std_context("signal additional subscribe")?;
        actor.steps(3).await; // subscribe; first receiver gone; first sender gone
        let state = actor.topics.get(&topic).expect("get registered topic");
        assert!(state.joined());

        // signal to drop the second handle, the topic should no longer be subscribed
        tx.send(()).await.std_context("signal drop handles")?;
        actor.steps(2).await; // second receiver gone; second sender gone
        assert!(!actor.topics.contains_key(&topic));

        // cleanup and ensure everything went as expected
        ct.cancel();
        let wait = Duration::from_secs(2);
        timeout(wait, ep1_handle)
            .await
            .std_context("wait endpoint1 task")?
            .std_context("join endpoint1 task")??;
        timeout(wait, ep2_handle)
            .await
            .std_context("wait endpoint2 task")?
            .std_context("join endpoint2 task")??;
        timeout(wait, go1_handle)
            .await
            .std_context("wait gossip1 task")?
            .std_context("join gossip1 task")??;
        timeout(wait, go2_handle)
            .await
            .std_context("wait gossip2 task")?
            .std_context("join gossip2 task")??;
        timeout(wait, actor.finish())
            .await
            .std_context("wait actor finish")?;

        Ok(())
    }

    /// A [`SendLoop`] must end once its send channel is closed, which is how a
    /// superseded connection is signalled. It used to block on `closed` forever
    /// instead, leaking one connection per churned link.
    ///
    /// [`SendLoop`]: util::SendLoop
    #[tokio::test]
    #[traced_test]
    async fn send_loop_terminates_when_send_channel_closed() -> Result {
        let rng = &mut rand::rngs::ChaCha12Rng::seed_from_u64(1);
        let (relay_map, relay_url, _guard) = iroh::test_utils::run_relay_server().await.unwrap();
        let ep1 = create_endpoint(rng, relay_map.clone(), None).await?;
        let ep2 = create_endpoint(rng, relay_map.clone(), None).await?;

        let memory_lookup = MemoryLookup::new();
        memory_lookup.add_endpoint_info(EndpointAddr::new(ep2.id()).with_relay_url(relay_url));
        ep1.address_lookup()?.add(memory_lookup);

        let ep2_id = ep2.id();
        // ep2 holds the connection open, so only the closed send channel can end
        // the send loop.
        let accept_task = task::spawn(async move {
            if let Some(incoming) = ep2.accept().await {
                if let Ok(conn) = incoming.await {
                    conn.closed().await;
                }
            }
        });

        let conn = ep1
            .connect(ep2_id, GOSSIP_ALPN)
            .await
            .std_context("connect")?;

        // A send channel with no sender left models a just-superseded connection.
        let (send_tx, send_rx) = mpsc::channel::<ProtoMessage>(1);
        drop(send_tx);

        // With the bug this blocks on `conn.closed()` forever and the timeout fires.
        let mut send_loop = util::SendLoop::new(conn, send_rx, 1024);
        let res = timeout(Duration::from_secs(5), send_loop.run(vec![])).await;
        assert!(
            res.is_ok(),
            "SendLoop must terminate when its send channel closes (superseded connection)"
        );
        res.expect("send loop returned")
            .std_context("send loop run")?;

        accept_task.abort();
        Ok(())
    }

    /// Test that endpoints can reconnect to each other.
    ///
    /// This test will create two endpoints subscribed to the same topic. The second endpoint will
    /// unsubscribe and then resubscribe and connection between the endpoints should succeed both
    /// times.
    // NOTE: This is a regression test
    #[tokio::test]
    #[traced_test]
    async fn can_reconnect() -> Result {
        let rng = &mut rand::rngs::ChaCha12Rng::seed_from_u64(1);
        let ct = CancellationToken::new();
        let (relay_map, relay_url, _guard) = iroh::test_utils::run_relay_server().await.unwrap();

        let (go1, ep1, ep1_handle, _test_actor_handle1) =
            Gossip::t_new(rng, Default::default(), relay_map.clone(), &ct).await?;

        let (go2, ep2, ep2_handle, _test_actor_handle2) =
            Gossip::t_new(rng, Default::default(), relay_map, &ct).await?;

        let endpoint_id1 = ep1.id();
        let endpoint_id2 = ep2.id();
        tracing::info!(
            endpoint_1 = %endpoint_id1.fmt_short(),
            endpoint_2 = %endpoint_id2.fmt_short(),
            "endpoints ready"
        );

        let topic: TopicId = blake3::hash(b"can_reconnect").into();
        tracing::info!(%topic, "joining");

        let ct2 = ct.child_token();
        // channel used to signal the second gossip instance to advance the test
        let (tx, mut rx) = mpsc::channel::<()>(1);
        let addr1 = EndpointAddr::new(endpoint_id1).with_relay_url(relay_url.clone());
        let memory_lookup = MemoryLookup::new();
        memory_lookup.add_endpoint_info(addr1);
        ep2.address_lookup()?.add(memory_lookup.clone());
        let go2_task = async move {
            let mut sub = go2.subscribe(topic, Vec::new()).await?;
            sub.joined().await?;

            rx.recv().await.expect("signal to unsubscribe");
            tracing::info!("unsubscribing");
            drop(sub);

            rx.recv().await.expect("signal to subscribe again");
            tracing::info!("resubscribing");
            let mut sub = go2.subscribe(topic, vec![endpoint_id1]).await?;

            sub.joined().await?;
            tracing::info!("subscription successful!");

            ct2.cancelled().await;

            Ok::<_, ApiError>(())
        }
        .instrument(tracing::debug_span!("endpoint_2", %endpoint_id2));
        let go2_handle = task::spawn(go2_task);

        let addr2 = EndpointAddr::new(endpoint_id2).with_relay_url(relay_url);
        memory_lookup.add_endpoint_info(addr2);
        ep1.address_lookup()?.add(memory_lookup);

        let mut sub = go1.subscribe(topic, vec![endpoint_id2]).await?;
        // wait for subscribed notification
        sub.joined().await?;

        // signal endpoint_2 to unsubscribe
        tx.send(()).await.std_context("signal unsubscribe")?;

        // we should receive a Neighbor down event
        let conn_timeout = Duration::from_millis(500);
        let ev = timeout(conn_timeout, sub.try_next())
            .await
            .std_context("wait neighbor down")??;
        assert_eq!(ev, Some(Event::NeighborDown(endpoint_id2)));
        tracing::info!("endpoint 2 left");

        // signal endpoint_2 to subscribe again
        tx.send(()).await.std_context("signal resubscribe")?;

        let conn_timeout = Duration::from_millis(500);
        let ev = timeout(conn_timeout, sub.try_next())
            .await
            .std_context("wait neighbor up")??;
        assert_eq!(ev, Some(Event::NeighborUp(endpoint_id2)));
        tracing::info!("endpoint 2 rejoined!");

        // cleanup and ensure everything went as expected
        ct.cancel();
        let wait = Duration::from_secs(2);
        timeout(wait, ep1_handle)
            .await
            .std_context("wait endpoint1 task")?
            .std_context("join endpoint1 task")??;
        timeout(wait, ep2_handle)
            .await
            .std_context("wait endpoint2 task")?
            .std_context("join endpoint2 task")??;
        timeout(wait, go2_handle)
            .await
            .std_context("wait gossip2 task")?
            .std_context("join gossip2 task")??;

        Result::Ok(())
    }

    /// A dial that fails while the peer is still `Pending` must not wedge the
    /// peer: once it becomes reachable, the next send to it must dial again.
    #[tokio::test]
    #[traced_test]
    async fn redial_after_failed_dial() -> Result {
        let rng = &mut rand::rngs::ChaCha12Rng::seed_from_u64(1);
        let ct = CancellationToken::new();
        let (relay_map, relay_url, _guard) = iroh::test_utils::run_relay_server().await.unwrap();

        let (go1, ep1, ep1_handle, _test_actor_handle1) =
            Gossip::t_new(rng, Default::default(), relay_map.clone(), &ct).await?;
        let (go2, ep2, ep2_handle, _test_actor_handle2) =
            Gossip::t_new(rng, Default::default(), relay_map, &ct).await?;
        let endpoint_id2 = ep2.id();

        // endpoint 1 has no address for endpoint 2 yet, so the bootstrap dial fails.
        let memory_lookup = MemoryLookup::new();
        ep1.address_lookup()?.add(memory_lookup.clone());

        let topic: TopicId = blake3::hash(b"redial_after_failed_dial").into();
        let _sub2 = go2.subscribe(topic, Vec::new()).await?;
        let sub1 = go1.subscribe(topic, vec![endpoint_id2]).await?;
        let (sender1, mut receiver1) = sub1.split();

        let metrics = go1.metrics().clone();
        timeout(Duration::from_secs(20), async {
            while metrics.actor_tick_dialer_failure.get() == 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .std_context("wait dial failure")?;
        tracing::info!("bootstrap dial failed, endpoint 2 becomes reachable now");

        memory_lookup.add_endpoint_info(EndpointAddr::new(endpoint_id2).with_relay_url(relay_url));
        sender1.join_peers(vec![endpoint_id2]).await?;

        let ev = timeout(Duration::from_secs(5), receiver1.try_next())
            .await
            .std_context("wait neighbor up after redial")??;
        assert_eq!(ev, Some(Event::NeighborUp(endpoint_id2)));

        ct.cancel();
        let wait = Duration::from_secs(2);
        timeout(wait, ep1_handle)
            .await
            .std_context("wait endpoint1 task")?
            .std_context("join endpoint1 task")??;
        timeout(wait, ep2_handle)
            .await
            .std_context("wait endpoint2 task")?
            .std_context("join endpoint2 task")??;

        Result::Ok(())
    }

    /// Keeps the WARN and ERROR events of `iroh_gossip::net` that the thread of the test logs.
    /// A test on the current-thread runtime runs every task on that thread, the actor included.
    struct KeepsNetWarnings(Arc<Mutex<Vec<String>>>);

    impl tracing::Subscriber for KeepsNetWarnings {
        fn enabled(&self, _: &tracing::Metadata<'_>) -> bool {
            true
        }
        fn new_span(&self, _: &tracing::span::Attributes<'_>) -> tracing::span::Id {
            tracing::span::Id::from_u64(1)
        }
        fn record(&self, _: &tracing::span::Id, _: &tracing::span::Record<'_>) {}
        fn record_follows_from(&self, _: &tracing::span::Id, _: &tracing::span::Id) {}
        fn event(&self, event: &tracing::Event<'_>) {
            let meta = event.metadata();
            if *meta.level() <= tracing::Level::WARN && meta.target() == "iroh_gossip::net" {
                self.0
                    .lock()
                    .expect("warnings mutex poisoned")
                    .push(format!("{event:?}"));
            }
        }
        fn enter(&self, _: &tracing::span::Id) {}
        fn exit(&self, _: &tracing::span::Id) {}
    }

    /// How the peer of `warnings_of_a_failed_dial` makes the dial fail.
    #[derive(Clone, Copy, PartialEq, Eq)]
    enum FailedDial {
        /// The peer refuses every connection, as a closing endpoint does.
        Refused,
        /// The dialer knows no address of the peer.
        NoAddress,
        /// The peer speaks another protocol, so the handshake ends in an error.
        WrongProtocol,
        /// The address of the peer leads to a socket that never answers.
        TimesOut,
        /// As `TimesOut`, while the dialer holds a live neighbor in the topic.
        TimesOutBesideANeighbor,
    }

    /// Dials a peer that fails the dial once, and returns the WARN and ERROR events of
    /// `iroh_gossip::net`. The dialer gives up on a silent peer after one second.
    async fn warnings_of_a_failed_dial(failed_dial: FailedDial) -> Result<Vec<String>> {
        let warnings = Arc::new(Mutex::new(Vec::new()));
        let _log = tracing::subscriber::set_default(KeepsNetWarnings(Arc::clone(&warnings)));
        let rng = &mut rand::rngs::ChaCha12Rng::seed_from_u64(1);
        let ct = CancellationToken::new();
        let (relay_map, relay_url, _guard) = iroh::test_utils::run_relay_server().await.unwrap();
        let idle_timeout = Duration::from_secs(1)
            .try_into()
            .expect("1 s is an idle timeout");
        let dialer = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Custom(relay_map.clone()))
            .secret_key(SecretKey::from_bytes(&rng.random()))
            .alpns(vec![GOSSIP_ALPN.to_vec()])
            .ca_tls_config(CaTlsConfig::insecure_skip_verify())
            .transport_config(
                QuicTransportConfig::builder()
                    .max_idle_timeout(Some(idle_timeout))
                    // The link to the neighbor must outlive the idle timeout of the dial.
                    .keep_alive_interval(Duration::from_millis(200))
                    .build(),
            )
            .bind()
            .await?;
        dialer.online().await;
        let (go1, ep1, ep1_handle, _test_actor_handle1) =
            Gossip::t_new_on(dialer, Default::default(), &ct);
        let silent = std::net::UdpSocket::bind("127.0.0.1:0").std_context("bind silent socket")?;
        let (relay_map_of_neighbor, relay_url_of_neighbor) = (relay_map.clone(), relay_url.clone());
        let peer = match failed_dial {
            FailedDial::WrongProtocol => {
                let peer = Endpoint::builder(presets::Minimal)
                    .relay_mode(RelayMode::Custom(relay_map))
                    .secret_key(SecretKey::from_bytes(&rng.random()))
                    .alpns(vec![b"not-gossip".to_vec()])
                    .ca_tls_config(CaTlsConfig::insecure_skip_verify())
                    .bind()
                    .await?;
                peer.online().await;
                Some(peer)
            }
            FailedDial::Refused => Some(create_endpoint(rng, relay_map, None).await?),
            FailedDial::NoAddress | FailedDial::TimesOut | FailedDial::TimesOutBesideANeighbor => {
                None
            }
        };
        let peer_id = match &peer {
            Some(peer) => peer.id(),
            None => SecretKey::from_bytes(&rng.random()).public(),
        };
        let answerer = peer.clone().map(|accepting| {
            spawn(async move {
                while let Some(incoming) = accepting.accept().await {
                    if failed_dial == FailedDial::Refused {
                        incoming.refuse();
                    } else if let Ok(connecting) = incoming.accept() {
                        let _ = connecting.await;
                    }
                }
            })
        });
        let addr = match failed_dial {
            FailedDial::NoAddress => None,
            FailedDial::TimesOut | FailedDial::TimesOutBesideANeighbor => Some(
                EndpointAddr::new(peer_id).with_ip_addr(
                    silent
                        .local_addr()
                        .std_context("address of silent socket")?,
                ),
            ),
            FailedDial::Refused | FailedDial::WrongProtocol => {
                Some(EndpointAddr::new(peer_id).with_relay_url(relay_url))
            }
        };
        if let Some(addr) = addr {
            let memory_lookup = MemoryLookup::new();
            memory_lookup.add_endpoint_info(addr);
            ep1.address_lookup()?.add(memory_lookup);
        }

        let topic: TopicId = blake3::hash(b"a_failed_dial_and_the_level_of_its_log").into();
        let _neighbor = if failed_dial == FailedDial::TimesOutBesideANeighbor {
            let (go2, ep2, ep2_handle, actor2) =
                Gossip::t_new(rng, Default::default(), relay_map_of_neighbor, &ct).await?;
            // The neighbor must have an address for the dialer too, but only the dialer dials.
            ep1.address_lookup()?.add({
                let lookup = MemoryLookup::new();
                lookup.add_endpoint_info(
                    EndpointAddr::new(ep2.id()).with_relay_url(relay_url_of_neighbor),
                );
                lookup
            });
            let _sub2 = go2.subscribe(topic, Vec::new()).await?;
            let (_sender1, mut receiver1) = go1.subscribe(topic, vec![ep2.id()]).await?.split();
            let up = timeout(Duration::from_secs(10), receiver1.try_next())
                .await
                .std_context("wait neighbor up")??;
            assert_eq!(up, Some(Event::NeighborUp(ep2.id())));
            Some((go2, ep2_handle, actor2, _sub2, _sender1, receiver1))
        } else {
            None
        };
        let _sub1 = go1.subscribe(topic, vec![peer_id]).await?;
        let metrics = go1.metrics().clone();
        timeout(Duration::from_secs(20), async {
            while metrics.actor_tick_dialer_failure.get() == 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .std_context("wait dial failure")?;

        ct.cancel();
        if let Some(answerer) = answerer {
            answerer.abort();
        }
        timeout(Duration::from_secs(2), ep1_handle)
            .await
            .std_context("wait endpoint1 task")?
            .std_context("join endpoint1 task")??;
        let warnings = warnings.lock().expect("warnings mutex poisoned").clone();
        Ok(warnings)
    }

    /// A peer that is closing refuses a new connection. That is a clear answer, and the caller
    /// retries or gives up, so it is not a fault to warn about.
    #[tokio::test]
    async fn a_dial_that_a_closing_peer_refuses_is_not_a_warning() -> Result {
        assert_eq!(
            warnings_of_a_failed_dial(FailedDial::Refused).await?,
            Vec::<String>::new()
        );
        Ok(())
    }

    /// A peer that the gossip learned by id, before anyone told the endpoint its address, cannot
    /// be dialed yet. The dial repeats with the next message to the peer, so a short gap is not a
    /// fault.
    #[tokio::test]
    async fn a_dial_to_a_peer_without_an_address_is_not_a_warning() -> Result {
        assert_eq!(
            warnings_of_a_failed_dial(FailedDial::NoAddress).await?,
            Vec::<String>::new()
        );
        Ok(())
    }

    /// A topic that still has a neighbor keeps its members. A peer in the passive view that is
    /// gone times out the dial as a matter of course, so the timeout is not a fault to warn about.
    #[tokio::test]
    async fn a_timed_out_dial_beside_an_active_neighbor_is_not_a_warning() -> Result {
        assert_eq!(
            warnings_of_a_failed_dial(FailedDial::TimesOutBesideANeighbor).await?,
            Vec::<String>::new()
        );
        Ok(())
    }

    /// A topic with no neighbor is a fault to see in the log: one warning once the spell is longer
    /// than the bound, none in the same spell, and none while a neighbor is there.
    #[test]
    fn a_topic_without_a_neighbor_is_isolated_once_after_the_bound() {
        let start = Instant::now();
        let mut isolation = Isolation::new(start);
        assert!(!isolation.warn_due(start + ISOLATED_WARN_AFTER / 2));
        assert!(isolation.warn_due(start + ISOLATED_WARN_AFTER * 2));
        assert!(!isolation.warn_due(start + ISOLATED_WARN_AFTER * 3));

        isolation.neighbors_changed(true, start + ISOLATED_WARN_AFTER * 4);
        assert!(!isolation.warn_due(start + ISOLATED_WARN_AFTER * 9));

        let lost = start + ISOLATED_WARN_AFTER * 10;
        isolation.neighbors_changed(false, lost);
        assert!(!isolation.warn_due(lost + ISOLATED_WARN_AFTER / 2));
        assert!(isolation.warn_due(lost + ISOLATED_WARN_AFTER * 2));
    }

    fn peer_id_of(seed: u8) -> EndpointId {
        SecretKey::from_bytes(&[seed; 32]).public()
    }

    /// A peer that stays without an address is a fault to see in the log: one warning once the
    /// spell is longer than the bound, and no more for the same spell.
    #[test]
    fn a_peer_that_never_gets_an_address_is_noted_once_after_the_bound() {
        let mut watch = NoAddressWatch::default();
        let peer = peer_id_of(1);
        let start = Instant::now();
        assert!(!watch.note(peer, start));
        assert!(!watch.note(peer, start + NO_ADDRESS_WARN_AFTER / 2));
        assert!(watch.note(
            peer,
            start + NO_ADDRESS_WARN_AFTER + Duration::from_millis(1)
        ));
        assert!(!watch.note(peer, start + NO_ADDRESS_WARN_AFTER * 2));
    }

    /// A peer whose address comes in time never reaches the bound: the dial that works ends the
    /// spell, and the next gap without an address starts a new one.
    #[test]
    fn a_peer_that_gets_its_address_in_time_is_not_noted() {
        let mut watch = NoAddressWatch::default();
        let peer = peer_id_of(2);
        let start = Instant::now();
        assert!(!watch.note(peer, start));
        watch.forget(peer);
        assert!(!watch.note(peer, start + NO_ADDRESS_WARN_AFTER * 2));
    }

    /// A spell is a run of failed dials. When the peer is not dialed for a long gap, the run is
    /// over: the next failure starts a new spell, and the old entry cannot stay for ever.
    #[test]
    fn a_spell_ends_when_the_peer_is_not_dialed_for_a_long_gap() {
        let mut watch = NoAddressWatch::default();
        let peer = peer_id_of(5);
        let start = Instant::now();
        assert!(!watch.note(peer, start));
        let later = start + NO_ADDRESS_SPELL_ENDS_AFTER + Duration::from_millis(1);
        assert!(!watch.note(peer, later), "the gap ended the spell");
        assert!(watch.note(
            peer,
            later + NO_ADDRESS_WARN_AFTER + Duration::from_millis(1)
        ));
    }

    /// The map of spells has a cap: the oldest one goes out first.
    #[test]
    fn the_spells_are_capped_and_the_oldest_goes_out_first() {
        let mut watch = NoAddressWatch::default();
        let start = Instant::now();
        let peer_of = |i: usize| {
            let mut bytes = [7u8; 32];
            bytes[..8].copy_from_slice(&(i as u64).to_le_bytes());
            SecretKey::from_bytes(&bytes).public()
        };
        for i in 0..=MAX_NO_ADDRESS_SPELLS {
            watch.note(peer_of(i), start + Duration::from_micros(i as u64));
        }
        assert_eq!(watch.spells.len(), MAX_NO_ADDRESS_SPELLS);
        assert!(!watch.spells.contains_key(&peer_of(0)), "the oldest stayed");
        assert!(watch.spells.contains_key(&peer_of(MAX_NO_ADDRESS_SPELLS)));
    }

    /// The spells of two peers do not mix.
    #[test]
    fn the_spell_of_a_peer_is_its_own() {
        let mut watch = NoAddressWatch::default();
        let start = Instant::now();
        assert!(!watch.note(peer_id_of(3), start));
        assert!(!watch.note(peer_id_of(4), start + NO_ADDRESS_WARN_AFTER * 2));
    }

    /// The actor warns once for a peer that has no address for longer than the bound, and the
    /// dials before that stay at debug.
    #[tokio::test]
    async fn a_peer_that_stays_without_an_address_gets_one_warning() -> Result {
        let warnings = Arc::new(Mutex::new(Vec::new()));
        let _log = tracing::subscriber::set_default(KeepsNetWarnings(Arc::clone(&warnings)));
        let rng = &mut rand::rngs::ChaCha12Rng::seed_from_u64(1);
        let ct = CancellationToken::new();
        let (relay_map, _relay_url, _guard) = iroh::test_utils::run_relay_server().await.unwrap();
        let (go1, _ep1, ep1_handle, _test_actor_handle1) =
            Gossip::t_new(rng, Default::default(), relay_map, &ct).await?;
        let peer_id = SecretKey::from_bytes(&rng.random()).public();
        let topic: TopicId = blake3::hash(b"a_peer_that_stays_without_an_address").into();
        let (sender, _receiver) = go1.subscribe(topic, Vec::new()).await?.split();
        let metrics = go1.metrics().clone();

        // One dial every 300 ms for 3 s: the bound of a test is 1 s.
        for _ in 0..10 {
            sender.join_peers(vec![peer_id]).await?;
            tokio::time::sleep(Duration::from_millis(300)).await;
        }
        assert!(
            metrics.actor_tick_dialer_failure.get() >= 5,
            "the peer was dialed {} times",
            metrics.actor_tick_dialer_failure.get()
        );

        ct.cancel();
        timeout(Duration::from_secs(2), ep1_handle)
            .await
            .std_context("wait endpoint1 task")?
            .std_context("join endpoint1 task")??;
        let warnings = warnings.lock().expect("warnings mutex poisoned").clone();
        assert_eq!(
            warnings
                .iter()
                .filter(|event| event.contains("dial without an address"))
                .count(),
            1,
            "{warnings:?}"
        );
        Ok(())
    }

    /// A dial that nobody asks for again runs to its own timeout and logs the warning. Only a new
    /// request for the same peer replaces a dial, and a replaced dial logs nothing.
    #[tokio::test]
    async fn a_dial_that_is_not_asked_again_times_out_with_a_warning() -> Result {
        let warnings = warnings_of_a_failed_dial(FailedDial::TimesOut).await?;
        assert!(
            warnings
                .iter()
                .any(|event| event.contains("dial failed: timed out")),
            "{warnings:?}"
        );
        Ok(())
    }

    /// Every other dial failure stays a warning: here the handshake fails with another code.
    #[tokio::test]
    async fn a_dial_that_fails_for_another_reason_is_a_warning() -> Result {
        let warnings = warnings_of_a_failed_dial(FailedDial::WrongProtocol).await?;
        assert!(
            warnings.iter().any(|event| event.contains("dial failed")),
            "{warnings:?}"
        );
        Ok(())
    }

    /// A dial that starts while nobody serves the key stays in flight until its idle timeout: the
    /// handshake backs off its retransmits, and the last one before the timeout is long past when
    /// the key gets served. A caller that asks for the peer again, as the healer of the app does,
    /// must get a new dial, so that the link comes soon after the key is served, not at the timeout.
    #[tokio::test]
    async fn a_dial_to_a_key_that_is_served_late_links_soon_after() -> Result {
        const SERVED_AFTER: Duration = Duration::from_secs(22);
        let warnings = Arc::new(Mutex::new(Vec::new()));
        let _log = tracing::subscriber::set_default(KeepsNetWarnings(Arc::clone(&warnings)));
        let rng = &mut rand::rngs::ChaCha12Rng::seed_from_u64(1);
        let ct = CancellationToken::new();
        let (relay_map, relay_url, _guard) = iroh::test_utils::run_relay_server().await.unwrap();
        let (go1, ep1, ep1_handle, _test_actor_handle1) =
            Gossip::t_new(rng, Default::default(), relay_map.clone(), &ct).await?;
        let secret_key = SecretKey::from_bytes(&rng.random());
        let peer_id = secret_key.public();
        let memory_lookup = MemoryLookup::new();
        memory_lookup.add_endpoint_info(EndpointAddr::new(peer_id).with_relay_url(relay_url));
        ep1.address_lookup()?.add(memory_lookup);

        let topic: TopicId = blake3::hash(b"a_dial_to_a_key_that_is_served_late").into();
        let (sender1, mut receiver1) = go1.subscribe(topic, vec![peer_id]).await?.split();

        let served_at = Arc::new(Mutex::new(None));
        let server = spawn({
            let served_at = Arc::clone(&served_at);
            let ct = ct.clone();
            async move {
                tokio::time::sleep(SERVED_AFTER).await;
                let peer = Endpoint::builder(presets::Minimal)
                    .relay_mode(RelayMode::Custom(relay_map))
                    .secret_key(secret_key)
                    .alpns(vec![GOSSIP_ALPN.to_vec()])
                    .ca_tls_config(CaTlsConfig::insecure_skip_verify())
                    .bind()
                    .await?;
                peer.online().await;
                *served_at.lock().expect("served_at mutex poisoned") =
                    Some(std::time::Instant::now());
                let (go2, _ep2, ep2_handle, _test_actor_handle2) =
                    Gossip::t_new_on(peer, Default::default(), &ct);
                let _sub2 = go2.subscribe(topic, Vec::new()).await?;
                ep2_handle.await.std_context("join endpoint2 task")??;
                Ok::<(), AnyError>(())
            }
        });

        // Ask for the peer every 400 ms until it links, as the healer of the app does.
        timeout(Duration::from_secs(50), async {
            let mut tick = tokio::time::interval(Duration::from_millis(400));
            loop {
                tokio::select! {
                    _ = tick.tick() => sender1.join_peers(vec![peer_id]).await?,
                    event = receiver1.try_next() => {
                        if event? == Some(Event::NeighborUp(peer_id)) {
                            return Ok::<(), AnyError>(());
                        }
                    }
                }
            }
        })
        .await
        .std_context("wait for the link")??;
        let served_at = served_at
            .lock()
            .expect("served_at mutex poisoned")
            .expect("the key is served before the link");
        let late_by = served_at.elapsed();
        assert!(
            late_by < Duration::from_secs(6),
            "the link came {late_by:?} after the key was served"
        );
        // The dials that were replaced were not faults.
        assert_eq!(
            *warnings.lock().expect("warnings mutex poisoned"),
            Vec::<String>::new()
        );

        ct.cancel();
        let wait = Duration::from_secs(2);
        timeout(wait, ep1_handle)
            .await
            .std_context("wait endpoint1 task")?
            .std_context("join endpoint1 task")??;
        timeout(wait, server)
            .await
            .std_context("wait the server task")?
            .std_context("join the server task")??;
        Result::Ok(())
    }

    /// A connection that dies before the gossip handshake never made the peer a
    /// proto neighbor, so the proto never asks to disconnect it. The dead `Active`
    /// entry must not swallow every later send: the next send must dial again.
    #[tokio::test]
    #[traced_test]
    async fn redial_after_connection_dies_before_handshake() -> Result {
        let rng = &mut rand::rngs::ChaCha12Rng::seed_from_u64(1);
        let ct = CancellationToken::new();
        let (relay_map, relay_url, _guard) = iroh::test_utils::run_relay_server().await.unwrap();

        let (go1, ep1, ep1_handle, _test_actor_handle1) =
            Gossip::t_new(rng, Default::default(), relay_map.clone(), &ct).await?;
        let ep2 = create_endpoint(rng, relay_map, None).await?;
        let go2 = Gossip::builder().spawn(ep2.clone());
        let endpoint_id2 = ep2.id();

        // endpoint 2 closes the first connection as soon as endpoint 1 starts to send
        // on it, and hands every later connection to its gossip.
        let ct2 = ct.child_token();
        let go2_accept = go2.clone();
        let ep2_accept = ep2.clone();
        let ep2_handle = task::spawn(async move {
            let mut first = true;
            loop {
                let incoming = tokio::select! {
                    biased;
                    _ = ct2.cancelled() => break,
                    incoming = ep2_accept.accept() => match incoming {
                        None => break,
                        Some(incoming) => incoming,
                    },
                };
                let conn = incoming
                    .accept()
                    .std_context("accept incoming")?
                    .await
                    .std_context("await incoming connection")?;
                if first {
                    first = false;
                    let mut stream = conn.accept_uni().await.std_context("accept uni")?;
                    let mut buf = [0u8; 1];
                    stream.read(&mut buf).await.std_context("read first byte")?;
                    tracing::info!("closing the first connection before the gossip handshake");
                    conn.close(1u32.into(), b"refused");
                } else {
                    go2_accept.handle_connection(conn).await?;
                }
            }
            Result::<(), AnyError>::Ok(())
        });

        let memory_lookup = MemoryLookup::new();
        memory_lookup.add_endpoint_info(EndpointAddr::new(endpoint_id2).with_relay_url(relay_url));
        ep1.address_lookup()?.add(memory_lookup);

        let topic: TopicId = blake3::hash(b"redial_after_connection_dies_before_handshake").into();
        let _sub2 = go2.subscribe(topic, Vec::new()).await?;
        let sub1 = go1.subscribe(topic, vec![endpoint_id2]).await?;
        let (sender1, mut receiver1) = sub1.split();

        // A join sent before the actor notices the dead connection is lost with it,
        // so the join is repeated until a neighbor comes up.
        let joined = async {
            loop {
                sender1.join_peers(vec![endpoint_id2]).await?;
                if let Ok(ev) = timeout(Duration::from_millis(200), receiver1.try_next()).await {
                    break ev;
                }
            }
        };
        let ev = timeout(Duration::from_secs(5), joined)
            .await
            .std_context("wait neighbor up after redial")??;
        assert_eq!(ev, Some(Event::NeighborUp(endpoint_id2)));

        ct.cancel();
        let wait = Duration::from_secs(2);
        timeout(wait, ep1_handle)
            .await
            .std_context("wait endpoint1 task")?
            .std_context("join endpoint1 task")??;
        timeout(wait, ep2_handle)
            .await
            .std_context("wait endpoint2 task")?
            .std_context("join endpoint2 task")??;

        Result::Ok(())
    }

    /// A `Join` queued behind a failed dial must survive, so an inbound
    /// connection can flush it. The first fix attempt (0ecff17) removed the whole
    /// `Pending` entry and lost that `Join` for good, and hyparview never
    /// re-issues one. `b` joins with no bootstrap peers, so the `Join` that `a`
    /// queued is the only one that can form this mesh.
    #[tokio::test]
    #[traced_test]
    async fn queued_join_survives_failed_dial_for_inbound_flush() -> Result {
        let rng = &mut rand::rngs::ChaCha12Rng::seed_from_u64(1);
        let ct = CancellationToken::new();
        let (relay_map, relay_url, _guard) = iroh::test_utils::run_relay_server().await.unwrap();

        let (go_a, ep_a, ep_a_handle, _actor_a) =
            Gossip::t_new(rng, Default::default(), relay_map.clone(), &ct).await?;
        let (go_b, ep_b, ep_b_handle, _actor_b) =
            Gossip::t_new(rng, Default::default(), relay_map, &ct).await?;
        let id_a = ep_a.id();
        let id_b = ep_b.id();

        // `b` can resolve `a`, but `a` has no address for `b`, so `a`'s
        // bootstrap dial fails immediately.
        let lookup_b = MemoryLookup::new();
        lookup_b.add_endpoint_info(EndpointAddr::new(id_a).with_relay_url(relay_url));
        ep_b.address_lookup()?.add(lookup_b);

        let topic: TopicId = blake3::hash(b"queued_join_survives_failed_dial").into();

        // No bootstrap peers: `b` sends no `Join`.
        let _sub_b = go_b.subscribe(topic, Vec::new()).await?;

        let sub_a = go_a.subscribe(topic, vec![id_b]).await?;
        let (_sender_a, mut receiver_a) = sub_a.split();

        let metrics_a = go_a.metrics().clone();
        timeout(Duration::from_secs(20), async {
            while metrics_a.actor_tick_dialer_failure.get() == 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .std_context("wait dial failure")?;
        tracing::info!("a's bootstrap dial failed; b now connects inbound");

        // The rescue path: `b` dials in, and the admission of the connection must flush the `Join`
        // still queued for `b`.
        let conn = ep_b
            .connect(id_a, GOSSIP_ALPN)
            .await
            .std_context("b connects to a")?;
        go_b.handle_connection(conn).await?;

        let ev = timeout(Duration::from_secs(5), receiver_a.try_next())
            .await
            .std_context("wait neighbor up from the flushed join")??;
        assert_eq!(ev, Some(Event::NeighborUp(id_b)));

        ct.cancel();
        let wait = Duration::from_secs(2);
        timeout(wait, ep_a_handle)
            .await
            .std_context("wait endpoint a task")?
            .std_context("join endpoint a task")??;
        timeout(wait, ep_b_handle)
            .await
            .std_context("wait endpoint b task")?
            .std_context("join endpoint b task")??;

        Result::Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn can_die_and_reconnect() -> Result {
        /// Runs a future in a separate runtime on a separate thread, cancelling everything
        /// abruptly once `cancel` is invoked.
        fn run_in_thread<T: Send + 'static>(
            cancel: CancellationToken,
            fut: impl std::future::Future<Output = T> + Send + 'static,
        ) -> std::thread::JoinHandle<Option<T>> {
            std::thread::spawn(move || {
                let rt = tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()
                    .unwrap();
                rt.block_on(async move { cancel.run_until_cancelled(fut).await })
            })
        }

        /// Spawns a new endpoint and gossip instance.
        async fn spawn_gossip(
            secret_key: SecretKey,
            relay_map: RelayMap,
        ) -> Result<(Router, Gossip), BindError> {
            let ep = Endpoint::builder(presets::Minimal)
                .relay_mode(RelayMode::Custom(relay_map))
                .secret_key(secret_key)
                .ca_tls_config(CaTlsConfig::insecure_skip_verify())
                .bind()
                .await?;
            let gossip = Gossip::builder().spawn(ep.clone());
            let router = Router::builder(ep)
                .accept(GOSSIP_ALPN, gossip.clone())
                .spawn();
            Ok((router, gossip))
        }

        /// Spawns a gossip endpoint, and broadcasts a single message, then sleep until cancelled externally.
        async fn broadcast_once(
            secret_key: SecretKey,
            relay_map: RelayMap,
            bootstrap_addr: EndpointAddr,
            topic_id: TopicId,
            message: String,
        ) -> Result {
            let (router, gossip) = spawn_gossip(secret_key, relay_map).await?;
            info!(endpoint_id = %router.endpoint().id().fmt_short(), "broadcast endpoint spawned");
            let bootstrap = vec![bootstrap_addr.id];
            let memory_lookup = MemoryLookup::new();
            memory_lookup.add_endpoint_info(bootstrap_addr);
            router.endpoint().address_lookup()?.add(memory_lookup);
            let mut topic = gossip.subscribe_and_join(topic_id, bootstrap).await?;
            topic.broadcast(message.as_bytes().to_vec().into()).await?;
            std::future::pending::<()>().await;
            Ok(())
        }

        let (relay_map, _relay_url, _guard) = iroh::test_utils::run_relay_server().await.unwrap();
        let rng = &mut rand::rngs::ChaCha12Rng::seed_from_u64(1);
        let topic_id = TopicId::from_bytes(rng.random());

        // spawn a gossip endpoint, send the endpoint's address on addr_tx,
        // then wait to receive `count` messages, and terminate.
        let (addr_tx, addr_rx) = tokio::sync::oneshot::channel();
        let (msgs_recv_tx, mut msgs_recv_rx) = tokio::sync::mpsc::channel(3);
        let recv_task = tokio::task::spawn({
            let relay_map = relay_map.clone();
            let secret_key = SecretKey::from_bytes(&rng.random());
            async move {
                let (router, gossip) = spawn_gossip(secret_key, relay_map).await?;
                // wait for the relay to be set. iroh currently has issues when trying
                // to immediately reconnect with changed direct addresses, but when the
                // relay path is available it works.
                // See https://github.com/n0-computer/iroh/pull/3372
                router.endpoint().online().await;
                let addr = router.endpoint().addr();
                info!(endpoint_id = %addr.id.fmt_short(), "recv endpoint spawned");
                addr_tx.send(addr).unwrap();
                let mut topic = gossip.subscribe_and_join(topic_id, vec![]).await?;
                while let Some(event) = topic.try_next().await.unwrap() {
                    if let Event::Received(message) = event {
                        let message = std::str::from_utf8(&message.content)
                            .std_context("decode broadcast message")?
                            .to_string();
                        msgs_recv_tx
                            .send(message)
                            .await
                            .std_context("forward received message")?;
                    }
                }
                Ok::<_, AnyError>(())
            }
        });

        let endpoint0_addr = addr_rx.await.std_context("receive endpoint address")?;
        let max_wait = Duration::from_secs(5);

        // spawn a endpoint, send a message, and then abruptly terminate the endpoint ungracefully
        // after the message was received on our receiver endpoint.
        let cancel = CancellationToken::new();
        let secret = SecretKey::from_bytes(&rng.random());
        let join_handle_1 = run_in_thread(
            cancel.clone(),
            broadcast_once(
                secret.clone(),
                relay_map.clone(),
                endpoint0_addr.clone(),
                topic_id,
                "msg1".to_string(),
            ),
        );
        // assert that we received the message on the receiver endpoint.
        let msg = timeout(max_wait, msgs_recv_rx.recv())
            .await
            .std_context("wait for first broadcast")?
            .std_context("receiver dropped channel")?;
        assert_eq!(&msg, "msg1");
        info!("kill broadcast endpoint");
        cancel.cancel();

        // spawns the endpoint again with the same endpoint id, and send another message
        let cancel = CancellationToken::new();
        let join_handle_2 = run_in_thread(
            cancel.clone(),
            broadcast_once(
                secret.clone(),
                relay_map.clone(),
                endpoint0_addr.clone(),
                topic_id,
                "msg2".to_string(),
            ),
        );
        // assert that we received the message on the receiver endpoint.
        // this means that the reconnect with the same endpoint id worked.
        let msg = timeout(max_wait, msgs_recv_rx.recv())
            .await
            .std_context("wait for second broadcast")?
            .std_context("receiver dropped channel")?;
        assert_eq!(&msg, "msg2");
        info!("kill broadcast endpoint");
        cancel.cancel();

        info!("kill recv endpoint");
        recv_task.abort();
        assert!(join_handle_1.join().unwrap().is_none());
        assert!(join_handle_2.join().unwrap().is_none());

        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn gossip_change_alpn() -> n0_error::Result<()> {
        let alpn = b"my-gossip-alpn";
        let topic_id = TopicId::from([0u8; 32]);

        let ep1 = Endpoint::bind(presets::Minimal).await?;
        let ep2 = Endpoint::bind(presets::Minimal).await?;
        let gossip1 = Gossip::builder().alpn(alpn).spawn(ep1.clone());
        let gossip2 = Gossip::builder().alpn(alpn).spawn(ep2.clone());
        let router1 = Router::builder(ep1).accept(alpn, gossip1.clone()).spawn();
        let router2 = Router::builder(ep2).accept(alpn, gossip2.clone()).spawn();

        let addr1 = router1.endpoint().addr();
        let id1 = addr1.id;
        let memory_lookup = MemoryLookup::new();
        memory_lookup.add_endpoint_info(addr1);
        router2.endpoint().address_lookup()?.add(memory_lookup);

        let mut topic1 = gossip1.subscribe(topic_id, vec![]).await?;
        let mut topic2 = gossip2.subscribe(topic_id, vec![id1]).await?;

        timeout(Duration::from_secs(3), topic1.joined())
            .await
            .std_context("wait topic1 join")??;
        timeout(Duration::from_secs(3), topic2.joined())
            .await
            .std_context("wait topic2 join")??;
        router1.shutdown().await.std_context("shutdown router1")?;
        router2.shutdown().await.std_context("shutdown router2")?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn gossip_rely_on_gossip_address_lookup() -> n0_error::Result<()> {
        let rng = &mut rand::rngs::ChaCha12Rng::seed_from_u64(1);

        async fn spawn(
            rng: &mut impl CryptoRng,
        ) -> n0_error::Result<(EndpointId, Router, Gossip, GossipSender, GossipReceiver)> {
            let topic_id = TopicId::from([0u8; 32]);
            let ep = Endpoint::builder(presets::Minimal)
                .secret_key(SecretKey::from_bytes(&rng.random()))
                .bind()
                .await?;
            let endpoint_id = ep.id();
            let gossip = Gossip::builder().spawn(ep.clone());
            let router = Router::builder(ep)
                .accept(GOSSIP_ALPN, gossip.clone())
                .spawn();
            let topic = gossip.subscribe(topic_id, vec![]).await?;
            let (sender, receiver) = topic.split();
            Ok((endpoint_id, router, gossip, sender, receiver))
        }

        // spawn 3 endpoints without relay or address lookup
        let (n1, r1, _g1, _tx1, mut rx1) = spawn(rng).await?;
        let (n2, r2, _g2, tx2, mut rx2) = spawn(rng).await?;
        let (n3, r3, _g3, tx3, mut rx3) = spawn(rng).await?;

        println!("endpoints {:?}", [n1, n2, n3]);

        // create a mem lookup that has only endpoint 1 addr info set
        let addr1 = r1.endpoint().addr();
        let lookup = MemoryLookup::new();
        lookup.add_endpoint_info(addr1);

        // add addr info of endpoint1 to endpoint2 and join endpoint1
        r2.endpoint().address_lookup()?.add(lookup.clone());
        tx2.join_peers(vec![n1]).await?;

        // await join endpoint2 -> nodde1
        timeout(Duration::from_secs(3), rx1.joined())
            .await
            .std_context("wait rx1 join")??;
        timeout(Duration::from_secs(3), rx2.joined())
            .await
            .std_context("wait rx2 join")??;

        // add addr info of endpoint1 to endpoint3 and join endpoint1
        r3.endpoint().address_lookup()?.add(lookup.clone());
        tx3.join_peers(vec![n1]).await?;

        // await join at endpoint3: n1 and n2
        // n2 only works because because we use gossip address lookup!
        let ev = timeout(Duration::from_secs(3), rx3.next())
            .await
            .std_context("wait rx3 first neighbor")?;
        assert!(matches!(ev, Some(Ok(Event::NeighborUp(_)))));
        let ev = timeout(Duration::from_secs(3), rx3.next())
            .await
            .std_context("wait rx3 second neighbor")?;
        assert!(matches!(ev, Some(Ok(Event::NeighborUp(_)))));

        assert_eq!(sorted(rx3.neighbors()), sorted([n1, n2]));

        let ev = timeout(Duration::from_secs(3), rx2.next())
            .await
            .std_context("wait rx2 neighbor")?;
        assert!(matches!(ev, Some(Ok(Event::NeighborUp(n))) if n == n3));

        let ev = timeout(Duration::from_secs(3), rx1.next())
            .await
            .std_context("wait rx1 neighbor")?;
        assert!(matches!(ev, Some(Ok(Event::NeighborUp(n))) if n == n3));

        tokio::try_join!(r1.shutdown(), r2.shutdown(), r3.shutdown())
            .std_context("shutdown routers")?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn leave_peers_drops_the_neighbor_and_stays_away() -> n0_error::Result<()> {
        let rng = &mut rand::rngs::ChaCha12Rng::seed_from_u64(2);
        let topic_id = TopicId::from([0u8; 32]);

        async fn spawn(
            rng: &mut impl CryptoRng,
            topic_id: TopicId,
        ) -> n0_error::Result<(EndpointId, Router, GossipSender, GossipReceiver)> {
            let ep = Endpoint::builder(presets::Minimal)
                .secret_key(SecretKey::from_bytes(&rng.random()))
                .bind()
                .await?;
            let endpoint_id = ep.id();
            let gossip = Gossip::builder().spawn(ep.clone());
            let router = Router::builder(ep)
                .accept(GOSSIP_ALPN, gossip.clone())
                .spawn();
            let topic = gossip.subscribe(topic_id, vec![]).await?;
            let (sender, receiver) = topic.split();
            Ok((endpoint_id, router, sender, receiver))
        }

        let (n1, r1, tx1, mut rx1) = spawn(rng, topic_id).await?;
        let (n2, r2, tx2, mut rx2) = spawn(rng, topic_id).await?;
        let lookup = MemoryLookup::new();
        lookup.add_endpoint_info(r1.endpoint().addr());
        r2.endpoint().address_lookup()?.add(lookup);
        tx2.join_peers(vec![n1]).await?;
        timeout(Duration::from_secs(3), rx1.joined())
            .await
            .std_context("wait rx1 join")??;
        timeout(Duration::from_secs(3), rx2.joined())
            .await
            .std_context("wait rx2 join")??;

        // No wait here on purpose: a Neighbor message of the join handshake can still be in flight.
        tx1.leave_peers(vec![n2]).await?;

        let ev = timeout(Duration::from_secs(3), rx1.next())
            .await
            .std_context("wait rx1 down")?;
        assert!(matches!(ev, Some(Ok(Event::NeighborDown(n))) if n == n2));
        let ev = timeout(Duration::from_secs(3), rx2.next())
            .await
            .std_context("wait rx2 down")?;
        assert!(matches!(ev, Some(Ok(Event::NeighborDown(n))) if n == n1));

        // No event, so no reconnect, within a bounded wait.
        let e1 = timeout(Duration::from_secs(2), rx1.next()).await;
        assert!(e1.is_err(), "rx1 got {e1:?}");
        let e2 = timeout(Duration::from_secs(2), rx2.next()).await;
        assert!(e2.is_err(), "rx2 got {e2:?}");
        assert!(rx1.neighbors().next().is_none());
        assert!(rx2.neighbors().next().is_none());

        tokio::try_join!(r1.shutdown(), r2.shutdown()).std_context("shutdown routers")?;
        Ok(())
    }

    #[tokio::test]
    #[traced_test]
    async fn leave_peers_is_not_undone_by_shuffles_of_a_third_peer() -> n0_error::Result<()> {
        let rng = &mut rand::rngs::ChaCha12Rng::seed_from_u64(3);
        let topic_id = TopicId::from([0u8; 32]);

        async fn spawn(
            rng: &mut impl CryptoRng,
            topic_id: TopicId,
        ) -> n0_error::Result<(EndpointId, Router, GossipSender, GossipReceiver)> {
            let ep = Endpoint::builder(presets::Minimal)
                .secret_key(SecretKey::from_bytes(&rng.random()))
                .bind()
                .await?;
            let endpoint_id = ep.id();
            let membership = proto::HyparviewConfig {
                shuffle_interval: Duration::from_millis(100),
                ..Default::default()
            };
            let gossip = Gossip::builder()
                .membership_config(membership)
                .spawn(ep.clone());
            let router = Router::builder(ep)
                .accept(GOSSIP_ALPN, gossip.clone())
                .spawn();
            let topic = gossip.subscribe(topic_id, vec![]).await?;
            let (sender, receiver) = topic.split();
            Ok((endpoint_id, router, sender, receiver))
        }

        let (n1, r1, tx1, mut rx1) = spawn(rng, topic_id).await?;
        let (n2, r2, tx2, mut rx2) = spawn(rng, topic_id).await?;
        let (_n3, r3, tx3, mut rx3) = spawn(rng, topic_id).await?;
        let lookup = MemoryLookup::new();
        lookup.add_endpoint_info(r1.endpoint().addr());
        r2.endpoint().address_lookup()?.add(lookup.clone());
        r3.endpoint().address_lookup()?.add(lookup);
        tx2.join_peers(vec![n1]).await?;
        tx3.join_peers(vec![n1]).await?;
        // n1 is linked with n2 and n3, and n2 is linked with n3.
        // `neighbors()` only changes while the receiver is polled, so drain the events.
        async fn drain(rx: &mut GossipReceiver, duration: Duration) {
            timeout(duration, async { while rx.next().await.is_some() {} })
                .await
                .ok();
        }
        for _ in 0..100 {
            drain(&mut rx1, Duration::from_millis(30)).await;
            drain(&mut rx2, Duration::from_millis(30)).await;
            drain(&mut rx3, Duration::from_millis(30)).await;
            if rx1.neighbors().count() == 2 && rx2.neighbors().count() == 2 {
                break;
            }
        }
        assert_eq!(rx1.neighbors().count(), 2, "n1 needs both neighbors");
        assert_eq!(rx2.neighbors().count(), 2, "n2 needs both neighbors");

        tx1.leave_peers(vec![n2]).await?;

        // 30 shuffle intervals. n3 keeps naming n1 to n2, and n2 to n1.
        for _ in 0..30 {
            drain(&mut rx1, Duration::from_millis(33)).await;
            drain(&mut rx2, Duration::from_millis(33)).await;
            drain(&mut rx3, Duration::from_millis(33)).await;
        }
        assert!(!rx1.neighbors().any(|n| n == n2), "n1 linked n2 again");
        assert!(!rx2.neighbors().any(|n| n == n1), "n2 linked n1 again");
        assert!(
            rx1.neighbors().any(|n| n != n2),
            "n1 lost its other neighbor"
        );

        // A Join links the pair again.
        tx1.join_peers(vec![n2]).await?;
        for _ in 0..100 {
            drain(&mut rx1, Duration::from_millis(30)).await;
            drain(&mut rx2, Duration::from_millis(30)).await;
            if rx1.neighbors().any(|n| n == n2) && rx2.neighbors().any(|n| n == n1) {
                break;
            }
        }
        assert!(rx1.neighbors().any(|n| n == n2), "n1 did not link n2");
        assert!(rx2.neighbors().any(|n| n == n1), "n2 did not link n1");

        drop((tx1, tx2, tx3, rx3));
        tokio::try_join!(r1.shutdown(), r2.shutdown(), r3.shutdown())
            .std_context("shutdown routers")?;
        Ok(())
    }

    /// A `Join` request for `topic`: the actor takes its sender as a neighbor, and the stuck-queue
    /// tests need any message to fill a queue with.
    fn join_for_test(topic: TopicId) -> ProtoMessage {
        ProtoMessage {
            topic,
            message: proto::topic::Message::Swarm(proto::hyparview::Message::Join(None)),
        }
    }

    /// A manual actor with fake peers: each has a send queue that the test reads or not.
    struct Rig {
        actor: Actor,
        topic: TopicId,
        peers: Vec<FakePeer>,
        events: broadcast::Receiver<ProtoEvent>,
        _endpoint_task: EndpointHandle,
        _relay: Box<dyn std::any::Any>,
    }

    struct FakePeer {
        id: EndpointId,
        tx: mpsc::Sender<ProtoMessage>,
        rx: mpsc::Receiver<ProtoMessage>,
    }

    impl Rig {
        /// Joins `n` fake peers: the actor has each as an active neighbor.
        async fn new(seed: u64, n: usize) -> Result<Self> {
            let rng = &mut rand::rngs::ChaCha12Rng::seed_from_u64(seed);
            let ct = CancellationToken::new();
            let (relay_map, _relay_url, relay) =
                iroh::test_utils::run_relay_server().await.unwrap();
            let (_gossip, mut actor, endpoint_task) =
                Gossip::t_new_with_actor(rng, Default::default(), relay_map, &ct).await?;
            let topic: TopicId = blake3::hash(b"actor_queue_rig").into();
            actor.topics.insert(topic, TopicState::default());
            let events = actor.topics[&topic].event_sender.subscribe();

            let mut peers = Vec::new();
            for conn_id in 0..n {
                let id = SecretKey::from_bytes(&rng.random()).public();
                let (tx, rx) = mpsc::channel(SEND_QUEUE_CAP);
                let mut state = PeerState::default();
                state.admit(
                    Conn::new(
                        conn_id,
                        ConnHandle {
                            send_tx: Some(tx.clone()),
                            conn: None,
                        },
                        true,
                        RxClock::new(),
                    ),
                    Instant::now(),
                );
                actor.peers.insert(id, state);
                peers.push(FakePeer { id, tx, rx });
            }
            let now = Instant::now();
            actor
                .handle_in_event(
                    proto::InEvent::Command(topic, ProtoCommand::Join(Vec::new())),
                    now,
                )
                .await;
            for peer in &mut peers {
                actor
                    .handle_in_event(
                        proto::InEvent::RecvMessage(peer.id, join_for_test(topic)),
                        now,
                    )
                    .await;
                while peer.rx.try_recv().is_ok() {}
            }
            Ok(Self {
                actor,
                topic,
                peers,
                events,
                _endpoint_task: endpoint_task,
                _relay: Box::new(relay),
            })
        }

        async fn broadcast(&mut self, payload: impl Into<Bytes>) {
            let command = proto::InEvent::Command(
                self.topic,
                ProtoCommand::Broadcast(payload.into(), Scope::Swarm),
            );
            self.actor.handle_in_event(command, Instant::now()).await;
        }

        /// Fills the send queue of a peer and keeps its receiver, so that it is never closed.
        fn stick(&mut self, peer: usize) {
            // Any message will do.
            let sample = join_for_test(self.topic);
            while self.peers[peer].tx.try_send(sample.clone()).is_ok() {}
        }

        /// Ends the send loop of a peer: the receiver of its queue is dropped, so that a send on
        /// the queue gets `Closed`, as it does when the send loop of a connection returned.
        fn end_send_loop(&mut self, peer: usize) {
            drop(std::mem::replace(
                &mut self.peers[peer].rx,
                mpsc::channel(1).1,
            ));
        }

        fn drain_data(&mut self, peer: usize) -> usize {
            let mut count = 0;
            while let Ok(message) = self.peers[peer].rx.try_recv() {
                count += usize::from(message.is_droppable());
            }
            count
        }

        fn neighbor_downs(&mut self) -> Vec<EndpointId> {
            let mut out = Vec::new();
            while let Ok(event) = self.events.try_recv() {
                if let ProtoEvent::NeighborDown(peer) = event {
                    out.push(peer);
                }
            }
            out
        }
    }

    /// The actor is one task. A peer whose send queue is full and never drained must not stop it
    /// from serving the other peers.
    #[tokio::test]
    #[traced_test]
    async fn a_peer_with_a_full_send_queue_does_not_stall_the_actor() -> Result {
        let mut rig = Rig::new(7, 2).await?;
        rig.stick(0);
        let now = Instant::now();
        let broadcast = proto::InEvent::Command(
            rig.topic,
            ProtoCommand::Broadcast(Bytes::from_static(b"hello"), Scope::Swarm),
        );
        timeout(
            Duration::from_secs(1),
            rig.actor.handle_in_event(broadcast, now),
        )
        .await
        .std_context("the actor stalled on the full send queue of one peer")?;
        assert_eq!(rig.drain_data(1), 1, "the other peer got the broadcast");
        assert_eq!(
            rig.actor.metrics.msgs_dropped_send_queue_full.get(),
            1,
            "the data message for the stuck peer is dropped and counted"
        );
        assert!(rig.actor.peers.contains_key(&rig.peers[0].id));
        assert!(
            rig.neighbor_downs().is_empty(),
            "a first refusal is not a disconnect"
        );
        Ok(())
    }

    /// A burst, such as the answer to an anti-entropy digest, is not an overflow.
    #[tokio::test]
    #[traced_test]
    async fn a_burst_to_a_healthy_peer_is_delivered_in_full() -> Result {
        let mut rig = Rig::new(8, 1).await?;
        for i in 0..500 {
            rig.broadcast(format!("message {i}")).await;
        }
        assert_eq!(rig.drain_data(0), 500);
        assert_eq!(rig.actor.metrics.msgs_dropped_send_queue_full.get(), 0);
        assert_eq!(rig.actor.metrics.send_overflow_disconnects.get(), 0);
        assert!(rig.neighbor_downs().is_empty());
        Ok(())
    }

    /// A peer that refuses messages for longer than the deadline is disconnected, and a third
    /// peer keeps receiving.
    #[tokio::test]
    #[traced_test]
    async fn a_stuck_peer_is_disconnected_after_the_deadline() -> Result {
        let mut rig = Rig::new(9, 2).await?;
        rig.actor.send_refused_deadline = Duration::from_millis(300);
        rig.stick(0);

        rig.broadcast("before the deadline").await;
        assert!(rig.actor.peers.contains_key(&rig.peers[0].id));
        assert_eq!(rig.actor.metrics.send_overflow_disconnects.get(), 0);

        tokio::time::sleep(Duration::from_millis(400)).await;
        rig.broadcast("after the deadline").await;

        let stuck = rig.peers[0].id;
        assert!(
            !rig.actor.peers.contains_key(&stuck),
            "the stuck peer is gone"
        );
        assert_eq!(rig.actor.metrics.send_overflow_disconnects.get(), 1);
        assert_eq!(rig.neighbor_downs(), vec![stuck]);
        assert_eq!(rig.drain_data(1), 2, "the third peer got both broadcasts");
        Ok(())
    }

    /// A message that must not be dropped is never dropped: the peer is disconnected instead.
    #[tokio::test]
    #[traced_test]
    async fn a_control_message_for_a_full_queue_disconnects_the_peer() -> Result {
        let mut rig = Rig::new(10, 2).await?;
        rig.stick(0);
        let stuck = rig.peers[0].id;
        // Leaving a peer sends it a `Disconnect`, which is a control message.
        let leave = proto::InEvent::Command(rig.topic, ProtoCommand::LeavePeers(vec![stuck]));
        rig.actor.handle_in_event(leave, Instant::now()).await;

        assert_eq!(rig.actor.metrics.msgs_dropped_send_queue_full.get(), 0);
        assert_eq!(rig.actor.metrics.send_overflow_disconnects.get(), 1);
        assert!(!rig.actor.peers.contains_key(&stuck));
        Ok(())
    }

    /// A peer whose only connection lost its send loop is disconnected at the next send. It does
    /// not wait for the result of the connection task: the arm of the event loop that reads it is
    /// behind the arms that carry data, so a busy actor would keep sending to the dead peer.
    #[tokio::test]
    #[traced_test]
    async fn a_peer_whose_send_loop_ended_is_disconnected_at_the_next_send() -> Result {
        let mut rig = Rig::new(12, 2).await?;
        let gone = rig.peers[0].id;
        rig.end_send_loop(0);

        rig.broadcast("after the send loop ended").await;
        // A second send finds no peer to send to and logs nothing more.
        rig.broadcast("and again").await;

        assert!(
            !rig.actor.peers.contains_key(&gone),
            "the peer with the dead send loop is gone"
        );
        assert_eq!(rig.neighbor_downs(), vec![gone]);
        assert_eq!(rig.drain_data(1), 2, "the other peer got both broadcasts");
        Ok(())
    }

    /// A connection of `endpoint` to a peer, closed with an application code by the peer
    /// (`close_reason()` is `ApplicationClosed`) or by `endpoint` (`LocallyClosed`).
    async fn connection_closed_in_order(
        endpoint: &Endpoint,
        by_the_peer: bool,
    ) -> Result<Connection> {
        let peer = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .alpns(vec![GOSSIP_ALPN.to_vec()])
            .ca_tls_config(CaTlsConfig::insecure_skip_verify())
            .bind()
            .await?;
        let port = peer.bound_sockets()[0].port();
        let addr = EndpointAddr::new(peer.id()).with_ip_addr(std::net::SocketAddr::from((
            std::net::Ipv4Addr::LOCALHOST,
            port,
        )));
        let accepting = peer.clone();
        let closer = spawn(async move {
            let incoming = accepting.accept().await.expect("an incoming connection");
            let conn = incoming
                .accept()
                .expect("accept the connection")
                .await
                .expect("finish the handshake");
            if by_the_peer {
                conn.close(7u32.into(), b"the peer closed it");
            }
            conn.closed().await;
        });
        let conn = endpoint.connect(addr, GOSSIP_ALPN).await?;
        if !by_the_peer {
            conn.close(8u32.into(), b"we closed it");
        }
        conn.closed().await;
        closer.await.std_context("join the closing peer")?;
        Ok(conn)
    }

    /// The actor sends to a peer whose connection was closed in order, by the peer or by us, so
    /// the send loop ended: that is the race of a leaving peer, not a fault to warn about. The
    /// reason of the close is on the connection, and the connection task logs it when it finishes.
    #[tokio::test]
    async fn a_send_on_a_connection_that_was_closed_in_order_is_not_a_warning() -> Result {
        for by_the_peer in [true, false] {
            let warnings = Arc::new(Mutex::new(Vec::new()));
            let _log = tracing::subscriber::set_default(KeepsNetWarnings(Arc::clone(&warnings)));
            let mut rig = Rig::new(14, 2).await?;
            let closed = connection_closed_in_order(&rig.actor.endpoint, by_the_peer).await?;
            let peer = rig.peers[0].id;
            let Some(PeerState::Active { conns, .. }) = rig.actor.peers.get_mut(&peer) else {
                panic!("the peer is active");
            };
            conns.get_mut(0).expect("the first connection").handle.conn = Some(closed);
            rig.end_send_loop(0);

            rig.broadcast("after the connection was closed").await;

            let warnings = warnings.lock().expect("warnings mutex poisoned").clone();
            assert_eq!(
                warnings,
                Vec::<String>::new(),
                "closed by the peer: {by_the_peer}"
            );
            assert!(!rig.actor.peers.contains_key(&peer), "the peer is dropped");
        }
        Ok(())
    }

    /// A connection of `endpoint` to a peer that vanishes: a relay of UDP packets carries the
    /// handshake and then stops, as the path to a crashed peer does. `dialer` gives up after one
    /// idle second, so `close_reason()` is `TimedOut`.
    async fn connection_to_a_vanished_peer() -> Result<Connection> {
        use iroh::endpoint::ConnectionError;
        let peer = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .alpns(vec![GOSSIP_ALPN.to_vec()])
            .ca_tls_config(CaTlsConfig::insecure_skip_verify())
            .bind()
            .await?;
        let peer_addr = std::net::SocketAddr::from((
            std::net::Ipv4Addr::LOCALHOST,
            peer.bound_sockets()[0].port(),
        ));
        let accepting = peer.clone();
        spawn(async move {
            while let Some(incoming) = accepting.accept().await {
                if let Ok(connecting) = incoming.accept() {
                    let _ = connecting.await;
                }
            }
        });
        let relay = Arc::new(
            tokio::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, 0))
                .await
                .std_context("bind the relay of packets")?,
        );
        let relay_addr = relay.local_addr().std_context("address of the relay")?;
        let cut = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let (relay_task, cut_task) = (Arc::clone(&relay), Arc::clone(&cut));
        spawn(async move {
            let mut dialer_addr = None;
            let mut buf = vec![0u8; 65536];
            while let Ok((len, from)) = relay_task.recv_from(&mut buf).await {
                if cut_task.load(std::sync::atomic::Ordering::SeqCst) {
                    continue;
                }
                let to = if from == peer_addr {
                    dialer_addr
                } else {
                    dialer_addr = Some(from);
                    Some(peer_addr)
                };
                if let Some(to) = to {
                    let _ = relay_task.send_to(&buf[..len], to).await;
                }
            }
        });
        let idle_timeout = Duration::from_secs(1)
            .try_into()
            .expect("1 s is an idle timeout");
        let dialer = Endpoint::builder(presets::Minimal)
            .relay_mode(RelayMode::Disabled)
            .ca_tls_config(CaTlsConfig::insecure_skip_verify())
            .transport_config(
                QuicTransportConfig::builder()
                    .max_idle_timeout(Some(idle_timeout))
                    .build(),
            )
            .bind()
            .await?;
        let addr = EndpointAddr::new(peer.id()).with_ip_addr(relay_addr);
        let conn = dialer.connect(addr, GOSSIP_ALPN).await?;
        cut.store(true, std::sync::atomic::Ordering::SeqCst);
        conn.closed().await;
        assert!(
            matches!(conn.close_reason(), Some(ConnectionError::TimedOut)),
            "the connection timed out: {:?}",
            conn.close_reason()
        );
        Ok(conn)
    }

    /// The actor sends to a peer that vanished, as a crashed one does: its connection timed out and
    /// the send loop ended. The protocol reports the loss as `NeighborDown`, so the send is not a
    /// fault to warn about.
    #[tokio::test]
    async fn a_send_on_a_connection_to_a_vanished_peer_is_not_a_warning() -> Result {
        let warnings = Arc::new(Mutex::new(Vec::new()));
        let _log = tracing::subscriber::set_default(KeepsNetWarnings(Arc::clone(&warnings)));
        let mut rig = Rig::new(16, 2).await?;
        let lost = connection_to_a_vanished_peer().await?;
        let peer = rig.peers[0].id;
        let Some(PeerState::Active { conns, .. }) = rig.actor.peers.get_mut(&peer) else {
            panic!("the peer is active");
        };
        conns.get_mut(0).expect("the first connection").handle.conn = Some(lost);
        rig.end_send_loop(0);

        rig.broadcast("after the peer vanished").await;

        let warnings = warnings.lock().expect("warnings mutex poisoned").clone();
        assert_eq!(warnings, Vec::<String>::new());
        assert!(!rig.actor.peers.contains_key(&peer), "the peer is dropped");
        Ok(())
    }

    /// Which reasons for the end of a connection are the loss of the peer, and which are a fault.
    #[test]
    fn only_a_lost_peer_or_an_orderly_close_ends_a_connection_without_a_fault() {
        use iroh::endpoint::{ConnectionError, TransportError, TransportErrorCode};
        let without_a_fault = [
            ConnectionError::LocallyClosed,
            ConnectionError::TimedOut,
            ConnectionError::Reset,
            ConnectionError::from(TransportError::new(
                TransportErrorCode::NO_VIABLE_PATH,
                String::new(),
            )),
        ];
        for reason in without_a_fault {
            assert!(is_no_fault(&reason), "{reason:?}");
        }
        let faults = [
            ConnectionError::VersionMismatch,
            ConnectionError::CidsExhausted,
            ConnectionError::from(TransportError::new(
                TransportErrorCode::PROTOCOL_VIOLATION,
                String::new(),
            )),
        ];
        for reason in faults {
            assert!(!is_no_fault(&reason), "{reason:?}");
        }
    }

    /// A peer with no connection to read a reason from keeps the warning.
    #[tokio::test]
    async fn a_send_on_a_connection_without_a_close_reason_is_a_warning() -> Result {
        let warnings = Arc::new(Mutex::new(Vec::new()));
        let _log = tracing::subscriber::set_default(KeepsNetWarnings(Arc::clone(&warnings)));
        let mut rig = Rig::new(15, 2).await?;
        rig.end_send_loop(0);

        rig.broadcast("after the send loop ended").await;

        let warnings = warnings.lock().expect("warnings mutex poisoned").clone();
        assert!(
            warnings
                .iter()
                .any(|event| event.contains("failed to send")),
            "{warnings:?}"
        );
        Ok(())
    }

    /// A peer with a second connection keeps the peer: the connection with the dead send loop
    /// leaves the set, the other one is chosen, and the protocol sees no `NeighborDown`.
    #[tokio::test]
    #[traced_test]
    async fn a_peer_with_a_second_connection_switches_when_the_chosen_send_loop_ended() -> Result {
        let mut rig = Rig::new(13, 1).await?;
        let peer = rig.peers[0].id;
        let (second_tx, mut second_rx) = mpsc::channel(SEND_QUEUE_CAP);
        let Some(PeerState::Active { conns, .. }) = rig.actor.peers.get_mut(&peer) else {
            panic!("the peer is active");
        };
        conns.admit(
            Conn::new(
                1,
                ConnHandle {
                    send_tx: Some(second_tx),
                    conn: None,
                },
                true,
                RxClock::new(),
            ),
            Instant::now(),
        );
        let chosen = conns.chosen().expect("a chosen connection").id;
        let other = 1 - chosen;
        // Ends the send loop of the chosen connection: its receiver is dropped.
        if chosen == 0 {
            rig.end_send_loop(0);
        } else {
            second_rx.close();
        }

        rig.broadcast("after the send loop ended").await;

        let Some(PeerState::Active { conns, .. }) = rig.actor.peers.get(&peer) else {
            panic!("the peer is still active: it has another connection");
        };
        assert_eq!(conns.chosen().map(|conn| conn.id), Some(other));
        assert!(conns.get(chosen).is_none(), "the dead connection is gone");
        assert!(
            rig.neighbor_downs().is_empty(),
            "the peer is not disconnected"
        );
        Ok(())
    }

    /// The queue of a peer that we dial and are not connected to has the same cap.
    #[tokio::test]
    #[traced_test]
    async fn the_queue_for_a_pending_dial_is_capped() -> Result {
        let mut rig = Rig::new(11, 1).await?;
        let ghost = rig.peers[0].id;
        // The peer is a neighbor, but there is no connection: its messages wait for a dial.
        rig.actor.peers.insert(ghost, PeerState::default());
        for i in 0..(PENDING_QUEUE_CAP + 100) {
            rig.broadcast(format!("message {i}")).await;
        }
        let Some(PeerState::Pending { queue }) = rig.actor.peers.get(&ghost) else {
            panic!("the peer must still be pending");
        };
        assert_eq!(queue.len(), PENDING_QUEUE_CAP);
        assert_eq!(rig.actor.metrics.msgs_dropped_send_queue_full.get(), 100);
        Ok(())
    }

    fn sorted<T: Ord>(input: impl IntoIterator<Item = T>) -> Vec<T> {
        let mut out: Vec<_> = input.into_iter().collect();
        out.sort();
        out
    }

    /// Test that dropping sender doesn't close topic while receiver is still listening.
    ///
    /// This is a common footgun: users split a GossipTopic, drop the sender early,
    /// and expect the receiver to keep working. With the bug (using && in still_needed),
    /// the topic closes immediately when sender is dropped.
    #[tokio::test]
    #[traced_test]
    async fn topic_stays_alive_after_sender_drop() -> n0_error::Result<()> {
        let topic_id = TopicId::from([99u8; 32]);

        let ep1 = Endpoint::bind(presets::Minimal).await?;
        let ep2 = Endpoint::bind(presets::Minimal).await?;
        let gossip1 = Gossip::builder().spawn(ep1.clone());
        let gossip2 = Gossip::builder().spawn(ep2.clone());
        let router1 = Router::builder(ep1)
            .accept(crate::ALPN, gossip1.clone())
            .spawn();
        let router2 = Router::builder(ep2)
            .accept(crate::ALPN, gossip2.clone())
            .spawn();

        let addr1 = router1.endpoint().addr();
        let id1 = addr1.id;
        let mem_lookup = MemoryLookup::new();
        mem_lookup.add_endpoint_info(addr1);
        router2.endpoint().address_lookup()?.add(mem_lookup);

        let topic1 = gossip1.subscribe(topic_id, vec![]).await?;
        let topic2 = gossip2.subscribe(topic_id, vec![id1]).await?;

        let (tx1, mut rx1) = topic1.split();
        let (tx2, mut rx2) = topic2.split();

        // Wait for mesh to form
        timeout(Duration::from_secs(3), rx1.joined())
            .await
            .std_context("wait rx1 join")??;
        timeout(Duration::from_secs(3), rx2.joined())
            .await
            .std_context("wait rx2 join")??;

        // Node 1 drops its sender - simulating the footgun where user drops sender early
        drop(tx1);

        // Node 2 sends a message - receiver on node 1 should still get it
        tx2.broadcast(b"hello from node2".to_vec().into()).await?;

        // Node 1's receiver should still work and receive the message
        let event = timeout(Duration::from_secs(3), rx1.next())
            .await
            .std_context("wait for message on rx1")?;

        match event {
            Some(Ok(Event::Received(msg))) => {
                assert_eq!(&msg.content[..], b"hello from node2");
            }
            other => panic!("expected Received event, got {:?}", other),
        }

        drop(tx2);
        drop(rx1);
        drop(rx2);
        router1.shutdown().await.std_context("shutdown router1")?;
        router2.shutdown().await.std_context("shutdown router2")?;
        Ok(())
    }

    #[test]
    fn the_connection_the_lower_id_dialed_wins_on_both_sides() {
        let mut rng = rand::rngs::ChaCha12Rng::seed_from_u64(7);
        let first = SecretKey::from_bytes(&rng.random()).public();
        let second = SecretKey::from_bytes(&rng.random()).public();
        let (low, high) = if first < second {
            (first, second)
        } else {
            (second, first)
        };
        // The connection that the lower id dialed: the lower side dialed it, the higher accepted it.
        assert!(dialed_by_lower_id(low, high, true));
        assert!(dialed_by_lower_id(high, low, false));
        // The connection that the higher id dialed loses on both sides.
        assert!(!dialed_by_lower_id(high, low, true));
        assert!(!dialed_by_lower_id(low, high, false));
    }

    #[test]
    fn a_connection_drains_only_after_a_clean_end_of_its_send_queue() {
        // The send queue closed: the connection was replaced, and its read loop runs on.
        assert!(drains(true, false));
        // A write error, or a connection that is closed already: nothing left to read.
        assert!(!drains(false, false));
        assert!(!drains(true, true));
        assert!(!drains(false, true));
    }

    /// How the two peers of a round find each other.
    #[derive(Clone, Copy)]
    enum JoinBy {
        /// Both join the other as a bootstrap peer.
        Join,
        /// Both subscribe alone and then ask the other for a link with low priority.
        NeighborPeers,
    }

    /// Waits until both peers have reported a neighbor up, so that the settling time that follows
    /// starts after the link, whichever way it was made.
    async fn wait_for_neighbor_up(go1: &Gossip, go2: &Gossip) {
        timeout(Duration::from_secs(10), async {
            while go1.metrics().neighbor_up.get() == 0 || go2.metrics().neighbor_up.get() == 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("both peers link");
    }

    /// Two real peers connect to each other at once, so each dials the other. Returns the
    /// `neighbor_up` and `neighbor_down` counts of the two peers after the dust has settled.
    async fn crossing_round(
        rng: &mut rand::rngs::ChaCha12Rng,
        relay_map: RelayMap,
        relay_url: RelayUrl,
        round: usize,
        by: JoinBy,
    ) -> ((u64, u64), (u64, u64)) {
        let memory_lookup = MemoryLookup::new();
        let ep1 = create_endpoint(rng, relay_map.clone(), Some(memory_lookup.clone()))
            .await
            .unwrap();
        let ep2 = create_endpoint(rng, relay_map, Some(memory_lookup.clone()))
            .await
            .unwrap();
        let go1 = Gossip::builder().spawn(ep1.clone());
        let go2 = Gossip::builder().spawn(ep2.clone());
        let cancel = CancellationToken::new();
        let tasks = [
            spawn(endpoint_loop(ep1.clone(), go1.clone(), cancel.clone())),
            spawn(endpoint_loop(ep2.clone(), go2.clone(), cancel.clone())),
        ];
        memory_lookup
            .add_endpoint_info(EndpointAddr::new(ep1.id()).with_relay_url(relay_url.clone()));
        memory_lookup.add_endpoint_info(EndpointAddr::new(ep2.id()).with_relay_url(relay_url));
        let topic: TopicId = blake3::hash(format!("crossing {round}").as_bytes()).into();

        // Kept alive until the end of the round: a dropped topic leaves it.
        let _keep: Box<dyn std::any::Any> = match by {
            JoinBy::Join => {
                let topics = [
                    go1.subscribe_and_join(topic, vec![ep2.id()]),
                    go2.subscribe_and_join(topic, vec![ep1.id()]),
                ]
                .try_join()
                .await
                .unwrap();
                wait_for_neighbor_up(&go1, &go2).await;
                Box::new(topics)
            }
            JoinBy::NeighborPeers => {
                let [topic1, topic2] = [go1.subscribe(topic, vec![]), go2.subscribe(topic, vec![])]
                    .try_join()
                    .await
                    .unwrap();
                let (sender1, receiver1) = topic1.split();
                let (sender2, receiver2) = topic2.split();
                let (first, second) = tokio::join!(
                    sender1.neighbor_peers(vec![ep2.id()]),
                    sender2.neighbor_peers(vec![ep1.id()])
                );
                first.unwrap();
                second.unwrap();
                wait_for_neighbor_up(&go1, &go2).await;
                Box::new((sender1, receiver1, sender2, receiver2))
            }
        };
        tokio::time::sleep(Duration::from_secs(2)).await;

        let counts = (
            (
                go1.metrics().neighbor_up.get(),
                go1.metrics().neighbor_down.get(),
            ),
            (
                go2.metrics().neighbor_up.get(),
                go2.metrics().neighbor_down.get(),
            ),
        );
        cancel.cancel();
        for task in tasks {
            task.await.unwrap().unwrap();
        }
        counts
    }

    /// Both peers join each other at once, so each dials the other. Each side holds the
    /// connection it dialed and the one it accepted. Both must keep the same one: if each keeps
    /// its newest, each keeps the one that the other closes, and both report the neighbor down.
    #[tokio::test]
    #[traced_test]
    async fn two_peers_that_dial_each_other_at_once_keep_one_connection() {
        let mut rng = rand::rngs::ChaCha12Rng::seed_from_u64(1);
        let (relay_map, relay_url, _guard) = iroh::test_utils::run_relay_server().await.unwrap();
        for round in 0..6 {
            let counts = crossing_round(
                &mut rng,
                relay_map.clone(),
                relay_url.clone(),
                round,
                JoinBy::Join,
            )
            .await;
            assert_eq!(
                counts,
                ((1, 0), (1, 0)),
                "round {round}: (neighbor_up, neighbor_down) of both peers"
            );
        }
    }

    /// Like `endpoint_loop`, but a connection is handed to gossip only after `hold`, as the accept
    /// gate of a transport does while it waits for a direct path.
    async fn gated_endpoint_loop(
        endpoint: Endpoint,
        gossip: Gossip,
        hold: Duration,
        cancel: CancellationToken,
    ) {
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                incoming = endpoint.accept() => {
                    let Some(incoming) = incoming else { break };
                    let Ok(connecting) = incoming.accept() else { continue };
                    let Ok(connection) = connecting.await else { continue };
                    let gossip = gossip.clone();
                    task::spawn(async move {
                        tokio::time::sleep(hold).await;
                        let _ = gossip.handle_connection(connection).await;
                    });
                }
            }
        }
    }

    /// Two peers dial each other at once, and the accept gate of each holds the dial of the other
    /// for the given time. Returns the `(neighbor_up, neighbor_down)` counts of both peers after
    /// the link settled.
    async fn held_crossing_round(
        hold1: Duration,
        hold2: Duration,
        tag: &str,
    ) -> ((u64, u64), (u64, u64)) {
        let mut rng = rand::rngs::ChaCha12Rng::seed_from_u64(11);
        let (relay_map, relay_url, _guard) = iroh::test_utils::run_relay_server().await.unwrap();
        let memory_lookup = MemoryLookup::new();
        let ep1 = create_endpoint(&mut rng, relay_map.clone(), Some(memory_lookup.clone()))
            .await
            .unwrap();
        let ep2 = create_endpoint(&mut rng, relay_map, Some(memory_lookup.clone()))
            .await
            .unwrap();
        let go1 = Gossip::builder().spawn(ep1.clone());
        let go2 = Gossip::builder().spawn(ep2.clone());
        let cancel = CancellationToken::new();
        let tasks = [
            spawn(gated_endpoint_loop(
                ep1.clone(),
                go1.clone(),
                hold1,
                cancel.clone(),
            )),
            spawn(gated_endpoint_loop(
                ep2.clone(),
                go2.clone(),
                hold2,
                cancel.clone(),
            )),
        ];
        memory_lookup
            .add_endpoint_info(EndpointAddr::new(ep1.id()).with_relay_url(relay_url.clone()));
        memory_lookup.add_endpoint_info(EndpointAddr::new(ep2.id()).with_relay_url(relay_url));
        let topic: TopicId = blake3::hash(tag.as_bytes()).into();

        let _topics = [
            go1.subscribe(topic, vec![ep2.id()]),
            go2.subscribe(topic, vec![ep1.id()]),
        ]
        .try_join()
        .await
        .unwrap();

        // Both gates let the dials through, then the losing connection is read for `DRAIN` and
        // closed. A link that was kept stays up; one that both sides lost goes down.
        tokio::time::sleep(hold1.max(hold2) + DRAIN + Duration::from_secs(3)).await;
        let counts = (
            (
                go1.metrics().neighbor_up.get(),
                go1.metrics().neighbor_down.get(),
            ),
            (
                go2.metrics().neighbor_up.get(),
                go2.metrics().neighbor_down.get(),
            ),
        );
        cancel.cancel();
        for task in tasks {
            task.await.unwrap();
        }
        counts
    }

    /// The gates of both sides hold the dial of the other for longer than any fixed time since a
    /// connection became active. Each side then reads the dial of the other late. Both must still
    /// keep the same connection.
    #[tokio::test]
    #[traced_test]
    async fn two_peers_whose_gates_hold_the_dials_equally_long_keep_one_connection() {
        let hold = Duration::from_secs(7);
        let counts = held_crossing_round(hold, hold, "held equally").await;
        assert_eq!(
            counts,
            ((1, 0), (1, 0)),
            "(neighbor_up, neighbor_down) of both peers"
        );
    }

    /// The same, when one gate holds its dial a little longer than the other.
    #[tokio::test]
    #[traced_test]
    async fn two_peers_whose_gates_hold_the_dials_unequally_long_keep_one_connection() {
        let counts = held_crossing_round(
            Duration::from_secs(6),
            Duration::from_secs(8),
            "held unequally",
        )
        .await;
        assert_eq!(
            counts,
            ((1, 0), (1, 0)),
            "(neighbor_up, neighbor_down) of both peers"
        );
    }

    /// The same, with holds longer than the liveness bound. The active connection is then alive
    /// only if its task saw datagrams in the last bound, which an idle held connection gets from
    /// the keep-alive pings of the peer. This is the shape of the field, where nothing bounds a
    /// hold.
    #[tokio::test]
    #[traced_test]
    async fn two_peers_whose_gates_hold_the_dials_longer_than_the_bound_keep_one_connection() {
        let hold = Duration::from_secs(12);
        let counts = held_crossing_round(hold, hold, "held beyond the bound").await;
        assert_eq!(
            counts,
            ((1, 0), (1, 0)),
            "(neighbor_up, neighbor_down) of both peers"
        );
    }

    /// The same, with unequal holds that are both longer than the liveness bound.
    #[tokio::test]
    #[traced_test]
    async fn two_peers_whose_gates_hold_the_dials_unequally_beyond_the_bound_keep_one_connection() {
        let counts = held_crossing_round(
            Duration::from_secs(11),
            Duration::from_secs(14),
            "held unequally beyond the bound",
        )
        .await;
        assert_eq!(
            counts,
            ((1, 0), (1, 0)),
            "(neighbor_up, neighbor_down) of both peers"
        );
    }

    /// Like `endpoint_loop`, but the gate of this side refuses the connections that the lower
    /// endpoint id dialed (or the higher one, with `refuse_lower == false`): it holds them for
    /// `refuse_after` and then closes them without handing them to gossip. The others are handed
    /// over at once.
    async fn refusing_endpoint_loop(
        endpoint: Endpoint,
        gossip: Gossip,
        refuse_lower: bool,
        refuse_after: Duration,
        cancel: CancellationToken,
    ) {
        loop {
            tokio::select! {
                biased;
                _ = cancel.cancelled() => break,
                incoming = endpoint.accept() => {
                    let Some(incoming) = incoming else { break };
                    let Ok(connecting) = incoming.accept() else { continue };
                    let Ok(connection) = connecting.await else { continue };
                    // This side accepted the connection, so the peer dialed it.
                    let dialed_by_lower =
                        dialed_by_lower_id(endpoint.id(), connection.remote_id(), false);
                    if dialed_by_lower == refuse_lower {
                        task::spawn(async move {
                            tokio::time::sleep(refuse_after).await;
                            connection.close(0u32.into(), b"refused");
                        });
                    } else {
                        let gossip = gossip.clone();
                        task::spawn(async move {
                            let _ = gossip.handle_connection(connection).await;
                        });
                    }
                }
            }
        }
    }

    /// Two peers dial each other at once. With `refuse_lower`, the gate of the higher id refuses the
    /// connection that the lower id dialed; without it, the gate of the lower id refuses the one
    /// that the higher id dialed. Returns `(neighbor_up, neighbor_down)` of both
    /// peers, lower id first, after the link settled.
    async fn one_sided_refusal_round(refuse_lower: bool, tag: &str) -> ((u64, u64), (u64, u64)) {
        let mut rng = rand::rngs::ChaCha12Rng::seed_from_u64(41);
        let (relay_map, relay_url, _guard) = iroh::test_utils::run_relay_server().await.unwrap();
        let memory_lookup = MemoryLookup::new();
        let mut eps = Vec::new();
        for _ in 0..2 {
            eps.push(
                create_endpoint(&mut rng, relay_map.clone(), Some(memory_lookup.clone()))
                    .await
                    .unwrap(),
            );
        }
        eps.sort_by_key(|ep| ep.id());
        let (low, high) = (eps.remove(0), eps.remove(0));
        let go_low = Gossip::builder().spawn(low.clone());
        let go_high = Gossip::builder().spawn(high.clone());
        let cancel = CancellationToken::new();
        // The higher id refuses the dial of the lower id, or the lower id refuses the dial of the
        // higher id. The other side hands everything over at once.
        let refusing = Duration::from_secs(6);
        let plain = |ep: &Endpoint, go: &Gossip, cancel: &CancellationToken| {
            let (ep, go, cancel) = (ep.clone(), go.clone(), cancel.clone());
            spawn(async move { endpoint_loop(ep, go, cancel).await.unwrap() })
        };
        let tasks = if refuse_lower {
            [
                plain(&low, &go_low, &cancel),
                spawn(refusing_endpoint_loop(
                    high.clone(),
                    go_high.clone(),
                    true,
                    refusing,
                    cancel.clone(),
                )),
            ]
        } else {
            [
                spawn(refusing_endpoint_loop(
                    low.clone(),
                    go_low.clone(),
                    false,
                    refusing,
                    cancel.clone(),
                )),
                plain(&high, &go_high, &cancel),
            ]
        };
        memory_lookup
            .add_endpoint_info(EndpointAddr::new(low.id()).with_relay_url(relay_url.clone()));
        memory_lookup.add_endpoint_info(EndpointAddr::new(high.id()).with_relay_url(relay_url));
        let topic: TopicId = blake3::hash(tag.as_bytes()).into();
        let _topics = [
            go_low.subscribe(topic, vec![high.id()]),
            go_high.subscribe(topic, vec![low.id()]),
        ]
        .try_join()
        .await
        .unwrap();

        // The refused connection closes at 6 s. The link must be up before that, and stay up.
        tokio::time::sleep(Duration::from_secs(12)).await;
        let counts = (
            (
                go_low.metrics().neighbor_up.get(),
                go_low.metrics().neighbor_down.get(),
            ),
            (
                go_high.metrics().neighbor_up.get(),
                go_high.metrics().neighbor_down.get(),
            ),
        );
        cancel.cancel();
        for task in tasks {
            task.await.unwrap();
        }
        counts
    }

    /// The gate of the higher id refuses the connection that the lower id dialed, and hands over
    /// the one it dialed itself. The lower id keeps its own dial by the old rule, reads the other
    /// only and closes it, and the pair has no connection. The pair must take the one that works.
    #[tokio::test]
    #[traced_test]
    async fn a_gate_that_refuses_the_dial_of_the_lower_id_leaves_the_pair_on_the_other_one() {
        let counts = one_sided_refusal_round(true, "refuse the dial of the lower id").await;
        assert_eq!(
            counts,
            ((1, 0), (1, 0)),
            "(neighbor_up, neighbor_down) of lower, higher"
        );
    }

    /// The same when the connection that is refused is the one the rule would drop anyway: a guard.
    #[tokio::test]
    #[traced_test]
    async fn a_gate_that_refuses_the_dial_of_the_higher_id_changes_nothing() {
        let counts = one_sided_refusal_round(false, "refuse the dial of the higher id").await;
        assert_eq!(
            counts,
            ((1, 0), (1, 0)),
            "(neighbor_up, neighbor_down) of lower, higher"
        );
    }

    /// A peer dies with no close and starts again with the same id, `silence` after it died. Waits
    /// for the restarted peer to link.
    async fn restart_round(silence: Duration, tag: &str) {
        let (relay_map, relay_url, _guard) = iroh::test_utils::run_relay_server().await.unwrap();
        let memory_lookup = MemoryLookup::new();
        let ep_a = create_endpoint(
            &mut rand::rngs::ChaCha12Rng::seed_from_u64(31),
            relay_map.clone(),
            Some(memory_lookup.clone()),
        )
        .await
        .unwrap();
        let id_a = ep_a.id();
        let go_a = Gossip::builder().spawn(ep_a.clone());
        let cancel = CancellationToken::new();
        let task_a = spawn(endpoint_loop(ep_a.clone(), go_a.clone(), cancel.clone()));
        memory_lookup.add_endpoint_info(EndpointAddr::new(id_a).with_relay_url(relay_url));
        let topic: TopicId = blake3::hash(tag.as_bytes()).into();
        let _topic_a = go_a.subscribe(topic, vec![]).await.unwrap();

        // The peer lives in a runtime of its own, which is dropped without a close: its tasks end
        // and its socket goes away, and nothing tells `a` that the connection is over.
        // `block_on` is not allowed on a thread of the test runtime, so it runs on a blocking one.
        let (relay_map_b, lookup_b) = (relay_map.clone(), memory_lookup.clone());
        let (runtime_b, topic_b) = tokio::task::spawn_blocking(move || {
            let runtime_b = tokio::runtime::Builder::new_multi_thread()
                .worker_threads(2)
                .enable_all()
                .build()
                .unwrap();
            let topic_b = runtime_b.block_on(async {
                let ep_b = create_endpoint(
                    &mut rand::rngs::ChaCha12Rng::seed_from_u64(32),
                    relay_map_b,
                    Some(lookup_b),
                )
                .await
                .unwrap();
                let go_b = Gossip::builder().spawn(ep_b.clone());
                spawn(endpoint_loop(ep_b, go_b.clone(), CancellationToken::new()));
                go_b.subscribe(topic, vec![id_a]).await.unwrap()
            });
            (runtime_b, topic_b)
        })
        .await
        .unwrap();
        timeout(Duration::from_secs(10), async {
            while go_a.metrics().neighbor_up.get() == 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the peer links before it dies");
        runtime_b.shutdown_background();
        drop(topic_b);

        // Longer than the liveness bound (10 s), shorter than the idle timeout of a path (15 s,
        // `PATH_MAX_IDLE_TIMEOUT` in iroh) and of the connection (30 s). The margin to 15 s is
        // 3 s, so a loaded host can make this test flake; the rule is not at fault then.
        tokio::time::sleep(silence).await;
        assert_eq!(
            go_a.metrics().neighbor_down.get(),
            0,
            "the old connection must still be open on `a`, or the test does not test a silent one"
        );

        let ep_b2 = create_endpoint(
            &mut rand::rngs::ChaCha12Rng::seed_from_u64(32),
            relay_map,
            Some(memory_lookup.clone()),
        )
        .await
        .unwrap();
        let go_b2 = Gossip::builder().spawn(ep_b2.clone());
        let task_b2 = spawn(endpoint_loop(ep_b2, go_b2.clone(), cancel.clone()));
        let _topic_b2 = go_b2.subscribe(topic, vec![id_a]).await.unwrap();
        timeout(Duration::from_secs(8), async {
            while go_b2.metrics().neighbor_up.get() == 0 {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .expect("the restarted peer links at once");

        cancel.cancel();
        task_a.await.unwrap().unwrap();
        task_b2.await.unwrap().unwrap();
    }

    /// The old connection received nothing for longer than the liveness bound, so it ranks below
    /// the dial of the restarted peer at once.
    #[tokio::test]
    #[traced_test]
    async fn a_peer_that_restarts_after_its_old_connection_went_silent_is_kept() {
        restart_round(Duration::from_secs(12), "restart after silence").await;
    }

    /// The old connection looks alive for a few seconds more. The dial of the restarted peer is
    /// parked until it carries its first message, and then it wins. This is a guard of the new
    /// rule: it is also green on ea2f887.
    #[tokio::test]
    #[traced_test]
    async fn a_peer_that_restarts_inside_the_liveness_bound_is_kept_when_it_speaks() {
        restart_round(Duration::from_secs(3), "restart inside the bound").await;
    }

    /// The same, when both peers ask the other for a link with `neighbor_peers`.
    #[tokio::test]
    #[traced_test]
    async fn two_peers_that_ask_each_other_for_a_link_at_once_keep_one_connection() {
        let mut rng = rand::rngs::ChaCha12Rng::seed_from_u64(2);
        let (relay_map, relay_url, _guard) = iroh::test_utils::run_relay_server().await.unwrap();
        for round in 0..6 {
            let counts = crossing_round(
                &mut rng,
                relay_map.clone(),
                relay_url.clone(),
                round,
                JoinBy::NeighborPeers,
            )
            .await;
            assert_eq!(
                counts,
                ((1, 0), (1, 0)),
                "round {round}: (neighbor_up, neighbor_down) of both peers"
            );
        }
    }
}
