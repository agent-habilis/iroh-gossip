//! Implementation of the HyParView membership protocol
//!
//! The implementation is based on [this paper][paper] by Joao Leitao, Jose Pereira, Luıs Rodrigues
//! and the [example implementation][impl] by Bartosz Sypytkowski
//!
//! [paper]: https://asc.di.fct.unl.pt/~jleitao/pdf/dsn07-leitao.pdf
//! [impl]: https://gist.github.com/Horusiath/84fac596101b197da0546d1697580d99

use std::collections::{HashMap, HashSet};

use derive_more::{From, Sub};
use n0_future::time::Duration;
use rand::{rngs::ThreadRng, Rng};
use serde::{Deserialize, Serialize};
use tracing::debug;

use super::{util::IndexSet, PeerData, PeerIdentity, PeerInfo, IO};

/// Input event for HyParView
#[derive(Debug)]
pub enum InEvent<PI> {
    /// A [`Message`] was received from a peer.
    RecvMessage(PI, Message<PI>),
    /// A timer has expired.
    TimerExpired(Timer<PI>),
    /// A peer was disconnected on the IO layer.
    PeerDisconnected(PI),
    /// Send a join request to a peer.
    RequestJoin(PI),
    /// Ask the given peers for a link with low priority, while the active view has a free slot.
    RequestNeighbors(Vec<PI>),
    /// Update the peer data that is transmitted on join requests.
    UpdatePeerData(PeerData),
    /// Drop the given peers from the active view, telling each that we are not coming back.
    Leave(Vec<PI>),
    /// Quit the swarm, informing peers about us leaving.
    Quit,
}

/// Output event for HyParView
#[derive(Debug)]
pub enum OutEvent<PI> {
    /// Ask the IO layer to send a [`Message`] to peer `PI`.
    SendMessage(PI, Message<PI>),
    /// Schedule a [`Timer`].
    ScheduleTimer(Duration, Timer<PI>),
    /// Ask the IO layer to close the connection to peer `PI`.
    DisconnectPeer(PI),
    /// Emit an [`Event`] to the application.
    EmitEvent(Event<PI>),
    /// New [`PeerData`] was received for peer `PI`.
    PeerData(PI, PeerData),
}

/// Event emitted by the [`State`] to the application.
#[derive(Clone, Debug)]
pub enum Event<PI> {
    /// A peer was added to our set of active connections.
    NeighborUp(PI),
    /// A peer was removed from our set of active connections.
    NeighborDown(PI),
}

/// Kinds of timers HyParView needs to schedule.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Timer<PI> {
    DoShuffle,
    PendingNeighborRequest(PI),
    /// A request made with [`InEvent::RequestNeighbors`] got no answer: forget that it is pending.
    NeighborRequestExpired(PI),
}

/// How long a request made with [`InEvent::RequestNeighbors`] stays pending without an answer.
const NEIGHBOR_REQUEST_EXPIRY: Duration = Duration::from_secs(20);

/// Messages that we can send and receive from peers within the topic.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum Message<PI> {
    /// Sent to a peer if you want to join the swarm
    Join(Option<PeerData>),
    /// When receiving Join, ForwardJoin is forwarded to the peer's ActiveView to introduce the
    /// new member.
    ForwardJoin(ForwardJoin<PI>),
    /// A shuffle request is sent occasionally to re-shuffle the PassiveView with contacts from
    /// other peers.
    Shuffle(Shuffle<PI>),
    /// Peers reply to [`Message::Shuffle`] requests with a random peers from their active and
    /// passive views.
    ShuffleReply(ShuffleReply<PI>),
    /// Request to add sender to an active view of recipient. If [`Neighbor::priority`] is
    /// [`Priority::High`], the request cannot be denied.
    Neighbor(Neighbor),
    /// Request to disconnect from a peer.
    /// If [`Disconnect::alive`] is true, the other peer is not shutting down, so it should be
    /// added to the passive set.
    Disconnect(Disconnect),
}

/// The time-to-live for this message.
///
/// Each time a message is forwarded, the `Ttl` is decreased by 1. If the `Ttl` reaches 0, it
/// should not be forwarded further.
#[derive(From, Sub, Eq, PartialEq, Clone, Debug, Copy, Serialize, Deserialize)]
pub struct Ttl(pub u16);
impl Ttl {
    pub fn expired(&self) -> bool {
        *self == Ttl(0)
    }
    pub fn next(&self) -> Ttl {
        Ttl(self.0.saturating_sub(1))
    }
}

/// A message informing other peers that a new peer joined the swarm for this topic.
///
/// Will be forwarded in a random walk until `ttl` reaches 0.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ForwardJoin<PI> {
    /// The peer that newly joined the swarm
    peer: PeerInfo<PI>,
    /// The time-to-live for this message
    ttl: Ttl,
}

/// Shuffle messages are sent occasionally to shuffle our passive view with peers from other peer's
/// active and passive views.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Shuffle<PI> {
    /// The peer that initiated the shuffle request.
    origin: PI,
    /// A random subset of the active and passive peers of the `origin` peer.
    nodes: Vec<PeerInfo<PI>>,
    /// The time-to-live for this message.
    ttl: Ttl,
}

/// Once a shuffle messages reaches a [`Ttl`] of 0, a peer replies with a `ShuffleReply`.
///
/// The reply is sent to the peer that initiated the shuffle and contains a subset of the active
/// and passive views of the peer at the end of the random walk.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ShuffleReply<PI> {
    /// A random subset of the active and passive peers of the peer sending the `ShuffleReply`.
    nodes: Vec<PeerInfo<PI>>,
}

/// The priority of a `Join` message
///
/// This is `High` if the sender does not have any active peers, and `Low` otherwise.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub enum Priority {
    /// High priority join that may not be denied.
    ///
    /// A peer may only send high priority joins if it doesn't have any active peers at the moment.
    High,
    /// Low priority join that can be denied.
    Low,
}

/// A neighbor message is sent after adding a peer to our active view to inform them that we are
/// now neighbors.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Neighbor {
    /// The priority of the `Join` or `ForwardJoin` message that triggered this neighbor request.
    priority: Priority,
    /// The user data of the peer sending this message.
    data: Option<PeerData>,
}

/// Message sent when leaving the swarm or closing down to inform peers about us being gone.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct Disconnect {
    /// Whether we are actually shutting down or closing the connection only because our limits are
    /// reached.
    alive: bool,
    /// Whether the sender left on purpose and wants to stay unlinked until a [`Message::Join`].
    ///
    /// This reuses the obsolete `respond` field, so the wire format is unchanged. No peer of
    /// this fork ever sent `true` there.
    left: bool,
}

/// Configuration for the swarm membership layer
#[derive(Clone, Debug, Serialize, Deserialize)]
#[serde(default)]
pub struct Config {
    /// Number of peers to which active connections are maintained
    pub active_view_capacity: usize,
    /// Number of peers for which contact information is remembered,
    /// but to which we are not actively connected to.
    pub passive_view_capacity: usize,
    /// Number of hops a `ForwardJoin` message is propagated until the new peer's info
    /// is added to a peer's active view.
    pub active_random_walk_length: Ttl,
    /// Number of hops a `ForwardJoin` message is propagated until the new peer's info
    /// is added to a peer's passive view.
    pub passive_random_walk_length: Ttl,
    /// Number of hops a `Shuffle` message is propagated until a peer replies to it.
    pub shuffle_random_walk_length: Ttl,
    /// Number of active peers to be included in a `Shuffle` request.
    pub shuffle_active_view_count: usize,
    /// Number of passive peers to be included in a `Shuffle` request.
    pub shuffle_passive_view_count: usize,
    /// Interval duration for shuffle requests
    pub shuffle_interval: Duration,
    /// Timeout after which a `Neighbor` request is considered failed
    pub neighbor_request_timeout: Duration,
}
impl Default for Config {
    /// Default values for the HyParView layer
    fn default() -> Self {
        Self {
            // From the paper (p9)
            active_view_capacity: 5,
            // From the paper (p9)
            passive_view_capacity: 30,
            // From the paper (p9)
            active_random_walk_length: Ttl(6),
            // From the paper (p9)
            passive_random_walk_length: Ttl(3),
            // From the paper (p9)
            shuffle_random_walk_length: Ttl(6),
            // From the paper (p9)
            shuffle_active_view_count: 3,
            // From the paper (p9)
            shuffle_passive_view_count: 4,
            // Wild guess
            shuffle_interval: Duration::from_secs(60),
            // Wild guess
            neighbor_request_timeout: Duration::from_millis(500),
        }
    }
}

#[derive(Default, Debug, Clone)]
pub struct Stats {
    total_connections: usize,
}

/// A node remembers at most this many times `passive_view_capacity` peers that left on purpose.
const LEFT_CAPACITY_FACTOR: usize = 8;

/// A node remembers the peers it answered with a `Neighbor`, at most this many in all: the active
/// and the passive view together, `1` at least. See [`State::answered_neighbors`].
const ANSWERED_CAPACITY_MIN: usize = 1;

/// The state of the HyParView protocol
#[derive(Debug)]
pub struct State<PI, RG = ThreadRng> {
    /// Our peer identity
    me: PI,
    /// Our opaque user data to transmit to peers on join messages
    me_data: Option<PeerData>,
    /// The active view, i.e. peers we are connected to
    pub(crate) active_view: IndexSet<PI>,
    /// The passive view, i.e. peers we know about but are not connected to at the moment
    pub(crate) passive_view: IndexSet<PI>,
    /// Protocol configuration (cannot change at runtime)
    config: Config,
    /// Whether a shuffle timer is currently scheduled
    shuffle_scheduled: bool,
    /// Random number generator
    rng: RG,
    /// Statistics
    pub(crate) stats: Stats,
    /// The set of neighbor requests we sent out but did not yet receive a reply for
    pending_neighbor_requests: HashSet<PI>,
    /// The peers we answered with a `Neighbor` and that did not send us anything since. Oldest first.
    ///
    /// A `Neighbor` message does not say whether it is a request or an answer: a node reads it as an
    /// answer only when it holds an entry for the sender, either in `pending_neighbor_requests`
    /// (we asked) or here (we answered). Without the second, a node that forgot its own answer reads
    /// a late or doubled `Neighbor` as a new request and answers it, and two such nodes answer each
    /// other for ever. The entry is spent by the next `Neighbor` from that peer and dropped with the
    /// link, the request or the peer. It is a separate set so that an answered peer does not look like
    /// a request in flight (`handle_request_neighbors` skips pending peers and counts them against
    /// the free slots of the active view).
    answered_neighbors: indexmap::IndexSet<PI>,
    /// The opaque user peer data we received for other peers
    peer_data: HashMap<PI, PeerData>,
    /// List of peers that are disconnecting, but which we want to keep in the passive set once the connection closes
    alive_disconnect_peers: HashSet<PI>,
    /// Peers that left us, or that we left, on purpose. Oldest first.
    ///
    /// A peer in this set is not added to the passive view and not adopted from a `ForwardJoin`.
    /// Its `Neighbor` request is refused. Only a `Join` from either side clears it.
    left: indexmap::IndexSet<PI>,
}

impl<PI, RG> State<PI, RG>
where
    PI: PeerIdentity,
    RG: Rng,
{
    pub fn new(me: PI, me_data: Option<PeerData>, config: Config, rng: RG) -> Self {
        Self {
            me,
            me_data,
            active_view: IndexSet::new(),
            passive_view: IndexSet::new(),
            config,
            shuffle_scheduled: false,
            rng,
            stats: Stats::default(),
            pending_neighbor_requests: Default::default(),
            answered_neighbors: Default::default(),
            peer_data: Default::default(),
            alive_disconnect_peers: Default::default(),
            left: Default::default(),
        }
    }

    pub fn handle(&mut self, event: InEvent<PI>, io: &mut impl IO<PI>) {
        match event {
            InEvent::RecvMessage(from, message) => self.handle_message(from, message, io),
            InEvent::TimerExpired(timer) => match timer {
                Timer::DoShuffle => self.handle_shuffle_timer(io),
                Timer::PendingNeighborRequest(peer) => self.handle_pending_neighbor_timer(peer, io),
                Timer::NeighborRequestExpired(peer) => self.handle_neighbor_request_expired(peer),
            },
            InEvent::PeerDisconnected(peer) => self.handle_connection_closed(peer, io),
            InEvent::RequestJoin(peer) => self.handle_join(peer, io),
            InEvent::RequestNeighbors(peers) => self.handle_request_neighbors(peers, io),
            InEvent::UpdatePeerData(data) => {
                self.me_data = Some(data);
            }
            InEvent::Leave(peers) => self.handle_leave(peers, io),
            InEvent::Quit => self.handle_quit(io),
        }

        // this will only happen on the first call
        if !self.shuffle_scheduled {
            io.push(OutEvent::ScheduleTimer(
                self.config.shuffle_interval,
                Timer::DoShuffle,
            ));
            self.shuffle_scheduled = true;
        }
    }

    fn handle_message(&mut self, from: PI, message: Message<PI>, io: &mut impl IO<PI>) {
        let is_disconnect = matches!(message, Message::Disconnect(Disconnect { .. }));
        if !is_disconnect && !self.active_view.contains(&from) {
            self.stats.total_connections += 1;
        }
        match message {
            Message::Join(data) => self.on_join(from, data, io),
            Message::ForwardJoin(details) => self.on_forward_join(from, details, io),
            Message::Shuffle(details) => self.on_shuffle(from, details, io),
            Message::ShuffleReply(details) => self.on_shuffle_reply(details, io),
            Message::Neighbor(details) => self.on_neighbor(from, details, io),
            Message::Disconnect(details) => self.on_disconnect(from, details, io),
        }

        // Disconnect from passive nodes right after receiving a message.
        // TODO(frando): I'm not sure anymore that this is correct. Maybe remove?
        if !is_disconnect && !self.active_view.contains(&from) {
            io.push(OutEvent::DisconnectPeer(from));
        }
    }

    fn handle_join(&mut self, peer: PI, io: &mut impl IO<PI>) {
        self.left.shift_remove(&peer);
        io.push(OutEvent::SendMessage(
            peer,
            Message::Join(self.me_data.clone()),
        ));
    }

    /// Ask peers for a link with low priority, for as many free slots as the active view has.
    ///
    /// A peer with a full active view refuses a low priority request and keeps its neighbors,
    /// where a `Join` always gets in and evicts one. No `ForwardJoin` goes out. There is no refill
    /// timer: a dial that fails clears the pending request through `PeerDisconnected`, and the
    /// application decides when to ask again. A request that nobody answers stops being pending
    /// after [`NEIGHBOR_REQUEST_EXPIRY`] (`Timer::NeighborRequestExpired`), which only clears the
    /// pending entry.
    fn handle_request_neighbors(&mut self, peers: Vec<PI>, io: &mut impl IO<PI>) {
        for peer in peers {
            if self.active_view.len() + self.pending_neighbor_requests.len()
                >= self.config.active_view_capacity
            {
                break;
            }
            if peer == self.me
                || self.left.contains(&peer)
                || self.active_view.contains(&peer)
                || self.pending_neighbor_requests.contains(&peer)
            {
                continue;
            }
            self.send_neighbor(peer, Priority::Low, io);
            io.push(OutEvent::ScheduleTimer(
                NEIGHBOR_REQUEST_EXPIRY,
                Timer::NeighborRequestExpired(peer),
            ));
        }
    }

    /// A request that nobody answered stops being pending, so that the application can ask again.
    /// Unlike [`Self::handle_pending_neighbor_timer`], it does not touch the passive view and
    /// does not refill the active view.
    fn handle_neighbor_request_expired(&mut self, peer: PI) {
        self.pending_neighbor_requests.remove(&peer);
        self.answered_neighbors.shift_remove(&peer);
    }

    /// We received a disconnect message.
    fn on_disconnect(&mut self, peer: PI, details: Disconnect, io: &mut impl IO<PI>) {
        self.pending_neighbor_requests.remove(&peer);
        self.answered_neighbors.shift_remove(&peer);
        let is_alive = details.alive && !details.left;
        if self.active_view.contains(&peer) {
            self.remove_active(&peer, RemovalReason::DisconnectReceived { is_alive }, io);
        } else if is_alive && self.passive_view.contains(&peer) {
            self.alive_disconnect_peers.insert(peer);
        }
        // After `remove_active`, so that the tombstone also drops the peer data of a neighbor.
        if details.left {
            self.add_tombstone(peer);
        }
    }

    /// A connection was closed by the peer.
    fn handle_connection_closed(&mut self, peer: PI, io: &mut impl IO<PI>) {
        self.pending_neighbor_requests.remove(&peer);
        self.answered_neighbors.shift_remove(&peer);
        if self.active_view.contains(&peer) {
            self.remove_active(&peer, RemovalReason::ConnectionClosed, io);
        } else if !self.alive_disconnect_peers.remove(&peer) {
            self.passive_view.remove(&peer);
            self.peer_data.remove(&peer);
        }
    }

    /// Leave the given peers only. Other neighbors are kept and the active view is not refilled.
    ///
    /// Each peer in the active view gets a `Disconnect` with `alive = false` and `left = true`.
    /// We tombstone every named peer, also when it is only in the passive view: after a leave,
    /// only a `Join` from either side links the pair again.
    fn handle_leave(&mut self, peers: Vec<PI>, io: &mut impl IO<PI>) {
        for peer in peers {
            if peer == self.me {
                continue;
            }
            if self.active_view.remove(&peer).is_some() {
                io.push(OutEvent::EmitEvent(Event::NeighborDown(peer)));
                // `send_disconnect` sends a `ShuffleReply` first, so the far side learns some of
                // our other peers. Look here first if links to the leaver still come back.
                self.send_disconnect(peer, false, true, io);
            }
            self.add_tombstone(peer);
        }
    }

    /// Remember that `peer` left on purpose and forget everything else we know about it.
    ///
    /// The set is bounded and the oldest tombstone goes first. It has no deadline: a leave on
    /// purpose ends when either side sends a `Join`, not after a time.
    ///
    /// Limit: a peer can only tombstone itself, but one that leaves under more than `capacity`
    /// identities pushes older tombstones out. Then the first dial from a forgotten peer is
    /// accepted again.
    fn add_tombstone(&mut self, peer: PI) {
        self.passive_view.remove(&peer);
        self.alive_disconnect_peers.remove(&peer);
        self.pending_neighbor_requests.remove(&peer);
        self.answered_neighbors.shift_remove(&peer);
        if !self.active_view.contains(&peer) {
            self.peer_data.remove(&peer);
        }
        self.left.shift_remove(&peer);
        self.left.insert(peer);
        let capacity = (LEFT_CAPACITY_FACTOR * self.config.passive_view_capacity).max(1);
        while self.left.len() > capacity {
            // O(n) on an `IndexSet`, but n is at most `capacity` and this runs once per leave.
            self.left.shift_remove_index(0);
        }
    }

    fn handle_quit(&mut self, io: &mut impl IO<PI>) {
        for peer in self.active_view.clone().into_iter() {
            self.active_view.remove(&peer);
            self.send_disconnect(peer, false, false, io);
        }
    }

    fn send_disconnect(&mut self, peer: PI, alive: bool, left: bool, io: &mut impl IO<PI>) {
        // Before disconnecting, send a `ShuffleReply` with some of our nodes to
        // prevent the other node from running out of connections. This is especially
        // relevant if the other node just joined the swarm.
        self.send_shuffle_reply(
            peer,
            self.config.shuffle_active_view_count + self.config.shuffle_passive_view_count,
            io,
        );
        let message = Message::Disconnect(Disconnect { alive, left });
        io.push(OutEvent::SendMessage(peer, message));
        io.push(OutEvent::DisconnectPeer(peer));
    }

    fn on_join(&mut self, peer: PI, data: Option<PeerData>, io: &mut impl IO<PI>) {
        // A `Join` is the application of the peer asking for the link, so it ends a leave.
        self.left.shift_remove(&peer);
        // "A node that receives a join request will start by adding the new
        // node to its active view, even if it has to drop a random node from it. (6)"
        self.add_active(peer, data.clone(), Priority::High, true, io);

        // "The contact node c will then send to all other nodes in its active view a ForwardJoin
        // request containing the new node identifier. Associated to the join procedure,
        // there are two configuration parameters, named Active Random Walk Length (ARWL),
        // that specifies the maximum number of hops a ForwardJoin request is propagated,
        // and Passive Random Walk Length (PRWL), that specifies at which point in the walk the node
        // is inserted in a passive view. To use these parameters, the ForwardJoin request carries
        // a “time to live” field that is initially set to ARWL and decreased at every hop. (7)"
        let ttl = self.config.active_random_walk_length;
        let peer_info = PeerInfo { id: peer, data };
        for node in self.active_view.iter_without(&peer) {
            let message = Message::ForwardJoin(ForwardJoin {
                peer: peer_info.clone(),
                ttl,
            });
            io.push(OutEvent::SendMessage(*node, message));
        }
    }

    fn on_forward_join(&mut self, sender: PI, message: ForwardJoin<PI>, io: &mut impl IO<PI>) {
        let peer_id = message.peer.id;
        // The leave is between the joiner and us. Do not adopt it, but the walk is for the swarm,
        // so pass it on while its ttl lasts. At ttl 0 we drop it: a node without the tombstone
        // adopts at ttl 0, and if every node of a group holds the tombstone, a walk that is
        // forwarded at ttl 0 would go round the group for ever.
        if self.left.contains(&peer_id) {
            if !message.ttl.expired() {
                if let Some(next) = self
                    .active_view
                    .pick_random_without(&[&sender], &mut self.rng)
                {
                    let message = Message::ForwardJoin(ForwardJoin {
                        peer: message.peer,
                        ttl: message.ttl.next(),
                    });
                    io.push(OutEvent::SendMessage(*next, message));
                }
            }
            return;
        }
        // If the peer is already in our active view, we renew our neighbor relationship.
        if self.active_view.contains(&peer_id) {
            self.insert_peer_info(message.peer.clone(), io);
            // A renew is not tracked: the peer is already in the active view, so a pending entry
            // for it would count the same peer twice in `handle_request_neighbors`. A peer that
            // answered us before reads the renew as an answer, and nothing comes back.
            io.push(OutEvent::SendMessage(
                peer_id,
                Message::Neighbor(Neighbor {
                    priority: Priority::High,
                    data: self.me_data.clone(),
                }),
            ));
            // The walk goes on while its ttl lasts. The contact node of a Join holds the joiner
            // already, so a walk that stopped here would stop at the contact node as soon as the
            // other nodes are linked to each other, and the joiner would get no link to a real
            // peer. The joiner is never the next hop.
            if !message.ttl.expired() {
                if let Some(next) = self
                    .active_view
                    .pick_random_without(&[&sender, &peer_id], &mut self.rng)
                {
                    let message = Message::ForwardJoin(ForwardJoin {
                        peer: message.peer,
                        ttl: message.ttl.next(),
                    });
                    io.push(OutEvent::SendMessage(*next, message));
                }
            }
        }
        // "i) If the time to live is equal to zero or if the number of nodes in p’s active view is equal to one,
        // it will add the new node to its active view (7)"
        else if message.ttl.expired() || self.active_view.len() <= 1 {
            self.insert_peer_info(message.peer, io);
            // Modification from paper: Instead of adding the peer directly to our active view,
            // we only send the Neighbor message. We will add the peer to our active view once we receive a
            // reply from our neighbor.
            // This prevents us adding unreachable peers to our active view.
            self.send_neighbor(peer_id, Priority::High, io);
        } else {
            // "ii) If the time to live is equal to PRWL, p will insert the new node into its passive view"
            if message.ttl == self.config.passive_random_walk_length {
                self.add_passive(peer_id, message.peer.data.clone(), io);
            }
            // "iii) The time to live field is decremented."
            // "iv) If, at this point, n has not been inserted
            // in p’s active view, p will forward the request to a random node in its active view
            // (different from the one from which the request was received)."
            if !self.active_view.contains(&peer_id)
                && !self.pending_neighbor_requests.contains(&peer_id)
            {
                match self
                    .active_view
                    .pick_random_without(&[&sender], &mut self.rng)
                {
                    None => {
                        unreachable!("if the peer was not added, there are at least two peers in our active view.");
                    }
                    Some(next) => {
                        let message = Message::ForwardJoin(ForwardJoin {
                            peer: message.peer,
                            ttl: message.ttl.next(),
                        });
                        io.push(OutEvent::SendMessage(*next, message));
                    }
                }
            }
        }
    }

    fn on_neighbor(&mut self, from: PI, details: Neighbor, io: &mut impl IO<PI>) {
        // Both entries are spent by this message, so that a doubled `Neighbor` is read as an
        // answer once and not answered, whichever of the two the first one found.
        let was_asked = self.pending_neighbor_requests.remove(&from);
        let was_answered = self.answered_neighbors.shift_remove(&from);
        let is_reply = was_asked || was_answered;
        // This refuses a `High` priority request on purpose, against the HyParView paper: after a
        // leave, only a `Join` links the pair again. The refusal tells the far side to tombstone
        // us too, which ends a joint dial.
        if self.left.contains(&from) {
            self.send_disconnect(from, false, true, io);
            return;
        }
        let do_reply = !is_reply;
        // "A node q that receives a high priority neighbor request will always accept the request, even
        // if it has to drop a random member from its active view (again, the member that is dropped will
        // receive a Disconnect notification). If a node q receives a low priority Neighbor request, it will
        // only accept the request if it has a free slot in its active view, otherwise it will refuse the request."
        if !self.add_active(from, details.data, details.priority, do_reply, io) {
            self.send_disconnect(from, true, false, io);
        }
        // When `do_reply` is set, the Neighbor message that `add_active` just sent answers the
        // request we received. It is not a request in flight, so it must not wait in the pending
        // set; the answer is remembered in its own set, so that the next `Neighbor` from this peer
        // is read as an answer (and not answered again).
        if do_reply {
            self.pending_neighbor_requests.remove(&from);
            self.remember_answer(from);
        }
    }

    /// Remember that we answered a `Neighbor` request of `peer` (see [`State::answered_neighbors`]).
    fn remember_answer(&mut self, peer: PI) {
        self.answered_neighbors.shift_remove(&peer);
        self.answered_neighbors.insert(peer);
        let capacity = (self.config.active_view_capacity + self.config.passive_view_capacity)
            .max(ANSWERED_CAPACITY_MIN);
        while self.answered_neighbors.len() > capacity {
            // O(n) on an `IndexSet`, but n is at most `capacity` and this runs once per answer.
            self.answered_neighbors.shift_remove_index(0);
        }
    }

    /// Get the peer [`PeerInfo`] for a peer.
    fn peer_info(&self, id: &PI) -> PeerInfo<PI> {
        let data = self.peer_data.get(id).cloned();
        PeerInfo { id: *id, data }
    }

    fn insert_peer_info(&mut self, peer_info: PeerInfo<PI>, io: &mut impl IO<PI>) {
        if let Some(data) = peer_info.data {
            let old = self.peer_data.remove(&peer_info.id);
            let same = matches!(old, Some(old) if old == data);
            if !same && !data.0.is_empty() {
                io.push(OutEvent::PeerData(peer_info.id, data.clone()));
            }
            self.peer_data.insert(peer_info.id, data);
        }
    }

    /// Handle a [`Message::Shuffle`]
    ///
    /// > A node q that receives a Shuffle request will first decrease its time to live. If the time
    /// > to live of the message is greater than zero and the number of nodes in q’s active view is
    /// > greater than 1, the node will select a random node from its active view, different from the
    /// > one he received this shuffle message from, and simply forwards the Shuffle request.
    /// > Otherwise, node q accepts the Shuffle request and send back (p.8)
    fn on_shuffle(&mut self, from: PI, shuffle: Shuffle<PI>, io: &mut impl IO<PI>) {
        if shuffle.ttl.expired() || self.active_view.len() <= 1 {
            let len = shuffle.nodes.len();
            for node in shuffle.nodes {
                self.add_passive(node.id, node.data, io);
            }
            self.send_shuffle_reply(shuffle.origin, len, io);
        } else if let Some(node) = self
            .active_view
            .pick_random_without(&[&shuffle.origin, &from], &mut self.rng)
        {
            let message = Message::Shuffle(Shuffle {
                origin: shuffle.origin,
                nodes: shuffle.nodes,
                ttl: shuffle.ttl.next(),
            });
            io.push(OutEvent::SendMessage(*node, message));
        }
    }

    fn send_shuffle_reply(&mut self, to: PI, len: usize, io: &mut impl IO<PI>) {
        let mut nodes = self.passive_view.shuffled_and_capped(len, &mut self.rng);
        // If we don't have enough passive nodes for the expected length, we fill with
        // active nodes.
        if nodes.len() < len {
            nodes.extend(
                self.active_view
                    .shuffled_and_capped(len - nodes.len(), &mut self.rng),
            );
        }
        let nodes = nodes.into_iter().map(|id| self.peer_info(&id));
        let message = Message::ShuffleReply(ShuffleReply {
            nodes: nodes.collect(),
        });
        io.push(OutEvent::SendMessage(to, message));
    }

    fn on_shuffle_reply(&mut self, message: ShuffleReply<PI>, io: &mut impl IO<PI>) {
        for node in message.nodes {
            self.add_passive(node.id, node.data, io);
        }
        self.refill_active_from_passive(&[], io);
    }

    fn handle_shuffle_timer(&mut self, io: &mut impl IO<PI>) {
        if let Some(node) = self.active_view.pick_random(&mut self.rng) {
            let active = self.active_view.shuffled_without_and_capped(
                &[node],
                self.config.shuffle_active_view_count,
                &mut self.rng,
            );
            let passive = self.passive_view.shuffled_without_and_capped(
                &[node],
                self.config.shuffle_passive_view_count,
                &mut self.rng,
            );
            let nodes = active
                .iter()
                .chain(passive.iter())
                .map(|id| self.peer_info(id));
            let me = PeerInfo {
                id: self.me,
                data: self.me_data.clone(),
            };
            let nodes = nodes.chain([me]);
            let message = Shuffle {
                origin: self.me,
                nodes: nodes.collect(),
                ttl: self.config.shuffle_random_walk_length,
            };
            io.push(OutEvent::SendMessage(*node, Message::Shuffle(message)));
        }
        io.push(OutEvent::ScheduleTimer(
            self.config.shuffle_interval,
            Timer::DoShuffle,
        ));
    }

    fn passive_is_full(&self) -> bool {
        self.passive_view.len() >= self.config.passive_view_capacity
    }

    fn active_is_full(&self) -> bool {
        self.active_view.len() >= self.config.active_view_capacity
    }

    /// Add a peer to the passive view.
    ///
    /// If the passive view is full, it will first remove a random peer and then insert the new peer.
    /// If a peer is currently in the active view, or left on purpose, it will not be added.
    fn add_passive(&mut self, peer: PI, data: Option<PeerData>, io: &mut impl IO<PI>) {
        // Check before `insert_peer_info`: a peer in no view must not keep peer data.
        if self.left.contains(&peer) {
            return;
        }
        self.insert_peer_info((peer, data).into(), io);
        if self.active_view.contains(&peer) || self.passive_view.contains(&peer) || peer == self.me
        {
            return;
        }
        if self.passive_is_full() {
            self.passive_view.remove_random(&mut self.rng);
        }
        self.passive_view.insert(peer);
    }

    /// Remove a peer from the active view.
    ///
    /// If `reason` is [`RemovalReason::Random`], a [`Disconnect`] message will be sent to the peer.
    fn remove_active(&mut self, peer: &PI, reason: RemovalReason, io: &mut impl IO<PI>) {
        if let Some(idx) = self.active_view.get_index_of(peer) {
            let removed_peer = self.remove_active_by_index(idx, reason, io).unwrap();
            self.refill_active_from_passive(&[&removed_peer], io);
        }
    }

    fn refill_active_from_passive(&mut self, skip_peers: &[&PI], io: &mut impl IO<PI>) {
        if self.active_view.len() + self.pending_neighbor_requests.len()
            >= self.config.active_view_capacity
        {
            return;
        }
        // "When a node p suspects that one of the nodes present in its active view has failed
        // (by either disconnecting or blocking), it selects a random node q from its passive view and
        // attempts to establish a TCP connection with q. If the connection fails to establish,
        // node q is considered failed and removed from p’s passive view; another node q′ is selected
        // at random and a new attempt is made. The procedure is repeated until a connection is established
        // with success." (p7)
        let mut skip_peers = skip_peers.to_vec();
        skip_peers.extend(self.pending_neighbor_requests.iter());

        if let Some(node) = self
            .passive_view
            .pick_random_without(&skip_peers, &mut self.rng)
            .copied()
        {
            let priority = match self.active_view.is_empty() {
                true => Priority::High,
                false => Priority::Low,
            };
            self.send_neighbor(node, priority, io);
            // schedule a timer that checks if the node replied with a neighbor message,
            // otherwise try again with another passive node.
            io.push(OutEvent::ScheduleTimer(
                self.config.neighbor_request_timeout,
                Timer::PendingNeighborRequest(node),
            ));
        };
    }

    fn handle_pending_neighbor_timer(&mut self, peer: PI, io: &mut impl IO<PI>) {
        if self.pending_neighbor_requests.remove(&peer) {
            self.passive_view.remove(&peer);
            self.refill_active_from_passive(&[], io);
        }
    }

    fn remove_active_by_index(
        &mut self,
        peer_index: usize,
        reason: RemovalReason,
        io: &mut impl IO<PI>,
    ) -> Option<PI> {
        if let Some(peer) = self.active_view.remove_index(peer_index) {
            io.push(OutEvent::EmitEvent(Event::NeighborDown(peer)));

            match reason {
                // send a disconnect message, then close connection.
                RemovalReason::Random => self.send_disconnect(peer, true, false, io),
                // close connection without sending anything further.
                RemovalReason::DisconnectReceived { is_alive: _ } => {
                    io.push(OutEvent::DisconnectPeer(peer))
                }
                RemovalReason::ConnectionClosed => io.push(OutEvent::DisconnectPeer(peer)),
            }

            let keep_as_passive = match reason {
                // keep alive if previously marked as alive.
                RemovalReason::ConnectionClosed => self.alive_disconnect_peers.remove(&peer),
                // keep alive if other peer said to be still alive.
                RemovalReason::DisconnectReceived { is_alive } => is_alive,
                // keep alive (only we are removing for now)
                RemovalReason::Random => true,
            };

            if keep_as_passive {
                let data = self.peer_data.remove(&peer);
                self.add_passive(peer, data, io);
                // mark peer as alive, so it doesn't get removed from the passive view if the conn closes.
                if !matches!(reason, RemovalReason::ConnectionClosed) {
                    self.alive_disconnect_peers.insert(peer);
                }
            }
            debug!(other = ?peer, "removed from active view, reason: {reason:?}");
            Some(peer)
        } else {
            None
        }
    }

    /// Remove a random peer from the active view.
    fn free_random_slot_in_active_view(&mut self, io: &mut impl IO<PI>) {
        if let Some(index) = self.active_view.pick_random_index(&mut self.rng) {
            self.remove_active_by_index(index, RemovalReason::Random, io);
        }
    }

    /// Add a peer to the active view.
    ///
    /// If the active view is currently full, a random peer will be removed first.
    /// Sends a Neighbor message to the peer. If high_priority is true, the peer
    /// may not deny the Neighbor request.
    fn add_active(
        &mut self,
        peer: PI,
        data: Option<PeerData>,
        priority: Priority,
        reply: bool,
        io: &mut impl IO<PI>,
    ) -> bool {
        if peer == self.me {
            return false;
        }
        self.insert_peer_info((peer, data).into(), io);
        if self.active_view.contains(&peer) {
            if reply {
                self.send_neighbor(peer, priority, io);
            }
            return true;
        }
        match (priority, self.active_is_full()) {
            (Priority::High, is_full) => {
                if is_full {
                    self.free_random_slot_in_active_view(io);
                }
                self.add_active_unchecked(peer, Priority::High, reply, io);
                true
            }
            (Priority::Low, false) => {
                self.add_active_unchecked(peer, Priority::Low, reply, io);
                true
            }
            (Priority::Low, true) => false,
        }
    }

    fn add_active_unchecked(
        &mut self,
        peer: PI,
        priority: Priority,
        reply: bool,
        io: &mut impl IO<PI>,
    ) {
        self.passive_view.remove(&peer);
        if self.active_view.insert(peer) {
            debug!(other = ?peer, "add to active view");
            io.push(OutEvent::EmitEvent(Event::NeighborUp(peer)));
            if reply {
                self.send_neighbor(peer, priority, io);
            }
        }
    }

    fn send_neighbor(&mut self, peer: PI, priority: Priority, io: &mut impl IO<PI>) {
        if self.pending_neighbor_requests.insert(peer) {
            let message = Message::Neighbor(Neighbor {
                priority,
                data: self.me_data.clone(),
            });
            io.push(OutEvent::SendMessage(peer, message));
        }
    }
}

#[derive(Debug)]
enum RemovalReason {
    /// A peer is removed because the connection was closed ungracefully.
    ConnectionClosed,
    /// A peer is removed because we received a disconnect message.
    DisconnectReceived { is_alive: bool },
    /// A peer is removed after random selection to make room for a newly joined peer.
    Random,
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;

    use rand::{rngs::ChaCha12Rng, SeedableRng};

    use super::*;
    use crate::proto::{topic, Event as TopicEvent};

    type Io = VecDeque<topic::OutEvent<u64>>;
    type TestState = State<u64, ChaCha12Rng>;

    fn state(me: u64) -> TestState {
        State::new(me, None, Config::default(), ChaCha12Rng::seed_from_u64(me))
    }

    fn with_active(me: u64, peers: &[u64]) -> TestState {
        let mut state = state(me);
        for peer in peers {
            state.active_view.insert(*peer);
        }
        state
    }

    fn recv(state: &mut TestState, from: u64, message: Message<u64>, io: &mut Io) {
        state.handle(InEvent::RecvMessage(from, message), io);
    }

    fn sent(io: &Io) -> Vec<(u64, Message<u64>)> {
        io.iter()
            .filter_map(|event| match event {
                topic::OutEvent::SendMessage(to, topic::Message::Swarm(message)) => {
                    Some((*to, message.clone()))
                }
                _ => None,
            })
            .collect()
    }

    fn neighbor_ups(io: &Io) -> usize {
        io.iter()
            .filter(|e| matches!(e, topic::OutEvent::EmitEvent(TopicEvent::NeighborUp(_))))
            .count()
    }

    fn disconnect(alive: bool, left: bool) -> Message<u64> {
        Message::Disconnect(Disconnect { alive, left })
    }

    fn neighbor(priority: Priority) -> Message<u64> {
        Message::Neighbor(Neighbor {
            priority,
            data: None,
        })
    }

    fn forward_join(joiner: u64, ttl: u16) -> Message<u64> {
        Message::ForwardJoin(ForwardJoin {
            peer: PeerInfo {
                id: joiner,
                data: None,
            },
            ttl: Ttl(ttl),
        })
    }

    // The answer to a Neighbor request is a Neighbor message too, but nobody answers it. If it
    // is recorded as a pending request, the entry stays for the life of the link, and
    // the refill rule that counts pending requests stops short of the active view capacity.
    #[test]
    fn a_join_handshake_leaves_no_pending_neighbor_request() {
        let mut a = state(0);
        let mut b = state(1);
        let mut io = Io::new();
        let neighbor_to = |io: &Io, peer: u64| {
            sent(io)
                .into_iter()
                .find(|(to, message)| *to == peer && matches!(message, Message::Neighbor(_)))
                .map(|(_, message)| message)
                .expect("a Neighbor message")
        };

        // b joins a. a asks b for the link, and b answers.
        recv(&mut a, 1, Message::Join(None), &mut io);
        let request = neighbor_to(&io, 1);
        io.clear();
        recv(&mut b, 0, request, &mut io);
        let answer = neighbor_to(&io, 0);
        io.clear();
        recv(&mut a, 1, answer, &mut io);

        assert!(a.active_view.contains(&1) && b.active_view.contains(&0));
        assert!(
            a.pending_neighbor_requests.is_empty(),
            "a: {:?}",
            a.pending_neighbor_requests
        );
        assert!(
            b.pending_neighbor_requests.is_empty(),
            "b: {:?}",
            b.pending_neighbor_requests
        );
    }

    // A failed dial clears the pending request, and the next ForwardJoin asks again, so the peer
    // gets two Neighbor requests and answers both. The first answer ends the request of the
    // asking side; the second one finds no entry. Nobody answers an answer, so the exchange must
    // end. When a node forgets its own answer at once, no node holds an entry that tells it that
    // the next Neighbor is an answer, and each side answers the answer of the other for ever.
    #[test]
    fn two_requests_after_a_failed_dial_do_not_start_a_ping_pong() {
        let mut carol = state(0);
        let mut bob = state(1);
        let rendezvous = 2;
        let (mut carol_io, mut bob_io) = (Io::new(), Io::new());

        recv(&mut bob, rendezvous, forward_join(0, 0), &mut bob_io);
        bob.handle(InEvent::PeerDisconnected(0), &mut bob_io);
        recv(&mut bob, rendezvous, forward_join(0, 0), &mut bob_io);
        let requests: Vec<_> = sent(&bob_io)
            .into_iter()
            .filter(|(to, message)| *to == 0 && matches!(message, Message::Neighbor(_)))
            .collect();
        assert_eq!(requests.len(), 2, "bob asked carol twice");
        bob_io.clear();

        for (_, message) in requests {
            recv(&mut carol, 1, message, &mut carol_io);
        }
        let mut crossed = 0;
        let mut to_bob = sent(&carol_io);
        carol_io.clear();
        for _ in 0..200 {
            crossed += to_bob.len();
            for (_, message) in to_bob {
                recv(&mut bob, 0, message, &mut bob_io);
            }
            let to_carol = sent(&bob_io);
            bob_io.clear();
            crossed += to_carol.len();
            for (_, message) in to_carol {
                recv(&mut carol, 1, message, &mut carol_io);
            }
            to_bob = sent(&carol_io);
            carol_io.clear();
            if to_bob.is_empty() {
                break;
            }
        }
        assert!(
            crossed <= 4,
            "the exchange did not stop: {crossed} Neighbor messages crossed"
        );
    }

    // A second copy of a Neighbor request reaches a node after the link is up. Nobody answers
    // an answer, so the exchange must end.
    #[test]
    fn a_late_copy_of_a_neighbor_request_does_not_start_a_ping_pong() {
        let mut a = state(0);
        let mut b = state(1);
        let mut io = Io::new();
        let neighbor_to = |io: &Io, peer: u64| {
            sent(io)
                .into_iter()
                .find(|(to, message)| *to == peer && matches!(message, Message::Neighbor(_)))
                .map(|(_, message)| message)
                .expect("a Neighbor message")
        };

        recv(&mut a, 1, Message::Join(None), &mut io);
        let request = neighbor_to(&io, 1);
        io.clear();
        recv(&mut b, 0, request.clone(), &mut io);
        let answer = neighbor_to(&io, 0);
        io.clear();
        recv(&mut a, 1, answer, &mut io);

        recv(&mut b, 0, request, &mut io);
        let mut crossed = 0;
        let mut to_a = sent(&io);
        io.clear();
        for _ in 0..200 {
            crossed += to_a.len();
            for (_, message) in to_a {
                recv(&mut a, 1, message, &mut io);
            }
            let to_b = sent(&io);
            io.clear();
            crossed += to_b.len();
            for (_, message) in to_b {
                recv(&mut b, 0, message, &mut io);
            }
            to_a = sent(&io);
            io.clear();
            if to_a.is_empty() {
                break;
            }
        }
        assert!(
            crossed <= 4,
            "the exchange did not stop: {crossed} Neighbor messages crossed"
        );
    }

    // A renew (a ForwardJoin for a peer that is already in the active view) sends a Neighbor
    // that no entry tracks, so the active peer is not counted a second time as a request in
    // flight. A peer that answered us before reads the renew as an answer and sends nothing back.
    #[test]
    fn a_renew_of_an_active_peer_holds_no_pending_entry_and_gets_no_answer() {
        let mut a = state(0);
        let mut b = state(1);
        let rendezvous = 2;
        let mut io = Io::new();
        let neighbor_to = |io: &Io, peer: u64| {
            sent(io)
                .into_iter()
                .find(|(to, message)| *to == peer && matches!(message, Message::Neighbor(_)))
                .map(|(_, message)| message)
                .expect("a Neighbor message")
        };

        a.handle(InEvent::RequestNeighbors(vec![1]), &mut io);
        let request = neighbor_to(&io, 1);
        io.clear();
        recv(&mut b, 0, request, &mut io);
        let answer = neighbor_to(&io, 0);
        io.clear();
        recv(&mut a, 1, answer, &mut io);
        io.clear();
        assert!(a.active_view.contains(&1), "a holds b after the answer");
        assert!(a.pending_neighbor_requests.is_empty());

        recv(&mut a, rendezvous, forward_join(1, 0), &mut io);
        let renew = neighbor_to(&io, 1);
        io.clear();
        assert!(
            a.pending_neighbor_requests.is_empty(),
            "a renew must not wait in the pending set"
        );
        recv(&mut b, 0, renew, &mut io);
        assert!(
            sent(&io).is_empty(),
            "b answered the renew: {:?}",
            sent(&io)
        );
    }

    // The entry that an answer left outlives the link it belongs to when our connection to the peer
    // stays up with no Disconnect and no close, for example after the peer shed its side. Nothing
    // else drops it then, and a genuine request of the peer would be read as an answer: it would
    // get no Neighbor back, and the pair would be a half link.
    #[test]
    fn a_stale_answer_does_not_absorb_a_genuine_request_after_its_expiry() {
        let mut a = state(0);
        let mut io = Io::new();
        let answered_to_one = |io: &Io| {
            sent(io)
                .iter()
                .any(|(to, message)| *to == 1 && matches!(message, Message::Neighbor(_)))
        };

        recv(&mut a, 1, neighbor(Priority::High), &mut io);
        assert!(answered_to_one(&io), "a answers the first request");
        let expiries: Vec<_> = io
            .iter()
            .filter_map(|event| match event {
                topic::OutEvent::ScheduleTimer(after, topic::Timer::Swarm(timer))
                    if *after == NEIGHBOR_REQUEST_EXPIRY =>
                {
                    Some(timer.clone())
                }
                _ => None,
            })
            .collect();
        assert!(!expiries.is_empty(), "the answer schedules no expiry");
        io.clear();

        for timer in expiries {
            a.handle(InEvent::TimerExpired(timer), &mut io);
        }
        io.clear();
        recv(&mut a, 1, neighbor(Priority::High), &mut io);
        assert!(
            answered_to_one(&io),
            "the genuine request after the expiry got no answer: {:?}",
            sent(&io)
        );
    }

    // Three nodes are linked to each other, and a fourth one joins through the first. Its Join
    // starts a ForwardJoin walk, and the walk must end in a Neighbor request to the joiner from
    // a node that is not the one it joined through. No network: every message is delivered.
    #[test]
    fn a_joiner_gets_a_link_to_a_peer_that_it_did_not_join_through() {
        let (rendezvous, creator, a, joiner) = (0u64, 1u64, 2u64, 3u64);
        let mut nodes = [
            with_active(rendezvous, &[creator, a]),
            with_active(creator, &[rendezvous, a]),
            with_active(a, &[rendezvous, creator]),
            state(joiner),
        ];
        let mut io = Io::new();
        let mut queue: VecDeque<(u64, u64, Message<u64>)> = VecDeque::new();

        nodes[joiner as usize].handle(InEvent::RequestJoin(rendezvous), &mut io);
        queue.extend(
            sent(&io)
                .into_iter()
                .map(|(to, message)| (joiner, to, message)),
        );
        io.clear();
        let mut joiner_neighbors = 0;
        for _ in 0..200 {
            let Some((from, to, message)) = queue.pop_front() else {
                break;
            };
            if matches!(message, Message::Neighbor(_)) && (from == joiner || to == joiner) {
                joiner_neighbors += 1;
            }
            recv(&mut nodes[to as usize], from, message, &mut io);
            queue.extend(
                sent(&io)
                    .into_iter()
                    .map(|(next, message)| (to, next, message)),
            );
            io.clear();
        }

        let joiner_view = &nodes[joiner as usize].active_view;
        assert!(
            joiner_view.contains(&creator) || joiner_view.contains(&a),
            "the joiner is linked to {:?} only, after {joiner_neighbors} Neighbor messages",
            joiner_view.iter().collect::<Vec<_>>()
        );
        assert!(
            queue.is_empty(),
            "the exchange did not end: {} messages left",
            queue.len()
        );
        // A guard against a storm, and a count that is exact on purpose. The exchange has 13 Neighbor
        // messages that name the joiner, in this order (the walk is forced in a clique of three):
        //  1-2   the handshake: the request of the contact node and the answer of the joiner;
        //  3-4   two renews of the contact node (ttl 4): #3 is absorbed by the answered_neighbors
        //        entry that #2 left, #4 is answered by the joiner (5);
        //  5-6   the joiner's answer and the answer of the contact node to that answer (6);
        //  7-8   two renews (ttl 1): #7 is answered by the joiner (9), #8 is absorbed;
        //  9     the joiner's answer to #7, absorbed by the entry that #6 left at the contact node;
        //  10-11 the Neighbor requests of the two walk ends (ttl 0) to the joiner;
        //  12-13 the answers of the joiner.
        // #6 is the price of the untracked renew of 709b650: a renew is not in the pending set of
        // its sender, so the answer to it reads as a request. A change of that cost shows here as
        // 12 or 14, and the table must be updated with it.
        assert_eq!(
            joiner_neighbors, 13,
            "{joiner_neighbors} Neighbor messages named the joiner"
        );
    }

    fn low_neighbor_requests(io: &Io) -> Vec<u64> {
        sent(io)
            .into_iter()
            .filter(|(_, message)| {
                matches!(
                    message,
                    Message::Neighbor(Neighbor {
                        priority: Priority::Low,
                        ..
                    })
                )
            })
            .map(|(to, _)| to)
            .collect()
    }

    #[test]
    fn request_neighbors_asks_with_low_priority_up_to_the_free_slots() {
        // Capacity 5, three neighbors: two free slots, so two requests of ten ids.
        let mut a = with_active(0, &[1, 2, 3]);
        let mut io = Io::new();
        a.handle(InEvent::RequestNeighbors((10..20).collect()), &mut io);

        assert_eq!(low_neighbor_requests(&io), vec![10, 11]);
        assert_eq!(sent(&io).len(), 2, "no Join and no other message");
        assert!(
            a.active_view.len() == 3,
            "a request does not add a neighbor"
        );
    }

    #[test]
    fn request_neighbors_skips_neighbors_pending_requests_and_left_peers() {
        let mut a = with_active(0, &[1]);
        a.left.insert(2);
        let mut io = Io::new();
        a.handle(InEvent::RequestNeighbors(vec![1, 2, 3]), &mut io);
        assert_eq!(low_neighbor_requests(&io), vec![3]);

        // 3 is pending now: a second call does not ask again, and asks 4 within the budget.
        io.clear();
        a.handle(InEvent::RequestNeighbors(vec![3, 4]), &mut io);
        assert_eq!(low_neighbor_requests(&io), vec![4]);
    }

    #[test]
    fn request_neighbors_sends_nothing_when_the_active_view_is_full() {
        let mut a = with_active(0, &[1, 2, 3, 4, 5]);
        let mut io = Io::new();
        a.handle(InEvent::RequestNeighbors(vec![10, 11]), &mut io);
        assert!(sent(&io).is_empty());
    }

    #[test]
    fn request_neighbors_sets_no_refill_timer() {
        // A failed dial clears the pending request through `PeerDisconnected`, and the
        // application asks again later. A refill timer here would also take a peer from the
        // passive view. The only timer is the expiry of the pending entry.
        let mut a = state(0);
        let mut io = Io::new();
        a.handle(InEvent::RequestNeighbors(vec![10]), &mut io);
        assert!(!io.iter().any(|event| matches!(
            event,
            topic::OutEvent::ScheduleTimer(
                _,
                topic::Timer::Swarm(Timer::PendingNeighborRequest(_))
            )
        )));
        let expiries: Vec<_> = io
            .iter()
            .filter_map(|event| match event {
                topic::OutEvent::ScheduleTimer(
                    after,
                    topic::Timer::Swarm(Timer::NeighborRequestExpired(peer)),
                ) => Some((*after, *peer)),
                _ => None,
            })
            .collect();
        assert_eq!(expiries, vec![(NEIGHBOR_REQUEST_EXPIRY, 10)]);
        a.handle(InEvent::PeerDisconnected(10), &mut io);
        assert!(a.pending_neighbor_requests.is_empty());
    }

    #[test]
    fn an_expired_request_stops_being_pending_and_changes_nothing_else() {
        let mut a = with_active(0, &[1]);
        a.passive_view.insert(20);
        let mut io = Io::new();
        a.handle(InEvent::RequestNeighbors(vec![10]), &mut io);
        assert!(a.pending_neighbor_requests.contains(&10));
        io.clear();

        a.handle(
            InEvent::TimerExpired(Timer::NeighborRequestExpired(10)),
            &mut io,
        );

        assert!(a.pending_neighbor_requests.is_empty());
        assert!(
            a.passive_view.contains(&20),
            "the passive view is untouched"
        );
        assert!(sent(&io).is_empty(), "no refill: nothing is sent");
        // The application can ask again.
        a.handle(InEvent::RequestNeighbors(vec![10]), &mut io);
        assert_eq!(low_neighbor_requests(&io), vec![10]);
    }

    #[test]
    fn crossing_requests_link_both_sides_with_no_third_message() {
        // a and b ask each other for a link at the same time. Each takes the request of the
        // other as the answer to its own, so both link, and nobody sends a third message.
        let mut a = state(0);
        let mut b = state(1);
        let (mut io_a, mut io_b) = (Io::new(), Io::new());
        a.handle(InEvent::RequestNeighbors(vec![1]), &mut io_a);
        b.handle(InEvent::RequestNeighbors(vec![0]), &mut io_b);
        let to_b = low_neighbor_requests(&io_a);
        let to_a = low_neighbor_requests(&io_b);
        assert_eq!((to_b, to_a), (vec![1], vec![0]));
        let (request_of_a, request_of_b) = (sent(&io_a).remove(0).1, sent(&io_b).remove(0).1);
        io_a.clear();
        io_b.clear();

        recv(&mut b, 0, request_of_a, &mut io_b);
        recv(&mut a, 1, request_of_b, &mut io_a);

        assert!(a.active_view.contains(&1) && b.active_view.contains(&0));
        assert!(
            sent(&io_a).is_empty() && sent(&io_b).is_empty(),
            "no third message"
        );
        assert!(a.pending_neighbor_requests.is_empty());
        assert!(b.pending_neighbor_requests.is_empty());
    }

    /// Delivers what the states 0 (`a`) and 1 (`b`) send to each other, until nothing is left.
    /// Returns the number of messages delivered, or `None` if more than `limit` rounds were needed.
    fn settle(
        a: &mut TestState,
        b: &mut TestState,
        io_a: &mut Io,
        io_b: &mut Io,
        limit: usize,
    ) -> Option<usize> {
        let mut delivered = 0;
        for _ in 0..limit {
            let to_b: Vec<_> = sent(io_a)
                .into_iter()
                .filter(|(to, _)| *to == 1)
                .map(|(_, message)| message)
                .collect();
            let to_a: Vec<_> = sent(io_b)
                .into_iter()
                .filter(|(to, _)| *to == 0)
                .map(|(_, message)| message)
                .collect();
            io_a.clear();
            io_b.clear();
            if to_a.is_empty() && to_b.is_empty() {
                return Some(delivered);
            }
            for message in to_b {
                delivered += 1;
                recv(b, 0, message, io_b);
            }
            for message in to_a {
                delivered += 1;
                recv(a, 1, message, io_a);
            }
        }
        None
    }

    #[test]
    fn one_request_costs_two_messages() {
        let mut a = state(0);
        let mut b = state(1);
        let (mut io_a, mut io_b) = (Io::new(), Io::new());
        a.handle(InEvent::RequestNeighbors(vec![1]), &mut io_a);

        let delivered = settle(&mut a, &mut b, &mut io_a, &mut io_b, 20);

        assert_eq!(delivered, Some(2), "the request and its answer");
        assert!(a.active_view.contains(&1) && b.active_view.contains(&0));
    }

    #[test]
    fn crossing_requests_settle_with_at_most_one_extra_message() {
        let mut a = state(0);
        let mut b = state(1);
        let (mut io_a, mut io_b) = (Io::new(), Io::new());
        a.handle(InEvent::RequestNeighbors(vec![1]), &mut io_a);
        b.handle(InEvent::RequestNeighbors(vec![0]), &mut io_b);

        let delivered = settle(&mut a, &mut b, &mut io_a, &mut io_b, 20);

        assert!(a.active_view.contains(&1) && b.active_view.contains(&0));
        let delivered = delivered.expect("the exchange ends");
        assert!(
            delivered <= 3,
            "two requests and at most one extra message: {delivered}"
        );
    }

    #[test]
    fn a_crossing_request_that_is_lost_still_links_both_sides() {
        // a and b ask each other at once. The request of a never arrives (its connection was
        // the loser of a crossing, and was closed unread). b's request does arrive, and a
        // reads it as the answer to its own.
        let mut a = state(0);
        let mut b = state(1);
        let (mut io_a, mut io_b) = (Io::new(), Io::new());
        a.handle(InEvent::RequestNeighbors(vec![1]), &mut io_a);
        b.handle(InEvent::RequestNeighbors(vec![0]), &mut io_b);
        io_a.clear();
        let request_of_b = sent(&io_b).remove(0).1;
        io_b.clear();

        recv(&mut a, 1, request_of_b, &mut io_a);
        let delivered = settle(&mut a, &mut b, &mut io_a, &mut io_b, 20);
        assert!(delivered.is_some(), "the exchange ends");
        // b still waits for an answer that will not come. Its request expires, and the
        // application asks again: a holds b and answers, and b reads that as the answer.
        assert!(b.pending_neighbor_requests.contains(&0));
        b.handle(
            InEvent::TimerExpired(Timer::NeighborRequestExpired(0)),
            &mut io_b,
        );
        b.handle(InEvent::RequestNeighbors(vec![0]), &mut io_b);
        let delivered = settle(&mut a, &mut b, &mut io_a, &mut io_b, 20);

        assert!(delivered.is_some(), "the exchange ends");
        assert!(
            a.active_view.contains(&1) && b.active_view.contains(&0),
            "a holds b: {}, b holds a: {}",
            a.active_view.contains(&1),
            b.active_view.contains(&0)
        );
    }

    #[test]
    fn a_late_answer_to_a_request_still_links_the_pair() {
        let mut a = state(0);
        let mut io = Io::new();
        a.handle(InEvent::RequestNeighbors(vec![10]), &mut io);
        io.clear();
        recv(&mut a, 10, neighbor(Priority::Low), &mut io);
        assert!(a.active_view.contains(&10));
        assert_eq!(neighbor_ups(&io), 1);
        assert!(a.pending_neighbor_requests.is_empty());
    }

    #[test]
    fn a_refused_request_keeps_the_peer_in_the_passive_view() {
        let mut a = state(0);
        a.passive_view.insert(10);
        let mut io = Io::new();
        a.handle(InEvent::RequestNeighbors(vec![10]), &mut io);
        recv(&mut a, 10, disconnect(true, false), &mut io);
        assert!(a.active_view.is_empty());
        assert!(a.pending_neighbor_requests.is_empty());
        assert!(a.passive_view.contains(&10));
    }

    #[test]
    fn disconnect_wire_format_is_unchanged() {
        let bytes = |alive, left| postcard::to_stdvec(&Disconnect { alive, left }).unwrap();
        assert_eq!(bytes(true, false), [1, 0]);
        assert_eq!(bytes(false, true), [0, 1]);
    }

    #[test]
    fn leave_tombstones_a_peer_that_is_only_in_the_passive_view() {
        let mut a = state(0);
        a.passive_view.insert(1);
        let mut io = Io::new();
        a.handle(InEvent::Leave(vec![1]), &mut io);
        assert!(a.left.contains(&1));
        assert!(!a.passive_view.contains(&1));
        assert!(sent(&io).is_empty());
    }

    #[test]
    fn leave_sends_a_left_disconnect_and_keeps_other_neighbors() {
        let mut a = with_active(0, &[1, 2]);
        let mut io = Io::new();
        a.handle(InEvent::Leave(vec![1]), &mut io);
        assert!(sent(&io).contains(&(1, disconnect(false, true))));
        assert!(a.active_view.contains(&2));
        assert!(!a.active_view.contains(&1));
    }

    #[test]
    fn neighbor_request_from_a_left_peer_is_refused_even_with_high_priority() {
        let mut a = state(0);
        a.passive_view.insert(1);
        let mut io = Io::new();
        a.handle(InEvent::Leave(vec![1]), &mut io);
        io.clear();
        recv(&mut a, 1, neighbor(Priority::High), &mut io);
        assert!(!a.active_view.contains(&1));
        assert!(sent(&io).contains(&(1, disconnect(false, true))));
        assert_eq!(neighbor_ups(&io), 0);
    }

    #[test]
    fn received_left_disconnect_tombstones_and_forgets_the_peer() {
        let mut b = with_active(1, &[0, 2]);
        b.passive_view.insert(3);
        b.peer_data.insert(0, PeerData::new(vec![1]));
        let mut io = Io::new();
        recv(&mut b, 0, disconnect(false, true), &mut io);
        assert!(b.left.contains(&0));
        assert!(!b.active_view.contains(&0));
        assert!(!b.passive_view.contains(&0));
        assert!(!b.peer_data.contains_key(&0));
    }

    #[test]
    fn refused_high_priority_neighbor_is_healed_by_a_join_in_one_flow() {
        let mut a = with_active(0, &[1]);
        let mut io = Io::new();
        a.handle(InEvent::Leave(vec![1]), &mut io);
        io.clear();

        // The peer asks for a link with high priority, as an isolated node does.
        recv(&mut a, 1, neighbor(Priority::High), &mut io);
        assert!(a.active_view.is_empty());
        assert!(sent(&io).contains(&(1, disconnect(false, true))));

        // The same peer sends a Join. The link comes back and a Neighbor works again.
        recv(&mut a, 1, Message::Join(None), &mut io);
        assert!(a.active_view.contains(&1));
        assert!(a.left.is_empty());
    }

    #[test]
    fn plain_disconnect_does_not_tombstone() {
        let mut b = with_active(1, &[0]);
        let mut io = Io::new();
        recv(&mut b, 0, disconnect(false, false), &mut io);
        assert!(b.left.is_empty());
        recv(&mut b, 0, neighbor(Priority::High), &mut io);
        assert!(b.active_view.contains(&0));
    }

    #[test]
    fn shuffle_reply_naming_a_left_peer_adds_neither_view_nor_peer_data() {
        let mut b = with_active(1, &[2]);
        let mut io = Io::new();
        recv(&mut b, 2, disconnect(false, true), &mut io);
        recv(&mut b, 3, disconnect(false, true), &mut io);
        let reply = Message::ShuffleReply(ShuffleReply {
            nodes: vec![
                PeerInfo {
                    id: 2,
                    data: Some(PeerData::new(vec![1, 2, 3])),
                },
                PeerInfo { id: 4, data: None },
            ],
        });
        recv(&mut b, 5, reply, &mut io);
        assert!(!b.passive_view.contains(&2));
        assert!(!b.peer_data.contains_key(&2));
        assert!(b.passive_view.contains(&4));
    }

    #[test]
    fn forward_join_for_a_left_joiner_is_passed_on_while_the_ttl_lasts() {
        let mut b = with_active(1, &[2, 3]);
        let mut io = Io::new();
        recv(&mut b, 0, disconnect(false, true), &mut io);
        io.clear();
        recv(&mut b, 2, forward_join(0, 3), &mut io);
        // No Neighbor to the joiner, and the walk goes on to the other neighbor.
        assert_eq!(sent(&io), vec![(3, forward_join(0, 2))]);

        // At ttl 0 the walk ends here.
        io.clear();
        recv(&mut b, 2, forward_join(0, 0), &mut io);
        assert!(sent(&io).is_empty());
        assert!(!b.pending_neighbor_requests.contains(&0));
    }

    /// Three nodes, each with the other two active, all holding the tombstone of the joiner.
    #[test]
    fn forward_join_walk_ends_when_every_node_holds_the_tombstone() {
        let mut nodes: Vec<TestState> = (1..=3)
            .map(|me| {
                let others: Vec<u64> = (1..=3).filter(|p| *p != me).collect();
                let mut node = with_active(me, &others);
                node.left.insert(9);
                node
            })
            .collect();
        let mut in_flight = vec![(1u64, 2u64, forward_join(9, 6))];
        let mut sends = 0;
        while let Some((from, to, message)) = in_flight.pop() {
            sends += 1;
            assert!(sends <= 20, "the walk does not end");
            let mut io = Io::new();
            recv(&mut nodes[to as usize - 1], from, message, &mut io);
            for (next, message) in sent(&io) {
                in_flight.push((to, next, message));
            }
        }
        assert!(sends <= 7, "one send per ttl step at most, got {sends}");
    }

    #[test]
    fn forward_join_for_a_left_joiner_is_dropped_without_another_neighbor() {
        let mut b = with_active(1, &[2]);
        let mut io = Io::new();
        recv(&mut b, 0, disconnect(false, true), &mut io);
        io.clear();
        recv(&mut b, 2, forward_join(0, 0), &mut io);
        assert!(sent(&io).is_empty());
        assert!(!b.pending_neighbor_requests.contains(&0));
    }

    #[test]
    fn join_from_either_side_clears_the_tombstone() {
        // The leaver rejoins.
        let mut b = with_active(1, &[0]);
        let mut io = Io::new();
        recv(&mut b, 0, disconnect(false, true), &mut io);
        assert!(b.left.contains(&0));
        recv(&mut b, 0, Message::Join(None), &mut io);
        assert!(b.left.is_empty());
        assert!(b.active_view.contains(&0));

        // The side that left asks for the link again.
        let mut a = with_active(0, &[1]);
        a.handle(InEvent::Leave(vec![1]), &mut io);
        assert!(a.left.contains(&1));
        io.clear();
        a.handle(InEvent::RequestJoin(1), &mut io);
        assert!(a.left.is_empty());
        assert!(sent(&io)
            .iter()
            .any(|(to, m)| *to == 1 && matches!(m, Message::Join(_))));
    }

    #[test]
    fn tombstones_are_bounded_and_the_oldest_goes_first() {
        let config = Config {
            passive_view_capacity: 2,
            ..Default::default()
        };
        let mut a = State::new(0u64, None, config, ChaCha12Rng::seed_from_u64(0));
        let mut io = Io::new();
        a.handle(InEvent::Leave((1..=20).collect()), &mut io);
        assert_eq!(a.left.len(), 16);
        assert!(!a.left.contains(&1));
        assert!(a.left.contains(&20));
    }

    /// A `Neighbor` from B is already on its way when A leaves B.
    #[test]
    fn crossing_neighbor_and_leave_end_with_tombstones_on_both_sides() {
        let mut a = with_active(0, &[1]);
        let mut b = state(1);
        b.passive_view.insert(0);
        let (mut a_io, mut b_io) = (Io::new(), Io::new());

        b.refill_active_from_passive(&[], &mut b_io);
        let b_to_a = sent(&b_io);
        assert!(matches!(b_to_a.as_slice(), [(0, Message::Neighbor(_))]));
        b_io.clear();

        a.handle(InEvent::Leave(vec![1]), &mut a_io);
        let a_to_b_leave = sent(&a_io);
        a_io.clear();

        for (_, message) in b_to_a {
            recv(&mut a, 1, message, &mut a_io);
        }
        let a_to_b_refusal = sent(&a_io);

        for (_, message) in a_to_b_leave.into_iter().chain(a_to_b_refusal) {
            recv(&mut b, 0, message, &mut b_io);
        }

        assert!(a.left.contains(&1) && b.left.contains(&0));
        assert!(a.active_view.is_empty() && b.active_view.is_empty());
        assert_eq!(neighbor_ups(&a_io) + neighbor_ups(&b_io), 0);
        assert!(sent(&b_io)
            .iter()
            .all(|(_, m)| !matches!(m, Message::Neighbor(_))));
    }
}
