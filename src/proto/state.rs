//! The protocol state of the `iroh-gossip` protocol.

use std::collections::{hash_map, HashMap, HashSet};

use n0_future::time::{Duration, Instant};
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};
use tracing::trace;

use crate::{
    metrics::Metrics,
    proto::{
        topic::{self, Command},
        util::idbytes_impls,
        Config, PeerData, PeerIdentity, MIN_MAX_MESSAGE_SIZE,
    },
};

/// The identifier for a topic
#[derive(Clone, Copy, Eq, PartialEq, Hash, Serialize, Ord, PartialOrd, Deserialize)]
pub struct TopicId([u8; 32]);
idbytes_impls!(TopicId, "TopicId");

impl TopicId {
    /// Convert to a hex string limited to the first 5 bytes for a friendly string
    /// representation of the key.
    pub fn fmt_short(&self) -> String {
        data_encoding::HEXLOWER.encode(&self.as_bytes()[..5])
    }
}

/// Protocol wire message
///
/// This is the wire frame of the `iroh-gossip` protocol.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Message<PI> {
    pub(crate) topic: TopicId,
    pub(crate) message: topic::Message<PI>,
}

impl<PI> Message<PI> {
    /// Get the kind of this message
    pub fn kind(&self) -> MessageKind {
        self.message.kind()
    }

    /// Whether the protocol repairs the loss of this message by itself, so that a sender
    /// that cannot queue it may drop it.
    ///
    /// This holds for a plumtree `Gossip` (another neighbor sends an `IHave`, and a `Graft` then
    /// fetches the message) and for an `IHave` (a lost repair hint). It does not hold for
    /// anything else: a lost `Join` or `Neighbor` leaves the two views of a link different, a
    /// lost `Disconnect` loses its `left` flag, and `Prune` and `Graft` are tree state.
    pub fn is_droppable(&self) -> bool {
        matches!(
            self.message,
            topic::Message::Gossip(
                super::plumtree::Message::Gossip(_) | super::plumtree::Message::IHave(_)
            )
        )
    }

    /// Whether this is a HyParView frame, whose meaning depends on its order with the other
    /// HyParView frames of its topic.
    #[cfg(feature = "net")]
    pub(crate) fn is_swarm(&self) -> bool {
        matches!(self.message, topic::Message::Swarm(_))
    }

    /// Whether this is a `Join`, which the protocol sends once and never again.
    #[cfg(feature = "net")]
    pub(crate) fn is_join(&self) -> bool {
        matches!(
            self.message,
            topic::Message::Swarm(super::hyparview::Message::Join(_))
        )
    }
}

impl<PI: Serialize> Message<PI> {
    pub(crate) fn postcard_header_size() -> usize {
        // We create a message that has no payload (gossip::Message::Prune), calculate the encoded size,
        // and subtract 1 for the discriminator of the inner gossip::Message enum.
        let m = Self {
            topic: TopicId(Default::default()),
            message: topic::Message::<PI>::Gossip(super::plumtree::Message::Prune),
        };
        postcard::experimental::serialized_size(&m).unwrap() - 1
    }
}

/// Whether this is a control or data message
#[derive(Debug)]
pub enum MessageKind {
    /// A data message.
    Data,
    /// A control message.
    Control,
}

impl<PI: Serialize> Message<PI> {
    /// Get the encoded size of this message
    pub fn size(&self) -> postcard::Result<usize> {
        postcard::experimental::serialized_size(&self)
    }
}

/// A timer to be registered into the runtime
///
/// As the implementation of the protocol is an IO-less state machine, registering timers does not
/// happen within the protocol implementation. Instead, these `Timer` structs are emitted as
/// [`OutEvent`]s. The implementer must register the timer in its runtime to be emitted on the specified [`Instant`],
/// and once triggered inject an [`InEvent::TimerExpired`] into the protocol state.
#[derive(Clone, Debug)]
pub struct Timer<PI> {
    topic: TopicId,
    timer: topic::Timer<PI>,
}

/// Input event to the protocol state.
#[derive(Clone, Debug)]
pub enum InEvent<PI> {
    /// Message received from the network.
    RecvMessage(PI, Message<PI>),
    /// Execute a command from the application.
    Command(TopicId, Command<PI>),
    /// Trigger a previously scheduled timer.
    TimerExpired(Timer<PI>),
    /// Peer disconnected on the network level.
    PeerDisconnected(PI),
    /// A dial to the peer failed, and the network level keeps what it queued for the peer, for a
    /// connection that the peer opens to us. A `Join` that waits for the peer keeps waiting.
    DialFailed(PI),
    /// Update the opaque peer data about yourself.
    UpdatePeerData(PeerData),
}

/// Output event from the protocol state.
#[derive(Debug, Clone)]
pub enum OutEvent<PI> {
    /// Send a message on the network
    SendMessage(PI, Message<PI>),
    /// Emit an event to the application.
    EmitEvent(TopicId, topic::Event<PI>),
    /// Schedule a timer. The runtime is responsible for sending an [InEvent::TimerExpired]
    /// after the duration.
    ScheduleTimer(Duration, Timer<PI>),
    /// Close the connection to a peer on the network level.
    DisconnectPeer(PI),
    /// One topic is done with a peer that another topic still uses: drop what waits for the peer
    /// in this topic only, and keep the connection.
    DisconnectPeerTopic(PI, TopicId),
    /// Updated peer data
    PeerData(PI, PeerData),
}

type ConnsMap<PI> = HashMap<PI, HashSet<TopicId>>;
type Outbox<PI> = Vec<OutEvent<PI>>;

enum InEventMapped<PI> {
    All(topic::InEvent<PI>),
    TopicEvent(TopicId, topic::InEvent<PI>),
}

impl<PI> From<InEvent<PI>> for InEventMapped<PI> {
    fn from(event: InEvent<PI>) -> InEventMapped<PI> {
        match event {
            InEvent::RecvMessage(from, Message { topic, message }) => {
                Self::TopicEvent(topic, topic::InEvent::RecvMessage(from, message))
            }
            InEvent::Command(topic, command) => {
                Self::TopicEvent(topic, topic::InEvent::Command(command))
            }
            InEvent::TimerExpired(Timer { topic, timer }) => {
                Self::TopicEvent(topic, topic::InEvent::TimerExpired(timer))
            }
            InEvent::PeerDisconnected(peer) => Self::All(topic::InEvent::PeerDisconnected(peer)),
            InEvent::DialFailed(peer) => Self::All(topic::InEvent::DialFailed(peer)),
            InEvent::UpdatePeerData(data) => Self::All(topic::InEvent::UpdatePeerData(data)),
        }
    }
}

/// The state of the `iroh-gossip` protocol.
///
/// The implementation works as an IO-less state machine. The implementer injects events through
/// [`Self::handle`], which returns an iterator of [`OutEvent`]s to be processed.
///
/// This struct contains a map of [`topic::State`] for each topic that was joined. It mostly acts as
/// a forwarder of [`InEvent`]s to matching topic state. Each topic's state is completely
/// independent; thus the actual protocol logic lives with [`topic::State`].
#[derive(Debug)]
pub struct State<PI, R> {
    me: PI,
    me_data: PeerData,
    config: Config,
    rng: R,
    states: HashMap<TopicId, topic::State<PI, R>>,
    outbox: Outbox<PI>,
    peer_topics: ConnsMap<PI>,
}

impl<PI: PeerIdentity, R: Rng + SeedableRng> State<PI, R> {
    /// Create a new protocol state instance.
    ///
    /// `me` is the [`PeerIdentity`] of the local node, `peer_data` is the initial [`PeerData`]
    /// (which can be updated over time).
    /// For the protocol to perform as recommended in the papers, the [`Config`] should be
    /// identical for all nodes in the network.
    ///
    /// ## Panics
    ///
    /// Panics if [`Config::max_message_size`] is below [`MIN_MAX_MESSAGE_SIZE`].
    pub fn new(me: PI, me_data: PeerData, config: Config, rng: R) -> Self {
        assert!(
            config.max_message_size >= MIN_MAX_MESSAGE_SIZE,
            "max_message_size must be at least {MIN_MAX_MESSAGE_SIZE}"
        );
        Self {
            me,
            me_data,
            config,
            rng,
            states: Default::default(),
            outbox: Default::default(),
            peer_topics: Default::default(),
        }
    }

    /// Get a reference to the node's [`PeerIdentity`]
    pub fn me(&self) -> &PI {
        &self.me
    }

    /// Get a reference to the protocol state for a topic.
    pub fn state(&self, topic: &TopicId) -> Option<&topic::State<PI, R>> {
        self.states.get(topic)
    }

    /// Resets the tracked stats for a topic.
    pub fn reset_stats(&mut self, topic: &TopicId) {
        if let Some(state) = self.states.get_mut(topic) {
            state.reset_stats();
        }
    }

    /// Get an iterator of all joined topics.
    pub fn topics(&self) -> impl Iterator<Item = &TopicId> {
        self.states.keys()
    }

    /// Get an iterator for the states of all joined topics.
    pub fn states(&self) -> impl Iterator<Item = (&TopicId, &topic::State<PI, R>)> {
        self.states.iter()
    }

    /// Check if a topic has any active (connected) peers.
    pub fn has_active_peers(&self, topic: &TopicId) -> bool {
        self.state(topic)
            .map(|s| s.has_active_peers())
            .unwrap_or(false)
    }

    /// Returns the maximum message size configured in the gossip protocol.
    pub fn max_message_size(&self) -> usize {
        self.config.max_message_size
    }

    /// Handle an [`InEvent`]
    ///
    /// This returns an iterator of [`OutEvent`]s that must be processed.
    pub fn handle(
        &mut self,
        event: InEvent<PI>,
        now: Instant,
        metrics: Option<&Metrics>,
    ) -> impl Iterator<Item = OutEvent<PI>> + '_ + use<'_, PI, R> {
        trace!("in : {event:?}");

        let event: InEventMapped<PI> = event.into();

        match event {
            InEventMapped::TopicEvent(topic, event) => {
                // when receiving a join command, initialize state if it doesn't exist
                if matches!(&event, topic::InEvent::Command(Command::Join(_peers))) {
                    if let hash_map::Entry::Vacant(e) = self.states.entry(topic) {
                        e.insert(topic::State::with_rng(
                            self.me,
                            Some(self.me_data.clone()),
                            self.config.clone(),
                            R::from_rng(&mut self.rng),
                        ));
                    }
                }

                // when receiving a quit command, note this and drop the topic state after
                // processing this last event
                let quit = matches!(event, topic::InEvent::Command(Command::Quit));

                // pass the event to the state handler
                if let Some(state) = self.states.get_mut(&topic) {
                    // when receiving messages, update our conn map to take note that this topic state may want
                    // to keep this connection
                    if let topic::InEvent::RecvMessage(from, _message) = &event {
                        self.peer_topics.entry(*from).or_default().insert(topic);
                    }
                    let out = state.handle(event, now);
                    for event in out {
                        handle_out_event(topic, event, &mut self.peer_topics, &mut self.outbox);
                    }
                }

                if quit {
                    self.states.remove(&topic);
                    self.forget_topic(topic);
                }
            }
            // when a peer disconnected on the network level, forward event to all states
            InEventMapped::All(event) => {
                if let topic::InEvent::UpdatePeerData(data) = &event {
                    self.me_data = data.clone();
                }
                for (topic, state) in self.states.iter_mut() {
                    let out = state.handle(event.clone(), now);
                    for event in out {
                        handle_out_event(*topic, event, &mut self.peer_topics, &mut self.outbox);
                    }
                }
                // The connection is gone, so no topic uses the peer any more. A stale entry would
                // turn the last real disconnect of the peer into a partial one.
                if let topic::InEvent::PeerDisconnected(peer) = &event {
                    self.peer_topics.remove(peer);
                }
            }
        }

        if let Some(metrics) = &metrics {
            track_neighbor_events(&self.outbox, metrics);
        }

        self.outbox.drain(..)
    }

    /// A topic that quit uses no peer any more. A peer that no other topic uses is disconnected:
    /// the disconnects that the quit itself sent cover only the peers of its active view.
    fn forget_topic(&mut self, topic: TopicId) {
        let outbox = &mut self.outbox;
        self.peer_topics.retain(|peer, topics| {
            if topics.remove(&topic) && topics.is_empty() {
                outbox.push(OutEvent::DisconnectPeer(*peer));
            }
            !topics.is_empty()
        });
    }
}

fn handle_out_event<PI: PeerIdentity>(
    topic: TopicId,
    event: topic::OutEvent<PI>,
    conns: &mut ConnsMap<PI>,
    outbox: &mut Outbox<PI>,
) {
    trace!("out: {event:?}");
    match event {
        topic::OutEvent::SendMessage(to, message) => {
            // A topic that sends to a peer uses the peer, as a topic that received from it does:
            // its later `DisconnectPeer` must reach the network level (also for a peer that never
            // sent us anything), and must not close what another topic still uses.
            conns.entry(to).or_default().insert(topic);
            outbox.push(OutEvent::SendMessage(to, Message { topic, message }))
        }
        topic::OutEvent::EmitEvent(event) => outbox.push(OutEvent::EmitEvent(topic, event)),
        topic::OutEvent::ScheduleTimer(delay, timer) => {
            outbox.push(OutEvent::ScheduleTimer(delay, Timer { topic, timer }))
        }
        topic::OutEvent::DisconnectPeer(peer) => {
            // Only the last topic that uses the peer closes the connection. A topic before it
            // drops only its own part. A peer that no topic uses has nothing to close.
            if let Some(topics) = conns.get_mut(&peer) {
                topics.remove(&topic);
                if topics.is_empty() {
                    conns.remove(&peer);
                    outbox.push(OutEvent::DisconnectPeer(peer));
                } else {
                    outbox.push(OutEvent::DisconnectPeerTopic(peer, topic));
                }
            }
        }
        topic::OutEvent::PeerData(peer, data) => outbox.push(OutEvent::PeerData(peer, data)),
    }
}

/// The message counters are not written here: the connection loops count each frame that they
/// write or read, so a message that is dropped before it is written does not count as sent.
fn track_neighbor_events<PI>(events: &[OutEvent<PI>], metrics: &Metrics) {
    for event in events {
        if let OutEvent::EmitEvent(_topic, event) = event {
            match event {
                super::Event::NeighborUp(_peer) => {
                    metrics.neighbor_up.inc();
                }
                super::Event::NeighborDown(_peer) => {
                    metrics.neighbor_down.inc();
                }
                _ => {}
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{hyparview, plumtree};

    fn message(message: topic::Message<u64>) -> Message<u64> {
        Message {
            topic: TopicId::from([0u8; 32]),
            message,
        }
    }

    /// Only a plumtree `Gossip` and `IHave` may be dropped by a sender that cannot queue them.
    /// (`Gossip` is covered by the burst test in `net.rs`, which counts the data messages that
    /// reach a peer. `Disconnect`, `Neighbor`, `Graft` have private fields, so the control test
    /// in `net.rs` covers `Disconnect` and these lines cover the rest of the rule.)
    #[test]
    fn only_data_and_ihave_are_droppable() {
        let swarm = |m| message(topic::Message::Swarm(m));
        let gossip = |m| message(topic::Message::Gossip(m));
        assert!(gossip(plumtree::Message::IHave(Vec::new())).is_droppable());
        assert!(!gossip(plumtree::Message::Prune).is_droppable());
        assert!(!swarm(hyparview::Message::Join(None)).is_droppable());
    }

    type TestState = State<u64, rand::rngs::ChaCha12Rng>;

    fn test_state() -> TestState {
        State::new(
            0,
            PeerData::new(Vec::new()),
            Config::default(),
            rand::SeedableRng::seed_from_u64(0),
        )
    }

    fn command(state: &mut TestState, topic: TopicId, command: Command<u64>) -> Vec<OutEvent<u64>> {
        state
            .handle(InEvent::Command(topic, command), Instant::now(), None)
            .collect()
    }

    fn shuffle_reply(topic: TopicId) -> Message<u64> {
        Message {
            topic,
            message: topic::Message::Swarm(hyparview::Message::ShuffleReply(
                hyparview::ShuffleReply::empty(),
            )),
        }
    }

    // A closed connection leaves no topic on the peer. Topic B joined peer 7, the connection
    // closed, and then peer 7 sends to topic A, where it is not active: the disconnect of A is the
    // last one and must close the connection, not leave it to a stale entry of B.
    #[test]
    fn a_closed_connection_leaves_no_topic_on_the_peer() {
        let (a, b) = (TopicId::from([1u8; 32]), TopicId::from([2u8; 32]));
        let mut state = test_state();
        command(&mut state, a, Command::Join(Vec::new()));
        command(&mut state, b, Command::Join(vec![7]));
        let _: Vec<_> = state
            .handle(InEvent::PeerDisconnected(7), Instant::now(), None)
            .collect();

        let out: Vec<_> = state
            .handle(
                InEvent::RecvMessage(7, shuffle_reply(a)),
                Instant::now(),
                None,
            )
            .collect();

        let full = out
            .iter()
            .filter(|e| matches!(e, OutEvent::DisconnectPeer(7)))
            .count();
        let partial = out
            .iter()
            .filter(|e| matches!(e, OutEvent::DisconnectPeerTopic(7, _)))
            .count();
        assert_eq!((full, partial), (1, 0), "the connection must close");
    }

    // The map of the topics that use each peer is empty once every topic quit.
    #[test]
    fn no_peer_keeps_a_topic_after_all_topics_quit() {
        let (a, b) = (TopicId::from([1u8; 32]), TopicId::from([2u8; 32]));
        let mut state = test_state();
        command(&mut state, a, Command::Join(Vec::new()));
        command(&mut state, b, Command::Join(vec![7]));
        let _: Vec<_> = state
            .handle(
                InEvent::RecvMessage(8, shuffle_reply(a)),
                Instant::now(),
                None,
            )
            .collect();

        command(&mut state, a, Command::Quit);
        command(&mut state, b, Command::Quit);

        assert!(state.peer_topics.is_empty(), "{:?}", state.peer_topics);
    }
}
