//! Which connection of a peer the actor sends on.
//!
//! Two peers can hold more than one connection to each other: both dial at once (a crossing), or one
//! dials again after a break that the other has not seen. Each side must send on the same one. A
//! side cannot tell from the dial alone which connection works for the peer: the gate of the peer
//! can hold a connection for a long time, or refuse it. What proves that a connection works for
//! the peer is a message that the peer sent on it, because the peer sends on its chosen
//! connection only and a connection that is not chosen carries nothing.
//!
//! The state of this module has no network in it: `now` comes from the caller and a connection is
//! a handle `H` that the module never looks into. The actor wraps it, and a test drives two of
//! them against each other.

use std::time::Duration;

use n0_future::time::Instant;

use super::{ConnId, RxClock, LIVENESS_BOUND};

/// How long a connection that is not chosen is kept before it is closed. It is twice the 15 s that
/// the gate of habilis holds a connection before it refuses it, so that a connection that the gate
/// of the peer refuses closes by itself before the fallback is gone. It bounds the cost: one more
/// connection per crossing for this long, whatever the traffic.
pub(super) const PARK: Duration = Duration::from_secs(30);

/// One connection of a peer.
#[derive(Debug)]
pub(super) struct Conn<H> {
    pub(super) id: ConnId,
    pub(super) handle: H,
    /// Whether the endpoint with the lower id dialed this connection. Both sides give the same answer.
    pub(super) dialed_by_lower: bool,
    /// Whether a message of the peer was read on this connection. It stays true.
    pub(super) carried: bool,
    pub(super) rx: RxClock,
    /// The order of arrival on this side, to tell two connections of the same kind apart.
    seq: u64,
    since: Instant,
    /// Whether the sender was dropped: the connection drains and closes, and is never chosen again.
    stopped: bool,
}

impl<H> Conn<H> {
    pub(super) fn new(id: ConnId, handle: H, dialed_by_lower: bool, rx: RxClock) -> Self {
        Self {
            id,
            handle,
            dialed_by_lower,
            carried: false,
            rx,
            seq: 0,
            since: Instant::now(),
            stopped: false,
        }
    }

    fn alive(&self, now: Instant) -> bool {
        self.rx.age_at(now) < LIVENESS_BOUND
    }

    /// Higher is better: a live connection, then one that carried, then the one that the lower id
    /// dialed, then the newest.
    fn rank(&self, now: Instant) -> (bool, bool, bool, u64) {
        (
            self.alive(now),
            self.carried,
            self.dialed_by_lower,
            self.seq,
        )
    }
}

/// What the actor must do after a change.
#[derive(Debug, PartialEq, Eq)]
pub(super) enum Action {
    /// Sends go to this connection from now on. The protocol is told nothing.
    Choose(ConnId),
    /// Drop the sender of this connection: it drains and closes.
    StopSending(ConnId),
}

#[derive(Debug)]
pub(super) struct ConnSet<H> {
    entries: Vec<Conn<H>>,
    chosen: Option<ConnId>,
    next_seq: u64,
}

impl<H> Default for ConnSet<H> {
    fn default() -> Self {
        Self {
            entries: Vec::new(),
            chosen: None,
            next_seq: 0,
        }
    }
}

impl<H> ConnSet<H> {
    pub(super) fn chosen(&self) -> Option<&Conn<H>> {
        let id = self.chosen?;
        self.entries.iter().find(|entry| entry.id == id)
    }

    #[cfg(test)]
    pub(super) fn entries(&self) -> &[Conn<H>] {
        &self.entries
    }

    pub(super) fn get_mut(&mut self, id: ConnId) -> Option<&mut Conn<H>> {
        self.entries.iter_mut().find(|entry| entry.id == id)
    }

    #[cfg(test)]
    pub(super) fn get(&self, id: ConnId) -> Option<&Conn<H>> {
        self.entries.iter().find(|entry| entry.id == id)
    }

    /// A new connection.
    pub(super) fn admit(&mut self, mut entry: Conn<H>, now: Instant) -> Vec<Action> {
        entry.seq = self.next_seq;
        entry.since = now;
        self.next_seq += 1;
        self.entries.push(entry);
        self.settle(now)
    }

    /// A message of the peer was read on `id`.
    pub(super) fn carried(&mut self, id: ConnId, now: Instant) -> Vec<Action> {
        if let Some(entry) = self.entries.iter_mut().find(|entry| entry.id == id) {
            entry.carried = true;
        }
        self.settle(now)
    }

    /// Time passed: a connection can have gone silent, or have been parked for too long.
    pub(super) fn tick(&mut self, now: Instant) -> Vec<Action> {
        self.settle(now)
    }

    /// The connection `id` is closed. Returns what to do now, and whether the peer has no
    /// connection to send on any more (one that was stopped and drains does not count).
    pub(super) fn closed(&mut self, id: ConnId, now: Instant) -> (Vec<Action>, bool) {
        self.entries.retain(|entry| entry.id != id);
        if self.chosen == Some(id) {
            self.chosen = None;
        }
        let actions = self.settle(now);
        (actions, self.chosen.is_none())
    }

    /// All the handles, to close them when the protocol drops the peer.
    pub(super) fn drain_all(&mut self) -> Vec<Conn<H>> {
        self.chosen = None;
        std::mem::take(&mut self.entries)
    }

    fn settle(&mut self, now: Instant) -> Vec<Action> {
        let mut actions = Vec::new();
        let best = self
            .entries
            .iter()
            .filter(|entry| !entry.stopped)
            .max_by_key(|entry| entry.rank(now))
            .map(|entry| entry.id);
        if best != self.chosen {
            self.chosen = best;
            if let Some(id) = best {
                actions.push(Action::Choose(id));
            }
        }
        let Some(chosen) = self.chosen() else {
            return actions;
        };
        let chosen_id = chosen.id;
        let chosen_alive = chosen.alive(now);
        // A connection is stopped only when it is dead or has been parked for too long. It is NOT
        // stopped because the chosen one carried a message: the peer may still choose it, and if
        // both sides stopped the connection that the other chose, the pair would have none.
        for entry in &mut self.entries {
            if entry.id == chosen_id || entry.stopped {
                continue;
            }
            let parked_too_long = now.saturating_duration_since(entry.since) >= PARK;
            let dead = chosen_alive && !entry.alive(now);
            if parked_too_long || dead {
                entry.stopped = true;
                actions.push(Action::StopSending(entry.id));
            }
        }
        actions
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet};

    use super::*;

    /// The connection that the lower id dialed (side A dials it).
    const X: ConnId = 1;
    /// The connection that the higher id dialed (side B dials it).
    const Y: ConnId = 2;
    const DRAIN_S: u64 = 2;
    /// How long the gate of habilis holds a connection before it refuses it.
    const REFUSAL_S: u64 = 15;
    const END_S: u64 = 60;

    /// When the gate of a side hands a connection that the peer dialed to the actor.
    #[derive(Clone, Copy, Debug, PartialEq, Eq)]
    enum Gate {
        At(u64),
        /// It never hands it over, and the connection is refused (closed) after `REFUSAL_S`.
        Refused,
    }

    const GATES: [Gate; 5] = [
        Gate::At(0),
        Gate::At(3),
        Gate::At(8),
        Gate::At(12),
        Gate::Refused,
    ];

    /// What one side does with the connections of the peer.
    trait Policy {
        fn admit(&mut self, id: ConnId, dialed_by_lower: bool, now: Instant) -> Vec<Action>;
        fn carried(&mut self, id: ConnId, now: Instant) -> Vec<Action>;
        fn closed(&mut self, id: ConnId, now: Instant) -> Vec<Action>;
        fn tick(&mut self, now: Instant) -> Vec<Action>;
        fn chosen(&self) -> Option<ConnId>;
        fn has(&self, id: ConnId) -> bool;
        /// The keep-alive of the connection: a datagram arrived.
        fn keep_alive(&mut self, now: Instant);
    }

    /// The rule of ea2f887, for the reference: the active connection is replaced by a new one,
    /// unless they cross and the active one was dialed by the lower id; the loser is closed.
    #[derive(Default)]
    struct Old {
        active: Option<(ConnId, bool)>,
        known: Vec<ConnId>,
        stops: Vec<ConnId>,
    }

    impl Policy for Old {
        fn admit(&mut self, id: ConnId, dialed_by_lower: bool, _now: Instant) -> Vec<Action> {
            self.known.push(id);
            match self.active {
                None => self.active = Some((id, dialed_by_lower)),
                Some((active_id, active_by_lower)) => {
                    // The rule of ea2f887 with a live active connection: the new one loses when the
                    // lower id dialed the active one and not the new one.
                    if active_by_lower && !dialed_by_lower {
                        self.stops.push(id);
                    } else {
                        self.stops.push(active_id);
                        self.active = Some((id, dialed_by_lower));
                    }
                }
            }
            std::mem::take(&mut self.stops)
                .into_iter()
                .map(Action::StopSending)
                .collect()
        }
        fn carried(&mut self, _id: ConnId, _now: Instant) -> Vec<Action> {
            Vec::new()
        }
        fn closed(&mut self, id: ConnId, _now: Instant) -> Vec<Action> {
            self.known.retain(|known| *known != id);
            if self.active.is_some_and(|(active_id, _)| active_id == id) {
                self.active = None;
            }
            Vec::new()
        }
        fn tick(&mut self, _now: Instant) -> Vec<Action> {
            Vec::new()
        }
        fn chosen(&self) -> Option<ConnId> {
            self.active.map(|(id, _)| id)
        }
        fn has(&self, id: ConnId) -> bool {
            self.known.contains(&id)
        }
        fn keep_alive(&mut self, _now: Instant) {}
    }

    #[derive(Default)]
    struct New(ConnSet<()>);

    impl Policy for New {
        fn admit(&mut self, id: ConnId, dialed_by_lower: bool, now: Instant) -> Vec<Action> {
            let rx = RxClock::new();
            rx.touch_at(now);
            self.0.admit(Conn::new(id, (), dialed_by_lower, rx), now)
        }
        fn carried(&mut self, id: ConnId, now: Instant) -> Vec<Action> {
            self.0.carried(id, now)
        }
        fn closed(&mut self, id: ConnId, now: Instant) -> Vec<Action> {
            self.0.closed(id, now).0
        }
        fn tick(&mut self, now: Instant) -> Vec<Action> {
            self.0.tick(now)
        }
        fn chosen(&self) -> Option<ConnId> {
            self.0.chosen().map(|entry| entry.id)
        }
        fn has(&self, id: ConnId) -> bool {
            self.0.get(id).is_some()
        }
        fn keep_alive(&mut self, now: Instant) {
            for entry in self.0.entries() {
                entry.rx.touch_at(now);
            }
        }
    }

    #[derive(Debug)]
    struct Outcome {
        chosen: [Option<ConnId>; 2],
        /// How many times each side chose a connection, the first choice included.
        choices: [usize; 2],
    }

    /// Two sides, A (the lower id) and B, and two connections: X, which A dialed, and Y, which B
    /// dialed. Each side holds the connection it dialed from the start. `gate_a` says when A
    /// admits Y, `gate_b` when B admits X. A side sends one message per second on its chosen
    /// connection; the peer reads it when it holds the connection, and a message sent before that
    /// waits in the gate and is read at the admission. A connection that a side stops closes for
    /// both after `DRAIN_S`. Nobody dials again, so this tests the choice and not the repair.
    fn run<P: Policy>(mut sides: [P; 2], gate_a: Gate, gate_b: Gate) -> Outcome {
        let base = Instant::now();
        let at = |second: u64| base + Duration::from_secs(second);
        let mut open = BTreeSet::from([X, Y]);
        let mut close_at: BTreeMap<ConnId, u64> = BTreeMap::new();
        let mut waiting: BTreeSet<(ConnId, usize)> = BTreeSet::new();
        let mut choices = [0usize; 2];

        fn apply(
            side: usize,
            actions: Vec<Action>,
            second: u64,
            choices: &mut [usize; 2],
            close_at: &mut BTreeMap<ConnId, u64>,
        ) {
            for action in actions {
                match action {
                    Action::Choose(_) => choices[side] += 1,
                    Action::StopSending(id) => {
                        close_at.entry(id).or_insert(second + DRAIN_S);
                    }
                }
            }
        }

        for second in 0..=END_S {
            let now = at(second);
            // Connections that close now.
            let due: Vec<ConnId> = close_at
                .iter()
                .filter(|(_, due)| **due == second)
                .map(|(id, _)| *id)
                .collect();
            let mut refused: Vec<ConnId> = Vec::new();
            if second == REFUSAL_S {
                if gate_a == Gate::Refused {
                    refused.push(Y);
                }
                if gate_b == Gate::Refused {
                    refused.push(X);
                }
            }
            for id in due.into_iter().chain(refused) {
                if open.remove(&id) {
                    for (side, set) in sides.iter_mut().enumerate() {
                        if set.has(id) {
                            let actions = set.closed(id, now);
                            apply(side, actions, second, &mut choices, &mut close_at);
                        }
                    }
                }
            }
            // Admissions: each side its own dial at once, the dial of the peer at its gate.
            let mut admits: Vec<(usize, ConnId, bool)> = Vec::new();
            if second == 0 {
                admits.push((0, X, true));
                admits.push((1, Y, false));
            }
            if gate_a == Gate::At(second) {
                admits.push((0, Y, false));
            }
            if gate_b == Gate::At(second) {
                admits.push((1, X, true));
            }
            for (side, id, by_lower) in admits {
                if !open.contains(&id) {
                    continue;
                }
                let actions = sides[side].admit(id, by_lower, now);
                apply(side, actions, second, &mut choices, &mut close_at);
                if waiting.contains(&(id, 1 - side)) {
                    let actions = sides[side].carried(id, now);
                    apply(side, actions, second, &mut choices, &mut close_at);
                }
            }
            for (side, set) in sides.iter_mut().enumerate() {
                set.keep_alive(now);
                let actions = set.tick(now);
                apply(side, actions, second, &mut choices, &mut close_at);
            }
            // One message per side on its chosen connection.
            for side in 0..2 {
                let Some(id) = sides[side].chosen() else {
                    continue;
                };
                if !open.contains(&id) {
                    continue;
                }
                if sides[1 - side].has(id) {
                    let actions = sides[1 - side].carried(id, now);
                    apply(1 - side, actions, second, &mut choices, &mut close_at);
                } else {
                    waiting.insert((id, side));
                }
            }
        }
        Outcome {
            chosen: [sides[0].chosen(), sides[1].chosen()],
            choices,
        }
    }

    /// Whether a connection is usable by both sides: the gate of the side that did not dial it
    /// does not refuse it.
    fn viable(gate_a: Gate, gate_b: Gate) -> Vec<ConnId> {
        let mut viable = Vec::new();
        if gate_b != Gate::Refused {
            viable.push(X);
        }
        if gate_a != Gate::Refused {
            viable.push(Y);
        }
        viable
    }

    #[test]
    fn the_old_rule_loses_the_pair_when_one_gate_refuses_the_dial_that_it_keeps() {
        // B refuses X, the connection the lower id dialed. A keeps X, reads Y only, and closes Y:
        // the one connection that B admits.
        let outcome = run([Old::default(), Old::default()], Gate::At(0), Gate::Refused);
        assert_eq!(outcome.chosen, [None, None], "{outcome:?}");
    }

    #[test]
    fn the_new_rule_ends_both_sides_on_one_connection_whatever_the_gates_do() {
        for gate_a in GATES {
            for gate_b in GATES {
                let outcome = run([New::default(), New::default()], gate_a, gate_b);
                let viable = viable(gate_a, gate_b);
                let what = format!(
                    "gate of A for Y: {gate_a:?}, gate of B for X: {gate_b:?}: {outcome:?}"
                );
                assert_eq!(outcome.chosen[0], outcome.chosen[1], "{what}");
                match viable.as_slice() {
                    [] => assert_eq!(outcome.chosen[0], None, "{what}"),
                    // Both work: the lower id's dial wins.
                    [X, Y] => assert_eq!(outcome.chosen[0], Some(X), "{what}"),
                    [only] => assert_eq!(outcome.chosen[0], Some(*only), "{what}"),
                    other => unreachable!("{other:?}"),
                }
                for choices in outcome.choices {
                    assert!(choices <= 3, "a side changed its mind too often: {what}");
                }
            }
        }
    }

    fn standing(
        id: ConnId,
        dialed_by_lower: bool,
        carried: bool,
        silent: Duration,
        now: Instant,
    ) -> Conn<()> {
        let rx = RxClock::new();
        rx.touch_at(now - silent);
        let mut entry = Conn::new(id, (), dialed_by_lower, rx);
        entry.carried = carried;
        entry
    }

    fn chosen_of(entries: Vec<Conn<()>>, now: Instant) -> ConnId {
        let mut set = ConnSet::default();
        for entry in entries {
            let carried = entry.carried;
            let id = entry.id;
            set.admit(entry, now);
            if carried {
                set.carried(id, now);
            }
        }
        set.chosen().expect("one is chosen").id
    }

    #[test]
    fn the_order_of_the_choice() {
        let now = Instant::now();
        let quiet = Duration::ZERO;
        let dead = LIVENESS_BOUND;
        // A connection that carried beats one that did not, whoever dialed it.
        assert_eq!(
            chosen_of(
                vec![
                    standing(1, true, false, quiet, now),
                    standing(2, false, true, quiet, now)
                ],
                now
            ),
            2
        );
        // Both carried, or neither: the one the lower id dialed.
        assert_eq!(
            chosen_of(
                vec![
                    standing(1, false, true, quiet, now),
                    standing(2, true, true, quiet, now)
                ],
                now
            ),
            2
        );
        assert_eq!(
            chosen_of(
                vec![
                    standing(1, false, false, quiet, now),
                    standing(2, true, false, quiet, now)
                ],
                now
            ),
            2
        );
        // The same kind: the newest.
        assert_eq!(
            chosen_of(
                vec![
                    standing(1, true, true, quiet, now),
                    standing(2, true, true, quiet, now)
                ],
                now
            ),
            2
        );
        // A live connection beats a dead one that carried: the restart case.
        assert_eq!(
            chosen_of(
                vec![
                    standing(1, true, true, dead, now),
                    standing(2, true, false, quiet, now)
                ],
                now
            ),
            2
        );
    }

    #[test]
    fn a_parked_connection_is_stopped_after_park_and_a_dead_one_at_once() {
        let now = Instant::now();
        let mut set = ConnSet::default();
        let actions = set.admit(standing(1, true, false, Duration::ZERO, now), now);
        assert_eq!(actions, vec![Action::Choose(1)]);
        let actions = set.admit(standing(2, false, false, Duration::ZERO, now), now);
        assert_eq!(actions, vec![]);
        // Nothing stops a parked connection for as long as PARK has not passed and it is alive.
        for entry in set.entries() {
            entry.rx.touch_at(now + PARK);
        }
        assert_eq!(set.tick(now + PARK - Duration::from_secs(1)), vec![]);
        assert_eq!(set.tick(now + PARK), vec![Action::StopSending(2)]);
        // The chosen connection closes and the stopped one drains: there is nothing to send on.
        let (_, none_left) = set.closed(1, now + PARK);
        assert!(none_left);
    }

    #[test]
    fn the_chosen_connection_closing_with_another_left_is_a_swap_not_a_disconnect() {
        let now = Instant::now();
        let mut set = ConnSet::default();
        set.admit(standing(1, true, false, Duration::ZERO, now), now);
        set.admit(standing(2, false, false, Duration::ZERO, now), now);
        let (actions, none_left) = set.closed(1, now);
        assert_eq!(actions, vec![Action::Choose(2)]);
        assert!(!none_left);
        let (actions, none_left) = set.closed(2, now);
        assert_eq!(actions, vec![]);
        assert!(none_left);
    }

    #[test]
    fn a_late_message_on_the_connection_the_peer_left_moves_nothing() {
        let now = Instant::now();
        let mut set = ConnSet::default();
        set.admit(standing(1, true, false, Duration::ZERO, now), now);
        set.admit(standing(2, false, false, Duration::ZERO, now), now);
        // Both sides settled on 1: the peer sent on it, and also once on 2 before it moved.
        assert_eq!(set.carried(1, now), vec![]);
        // One more message of the peer, read during the drain of the connection it left.
        assert_eq!(set.carried(2, now), vec![]);
        assert_eq!(set.chosen().map(|conn| conn.id), Some(1));
    }

    #[test]
    fn a_late_message_on_a_connection_that_did_not_carry_costs_one_swap_at_most() {
        let now = Instant::now();
        let mut set = ConnSet::default();
        let mut choices = 0;
        let mut count = |actions: Vec<Action>| {
            choices += actions
                .iter()
                .filter(|action| matches!(action, Action::Choose(_)))
                .count();
            assert!(!actions
                .iter()
                .any(|action| matches!(action, Action::StopSending(_))));
        };
        count(set.admit(standing(1, true, false, Duration::ZERO, now), now));
        count(set.admit(standing(2, false, false, Duration::ZERO, now), now));
        // The peer sends on 2 (its gate holds 1): this side moves to 2.
        count(set.carried(2, now));
        assert_eq!(set.chosen().map(|conn| conn.id), Some(2));
        // A late message of the peer on 1 (it sent on it before it moved): back to 1, the lower id's.
        count(set.carried(1, now));
        assert_eq!(set.chosen().map(|conn| conn.id), Some(1));
        // The next message on 2 changes nothing.
        count(set.carried(2, now));
        assert_eq!(set.chosen().map(|conn| conn.id), Some(1));
        assert!(choices <= 3, "{choices}");
    }
}
