//! Metrics for iroh-gossip

use iroh_metrics::{Counter, MetricsGroup};

use crate::proto::{
    state::MessageKind,
    topic::{Message, Variant},
};

/// Enum of metrics for the module
#[derive(Debug, Default, MetricsGroup)]
#[metrics(name = "gossip")]
pub struct Metrics {
    /// Number of control messages sent
    pub msgs_ctrl_sent: Counter,
    /// Number of control messages received
    pub msgs_ctrl_recv: Counter,
    /// Number of data messages sent
    pub msgs_data_sent: Counter,
    /// Number of data messages received
    pub msgs_data_recv: Counter,
    /// Total size of all data messages sent
    pub msgs_data_sent_size: Counter,
    /// Total size of all data messages received
    pub msgs_data_recv_size: Counter,
    /// Total size of all control messages sent
    pub msgs_ctrl_sent_size: Counter,
    /// Total size of all control messages received
    pub msgs_ctrl_recv_size: Counter,
    /// Number of `join` messages sent.
    pub msgs_join_sent: Counter,
    /// Bytes of `join` messages sent, with the 4 bytes of length prefix of each.
    pub msgs_join_sent_size: Counter,
    /// Number of `join` messages received.
    pub msgs_join_recv: Counter,
    /// Bytes of `join` messages received, with the 4 bytes of length prefix of each.
    pub msgs_join_recv_size: Counter,
    /// Number of `forward_join` messages sent.
    pub msgs_forward_join_sent: Counter,
    /// Bytes of `forward_join` messages sent, with the 4 bytes of length prefix of each.
    pub msgs_forward_join_sent_size: Counter,
    /// Number of `forward_join` messages received.
    pub msgs_forward_join_recv: Counter,
    /// Bytes of `forward_join` messages received, with the 4 bytes of length prefix of each.
    pub msgs_forward_join_recv_size: Counter,
    /// Number of `shuffle` messages sent.
    pub msgs_shuffle_sent: Counter,
    /// Bytes of `shuffle` messages sent, with the 4 bytes of length prefix of each.
    pub msgs_shuffle_sent_size: Counter,
    /// Number of `shuffle` messages received.
    pub msgs_shuffle_recv: Counter,
    /// Bytes of `shuffle` messages received, with the 4 bytes of length prefix of each.
    pub msgs_shuffle_recv_size: Counter,
    /// Number of `shuffle_reply` messages sent.
    pub msgs_shuffle_reply_sent: Counter,
    /// Bytes of `shuffle_reply` messages sent, with the 4 bytes of length prefix of each.
    pub msgs_shuffle_reply_sent_size: Counter,
    /// Number of `shuffle_reply` messages received.
    pub msgs_shuffle_reply_recv: Counter,
    /// Bytes of `shuffle_reply` messages received, with the 4 bytes of length prefix of each.
    pub msgs_shuffle_reply_recv_size: Counter,
    /// Number of `neighbor` messages sent.
    pub msgs_neighbor_sent: Counter,
    /// Bytes of `neighbor` messages sent, with the 4 bytes of length prefix of each.
    pub msgs_neighbor_sent_size: Counter,
    /// Number of `neighbor` messages received.
    pub msgs_neighbor_recv: Counter,
    /// Bytes of `neighbor` messages received, with the 4 bytes of length prefix of each.
    pub msgs_neighbor_recv_size: Counter,
    /// Number of `disconnect` messages sent.
    pub msgs_disconnect_sent: Counter,
    /// Bytes of `disconnect` messages sent, with the 4 bytes of length prefix of each.
    pub msgs_disconnect_sent_size: Counter,
    /// Number of `disconnect` messages received.
    pub msgs_disconnect_recv: Counter,
    /// Bytes of `disconnect` messages received, with the 4 bytes of length prefix of each.
    pub msgs_disconnect_recv_size: Counter,
    /// Number of `gossip` messages sent.
    pub msgs_gossip_sent: Counter,
    /// Bytes of `gossip` messages sent, with the 4 bytes of length prefix of each.
    pub msgs_gossip_sent_size: Counter,
    /// Number of `gossip` messages received.
    pub msgs_gossip_recv: Counter,
    /// Bytes of `gossip` messages received, with the 4 bytes of length prefix of each.
    pub msgs_gossip_recv_size: Counter,
    /// Number of `ihave` messages sent.
    pub msgs_ihave_sent: Counter,
    /// Bytes of `ihave` messages sent, with the 4 bytes of length prefix of each.
    pub msgs_ihave_sent_size: Counter,
    /// Number of `ihave` messages received.
    pub msgs_ihave_recv: Counter,
    /// Bytes of `ihave` messages received, with the 4 bytes of length prefix of each.
    pub msgs_ihave_recv_size: Counter,
    /// Number of `graft` messages sent.
    pub msgs_graft_sent: Counter,
    /// Bytes of `graft` messages sent, with the 4 bytes of length prefix of each.
    pub msgs_graft_sent_size: Counter,
    /// Number of `graft` messages received.
    pub msgs_graft_recv: Counter,
    /// Bytes of `graft` messages received, with the 4 bytes of length prefix of each.
    pub msgs_graft_recv_size: Counter,
    /// Number of `prune` messages sent.
    pub msgs_prune_sent: Counter,
    /// Bytes of `prune` messages sent, with the 4 bytes of length prefix of each.
    pub msgs_prune_sent_size: Counter,
    /// Number of `prune` messages received.
    pub msgs_prune_recv: Counter,
    /// Bytes of `prune` messages received, with the 4 bytes of length prefix of each.
    pub msgs_prune_recv_size: Counter,
    /// Number of times we connected to a peer
    pub neighbor_up: Counter,
    /// Number of times we disconnected from a peer
    pub neighbor_down: Counter,
    /// Number of times the main actor loop ticked
    pub actor_tick_main: Counter,
    /// Number of times the actor ticked for a message received
    pub actor_tick_rx: Counter,
    /// Number of times the actor ticked for an endpoint event
    pub actor_tick_endpoint: Counter,
    /// Number of times the actor ticked for a dialer event
    pub actor_tick_dialer: Counter,
    /// Number of times the actor ticked for a successful dialer event
    pub actor_tick_dialer_success: Counter,
    /// Number of times the actor ticked for a failed dialer event
    pub actor_tick_dialer_failure: Counter,
    /// Number of times the actor ticked for an incoming event
    pub actor_tick_in_event_rx: Counter,
    /// Number of times the actor ticked for a timer event
    pub actor_tick_timers: Counter,
    /// Number of data messages dropped because the send queue of a connection was full
    pub msgs_dropped_send_queue_full: Counter,
    /// Number of peers disconnected because their send queue refused a message for too long, or
    /// refused a message that must not be dropped
    pub send_overflow_disconnects: Counter,
}

impl Metrics {
    /// Count a message that was written to a connection: `bytes` is the length prefix and the
    /// encoded message. The counters of its kind (control or data) and of its variant go up.
    pub(crate) fn note_sent<PI>(&self, message: &Message<PI>, bytes: usize) {
        let bytes = bytes as u64;
        match message.kind() {
            MessageKind::Control => {
                self.msgs_ctrl_sent.inc();
                self.msgs_ctrl_sent_size.inc_by(bytes);
            }
            MessageKind::Data => {
                self.msgs_data_sent.inc();
                self.msgs_data_sent_size.inc_by(bytes);
            }
        }
        let (count, size) = self.sent_counters(message.variant());
        count.inc();
        size.inc_by(bytes);
    }

    /// Count a message that was read from a connection, as [`Self::note_sent`] does.
    pub(crate) fn note_recv<PI>(&self, message: &Message<PI>, bytes: usize) {
        let bytes = bytes as u64;
        match message.kind() {
            MessageKind::Control => {
                self.msgs_ctrl_recv.inc();
                self.msgs_ctrl_recv_size.inc_by(bytes);
            }
            MessageKind::Data => {
                self.msgs_data_recv.inc();
                self.msgs_data_recv_size.inc_by(bytes);
            }
        }
        let (count, size) = self.recv_counters(message.variant());
        count.inc();
        size.inc_by(bytes);
    }

    fn sent_counters(&self, variant: Variant) -> (&Counter, &Counter) {
        match variant {
            Variant::Join => (&self.msgs_join_sent, &self.msgs_join_sent_size),
            Variant::ForwardJoin => (
                &self.msgs_forward_join_sent,
                &self.msgs_forward_join_sent_size,
            ),
            Variant::Shuffle => (&self.msgs_shuffle_sent, &self.msgs_shuffle_sent_size),
            Variant::ShuffleReply => (
                &self.msgs_shuffle_reply_sent,
                &self.msgs_shuffle_reply_sent_size,
            ),
            Variant::Neighbor => (&self.msgs_neighbor_sent, &self.msgs_neighbor_sent_size),
            Variant::Disconnect => (&self.msgs_disconnect_sent, &self.msgs_disconnect_sent_size),
            Variant::Gossip => (&self.msgs_gossip_sent, &self.msgs_gossip_sent_size),
            Variant::IHave => (&self.msgs_ihave_sent, &self.msgs_ihave_sent_size),
            Variant::Graft => (&self.msgs_graft_sent, &self.msgs_graft_sent_size),
            Variant::Prune => (&self.msgs_prune_sent, &self.msgs_prune_sent_size),
        }
    }

    fn recv_counters(&self, variant: Variant) -> (&Counter, &Counter) {
        match variant {
            Variant::Join => (&self.msgs_join_recv, &self.msgs_join_recv_size),
            Variant::ForwardJoin => (
                &self.msgs_forward_join_recv,
                &self.msgs_forward_join_recv_size,
            ),
            Variant::Shuffle => (&self.msgs_shuffle_recv, &self.msgs_shuffle_recv_size),
            Variant::ShuffleReply => (
                &self.msgs_shuffle_reply_recv,
                &self.msgs_shuffle_reply_recv_size,
            ),
            Variant::Neighbor => (&self.msgs_neighbor_recv, &self.msgs_neighbor_recv_size),
            Variant::Disconnect => (&self.msgs_disconnect_recv, &self.msgs_disconnect_recv_size),
            Variant::Gossip => (&self.msgs_gossip_recv, &self.msgs_gossip_recv_size),
            Variant::IHave => (&self.msgs_ihave_recv, &self.msgs_ihave_recv_size),
            Variant::Graft => (&self.msgs_graft_recv, &self.msgs_graft_recv_size),
            Variant::Prune => (&self.msgs_prune_recv, &self.msgs_prune_recv_size),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::Metrics;
    use crate::proto::{
        hyparview,
        topic::{Message, Variant},
    };

    #[test]
    fn a_message_is_counted_by_kind_variant_and_bytes_in_both_directions() {
        let metrics = Metrics::default();
        let join: Message<u8> = Message::Swarm(hyparview::Message::Join(None));
        assert_eq!(join.variant(), Variant::Join);

        metrics.note_sent(&join, 10);
        metrics.note_sent(&join, 12);
        metrics.note_recv(&join, 7);

        assert_eq!(metrics.msgs_ctrl_sent.get(), 2);
        assert_eq!(metrics.msgs_ctrl_sent_size.get(), 22);
        assert_eq!(
            metrics.msgs_data_sent.get(),
            0,
            "a swarm message is control"
        );
        assert_eq!(metrics.msgs_join_sent.get(), 2);
        assert_eq!(metrics.msgs_join_sent_size.get(), 22);
        assert_eq!(metrics.msgs_ctrl_recv.get(), 1);
        assert_eq!(metrics.msgs_join_recv.get(), 1);
        assert_eq!(metrics.msgs_join_recv_size.get(), 7);
        assert_eq!(
            metrics.msgs_prune_sent.get(),
            0,
            "another variant stays at zero"
        );
    }
}
