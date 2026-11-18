# Changes from upstream iroh-gossip

## `Disconnect::left` reuses the obsolete `_respond` field

`proto/hyparview.rs`: the second field of `Disconnect` was `_respond`, obsolete and always `false`.
It is now `left`. Postcard writes fields by position, so the bytes are the same. An old receiver
ignores it. Upstream releases before v0.90.0 sent `respond: true`, but they used a different
struct (one field), which a current peer cannot decode, so no decodable `Disconnect` ever had
`true` there.

If upstream changes this field, the merge conflicts here. Keep `left` and its meaning.

## A refused dial is logged at debug

`net.rs`: a dial that the peer answers with `CONNECTION_REFUSED` ("the server refused to accept
a new connection") is logged at debug, not warn. A closing endpoint gives that answer, so it is
not a fault, and the caller already retries or gives up. Every other dial failure stays a warn.

If upstream changes the `dial failed` log, keep the split in `dial_was_refused`.

## A dial to a peer without an address is logged at debug, then at warn once

`net.rs`: a dial that fails with iroh's `ConnectWithOptsError::NoAddress` ("No addressing
information available") is logged at debug, not warn. The gossip learns a peer by id from the
overlay (a forward-join), often before the app told the endpoint where to reach it. In a mesh
where the app brings the address (WebRTC only) the gap was 90 ms to 400 ms in the loopback tests.

The dial is not retried by a timer. It repeats with the next message that the gossip sends to the
peer (`queue_dial` on every send in `PeerState::Pending`), so the retry is bounded by the rate of
the hyparview messages to that peer. A peer that never gets an address is dialed once per message,
each at debug. So that a real missing address stays visible, the log says it once at warn: when the
dials of a peer have failed for want of an address for longer than `NO_ADDRESS_WARN_AFTER` (30 s).
A dial that works, or fails in another way, ends the spell. So does a gap of 60 s with no dial of the
peer. At most 1024 peers are kept (`MAX_NO_ADDRESS_SPELLS`), the oldest out first. Every other dial
failure stays a warn.

If upstream changes the `dial failed` log, keep the split in `dial_had_no_address` and the
`NoAddressWatch` that counts the spell.

## A dial that our own close ended is logged at debug

`net.rs`: a dial that fails with `ConnectionError::LocallyClosed` (the text is `closed`) is logged
at debug, not warn. Our side closed it: the close of our endpoint ends the dials in flight. A node
that shuts down or crashes does that, and the app chose it. If upstream changes the `dial failed`
log, keep the split in `dial_was_closed_locally`.

## A dial that starts while our own close runs is logged at debug

`net.rs`: `dial_was_closed_locally` also takes the dials that start after the close of our endpoint
has begun. noq answers `EndpointStopping` from the first instant of the close, which iroh wraps as
`ConnectWithOptsError::Noq` (the text is `Unable to connect to remote`, and its cause is
`endpoint stopping`), and iroh answers `ConnectWithOptsError::EndpointClosed` once the close is
done. Both are logged at debug. The other errors of noq stay at warn: they are faults of this node.
The warn line of a failed dial logs the whole chain of the error (`{err:#}`), because the text of
`ConnectWithOptsError::Noq` hides its cause.

## A send on a connection that ended without a fault is logged at debug

`net.rs`: when the send loop of the chosen connection has ended and the connection ended without a
fault, the line is at debug, not warn. That is a close with an application code, by the peer or by
us (`close_reason()` is `ApplicationClosed` or `LocallyClosed`), and the loss of the peer: `TimedOut`,
`Reset`, or a transport error with the code `NO_VIABLE_PATH`. A peer that leaves closes in order. A
peer that crashes goes silent, and its connection ends with one of the others. The next send finds
the loop ended, and the protocol reports the loss as `NeighborDown`.

The close of our own endpoint is the third case. While `Endpoint::close` runs, a connection ends with
the transport error `INTERNAL_ERROR` ("endpoint driver future was dropped"), and the actor can still
send: the quit and disconnect messages of a leaving node, and the answers to them. That code is no
fault only while our own endpoint is closing, which `Endpoint::closed` says from the start of the
close (`Endpoint::is_closed` comes only after the connections ended). On an open endpoint it stays a
warn. Any other transport error, a version mismatch, an open connection and a missing connection
stay a warn too, and the warn line logs the close reason.

If upstream changes the `failed to send` log, keep the split in `ended_without_a_fault`.

## The byte counters of the metrics are filled

`metrics.rs`, `net.rs`, `net/util.rs` and `proto/state.rs`: upstream counts `msgs_ctrl_*` and
`msgs_data_*` in `State::handle`, for each message that the protocol emits or takes in, with the
size of the message without its prefix. The fork removes those writes from `State::handle` (it
keeps `neighbor_up` and `neighbor_down`), so that each frame counts once. The send loop now
counts each message that it writes, and the receive loop each message that it reads, by kind
(control or data) and by variant (`join`, `forward_join`, `shuffle`, `shuffle_reply`, `neighbor`,
`disconnect`, `gossip`, `ihave`, `graft`, `prune`: `msgs_<variant>_sent`, `_sent_size`, `_recv`,
`_recv_size`). A size is the 4 bytes of the length prefix and the postcard encoding of the message.
No behaviour changes: `write_frame` returns the bytes it wrote, `read_frame_sized` returns the bytes
it read, and the counters are atomic adds. The connection loops hold the `Arc<Metrics>` through
`ActorLink`.

If upstream starts to count these, drop this change and keep their names.

## A disconnect during a dial delivers the queue of the dial first

`net.rs`: the protocol sends `DisconnectPeer` after any message from a peer outside its active
view. If that comes while we dial the peer, the actor used to remove the peer, and the messages
that waited for the dial were lost. A `Join` is sent once, so after `leave_peers` a `join_peers`
could fail for good when a shuffle reply from the peer came in during a slow dial (CI saw it in
`leave_peers_is_not_undone_by_shuffles_of_a_third_peer`). Now a peer that is still pending and has
a queue is marked `disconnect_after_flush`: the dial delivers the queue, and the connection stays
writable for `JOIN_ANSWER_WAIT` (`close_at`), because the peer answers a `Join` with a `Neighbor`
that our `Neighbor` must answer on that connection. A send to the peer ends the wait; at its end
the peer is dropped, so its connection drains and closes, as the connection of an active peer does
on `DisconnectPeer`. A new send to a pending peer clears the mark, a failed dial drops a marked
peer, and a quit takes the frames of its topic out of every queue.

If upstream changes `PeerState` or the `DisconnectPeer` arm, keep the queue of a pending peer.
