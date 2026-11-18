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

## A message that finds the chosen send loop ended goes to the next connection

`net.rs` and `net/conns.rs`: a peer can have more than one connection (`ConnSet`). If the send loop
of the chosen connection has ended and another connection can take the sends, `send_on_live` takes
the dead one out of the set (`ConnSet::retire`), closes it, and sends the same message on the next
chosen one. Before, the message was dropped and the dead connection left only after the event. The
protocol sends a message once, so the drop lost it. Only the last connection still ends with the
`failed to send` path above, and the peer goes. A connection that the actor takes out because its
send loop ended closes with `SEND_LOOP_ENDED_CODE` (2) and the reason `send loop ended`, not code 0.

## The messages that a closed connection did not write go to the next connection

`net.rs` and `net/util.rs`: when a connection closes, its send loop stops, and upstream drops the
messages in its queue with the loop. Now `SendLoop` keeps the message that it writes at the front of
its backlog until the write ends, and `SendLoop::take_unsent` gives back the backlog and the queue.
The task of the connection returns them (`ConnLoopEnd`), and the actor sends them on the next
connection of the peer (`Actor::resend_unsent`). Only a peer that still holds the connection gets
them (`ConnSet::owns`, which also knows the connections that `ConnSet::retire` took out): a peer that
the protocol dropped, or that came back after the drop, must not get a `Disconnect` that was meant
for the old link. These messages can come after newer ones that went straight to the next
connection, and a message that a stopped write had partly sent comes twice. Plumtree takes both.
For HyParView the order changes the meaning: an old `Disconnect` after a newer `Neighbor` of the
same topic ends the new link. So the actor notes the connection that took the last HyParView frame
of each topic (`last_swarm` of the peer), and resends a HyParView frame only if that connection is
the closed one or no frame of the topic was noted. Of a topic that quit, only the `Disconnect` is
resent: a `Join` after the quit would put us back into the view of the peer for a topic that we left,
as `drop_queued_frames_of` prevents for the queue of a dial. A frame that was written to a QUIC
stream but not acknowledged is still lost with the connection.

A message that cannot be encoded within the maximum message size (`WriteError::TooLarge` or
`WriteError::Ser`) is a fault of that message, found before a byte of it is written. Upstream ended
the send loop on it, and so closed the connection. With the resend, that message would then go to
the next connection, which has the same limit and fails the same way. Now `SendLoop::write_backlog`
drops the message with a warn and writes the messages behind it on the same connection.

## A dial that connects after it was replaced or cancelled is not dropped

`net.rs`: upstream dropped the result of a dial that `replace_old_dial` or `cancel_dial` had taken
out. If that dial had connected, the drop closed the connection with code 0 and an empty reason. The
remote had accepted it and could choose it, so what the remote sent on it was lost. A cancel could
also drop the dial in its handshake, with the same close. And while no other dial was pending,
`Dialer::next_conn` did not join the old dial, so its connection stayed open and unread.

Now a cancel stops a dial only before its handshake starts (`connect_with_opts`). After that the
handshake runs to its end. `Dialer::next_conn` joins every dial and returns a connection of an old
dial as `DialEnd::Stale`. If we still know the peer, the actor admits that connection as one more
connection of the peer (`ConnSet` then chooses). If we dropped the peer, the actor closes it with
`DIAL_NOT_NEEDED_CODE` (1) and the reason `dial no longer needed`. A cancelled dial to a peer that
does not answer now runs until the handshake times out.

`forget_peer` cancels the dial of every peer that it drops, not only of a pending one. A peer whose
inbound connection came while our dial was pending is active and still has the dial. Before, that
dial connected after the drop as the current dial and made the peer again, with no topic that uses
it. Now it ends as a stale dial, and the actor closes it.

If upstream changes the `Dialer`, keep the rule that it never drops a connected dial.

## A new neighbor gets an `IHave` of each message in the cache

`proto/plumtree.rs`: upstream sends a message only to the neighbors that the node has when it first
holds the message. A neighbor that comes up later never gets it, so a peer that joins just after a
broadcast loses that broadcast. Now `on_neighbor_up` queues an `IHave` for each message in the cache
with a round (the `Swarm` scope) to the new neighbor, through the lazy push queue and its dispatch
timer, so a large cache splits into frames as before. The neighbor sends a `Graft` only for the ids it
does not have, and the node answers from the cache. A joiner thus gets up to 30 s of history (the
retention of the cache). A link that goes down and comes up again sends the list of ids again. If a
soak test shows a storm of these lists, bound the offer to 1 per peer per cache window.

`proto.rs`: `plumtree_smoke` counted every `Received` event after the 2 sections joined, which pinned
the old fault (no history to a late neighbor). With the user's approval it now counts by content: the
second message reaches all 5 other nodes, and the first one reaches the 3 nodes of the late section.

If upstream changes `NeighborUp` in plumtree, keep the offer of the cache.

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
`ActorLink`. The counters count only with the `net` feature: `Variant`, `Message::variant`,
`note_sent` and `note_recv` compile only with it, and without it the fields exist and stay at 0.

If upstream starts to count these, drop this change and keep their names.

## A peer whose `Join` waits for its answer is not disconnected

`proto/hyparview.rs`: the protocol sends `DisconnectPeer` after any message from a peer outside its
active view. A peer that we sent a `Join` to is outside the view until its `Neighbor` answer
comes, so a message from it before the answer (a shuffle reply) disconnected it. If that came
while we dialed the peer, the actor removed the peer and the `Join` in the queue of the dial. A
`Join` is sent once, so after `leave_peers` a `join_peers` could fail for good (CI saw it in
`leave_peers_is_not_undone_by_shuffles_of_a_third_peer`). On a live connection the disconnect
closed the connection that carries the answer. Now `pending_joins` holds each peer whose `Join`
waits: no `DisconnectPeer` goes to it until its `Neighbor`, a `Disconnect`, a closed connection, a
leave, or `JOIN_EXPIRY` (20 s, the bound of a neighbor request). Each `Join` has a generation in
its `Timer::JoinExpired`, so the timer of an older `Join` does not end a newer one. A message from
the peer, or a failed dial, sets `disconnect_at_expiry`: at the expiry such a peer, if it is still
outside the active view, is disconnected. A peer that did neither keeps its dial, which can take
longer than the expiry on a relay path.

A failed dial whose queue holds a `Join` sends the new `InEvent::DialFailed`, not
`PeerDisconnected` (`net.rs`, `handle_dial_failed`): the queue stays for a connection that the peer
opens to us, the `Join` keeps waiting in the protocol, and its expiry sends the `DisconnectPeer`
that drops the queue. Any other failed dial drops the queue at once.

`state.rs`: a topic that sends to a peer registers the peer in its connection map, as a topic that
received from it does, so that its `DisconnectPeer` reaches the network level also when the peer
never sent us anything. Upstream closed the connection on a `DisconnectPeer` of any topic that had
the peer (`list.remove(&topic) || list.is_empty()`), and dropped the queue of a topic that still
used the peer. Now only the last topic closes it; a topic before it sends `DisconnectPeerTopic`,
and the network level drops only the frames of that topic from the queue of a dial.

A leave (`InEvent::Leave`) of a peer whose `Join` waits and that is not in the active view sends
`DisconnectPeer`. The tombstone forgets the `Join` in the protocol, but the network level still held
it in the queue of the dial and delivered it if the dial connected, to a peer that we left. The
state level makes it a `DisconnectPeerTopic` while another topic uses the peer.

If upstream changes the disconnect in `handle_message`, keep the exception for a pending `Join`.

## Version 0.103.0 of the fork is not upstream 0.103.0

`Cargo.toml`, `metrics.rs`: the fork adds public fields to `Metrics` (the byte counters above). Its
`Metrics` had no `#[non_exhaustive]`, so a new field breaks a struct literal, and
`cargo semver-checks` asks for a new major version (`constructible_struct_adds_field`). The fork
marks `Metrics` `#[non_exhaustive]`, as upstream did in c5f44db7 (upstream 0.102.0 has it), and
moves to 0.103.0. Later counters then add no break. The fork's 0.103.0 is its own release: it is
not upstream 0.103.0, and the fork is pinned by git revision, so Cargo never mixes the two.

If the fork moves to an upstream release, take its version and keep `#[non_exhaustive]`.
