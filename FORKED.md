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

## A send on a connection that was closed in order is logged at debug

`net.rs`: when the send loop of the chosen connection has ended and the connection was closed
with an application code, by the peer or by us (`close_reason()` is `ApplicationClosed` or
`LocallyClosed`), the line is at debug, not warn. A peer that leaves does that, and the next send
finds the loop ended. A reset, a timeout, a transport error, an open connection and a missing
connection stay a warn.

If upstream changes the `failed to send` log, keep the split in `closed_in_order`.
