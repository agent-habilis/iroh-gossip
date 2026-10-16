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

## A dial to a peer without an address is logged at debug

`net.rs`: a dial that fails with iroh's `ConnectWithOptsError::NoAddress` ("No addressing
information available") is logged at debug, not warn. The gossip learns a peer by id, often before
anyone told the endpoint where to reach it, and the actor dials again when the address is known.
Every other dial failure stays a warn.

If upstream changes the `dial failed` log, keep the split in `dial_had_no_address`.

## A dial that is older than five seconds is replaced when its caller asks again

`net.rs`: `Dialer` drops a pending dial and starts a new one when `Command::JoinPeers` names the
same peer and the dial is older than `DIAL_REPLACED_AFTER` (5 s). Upstream skips every later request
while a dial is in flight. A dial that began while nobody served the peer then misses the peer for
up to the 30 s idle timeout after the peer is served, because the handshake backs off its
retransmits. Nothing replaces a dial by itself, and a dial that nobody asks for again still runs to
its timeout and logs `dial failed`. The replaced dial ends without a log line above debug (a
`generation` tells it from its successor).

If upstream changes `Dialer`, keep `replace_old_dial` and the `generation` check in `next_conn`.

## A send on a connection that was closed in order is logged at debug

`net.rs`: when the send loop of the chosen connection has ended and the connection was closed
with an application code, by the peer or by us (`close_reason()` is `ApplicationClosed` or
`LocallyClosed`), the line is at debug, not warn. A peer that leaves does that, and the next send
finds the loop ended. A reset, a timeout, a transport error, an open connection and a missing
connection stay a warn.

If upstream changes the `failed to send` log, keep the split in `closed_in_order`.
