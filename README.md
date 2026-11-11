# xmip-core-transport-mqtt

MQTT transport: one PUBLISH is one Stream, the topic beside it; a Location subscribes or publishes through a broker, or accepts clients directly. MQTT 3.1.1 at QoS 0 and 1. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A packet's remaining length may say up to 256 MiB; what a peer says is held to the 64 MiB Xmip reads in one body (`net::MAX_BODY`, through `net::ceiling::within`) before any of it is allocated.

A Send Location publishes on a client connected once per broker and kept (`transport::Pool`), CONNECT asking for no keep-alive; a client the broker closed is replaced and the publish goes again. The login is the transport capability's `Login`, beside the client identifier in `Presented`. Until 2026-09-27 every publish connected and disconnected.

A Location that logs in names its user in the `username` setting and its password with `secret = "<name>"`: the Location's secret, presented through `transport::secured::logged_in` and read each time a client connects — a send's publisher, a receive's subscription, and each one that replaces a client the broker closed — never for each publish on a kept one. MQTT 3.1.1 sends no password without a user name (section 3.1.2.9), so a Location that names a secret and no `username` is refused as the node starts. On the accepting side, `Session::login` is the login a client's CONNECT carried, password included, for what authenticates it; the session itself checks nothing. A CONNECT with a password and no user name, or a password that is not UTF-8 text, is refused.

A Receive Location subscribes once: its first receive connects and subscribes, and the subscription stays attached between receives, so what the broker delivers meanwhile waits in the socket for the next; each receive takes what came until the broker is quiet for the timeout (`transport::pool::delivered`). A subscription the broker closed is replaced. Until 2026-09-28 every receive connected and subscribed anew.

A `QoS` 1 delivery is acknowledged after the runtime's whole receive cycle, not as it arrives. Accepted: PUBACK on the subscription that received it, one packet written, nothing waited for. Refused: PUBACK too — MQTT 3.1.1 has no rejection, so a refused delivery is consumed and not sent again; the runtime audited the refusal and keeps the Stream. Failed: no PUBACK — MQTT 3.1.1 has no negative acknowledgement — and the subscription's connection is let go without DISCONNECT, because a broker sends an unacknowledged delivery again, with DUP, only when the client reconnects to its kept session; the next receive does. So the receive subscription connects with clean session off where it has a client id (`Client::connect_keeping_session`); an empty client id can only have a clean session, and a failed delivery is then lost with it.

A Receive Location's subscription and a Send Location's publisher never present the same client identifier: a broker lets go of a connection when another presents its identifier, and finds a kept session by it. Where the `client_id` setting is left out, the identifier is derived from the Location's configuration (`client_id::derived`): `xmip`, then `r` for a Receive Location or `s` for a Send Location, then the first nine bytes of the SHA-256 of the broker address and the topic as configured, joined by a line feed, in lower-case hex — 23 characters of `[0-9a-z]`, the most MQTT 3.1.1 obliges a broker to accept (section 3.1.3.1). The same configuration presents the same identifier on every start, so a receive finds its kept session again; nothing is read from a Location's name. A Location that sets `client_id` presents that, on the side it is configured for. Until 2026-10-02 every Location presented `xmip`. **A `QoS` 0 delivery is at-most-once**: the protocol has no acknowledgement for it, and the broker keeps nothing to send again, so a crash before it is durable loses it. `Session` reports a PUBACK as `Event::PubAcked` and delivers at `QoS` 1 with `Session::deliver_at_least_once`. Until 2026-10-02 a delivery was PUBACKed as it was read.

A send target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net), the one reading of a URI every technology calls: scheme, authority, path and decoded query. Until 2026-09-28 it was read through the transport capability's `socket::target`, which split it on its first slash and left the query in the path.

A Send Location publishes to the topic its `topic` setting names, on the broker its address names: it sends to the empty target (`Configured::own_target`, `transport::destination::OWN`), which `transport::destination::on_server` reads as that topic. A target naming another still wins — `plant/5`, or `mqtt://host:1883/plant/5` (ADR-0064, amendment 2026-11-10).

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
