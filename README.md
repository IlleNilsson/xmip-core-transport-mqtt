# xmip-core-transport-mqtt

MQTT transport: one PUBLISH is one Stream, the topic beside it; a Location subscribes or publishes through a broker, or accepts clients directly. MQTT 3.1.1 at QoS 0 and 1. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

A Send Location publishes on a client connected once per broker and kept (`transport::Pool`), CONNECT asking for no keep-alive; a client the broker closed is replaced and the publish goes again. The login is the transport capability's `Login`, beside the client identifier in `Credentials`. Until 2026-09-27 every publish connected and disconnected.

A Receive Location subscribes once: its first receive connects and subscribes, and the subscription stays attached between receives, so what the broker delivers meanwhile waits in the socket for the next; each receive takes what came until the broker is quiet for the timeout (`transport::pool::delivered`). A subscription the broker closed is replaced. Until 2026-09-28 every receive connected and subscribed anew.

A send target is read by `net::Target` in [xmip-core-library-net](https://github.com/IlleNilsson/xmip-core-library-net), the one reading of a URI every technology calls: scheme, authority, path and decoded query. Until 2026-09-28 it was read through the transport capability's `socket::target`, which split it on its first slash and left the query in the path.

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
