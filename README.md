# xmip-core-transport-mqtt

MQTT transport: one PUBLISH is one Stream, the topic beside it; a Location subscribes or publishes through a broker, or accepts clients directly. MQTT 3.1.1 at QoS 0 and 1. A technology of [xmip-core-transport](https://github.com/IlleNilsson/xmip-core-transport).

## Toolchain

`rust-toolchain.toml` pins the toolchain for the whole estate. Do not change it
here.

## Verification

The included workflow is manual-only and calls the versioned shared workflow at
`IlleNilsson/.github@v1`.
