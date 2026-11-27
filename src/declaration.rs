//! This technology's declaration: the transport guide's "The declaration",
//! `module/core/capability/transport/doc/adding-a-transport.md`.

use xcore::settings::{Applies, Kind, Presence, Setting, Settings};

/// Every setting a Location takes beyond its address, the broker,
/// `host:1883`, both sides connect to. The password is the secret the
/// Location names, never a setting.
pub const SETTINGS: &Settings = &Settings {
    technology: env!("CARGO_PKG_NAME"),
    settings: &[
        Setting {
            name: "topic",
            kind: Kind::Text,
            presence: Presence::Required,
            meaning: "What a Receive Location subscribes to, or a Send Location publishes to.",
            applies: Applies::Both,
        },
        Setting {
            name: "qos",
            kind: Kind::Integer {
                minimum: 0,
                maximum: 1,
            },
            presence: Presence::Optional,
            meaning: "The quality of service published and subscribed at; 1 when left out.",
            applies: Applies::Both,
        },
        Setting {
            name: "client_id",
            kind: Kind::Text,
            presence: Presence::Optional,
            meaning: "The client identifier this Location presents; derived from the \
                      broker address, the topic and the side when left out.",
            applies: Applies::Both,
        },
        Setting {
            name: "username",
            kind: Kind::Text,
            presence: Presence::Optional,
            meaning: "The user name presented when connecting, its password the Location's \
                      secret.",
            applies: Applies::Both,
        },
        transport::configured::timeout(
            Applies::Both,
            None,
            "How long a peer that stops mid-packet is waited on, and a quiet broker.",
        ),
    ],
};

/// This technology's schemes, as [`transport::Configured::SCHEMES`] defines
/// them.
pub const SCHEMES: &[&str] = &["mqtt"];
