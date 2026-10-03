//! The client identifier a Location presents, derived from what it is
//! configured with where it names none.
//!
//! A broker knows a client by its identifier: two connections presenting
//! the same one take each other's place, the first let go (MQTT 3.1.1,
//! section 3.1.4), and a session kept for a Receive Location is found again
//! by it. So a Send Location's publisher and a Receive Location's
//! subscription never present the same identifier, and a Receive Location
//! presents the same one every time it starts.
//!
//! **The derivation.** `xmip`, then `r` for a Receive Location's
//! subscription or `s` for a Send Location's publisher, then the first nine
//! bytes of the SHA-256 of the broker address and the topic as configured,
//! joined by a line feed, in lower-case hex: 23 characters of `[0-9a-z]`,
//! the most a broker is obliged to accept (section 3.1.3.1). The same
//! configuration gives the same identifier on every node and across
//! restarts; another broker address or topic gives another. Nothing is
//! read from a Location's name. A Location that sets `client_id` presents
//! that instead, on the side it is configured for.

use sha2::{Digest, Sha256};

/// Which side of a Location a client serves.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Side {
    /// A Receive Location's subscription.
    Receive,
    /// A Send Location's publisher.
    Send,
}

/// How many bytes of the digest the identifier carries: nine, eighteen hex
/// digits, so that with `xmip` and the side it is 23 characters.
const DIGEST_BYTES: usize = 9;

/// The client identifier a Location on `side` presents at `broker` about
/// `topic` where its configuration names none.
#[must_use]
pub fn derived(side: Side, broker: &str, topic: &str) -> String {
    let mut hash = Sha256::new();
    hash.update(broker.as_bytes());
    hash.update(b"\n");
    hash.update(topic.as_bytes());
    let digest = hash.finalize();
    let side = match side {
        Side::Receive => 'r',
        Side::Send => 's',
    };
    format!("xmip{side}{}", codec::hex::encode(&digest[..DIGEST_BYTES]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_two_sides_never_share_an_identifier_and_each_fits_any_broker() {
        let receive = derived(Side::Receive, "broker:1883", "plant/+/temperature");
        let send = derived(Side::Send, "broker:1883", "plant/+/temperature");
        assert_ne!(receive, send);
        for id in [&receive, &send] {
            assert_eq!(id.len(), 23, "{id}");
            assert!(
                id.bytes()
                    .all(|b| b.is_ascii_digit() || b.is_ascii_lowercase()),
                "{id}"
            );
        }
        assert!(receive.starts_with("xmipr") && send.starts_with("xmips"));
    }

    #[test]
    fn the_same_configuration_gives_the_same_identifier_and_another_another() {
        let at = |broker, topic| derived(Side::Receive, broker, topic);
        assert_eq!(at("broker:1883", "C1"), at("broker:1883", "C1"));
        assert_ne!(at("broker:1883", "C1"), at("broker:1883", "C2"));
        assert_ne!(at("broker:1883", "C1"), at("broker:1884", "C1"));
    }
}
