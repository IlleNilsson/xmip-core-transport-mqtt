//! The module's timing program (`doc/governance/rust-style.md` section
//! 5c): this transport held to how long Xmip takes — the millisecond rule
//! (`CONTRIBUTING.md`, *Rust is here for one reason*). A plain `cargo test`
//! leaves it out; the landing runs it after every other test of the module,
//! alone, one test at a time
//! (`cargo test --test measured --all-features -- --test-threads=1`), so
//! what is measured is Xmip's work and not its neighbors':
//!
//! - a thousand publishes on one client take a millisecond each at most
//!   beyond their wakes, round by round beside a plain wake, the fastest of
//!   five (`xcore::latency::Rounds`).

use std::time::Duration;

use transport::Transport;
use xcore::latency::Rounds;
use xmip_core_transport_mqtt::MqttTransport;

/// What a publish at `QoS` 1 wakes where a plain wake wakes one: the far end
/// at the PUBLISH, and this end at its PUBACK.
const WAKES: u32 = 2;

fn secs(n: u64) -> Duration {
    Duration::from_secs(n)
}

#[test]
fn a_thousand_publishes_take_a_millisecond_each_at_most() {
    const SENDS: usize = 1000;
    let far_end = MqttTransport::new("127.0.0.1:0", "probe").timing_out_after(secs(5));
    let (listener, address) = far_end.bind().expect("binding");
    let near = MqttTransport::new(address, "probe").timing_out_after(secs(5));
    let sender = std::thread::spawn(move || {
        let mut rounds = Rounds::crossing(WAKES);
        for n in 0..SENDS {
            rounds.round(|| near.send("sensor/1", n.to_string().as_bytes()))?;
        }
        Ok::<_, transport::TransportError>(rounds)
    });
    let mut session = far_end.accept_one(&listener).expect("accepting");
    for n in 0..SENDS {
        let arrived = session.next_publish().expect("publish").expect("one");
        assert_eq!(arrived.bytes, n.to_string().as_bytes());
    }
    let rounds = sender.join().expect("thread").expect("sending");
    rounds.hold("a publish, acknowledged");
}
