#![forbid(unsafe_code)]

//! Streams that arrive as MQTT application messages. One PUBLISH is one
//! Stream, the topic kept beside it.
//!
//! MQTT is the sensor's protocol: a broker in the middle, clients that publish
//! and subscribe by topic, over TCP on port 1883. A Receive Location connects
//! to the broker, subscribes to a filter and takes what the broker delivers;
//! a Send Location connects and publishes. Either may instead accept clients
//! directly through [`Session`], which is one client's worth of broker — the
//! shape a device that publishes straight to Xmip needs, and no more.
//!
//! A `QoS` 1 delivery is acknowledged after the runtime's receive cycle:
//! PUBACK when it accepted it, and when it refused it too — MQTT 3.1.1 has
//! no rejection, so a refused delivery is consumed and not sent again;
//! when the cycle failed, no PUBACK, and the subscription's connection is
//! let go, so the broker sends it again, with DUP, when the next receive
//! reconnects to the session the client id keeps. A `QoS` 0 delivery is
//! at-most-once: the protocol has no acknowledgement for it, and the
//! broker keeps nothing to send again.
//!
//! A Receive Location's subscription and a Send Location's publisher never
//! present the same client identifier: each is derived from the Location's
//! configuration ([`client_id`]), or is the `client_id` it sets.
//!
//! What is here is MQTT 3.1.1 at `QoS` 0 and 1. `QoS` 2 is a Journey's
//! exactly-once wearing a packet's clothes (ADR-0013 4c) and is not spoken.
//! TLS is `xmip-core-library-tls`'s, per ADR-0033, and joins here when it
//! reaches the socket.
//!
//! The origin URI carries what the packet knew:
//! `mqtt://broker/topic?qos=1`.

pub mod client;
pub mod client_id;
pub mod packet;
pub mod session;

use std::net::TcpListener;
use std::time::Duration;

pub use client::{Client, Presented};
use client_id::Side;
use net::Target;
pub use packet::{Packet, Publish};
pub use session::{Event, Session};
use transport::ArrivalIdentity;
use transport::Configured;
use transport::error::{Result, protocol_error};
use transport::listening::{Accepting, Listening};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::pool::delivered;
use transport::socket;
use transport::{Acknowledgement, Arrived, Directions, Login, Pool, Taken, Transport, Verdict};
use xcore::settings::{Applies, Kind, Presence, Read, Setting, Settings};

#[derive(Clone)]
pub struct MqttTransport {
    broker: String,
    topic: String,
    qos: u8,
    /// The client identifier the configuration names, where it names one;
    /// else each side's is derived ([`client_id::derived`]).
    client_id: Option<String>,
    login: Option<Login>,
    /// The secret its Location names, whose password the login presents,
    /// read at each connection (ADR-0019, amendment 2026-10-29).
    secret: Option<transport::Secured>,
    timeout: Option<Duration>,
    /// The clients a send publishes on, connected once per broker and kept.
    clients: Pool<Client>,
    /// The client a receive takes from, connected and subscribed on the
    /// first receive and kept subscribed.
    subscriptions: Pool<Client>,
}

impl MqttTransport {
    /// Speak to the broker at `broker` about `topic`.
    #[must_use]
    pub fn new(broker: impl Into<String>, topic: impl Into<String>) -> Self {
        Self {
            broker: broker.into(),
            topic: topic.into(),
            qos: 1,
            client_id: None,
            login: None,
            secret: None,
            timeout: None,
            clients: Pool::new(),
            subscriptions: Pool::new(),
        }
    }

    /// Publish and subscribe at `qos`, 0 or 1.
    #[must_use]
    const fn at_qos(mut self, qos: u8) -> Self {
        self.qos = if qos > 1 { 1 } else { qos };
        self
    }

    /// What a client on `side` presents when connecting: the configured
    /// client identifier, or the one derived for that side, and the login,
    /// its password the Location's secret read now.
    ///
    /// # Errors
    /// Where the Location's secret is not set, is no password, or could
    /// not be read.
    fn presented(&self, side: Side) -> Result<Presented> {
        Ok(Presented {
            client_id: self
                .client_id
                .clone()
                .unwrap_or_else(|| client_id::derived(side, &self.broker, &self.topic)),
            login: self
                .login
                .as_ref()
                .map(|login| transport::secured::logged_in(login, self.secret.as_ref(), "mqtt"))
                .transpose()?,
        })
    }

    /// Give up on a peer that stops mid-packet, and stop receiving when the
    /// broker has been quiet this long.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Connect to the broker as a Send Location's client.
    ///
    /// # Errors
    /// Where the Location's secret could not be presented, or the broker
    /// refused or could not be reached.
    pub fn connect(&self) -> Result<Client> {
        Client::connect(&self.broker, &self.presented(Side::Send)?, self.timeout)
    }

    /// Bind as the far end clients connect to, and report the address.
    ///
    /// # Errors
    /// Where the address is taken, malformed, or not permitted.
    pub fn bind(&self) -> Result<(TcpListener, String)> {
        socket::bind_tcp(&self.broker)
    }

    /// Accept one client on an already-bound listener.
    ///
    /// # Errors
    /// Where the connection could not be accepted or the handshake failed.
    pub fn accept_one(&self, listener: &TcpListener) -> Result<Session> {
        Session::accept(listener, self.timeout)
    }

    /// Where a target names the broker and topic itself —
    /// `mqtt://host:1883/a/b` — or is a topic alone on this transport's broker.
    fn resolve<'a>(&'a self, target: &'a str) -> (&'a str, &'a str) {
        match Target::under(Self::SCHEMES, target).map(|named| (named.authority(), named.path())) {
            Some((peer, "")) => (peer, &self.topic),
            Some(pair) => pair,
            None => (&self.broker, target),
        }
    }
}

impl Configured for MqttTransport {
    /// The address is the broker, `host:1883`, both sides connect to.
    const SETTINGS: &'static Settings = &Settings {
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
            Setting {
                name: "timeout",
                kind: Kind::Duration,
                presence: Presence::Optional,
                meaning: "How long a peer that stops mid-packet is waited on, and a quiet broker.",
                applies: Applies::Both,
            },
        ],
    };

    /// The schemes a Location's address is entered with to choose this technology.
    const SCHEMES: &'static [&'static str] = &["mqtt"];

    /// The password is not a setting: it is the secret the Location names,
    /// never its table; the login is built without it.
    fn configured(address: &str, settings: &Read) -> Result<Self> {
        let mut transport = Self::new(address, settings.text("topic"));
        if let Some(qos) = settings.optional_integer("qos") {
            // The declaration holds it at 0 or 1.
            transport = transport.at_qos(u8::try_from(qos).unwrap_or(1));
        }
        transport.client_id = settings.optional_text("client_id").map(str::to_string);
        transport.login = settings
            .optional_text("username")
            .map(|user| Login::new(user, ""));
        Ok(match settings.optional_duration("timeout") {
            Some(timeout) => transport.timing_out_after(timeout),
            None => transport,
        })
    }

    /// The password the Location's secret holds, beside its `username`,
    /// read at each connection. MQTT 3.1.1 sends no password without a
    /// user name (section 3.1.2.9), so a secret named without one is refused.
    fn secured_by(mut self, secret: transport::Secured) -> Result<Self> {
        if self.login.is_none() {
            return Err(transport::TransportError::permanent(format!(
                "the secret '{}' is presented as the password of the user the \"username\" \
                 setting names, and none is named: MQTT 3.1.1 sends no password without a \
                 user name",
                secret.name()
            )));
        }
        self.secret = Some(secret);
        Ok(self)
    }
}

impl Transport for MqttTransport {
    fn name(&self) -> &'static str {
        "mqtt"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    fn arrivals(&self) -> transport::Arrivals {
        transport::Arrivals::Ordered(
            "the acknowledgement goes on the session the receive reads from",
        )
    }

    fn waiting(&self) -> transport::Waiting {
        transport::Waiting::Waits("the broker is drained until quiet for the timeout")
    }

    /// Take what the broker delivers until it is quiet for the timeout, or
    /// disconnects, on the subscription the first receive made, on the
    /// session its client id keeps, and kept: what the broker delivered
    /// between two receives waits in the socket. Nothing is acknowledged
    /// here: a `QoS` 1 delivery's [`answering`] PUBACKs it after the cycle
    /// accepted or refused it, and lets the subscription go when the cycle
    /// failed, for the broker to send it again on the next connection. **A `QoS` 0
    /// delivery is at-most-once**: the protocol does not acknowledge it.
    fn receive_up_to(&self, most: usize) -> Result<Vec<Arrived>> {
        let publishes = self.subscriptions.exchange(
            self.broker.as_str(),
            || {
                let mut client = Client::connect_keeping_session(
                    &self.broker,
                    &self.presented(Side::Receive)?,
                    self.timeout,
                )?;
                client.subscribe(&self.topic, self.qos)?;
                Ok(client)
            },
            |client| delivered(client, most, Client::next_publish),
        )?;
        Ok(publishes
            .into_iter()
            .map(|publish| {
                let origin = format!(
                    "mqtt://{}/{}?qos={}",
                    self.broker, publish.topic, publish.qos
                );
                let acknowledgement = match publish.id {
                    Some(id) => answering(&self.subscriptions, &self.broker, id),
                    None => Acknowledgement::at_most_once(
                        "MQTT QoS 0 is never acknowledged: the broker sends it once and \
                         keeps nothing to send again",
                    ),
                };
                Arrived::whole(origin, publish.payload, acknowledgement).detected()
            })
            .collect())
    }

    fn sending(&self) -> transport::Sending {
        transport::Sending::Gathered(
            "one PUBLISH packet carries the whole body, its remaining length counted first",
        )
    }

    /// Publish on the client kept for the broker, connected on the first
    /// send to it.
    fn send_from(&self, target: &str, outgoing: transport::Outgoing<'_>) -> Result<()> {
        let bytes: &[u8] = &outgoing.gather()?;
        let (broker, topic) = self.resolve(target);
        self.clients.exchange(
            broker,
            || Client::connect(broker, &self.presented(Side::Send)?, self.timeout),
            |client| client.publish(topic, bytes, self.qos),
        )
    }
}

/// The acknowledgement of the `QoS` 1 delivery under packet identifier
/// `id` on the subscription `subscriptions` keeps for `broker`: PUBACK on
/// [`Verdict::Accepted`], one packet written and nothing waited for. PUBACK
/// on [`Verdict::Refused`] too: MQTT 3.1.1 has no rejection (section 3.4),
/// so a refused delivery is consumed rather than sent again — the runtime
/// audited the refusal, and keeps the Stream. On [`Verdict::Failed`]
/// nothing is sent and the subscription's connection is let go: a broker
/// sends an unacknowledged delivery again only when the client reconnects
/// to its kept session, which the next receive does. A packet identifier
/// names a delivery on one connection only, so the answer goes on the kept
/// subscription or not at all.
fn answering(subscriptions: &Pool<Client>, broker: &str, id: u16) -> Acknowledgement {
    let subscriptions = subscriptions.clone();
    let broker = broker.to_string();
    Acknowledgement::deferred(move |verdict| {
        subscriptions.kept(
            broker.as_str(),
            "the subscription that received the delivery is closed; \
             the broker sends it again on the kept session",
            |client| match verdict {
                Verdict::Accepted | Verdict::Refused(_) => client.puback(id),
                Verdict::Failed => {
                    client.let_go();
                    Ok(())
                }
            },
        )
    })
}

impl MqttTransport {
    /// Both ends on this machine: an ephemeral local port, the loopback
    /// timeout, one topic called `probe`.
    #[must_use]
    pub fn loopback() -> Self {
        Self::new("127.0.0.1:0", "probe").timing_out_after(LOOPBACK_TIMEOUT)
    }
}

impl Accepting for MqttTransport {
    fn take_one(self, listener: &TcpListener) -> Result<Taken> {
        let mut session = self.accept_one(listener)?;
        session
            .next_publish()?
            .ok_or_else(|| protocol_error("the client disconnected without publishing"))
    }
}

impl Loopback for MqttTransport {
    fn arrival_identity(&self) -> ArrivalIdentity {
        ArrivalIdentity::Unnamed(
            "the broker delivers it and names no sender; the peer is the broker",
        )
    }

    fn far_end(&self) -> Result<Box<dyn FarEnd>> {
        Ok(Box::new(Listening::new(self.clone(), self.bind()?)))
    }

    /// A fresh client to `address`, publishing at this transport's `QoS` on
    /// its topic, and the publish acknowledged before it returns.
    fn send_to(&self, address: &str, payload: &[u8]) -> Result<()> {
        Self {
            broker: address.to_string(),
            ..self.clone()
        }
        .send(&format!("mqtt://{address}/{}", self.topic), payload)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::assert_matches;
    use transport::payload::{edge_payloads, sized_payloads};

    #[test]
    fn mqtt_declares_its_settings_and_reads_through_them() {
        use xcore::settings::Given;
        assert_eq!(MqttTransport::SETTINGS.problems(), [] as [String; 0]);
        let given = [
            (
                "topic".to_string(),
                Given::Text("plant/+/temperature".to_string()),
            ),
            ("qos".to_string(), Given::Integer(0)),
            ("client_id".to_string(), Given::Text("line-4".to_string())),
            ("timeout".to_string(), Given::Text("5s".to_string())),
        ];
        let built = MqttTransport::open("broker:1883", Applies::Receive, &given).expect("built");
        assert_eq!(built.broker, "broker:1883");
        assert_eq!(built.topic, "plant/+/temperature");
        assert_eq!(built.qos, 0);
        assert_eq!(
            built.presented(Side::Receive).expect("presented").client_id,
            "line-4"
        );
        assert_eq!(built.timeout, Some(secs(5)));
        let Err(refused) = MqttTransport::open("broker:1883", Applies::Send, &given[1..]) else {
            panic!("topic is required");
        };
        assert!(refused.message.contains("\"topic\""), "{refused}");
        let derived =
            MqttTransport::open("broker:1883", Applies::Send, &given[..1]).expect("built");
        assert_eq!(
            derived.presented(Side::Send).expect("presented").client_id,
            client_id::derived(Side::Send, "broker:1883", "plant/+/temperature")
        );
    }

    #[test]
    fn a_client_publishes_to_a_session_and_is_acknowledged() {
        let far_end = MqttTransport::new("127.0.0.1:0", "probe").timing_out_after(secs(2));
        let (listener, address) = far_end.bind().expect("binding");
        let publisher = client_id::derived(Side::Send, &address, "probe");
        let sender = std::thread::spawn(move || {
            let near = MqttTransport::new(address.clone(), "probe").timing_out_after(secs(2));
            near.send("sensor/1", b"21.5")?;
            near.send(&format!("mqtt://{address}/sensor/2"), b"")
        });
        let mut session = far_end.accept_one(&listener).expect("accepting");
        assert_eq!(session.client_id(), publisher);
        let first = session.next_publish().expect("first").expect("one");
        assert_eq!(first.bytes, b"21.5");
        assert!(first.origin_uri.ends_with("/sensor/1?qos=1"));
        // The same broker, so the same client: connected once.
        let second = session.next_publish().expect("second").expect("one");
        assert!(second.origin_uri.ends_with("/sensor/2?qos=1"));
        assert_eq!(second.bytes, [] as [u8; 0]);
        sender.join().expect("thread").expect("sending");
    }

    #[test]
    fn a_thousand_publishes_connect_once_and_a_client_the_broker_closed_is_replaced() {
        const SENDS: usize = 1000;
        let far_end = MqttTransport::new("127.0.0.1:0", "probe").timing_out_after(secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let near = MqttTransport::new(address, "probe").timing_out_after(secs(5));
        let sending = near.clone();
        let sender = std::thread::spawn(move || {
            for n in 0..SENDS {
                sending.send("sensor/1", n.to_string().as_bytes())?;
            }
            sending.send("sensor/1", b"after the close")
        });
        // One CONNECT for every publish: one session accepted.
        let mut session = far_end.accept_one(&listener).expect("accepting");
        for n in 0..SENDS {
            let arrived = session.next_publish().expect("publish").expect("one");
            assert_eq!(arrived.bytes, n.to_string().as_bytes());
        }
        drop(session);
        let mut again = far_end.accept_one(&listener).expect("a new client");
        let last = again.next_publish().expect("publish").expect("one");
        assert_eq!(last.bytes, b"after the close");
        sender.join().expect("thread").expect("sending");
        assert_eq!(near.clients.opened(), 2);
    }

    #[test]
    fn a_session_delivers_to_a_subscribed_client() {
        let far_end = MqttTransport::new("127.0.0.1:0", "probe").timing_out_after(secs(2));
        let (listener, address) = far_end.bind().expect("binding");
        let receiver = std::thread::spawn(move || {
            MqttTransport::new(address, "sensor/#")
                .at_qos(0)
                .timing_out_after(secs(2))
                .receive()
        });
        let mut session = far_end.accept_one(&listener).expect("accepting");
        assert_eq!(
            session.next_event().expect("subscribed"),
            Some(Event::Subscribed(vec!["sensor/#".to_string()]))
        );
        assert_eq!(session.subscribed(), ["sensor/#"]);
        session.deliver("sensor/1", b"first").expect("first");
        session.deliver("sensor/2", b"second").expect("second");
        drop(session);
        let arrived = receiver.join().expect("thread").expect("receiving");
        assert_eq!(arrived.len(), 2);
        assert!(!arrived[0].defers(), "QoS 0 is at-most-once");
        assert!(arrived[1].origin_uri.ends_with("/sensor/2?qos=0"));
        let first = arrived.into_iter().next().expect("first");
        assert_eq!(first.taken().expect("taken").bytes, b"first");
    }

    #[test]
    fn five_receives_subscribe_once_and_a_subscription_the_broker_closed_is_replaced() {
        let far_end = MqttTransport::new("127.0.0.1:0", "probe").timing_out_after(secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let near = MqttTransport::new(address, "sensor/#")
            .at_qos(0)
            .timing_out_after(Duration::from_millis(100));
        let receiving = near.clone();
        let (taken, told) = std::sync::mpsc::channel();
        let receiver = std::thread::spawn(move || {
            let mut arrived = Vec::new();
            while arrived.len() < 6 {
                let now = receiving.receive()?;
                if !now.is_empty() {
                    taken.send(()).expect("told");
                }
                for one in now {
                    arrived.push(one.taken()?.bytes);
                }
            }
            Ok::<_, transport::TransportError>(arrived)
        });
        let subscribed = |session: &mut Session| {
            let event = session.next_event().expect("subscribed");
            assert_matches!(event, Some(Event::Subscribed(_)), "{event:?}");
        };
        // One SUBSCRIBE for every receive: one session accepted.
        let mut session = far_end.accept_one(&listener).expect("accepting");
        subscribed(&mut session);
        for round in 0..5u8 {
            session.deliver("sensor/1", &[round]).expect("delivered");
            told.recv().expect("taken");
        }
        drop(session);
        let mut again = far_end.accept_one(&listener).expect("a new client");
        subscribed(&mut again);
        again.deliver("sensor/1", &[5]).expect("delivered");
        let arrived = receiver.join().expect("thread").expect("receiving");
        assert_eq!(arrived, (0..6u8).map(|n| vec![n]).collect::<Vec<_>>());
        assert_eq!(near.subscriptions.opened(), 2);
    }

    #[test]
    fn a_qos_1_delivery_is_pubacked_when_accepted_or_refused_and_sent_again_when_failed() {
        let far_end = MqttTransport::new("127.0.0.1:0", "probe").timing_out_after(secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let subscriber = client_id::derived(Side::Receive, &address, "sensor/#");
        let near =
            MqttTransport::new(address, "sensor/#").timing_out_after(Duration::from_millis(200));
        let receiving = near.clone();
        let receiver = std::thread::spawn(move || {
            let mut arrived = receiving.receive()?.into_iter();
            let one = arrived.next().expect("one");
            assert!(one.defers(), "QoS 1 waits for the cycle");
            let one = one.taken()?;
            arrived
                .next()
                .expect("two")
                .refused(transport::Refusal::Unacceptable)?;
            arrived.next().expect("three").failed()?;
            // The next receive reconnects to the kept session.
            let again = transport::arrived::next_arrival(receiving.receive()?, "sent again")?;
            Ok::<_, transport::TransportError>((one, again.taken()?))
        });
        let mut session = far_end.accept_one(&listener).expect("accepting");
        assert_eq!(session.client_id(), subscriber, "the receive side's own");
        assert_matches!(
            session.next_event().expect("subscribed"),
            Some(Event::Subscribed(_))
        );
        for (topic, body, id) in [
            ("sensor/1", &b"one"[..], 1),
            ("sensor/2", b"two", 2),
            ("sensor/3", b"three", 3),
        ] {
            session
                .deliver_at_least_once(topic, body, id)
                .expect("delivered");
        }
        // The accepted and the refused one PUBACKed, consumed; the failed
        // one not, and the connection let go.
        for id in [1, 2] {
            assert_eq!(
                session.next_event().expect("puback"),
                Some(Event::PubAcked(id))
            );
        }
        assert_eq!(session.next_event().expect("let go"), None);
        let mut again = far_end.accept_one(&listener).expect("reconnected");
        assert_eq!(again.client_id(), subscriber, "the same session again");
        assert_matches!(
            again.next_event().expect("subscribed"),
            Some(Event::Subscribed(_))
        );
        again
            .deliver_at_least_once("sensor/3", b"three", 3)
            .expect("again");
        assert_eq!(
            again.next_event().expect("puback"),
            Some(Event::PubAcked(3))
        );
        let (one, three) = receiver.join().expect("thread").expect("receiving");
        assert_eq!(
            (one.bytes.as_slice(), three.bytes.as_slice()),
            (&b"one"[..], &b"three"[..])
        );
        assert!(three.origin_uri.ends_with("/sensor/3?qos=1"));
        assert_eq!(near.subscriptions.opened(), 2);
    }

    #[test]
    fn a_receive_keeps_the_session_its_client_id_names() {
        let named = MqttTransport::new("127.0.0.1:0", "probe")
            .presented(Side::Receive)
            .expect("presented");
        for (presented, clean) in [(named, false), (Presented::default(), true)] {
            let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
            let address = listener.local_addr().expect("address").to_string();
            let connected = std::thread::spawn(move || {
                let (mut stream, _) = listener.accept().expect("accept");
                packet::read(&mut stream).expect("read").expect("CONNECT")
            });
            // The far end closes without CONNACK: only the CONNECT is read.
            let _ = Client::connect_keeping_session(&address, &presented, Some(secs(1)));
            let Packet::Connect { clean_session, .. } = connected.join().expect("thread") else {
                panic!("not a CONNECT");
            };
            assert_eq!(clean_session, clean, "{:?}", presented.client_id);
        }
    }

    #[test]
    fn a_broker_that_refuses_is_a_permanent_error() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind");
        let address = listener.local_addr().expect("address").to_string();
        std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("accept");
            let _ = packet::read(&mut stream);
            let refusal = packet::encode(&Packet::ConnAck {
                session_present: false,
                code: 5,
            })
            .expect("encode");
            std::io::Write::write_all(&mut stream, &refusal).expect("write");
        });
        let error = MqttTransport::new(address, "t")
            .timing_out_after(secs(2))
            .connect()
            .err()
            .expect("refused");
        assert!(!error.retryable);
        assert!(error.message.contains("code 5"));
        assert!(MqttTransport::new("127.0.0.1:0", "t").claims().is_none());
    }

    #[test]
    fn the_loopback_round_returns_the_payload_and_its_origin() {
        let loopback = MqttTransport::loopback();
        let arrived = loopback.round(b"published").expect("round");
        assert_eq!(arrived.bytes, b"published");
        assert!(arrived.origin_uri.starts_with("mqtt://127.0.0.1:"));
        assert!(arrived.origin_uri.ends_with("/probe?qos=1"));
        assert!(loopback.ceiling().is_none());
        assert!(loopback.refuses(b"anything").is_none());
    }

    #[test]
    fn the_loopback_returns_the_edge_payloads_whole() {
        let loopback = MqttTransport::loopback();
        for (name, payload) in [edge_payloads(), sized_payloads()].concat() {
            let arrived = loopback.round(&payload).expect(name);
            assert!(arrived.bytes == payload, "{name} came back changed");
        }
    }

    #[test]
    fn the_password_is_the_locations_secret_read_at_each_connection() {
        use transport::secured::Kept;
        use xcore::SecretKind;
        let kept = std::sync::Arc::new(Kept::default());
        let far_end = MqttTransport::new("127.0.0.1:0", "probe").timing_out_after(secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let near = located(&address, Applies::Send, Some("party"))
            .secured_by(kept.secured("partner-x.mqtt"))
            .expect("kept");
        let refused = near.send("sensor/1", b"none").expect_err("not set");
        assert!(
            refused.message.contains("'partner-x.mqtt' is not set"),
            "{refused}"
        );
        assert!(!refused.retryable, "{refused}");
        listener.set_nonblocking(true).expect("nonblocking");
        assert_eq!(
            listener.accept().err().map(|error| error.kind()),
            Some(std::io::ErrorKind::WouldBlock),
            "refused before connecting"
        );
        listener.set_nonblocking(false).expect("blocking");

        let first = Kept::drawn();
        kept.set("partner-x.mqtt", SecretKind::Password, &first);
        // Each session closed after the publishes it takes.
        let far_end = std::thread::spawn(move || {
            [2, 1].map(|publishes| {
                let mut session = far_end.accept_one(&listener).expect("accepting");
                let login = session.login().cloned();
                let taken = (0..publishes)
                    .map(|_| session.next_publish().expect("publish").expect("one").bytes)
                    .collect::<Vec<_>>();
                (login, taken)
            })
        });
        near.send("sensor/1", b"one").expect("set now");
        // A kept connection does not read it again for each Message.
        let key = "-----BEGIN OPENSSH PRIVATE KEY-----";
        kept.set("partner-x.mqtt", SecretKind::Key, key);
        near.send("sensor/1", b"two")
            .expect("on the kept connection");
        // The far end closed it: the next connection reads it again.
        let wrong = near.send("sensor/1", b"three").expect_err("a key");
        assert!(
            wrong
                .message
                .contains("is a key, and mqtt presents a password"),
            "{wrong}"
        );
        let second = Kept::drawn();
        assert_ne!(first, second);
        kept.set("partner-x.mqtt", SecretKind::Password, &second);
        near.send("sensor/1", b"four").expect("replaced");
        assert_eq!(
            far_end.join().expect("thread"),
            [
                (
                    Some(Login::new("party", first)),
                    vec![b"one".to_vec(), b"two".to_vec()]
                ),
                (Some(Login::new("party", second)), vec![b"four".to_vec()]),
            ]
        );
        assert_eq!(near.clients.opened(), 2);
    }

    #[test]
    fn a_receive_presents_the_secret_and_without_one_the_user_name_goes_alone() {
        use transport::secured::Kept;
        let kept = std::sync::Arc::new(Kept::default());
        let drawn = Kept::drawn();
        kept.set("partner-x.mqtt", xcore::SecretKind::Password, &drawn);
        let subscribed = Some(Event::Subscribed(vec!["probe".to_string()]));
        let receiving = presents(|address| {
            let near = located(address, Applies::Receive, Some("party"))
                .secured_by(kept.secured("partner-x.mqtt"))
                .expect("kept");
            let _ = near.receive();
        });
        assert_eq!(
            receiving,
            (Some(Login::new("party", drawn)), subscribed.clone())
        );
        let alone = presents(|address| {
            let _ = located(address, Applies::Send, Some("party")).connect();
        });
        assert_eq!(alone.0, Some(Login::new("party", "")));
        let nobody = presents(|address| {
            let _ = located(address, Applies::Receive, None).receive();
        });
        assert_eq!(nobody, (None, subscribed));
    }

    #[test]
    fn a_session_refuses_a_password_it_cannot_keep() {
        for (username, password) in [
            (None, Some(b"alone".to_vec())),
            (Some("party".to_string()), Some(vec![0xff, 0xfe])),
        ] {
            let far_end = MqttTransport::new("127.0.0.1:0", "probe").timing_out_after(secs(2));
            let (listener, address) = far_end.bind().expect("binding");
            let connecting = std::thread::spawn(move || {
                let mut stream = std::net::TcpStream::connect(address).expect("connect");
                let connect = packet::encode(&Packet::Connect {
                    client_id: "device".to_string(),
                    clean_session: true,
                    username,
                    password,
                    keep_alive: 0,
                })
                .expect("encode");
                std::io::Write::write_all(&mut stream, &connect).expect("write");
                stream
            });
            let Err(refused) = far_end.accept_one(&listener) else {
                panic!("accepted a password it cannot keep");
            };
            assert!(!refused.retryable, "{refused}");
            assert!(refused.message.contains("password"), "{refused}");
            drop(connecting.join().expect("thread"));
        }
    }

    #[test]
    fn a_secret_named_without_a_user_name_is_refused() {
        let kept = std::sync::Arc::new(transport::secured::Kept::default());
        let refused = located("broker:1883", Applies::Send, None)
            .secured_by(kept.secured("partner-x.mqtt"))
            .err()
            .expect("no user name");
        assert!(!refused.retryable, "{refused}");
        assert!(
            refused.message.contains("'partner-x.mqtt'")
                && refused.message.contains("\"username\""),
            "{refused}"
        );
    }

    /// A Location of `address` on `side`, its topic `probe`, logging in as
    /// `username` where one is given.
    fn located(address: &str, side: Applies, username: Option<&str>) -> MqttTransport {
        use xcore::settings::Given;
        let mut given = vec![
            ("topic".to_string(), Given::Text("probe".to_string())),
            ("timeout".to_string(), Given::Text("2s".to_string())),
        ];
        if let Some(user) = username {
            given.push(("username".to_string(), Given::Text(user.to_string())));
        }
        MqttTransport::open(address, side, &given).expect("built")
    }

    /// The login the client `connecting` makes to a far end at the address
    /// it is given presents, as the far end's session keeps it, and what
    /// the client did next; the session is closed after that.
    fn presents(connecting: impl FnOnce(&str)) -> (Option<Login>, Option<Event>) {
        let far_end = MqttTransport::new("127.0.0.1:0", "probe").timing_out_after(secs(5));
        let (listener, address) = far_end.bind().expect("binding");
        let serving = std::thread::spawn(move || {
            let mut session = far_end.accept_one(&listener).expect("accepting");
            let login = session.login().cloned();
            (login, session.next_event().ok().flatten())
        });
        connecting(&address);
        serving.join().expect("thread")
    }

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }
}
