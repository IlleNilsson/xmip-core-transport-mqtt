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
//! What is here is MQTT 3.1.1 at `QoS` 0 and 1. `QoS` 2 is a Journey's
//! exactly-once wearing a packet's clothes (ADR-0013 4c) and is not spoken.
//! TLS is `xmip-core-library-tls`'s, per ADR-0033, and joins here when it
//! reaches the socket.
//!
//! The origin URI carries what the packet knew:
//! `mqtt://broker/topic?qos=1`.

pub mod client;
pub mod packet;
pub mod session;

use std::net::TcpListener;
use std::time::Duration;

pub use client::{Client, Credentials};
use net::Target;
pub use packet::{Packet, Publish};
pub use session::{Event, Session};
use transport::Configured;
use transport::error::{Result, protocol_error};
use transport::listening::{Accepting, Listening};
use transport::loopback::{FarEnd, LOOPBACK_TIMEOUT, Loopback};
use transport::pool::delivered;
use transport::socket;
use transport::{Arrived, Directions, Login, Pool, Transport};
use xcore::settings::{Applies, Kind, Presence, Read, Setting, Settings};

#[derive(Clone)]
pub struct MqttTransport {
    broker: String,
    topic: String,
    qos: u8,
    credentials: Credentials,
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
            credentials: Credentials {
                client_id: "xmip".to_string(),
                login: None,
            },
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

    /// Present these when connecting.
    #[must_use]
    pub fn presenting(mut self, credentials: Credentials) -> Self {
        self.credentials = credentials;
        self
    }

    /// Give up on a peer that stops mid-packet, and stop receiving when the
    /// broker has been quiet this long.
    #[must_use]
    pub const fn timing_out_after(mut self, timeout: Duration) -> Self {
        self.timeout = Some(timeout);
        self
    }

    /// Connect to the broker as a client.
    ///
    /// # Errors
    /// Where the broker refused or could not be reached.
    pub fn connect(&self) -> Result<Client> {
        Client::connect(&self.broker, &self.credentials, self.timeout)
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
        match Target::under(&["mqtt"], target).map(|named| (named.authority(), named.path())) {
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
                meaning: "The client identifier presented when connecting.",
                applies: Applies::Both,
            },
            Setting {
                name: "username",
                kind: Kind::Text,
                presence: Presence::Optional,
                meaning: "The user name presented when connecting, its password the credentials'.",
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

    /// The password is not a setting: it comes through the Location's
    /// `credentials`, never its table.
    fn configured(address: &str, settings: &Read) -> Result<Self> {
        let mut transport = Self::new(address, settings.text("topic"));
        if let Some(qos) = settings.optional_integer("qos") {
            // The declaration holds it at 0 or 1.
            transport = transport.at_qos(u8::try_from(qos).unwrap_or(1));
        }
        let mut credentials = transport.credentials.clone();
        if let Some(client_id) = settings.optional_text("client_id") {
            credentials.client_id = client_id.to_string();
        }
        credentials.login = settings
            .optional_text("username")
            .map(|user| Login::new(user, ""));
        transport = transport.presenting(credentials);
        Ok(match settings.optional_duration("timeout") {
            Some(timeout) => transport.timing_out_after(timeout),
            None => transport,
        })
    }
}

impl Transport for MqttTransport {
    fn name(&self) -> &'static str {
        "mqtt"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    /// Take what the broker delivers until it is quiet for the timeout, or
    /// disconnects, on the subscription the first receive made and kept:
    /// what the broker delivered between two receives waits in the socket.
    fn receive(&self) -> Result<Vec<Arrived>> {
        self.subscriptions.exchange(
            self.broker.as_str(),
            || {
                let mut client = self.connect()?;
                client.subscribe(&self.topic, self.qos)?;
                Ok(client)
            },
            |client| delivered(client, Client::next_publish),
        )
    }

    /// Publish on the client kept for the broker, connected on the first
    /// send to it.
    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let (broker, topic) = self.resolve(target);
        self.clients.exchange(
            broker,
            || Client::connect(broker, &self.credentials, self.timeout),
            |client| client.publish(topic, bytes, self.qos),
        )
    }
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
    fn take_one(self, listener: &TcpListener) -> Result<Arrived> {
        let mut session = self.accept_one(listener)?;
        session
            .next_publish()?
            .ok_or_else(|| protocol_error("the client disconnected without publishing"))
    }
}

impl Loopback for MqttTransport {
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
    use transport::payload::{edge_payloads, sized_payloads};

    #[test]
    fn mqtt_declares_its_settings_and_reads_through_them() {
        use xcore::settings::Given;
        assert!(MqttTransport::SETTINGS.problems().is_empty());
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
        assert_eq!(built.credentials.client_id, "line-4");
        assert_eq!(built.timeout, Some(secs(5)));
        let Err(refused) = MqttTransport::open("broker:1883", Applies::Send, &given[1..]) else {
            panic!("topic is required");
        };
        assert!(refused.message.contains("\"topic\""), "{refused}");
    }

    #[test]
    fn a_client_publishes_to_a_session_and_is_acknowledged() {
        let far_end = MqttTransport::new("127.0.0.1:0", "probe").timing_out_after(secs(2));
        let (listener, address) = far_end.bind().expect("binding");
        let sender = std::thread::spawn(move || {
            let near = MqttTransport::new(address.clone(), "probe").timing_out_after(secs(2));
            near.send("sensor/1", b"21.5")?;
            near.send(&format!("mqtt://{address}/sensor/2"), b"")
        });
        let mut session = far_end.accept_one(&listener).expect("accepting");
        assert_eq!(session.client_id(), "xmip");
        let first = session.next_publish().expect("first").expect("one");
        assert_eq!(first.bytes, b"21.5");
        assert!(first.origin_uri.ends_with("/sensor/1?qos=1"));
        // The same broker, so the same client: connected once.
        let second = session.next_publish().expect("second").expect("one");
        assert!(second.origin_uri.ends_with("/sensor/2?qos=1"));
        assert!(second.bytes.is_empty());
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
            let began = std::time::Instant::now();
            for n in 0..SENDS {
                sending.send("sensor/1", n.to_string().as_bytes())?;
            }
            let took = began.elapsed();
            // Generous for a debug build under load: a millisecond a publish.
            assert!(took < Duration::from_millis(SENDS as u64), "{took:?}");
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
        assert_eq!(arrived[0].bytes, b"first");
        assert!(arrived[1].origin_uri.ends_with("/sensor/2?qos=0"));
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
                arrived.extend(now.into_iter().map(|one| one.bytes));
            }
            Ok::<_, transport::TransportError>(arrived)
        });
        let subscribed = |session: &mut Session| {
            let event = session.next_event().expect("subscribed");
            assert!(matches!(event, Some(Event::Subscribed(_))), "{event:?}");
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

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }
}
