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
//! TLS is the transport capability's, per ADR-0033, and joins here when it
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
pub use packet::{Packet, Publish};
pub use session::{Event, Session};
use transport::error::Result;
use transport::socket;
use transport::{Arrived, Directions, Transport};

pub struct MqttTransport {
    broker: String,
    topic: String,
    qos: u8,
    credentials: Credentials,
    timeout: Option<Duration>,
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
                username: None,
                password: None,
            },
            timeout: None,
        }
    }

    /// Publish and subscribe at `qos`, 0 or 1.
    #[must_use]
    pub const fn at_qos(mut self, qos: u8) -> Self {
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
        match socket::target("mqtt", target) {
            Some((peer, "")) => (peer, &self.topic),
            Some(pair) => pair,
            None => (&self.broker, target),
        }
    }
}

impl Transport for MqttTransport {
    fn name(&self) -> &'static str {
        "mqtt"
    }

    fn directions(&self) -> Directions {
        Directions::BOTH
    }

    /// Subscribe and take what the broker delivers until it is quiet for the
    /// timeout, or disconnects.
    fn receive(&self) -> Result<Vec<Arrived>> {
        let mut client = self.connect()?;
        client.subscribe(&self.topic, self.qos)?;
        let mut arrived = Vec::new();
        loop {
            match client.next_publish() {
                Ok(Some(message)) => arrived.push(message),
                Ok(None) => break,
                Err(error) if error.retryable && !arrived.is_empty() => break,
                Err(error) => return Err(error),
            }
        }
        Ok(arrived)
    }

    fn send(&self, target: &str, bytes: &[u8]) -> Result<()> {
        let (broker, topic) = self.resolve(target);
        let mut client = Client::connect(broker, &self.credentials, self.timeout)?;
        client.publish(topic, bytes, self.qos)?;
        client.disconnect();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
        assert!(session.next_publish().expect("closed").is_none());
        let mut session = far_end.accept_one(&listener).expect("second");
        let second = session.next_publish().expect("second").expect("one");
        assert!(second.origin_uri.ends_with("/sensor/2?qos=1"));
        assert!(second.bytes.is_empty());
        sender.join().expect("thread").expect("sending");
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

    fn secs(n: u64) -> Duration {
        Duration::from_secs(n)
    }
}
