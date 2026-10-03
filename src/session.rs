//! The broker's side of one connection: what a Receive Location that accepts
//! clients directly runs, and what a test puts at the far end.
//!
//! Not a broker. One session serves one client and keeps no topic tree; what
//! it takes is handed up as Streams and what it is given is delivered to its
//! one client. A Location that needs fan-out talks to a broker through
//! [`crate::Client`].

use std::io::Write;
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::time::Duration;

use transport::Taken;
use transport::error::{Result, classify, protocol_error};
use transport::socket;

use crate::packet::{Packet, Publish, encode, read};

/// What a client did, as [`Session::next_event`] reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Event {
    /// The client published; here is what it published.
    Published(Taken),
    /// The client subscribed to these filters, and was granted them.
    Subscribed(Vec<String>),
    /// The client acknowledged the `QoS` 1 delivery under this packet
    /// identifier.
    PubAcked(u16),
}

pub struct Session {
    stream: TcpStream,
    peer: SocketAddr,
    client_id: String,
    subscribed: Vec<String>,
}

impl Session {
    /// Accept one client on `listener` and complete its CONNECT.
    ///
    /// # Errors
    /// Where the connection could not be accepted or the client did not
    /// open with CONNECT.
    pub fn accept(listener: &TcpListener, timeout: Option<Duration>) -> Result<Self> {
        let (stream, peer) = socket::accept_tcp(listener, timeout)?;
        let mut session = Self {
            stream,
            peer,
            client_id: String::new(),
            subscribed: Vec::new(),
        };
        match read(&mut session.stream)? {
            Some(Packet::Connect { client_id, .. }) => session.client_id = client_id,
            _ => return Err(protocol_error("the client did not open with CONNECT")),
        }
        session.write(&Packet::ConnAck {
            session_present: false,
            code: 0,
        })?;
        Ok(session)
    }

    /// Who connected, as the client named itself.
    #[must_use]
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// The filters the client subscribed to so far.
    #[must_use]
    pub fn subscribed(&self) -> &[String] {
        &self.subscribed
    }

    /// The next message the client publishes, or `None` when it disconnected.
    /// Subscriptions are granted on the way and pings answered.
    ///
    /// # Errors
    /// Where the connection broke, or nothing arrived before the timeout.
    pub fn next_publish(&mut self) -> Result<Option<Taken>> {
        loop {
            match self.next_event()? {
                Some(Event::Published(taken)) => return Ok(Some(taken)),
                Some(Event::Subscribed(_) | Event::PubAcked(_)) => {}
                None => return Ok(None),
            }
        }
    }

    /// The next thing the client did, or `None` when it disconnected. Pings
    /// are answered on the way.
    ///
    /// # Errors
    /// Where the connection broke, nothing arrived before the timeout, or the
    /// client sent what only a broker sends.
    pub fn next_event(&mut self) -> Result<Option<Event>> {
        loop {
            match read(&mut self.stream)? {
                Some(Packet::Publish(publish)) => {
                    if let Some(id) = publish.id {
                        self.write(&Packet::PubAck(id))?;
                    }
                    let origin =
                        format!("mqtt://{}/{}?qos={}", self.peer, publish.topic, publish.qos);
                    return Ok(Some(Event::Published(Taken::new(origin, publish.payload))));
                }
                Some(Packet::Subscribe { id, filters }) => {
                    let codes = filters.iter().map(|(_, qos)| (*qos).min(1)).collect();
                    let filters: Vec<String> = filters.into_iter().map(|(f, _)| f).collect();
                    self.subscribed.extend(filters.iter().cloned());
                    self.write(&Packet::SubAck { id, codes })?;
                    return Ok(Some(Event::Subscribed(filters)));
                }
                Some(Packet::PingReq) => self.write(&Packet::PingResp)?,
                Some(Packet::Disconnect) | None => return Ok(None),
                Some(Packet::PubAck(id)) => return Ok(Some(Event::PubAcked(id))),
                Some(other) => {
                    return Err(protocol_error(format!("{other:?} from a client")));
                }
            }
        }
    }

    /// Deliver `payload` on `topic` to the client, at `QoS` 0.
    ///
    /// # Errors
    /// Where the client went away.
    pub fn deliver(&mut self, topic: &str, payload: &[u8]) -> Result<()> {
        self.deliver_at(topic, payload, None)
    }

    /// Deliver `payload` on `topic` to the client at `QoS` 1 under packet
    /// identifier `id`, for the client to PUBACK ([`Event::PubAcked`]).
    ///
    /// # Errors
    /// Where the client went away.
    pub fn deliver_at_least_once(&mut self, topic: &str, payload: &[u8], id: u16) -> Result<()> {
        self.deliver_at(topic, payload, Some(id))
    }

    fn deliver_at(&mut self, topic: &str, payload: &[u8], id: Option<u16>) -> Result<()> {
        self.write(&Packet::Publish(Publish {
            topic: topic.to_string(),
            qos: u8::from(id.is_some()),
            retain: false,
            id,
            payload: payload.to_vec(),
        }))
    }

    fn write(&mut self, packet: &Packet) -> Result<()> {
        self.stream
            .write_all(&encode(packet)?)
            .map_err(|e| classify("writing a packet", &e))?;
        self.stream
            .flush()
            .map_err(|e| classify("flushing a packet", &e))
    }
}
