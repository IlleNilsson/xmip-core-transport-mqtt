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

use transport::error::{Result, classify, protocol_error};
use transport::socket;
use transport::{Login, Taken};

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
    login: Option<Login>,
    subscribed: Vec<String>,
}

impl Session {
    /// Accept one client on `listener` and complete its CONNECT.
    ///
    /// # Errors
    /// Where the connection could not be accepted, the client did not
    /// open with CONNECT, or its password is not text.
    pub fn accept(listener: &TcpListener, timeout: Option<Duration>) -> Result<Self> {
        let (stream, peer) = socket::accept_tcp(listener, timeout)?;
        let mut session = Self {
            stream,
            peer,
            client_id: String::new(),
            login: None,
            subscribed: Vec::new(),
        };
        match read(&mut session.stream)? {
            Some(Packet::Connect {
                client_id,
                username,
                password,
                ..
            }) => {
                session.client_id = client_id;
                session.login = carried(username, password)?;
            }
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

    /// The login the client's CONNECT carried: its user name, and its
    /// password, empty where it sent none. `None` where it named no user.
    /// Nothing here checks it.
    #[must_use]
    pub const fn login(&self) -> Option<&Login> {
        self.login.as_ref()
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

/// The login a CONNECT's `username` and `password` make. A password is
/// text to Xmip, as the secret it is held to is; one that is not UTF-8
/// is refused rather than changed. A password without a user name never
/// gets here: [`read`] refuses that CONNECT.
fn carried(username: Option<String>, password: Option<Vec<u8>>) -> Result<Option<Login>> {
    let Some(user) = username else {
        return Ok(None);
    };
    let password = String::from_utf8(password.unwrap_or_default())
        .map_err(|_| protocol_error("a CONNECT whose password is not UTF-8 text"))?;
    Ok(Some(Login::new(user, password)))
}
