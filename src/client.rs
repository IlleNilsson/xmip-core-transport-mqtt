//! The client's side of one connection to a broker.

use std::io::Write;
use std::net::TcpStream;
use std::time::Duration;

use transport::error::{Result, classify, protocol_error};
use transport::pool::{Pooled, alive};
use transport::{Login, socket};

use crate::packet::{Packet, Publish, encode, read};

/// What a Location presents when it connects: its client identifier, and
/// the login where the broker asks for one. An empty password is sent as
/// none, which MQTT 3.1.1 allows beside a user name.
#[derive(Clone, Debug, Default)]
pub struct Credentials {
    pub client_id: String,
    pub login: Option<Login>,
}

/// One connected client: publishes, subscribes, takes what the broker sends.
/// Kept between publishes while the broker keeps it open: CONNECT says no
/// keep-alive, so an idle client is never timed out.
pub struct Client {
    stream: TcpStream,
    next_id: u16,
}

impl Client {
    /// Connect to `broker` on a clean session and complete the CONNECT
    /// handshake.
    ///
    /// # Errors
    /// Where the broker refused the connection or could not be reached.
    pub fn connect(
        broker: &str,
        credentials: &Credentials,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        Self::open(broker, credentials, timeout, true)
    }

    /// Connect to `broker` keeping the session the client id names, where
    /// it names one: the broker sends again, with DUP, every `QoS` 1
    /// delivery this client had not acknowledged when it reconnects. An
    /// empty client id can only have a clean session (MQTT 3.1.1, 3.1.3.1).
    ///
    /// # Errors
    /// Where the broker refused the connection or could not be reached.
    pub fn connect_keeping_session(
        broker: &str,
        credentials: &Credentials,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        Self::open(
            broker,
            credentials,
            timeout,
            credentials.client_id.is_empty(),
        )
    }

    fn open(
        broker: &str,
        credentials: &Credentials,
        timeout: Option<Duration>,
        clean_session: bool,
    ) -> Result<Self> {
        let stream = socket::connect_tcp(broker, timeout)?;
        let mut client = Self { stream, next_id: 0 };
        let login = credentials.login.as_ref();
        client.write(&Packet::Connect {
            client_id: credentials.client_id.clone(),
            clean_session,
            username: login.map(|login| login.user.clone()),
            password: login
                .filter(|login| !login.password.is_empty())
                .map(|login| login.password.as_bytes().to_vec()),
            keep_alive: 0,
        })?;
        match read(&mut client.stream)? {
            Some(Packet::ConnAck { code: 0, .. }) => Ok(client),
            Some(Packet::ConnAck { code, .. }) => Err(protocol_error(format!(
                "the broker refused the connection with code {code}"
            ))),
            _ => Err(protocol_error("the broker did not answer CONNECT")),
        }
    }

    /// Publish `payload` on `topic` at `qos`, waiting for the PUBACK at `QoS` 1.
    ///
    /// # Errors
    /// Where the broker went away, or acknowledged another packet.
    pub fn publish(&mut self, topic: &str, payload: &[u8], qos: u8) -> Result<()> {
        let id = (qos > 0).then(|| self.take_id());
        self.write(&Packet::Publish(Publish {
            topic: topic.to_string(),
            qos,
            retain: false,
            id,
            payload: payload.to_vec(),
        }))?;
        if let Some(id) = id {
            match self.read_while_serving()? {
                Some(Packet::PubAck(acked)) if acked == id => {}
                Some(Packet::PubAck(_)) => {
                    return Err(protocol_error("a PUBACK for another packet"));
                }
                _ => return Err(protocol_error("the broker did not acknowledge")),
            }
        }
        Ok(())
    }

    /// Subscribe to `filter` at `qos`, waiting for the SUBACK.
    ///
    /// # Errors
    /// Where the broker refused the subscription or went away.
    pub fn subscribe(&mut self, filter: &str, qos: u8) -> Result<()> {
        let id = self.take_id();
        self.write(&Packet::Subscribe {
            id,
            filters: vec![(filter.to_string(), qos)],
        })?;
        match self.read_while_serving()? {
            Some(Packet::SubAck { id: acked, codes }) if acked == id => {
                if codes.first().is_some_and(|c| *c == 0x80) {
                    return Err(protocol_error(format!("the broker refused {filter}")));
                }
                Ok(())
            }
            _ => Err(protocol_error("the broker did not answer SUBSCRIBE")),
        }
    }

    /// The next message the broker delivers, or `None` when it
    /// disconnected. Not acknowledged: a `QoS` 1 delivery waits for
    /// [`Client::puback`].
    ///
    /// # Errors
    /// Where the connection broke, or nothing arrived before the timeout.
    pub fn next_publish(&mut self) -> Result<Option<Publish>> {
        match self.read_while_serving()? {
            Some(Packet::Publish(publish)) => Ok(Some(publish)),
            _ => Ok(None),
        }
    }

    /// PUBACK the `QoS` 1 delivery under packet identifier `id`: the broker
    /// is done with it.
    ///
    /// # Errors
    /// Where the broker went away.
    pub fn puback(&mut self, id: u16) -> Result<()> {
        self.write(&Packet::PubAck(id))
    }

    /// Close the connection without DISCONNECT, so the broker keeps what
    /// this client had not acknowledged for when it reconnects; the client
    /// is no longer [`Pooled::usable`].
    pub fn let_go(&mut self) {
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
    }

    /// Say goodbye and close. A broker that already closed — after the last
    /// PUBACK, say — is not a failure: everything acknowledged was delivered.
    pub fn disconnect(mut self) {
        let _ = self.write(&Packet::Disconnect);
    }

    /// Read the next packet that is not a ping, answering the pings.
    fn read_while_serving(&mut self) -> Result<Option<Packet>> {
        loop {
            match read(&mut self.stream)? {
                Some(Packet::PingReq) => self.write(&Packet::PingResp)?,
                Some(Packet::PingResp) => {}
                Some(Packet::Disconnect) | None => return Ok(None),
                other => return Ok(other),
            }
        }
    }

    fn take_id(&mut self) -> u16 {
        self.next_id = self.next_id.wrapping_add(1).max(1);
        self.next_id
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

impl Pooled for Client {
    /// While the broker has not closed the connection.
    fn usable(&mut self) -> bool {
        alive(&self.stream)
    }
}
