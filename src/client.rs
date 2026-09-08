//! The client's side of one connection to a broker.

use std::io::Write;
use std::net::TcpStream;
use std::time::Duration;

use transport::Arrived;
use transport::error::{Result, classify, protocol_error};
use transport::socket;

use crate::packet::{Packet, Publish, encode, read};

/// What a Location presents when it connects.
#[derive(Clone, Debug, Default)]
pub struct Credentials {
    pub client_id: String,
    pub username: Option<String>,
    pub password: Option<Vec<u8>>,
}

/// One connected client: publishes, subscribes, takes what the broker sends.
pub struct Client {
    stream: TcpStream,
    broker: String,
    next_id: u16,
}

impl Client {
    /// Connect to `broker` and complete the CONNECT handshake.
    ///
    /// # Errors
    /// Where the broker refused the connection or could not be reached.
    pub fn connect(
        broker: &str,
        credentials: &Credentials,
        timeout: Option<Duration>,
    ) -> Result<Self> {
        let stream = socket::connect_tcp(broker, timeout)?;
        let mut client = Self {
            stream,
            broker: broker.to_string(),
            next_id: 0,
        };
        client.write(&Packet::Connect {
            client_id: credentials.client_id.clone(),
            username: credentials.username.clone(),
            password: credentials.password.clone(),
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

    /// The next message the broker delivers, or `None` when it disconnected.
    ///
    /// # Errors
    /// Where the connection broke, or nothing arrived before the timeout.
    pub fn next_publish(&mut self) -> Result<Option<Arrived>> {
        match self.read_while_serving()? {
            Some(Packet::Publish(publish)) => {
                if let Some(id) = publish.id {
                    self.write(&Packet::PubAck(id))?;
                }
                Ok(Some(Arrived::new(
                    format!(
                        "mqtt://{}/{}?qos={}",
                        self.broker, publish.topic, publish.qos
                    ),
                    publish.payload,
                )))
            }
            _ => Ok(None),
        }
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
