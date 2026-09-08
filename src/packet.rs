//! MQTT 3.1.1 control packets: the fixed header, the remaining-length
//! encoding, and the seven packets a Location meets.
//!
//! What is not here: wills, retained-message state, `QoS` 2's four-way
//! handshake. A Location publishes at `QoS` 0 or 1 and subscribes at the same;
//! exactly-once is a Journey's business, not a packet's (ADR-0013 4c).

use std::io::Read;

use transport::error::{Result, classify, protocol_error};

/// The most a remaining length may say: four bytes of seven bits.
pub const MAX_REMAINING: usize = 268_435_455;

/// One application message, as PUBLISH carries it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Publish {
    pub topic: String,
    pub qos: u8,
    pub retain: bool,
    /// Present at `QoS` 1 and above.
    pub id: Option<u16>,
    pub payload: Vec<u8>,
}

/// The control packets this transport speaks.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Packet {
    Connect {
        client_id: String,
        username: Option<String>,
        password: Option<Vec<u8>>,
        keep_alive: u16,
    },
    ConnAck {
        session_present: bool,
        code: u8,
    },
    Publish(Publish),
    PubAck(u16),
    Subscribe {
        id: u16,
        filters: Vec<(String, u8)>,
    },
    SubAck {
        id: u16,
        codes: Vec<u8>,
    },
    PingReq,
    PingResp,
    Disconnect,
}

/// Encode `packet` as bytes on the wire.
///
/// # Errors
/// A body over [`MAX_REMAINING`], or a string over 65535 bytes.
pub fn encode(packet: &Packet) -> Result<Vec<u8>> {
    let mut body = Vec::new();
    let head = match packet {
        Packet::Connect {
            client_id,
            username,
            password,
            keep_alive,
        } => {
            string(&mut body, "MQTT")?;
            body.push(4);
            let mut flags = 0x02;
            if username.is_some() {
                flags |= 0x80;
            }
            if password.is_some() {
                flags |= 0x40;
            }
            body.push(flags);
            body.extend_from_slice(&keep_alive.to_be_bytes());
            string(&mut body, client_id)?;
            if let Some(name) = username {
                string(&mut body, name)?;
            }
            if let Some(secret) = password {
                bytes(&mut body, secret)?;
            }
            0x10
        }
        Packet::ConnAck {
            session_present,
            code,
        } => {
            body.push(u8::from(*session_present));
            body.push(*code);
            0x20
        }
        Packet::Publish(publish) => {
            string(&mut body, &publish.topic)?;
            if publish.qos > 0 {
                body.extend_from_slice(&publish.id.unwrap_or(1).to_be_bytes());
            }
            body.extend_from_slice(&publish.payload);
            0x30 | (publish.qos.min(2) << 1) | u8::from(publish.retain)
        }
        Packet::PubAck(id) => {
            body.extend_from_slice(&id.to_be_bytes());
            0x40
        }
        Packet::Subscribe { id, filters } => {
            body.extend_from_slice(&id.to_be_bytes());
            for (filter, qos) in filters {
                string(&mut body, filter)?;
                body.push(*qos);
            }
            0x82
        }
        Packet::SubAck { id, codes } => {
            body.extend_from_slice(&id.to_be_bytes());
            body.extend_from_slice(codes);
            0x90
        }
        Packet::PingReq => 0xc0,
        Packet::PingResp => 0xd0,
        Packet::Disconnect => 0xe0,
    };
    if body.len() > MAX_REMAINING {
        return Err(protocol_error("a packet over what MQTT can frame"));
    }
    let mut out = vec![head];
    let mut remaining = body.len();
    loop {
        let mut byte = u8::try_from(remaining % 128).unwrap_or(0);
        remaining /= 128;
        if remaining > 0 {
            byte |= 0x80;
        }
        out.push(byte);
        if remaining == 0 {
            break;
        }
    }
    out.extend_from_slice(&body);
    Ok(out)
}

/// Read one packet, or `None` when the peer closed between packets.
///
/// # Errors
/// A connection that closes mid-packet, a malformed length, or a packet type
/// this transport does not speak.
pub fn read(reader: &mut impl Read) -> Result<Option<Packet>> {
    let mut head = [0u8; 1];
    let first = reader
        .read(&mut head)
        .map_err(|e| classify("reading the fixed header", &e))?;
    if first == 0 {
        return Ok(None);
    }
    let mut remaining = 0usize;
    let mut shift = 0u32;
    loop {
        let mut byte = [0u8; 1];
        reader
            .read_exact(&mut byte)
            .map_err(|e| classify("reading the remaining length", &e))?;
        remaining += usize::from(byte[0] & 0x7f) << shift;
        if byte[0] & 0x80 == 0 {
            break;
        }
        shift += 7;
        if shift > 21 {
            return Err(protocol_error("a remaining length over four bytes"));
        }
    }
    let mut body = vec![0u8; remaining];
    reader
        .read_exact(&mut body)
        .map_err(|e| classify("reading the packet body", &e))?;
    decode(head[0], &body).map(Some)
}

fn decode(head: u8, body: &[u8]) -> Result<Packet> {
    let mut at = 0usize;
    match head >> 4 {
        1 => {
            let name = take_string(body, &mut at)?;
            let level = take_u8(body, &mut at)?;
            if name != "MQTT" || level != 4 {
                return Err(protocol_error("a CONNECT that is not MQTT 3.1.1"));
            }
            let flags = take_u8(body, &mut at)?;
            let keep_alive = take_u16(body, &mut at)?;
            let client_id = take_string(body, &mut at)?;
            if flags & 0x04 != 0 {
                take_string(body, &mut at)?;
                take_bytes(body, &mut at)?;
            }
            let username = if flags & 0x80 != 0 {
                Some(take_string(body, &mut at)?)
            } else {
                None
            };
            let password = if flags & 0x40 != 0 {
                Some(take_bytes(body, &mut at)?)
            } else {
                None
            };
            Ok(Packet::Connect {
                client_id,
                username,
                password,
                keep_alive,
            })
        }
        2 => Ok(Packet::ConnAck {
            session_present: take_u8(body, &mut at)? & 1 == 1,
            code: take_u8(body, &mut at)?,
        }),
        3 => {
            let qos = (head >> 1) & 0x03;
            let topic = take_string(body, &mut at)?;
            let id = if qos > 0 {
                Some(take_u16(body, &mut at)?)
            } else {
                None
            };
            Ok(Packet::Publish(Publish {
                topic,
                qos,
                retain: head & 1 == 1,
                id,
                payload: body[at..].to_vec(),
            }))
        }
        4 => Ok(Packet::PubAck(take_u16(body, &mut at)?)),
        8 => {
            let id = take_u16(body, &mut at)?;
            let mut filters = Vec::new();
            while at < body.len() {
                let filter = take_string(body, &mut at)?;
                filters.push((filter, take_u8(body, &mut at)?));
            }
            Ok(Packet::Subscribe { id, filters })
        }
        9 => Ok(Packet::SubAck {
            id: take_u16(body, &mut at)?,
            codes: body[at..].to_vec(),
        }),
        12 => Ok(Packet::PingReq),
        13 => Ok(Packet::PingResp),
        14 => Ok(Packet::Disconnect),
        other => Err(protocol_error(format!(
            "a control packet of type {other} this transport does not speak"
        ))),
    }
}

fn string(out: &mut Vec<u8>, text: &str) -> Result<()> {
    bytes(out, text.as_bytes())
}

fn bytes(out: &mut Vec<u8>, data: &[u8]) -> Result<()> {
    let length =
        u16::try_from(data.len()).map_err(|_| protocol_error("a string over 65535 bytes"))?;
    out.extend_from_slice(&length.to_be_bytes());
    out.extend_from_slice(data);
    Ok(())
}

fn take_u8(body: &[u8], at: &mut usize) -> Result<u8> {
    let byte = *body
        .get(*at)
        .ok_or_else(|| protocol_error("a packet shorter than its header"))?;
    *at += 1;
    Ok(byte)
}

fn take_u16(body: &[u8], at: &mut usize) -> Result<u16> {
    let high = take_u8(body, at)?;
    let low = take_u8(body, at)?;
    Ok(u16::from_be_bytes([high, low]))
}

fn take_bytes(body: &[u8], at: &mut usize) -> Result<Vec<u8>> {
    let length = usize::from(take_u16(body, at)?);
    let end = *at + length;
    let data = body
        .get(*at..end)
        .ok_or_else(|| protocol_error("a length that runs past the packet"))?;
    *at = end;
    Ok(data.to_vec())
}

fn take_string(body: &[u8], at: &mut usize) -> Result<String> {
    String::from_utf8(take_bytes(body, at)?)
        .map_err(|_| protocol_error("a string that is not UTF-8"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(packet: &Packet) {
        let bytes = encode(packet).expect("encode");
        let back = read(&mut bytes.as_slice()).expect("read").expect("one");
        assert_eq!(&back, packet);
    }

    #[test]
    fn every_packet_round_trips() {
        round_trip(&Packet::Connect {
            client_id: "xmip".into(),
            username: Some("u".into()),
            password: Some(b"p".to_vec()),
            keep_alive: 30,
        });
        round_trip(&Packet::Connect {
            client_id: String::new(),
            username: None,
            password: None,
            keep_alive: 0,
        });
        round_trip(&Packet::ConnAck {
            session_present: true,
            code: 0,
        });
        round_trip(&Packet::Publish(Publish {
            topic: "a/b".into(),
            qos: 1,
            retain: true,
            id: Some(7),
            payload: vec![1, 2, 3],
        }));
        round_trip(&Packet::Publish(Publish {
            topic: "a".into(),
            qos: 0,
            retain: false,
            id: None,
            payload: Vec::new(),
        }));
        round_trip(&Packet::PubAck(9));
        round_trip(&Packet::Subscribe {
            id: 3,
            filters: vec![("a/#".into(), 1), ("b".into(), 0)],
        });
        round_trip(&Packet::SubAck {
            id: 3,
            codes: vec![1, 0],
        });
        round_trip(&Packet::PingReq);
        round_trip(&Packet::PingResp);
        round_trip(&Packet::Disconnect);
    }

    #[test]
    fn a_long_body_takes_a_long_remaining_length() {
        let publish = Packet::Publish(Publish {
            topic: "t".into(),
            qos: 0,
            retain: false,
            id: None,
            payload: vec![0x2a; 70_000],
        });
        let bytes = encode(&publish).expect("encode");
        assert_eq!(bytes[1] & 0x80, 0x80, "continues");
        assert_eq!(bytes.len(), 1 + 3 + 3 + 70_000);
        round_trip(&publish);
    }

    #[test]
    fn malformed_packets_are_refused() {
        assert!(read(&mut &[][..]).expect("closed").is_none());
        assert!(read(&mut &[0x30, 0x05, 0x00][..]).is_err(), "mid-packet");
        assert!(
            read(&mut &[0x30, 0x80, 0x80, 0x80, 0x80, 0x01][..]).is_err(),
            "five length bytes"
        );
        assert!(read(&mut &[0x50, 0x00][..]).is_err(), "PUBREC");
        assert!(read(&mut &[0x30, 0x02, 0x00, 0x05][..]).is_err(), "past");
        assert!(
            read(&mut &[0x10, 0x08, 0x00, 0x04, b'M', b'Q', b'T', b'T', 3, 0][..]).is_err(),
            "3.1"
        );
    }
}
