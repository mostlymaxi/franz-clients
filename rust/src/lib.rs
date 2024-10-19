#![doc = include_str!("../README.md")]

use std::{
    io::{BufRead, BufReader, BufWriter, Write},
    net::{TcpStream, ToSocketAddrs},
    thread,
    time::Duration,
};

/// An abstraction for the number we send to the server to set
/// what we want our client to do
#[derive(Copy, Clone)]
pub enum Api {
    Produce,
    Consume,
}

/// A simple Franz Producer that sends messages to the broker.
///
/// Note: This producer does not use any internal buffering!
///
/// ```rust,ignore
/// use franz_client::{Producer, FranzClientError};
///
/// fn main() -> Result<(), FranzClientError> {
///     let mut producer = Producer::new("127.0.0.1:8085", "test")?;
///     producer.send_unbuffered("i was here! :3")?;
///     producer.flush()
/// }
/// ```
pub struct Producer {
    inner: BufWriter<TcpStream>,
}

#[derive(thiserror::Error, Debug)]
pub enum FranzClientError {
    #[error(transparent)]
    AddrParseError(#[from] std::net::AddrParseError),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
}

fn send_handshake(
    sock: &mut BufWriter<TcpStream>,
    handshake: &str,
) -> Result<(), FranzClientError> {
    sock.write_all(&(handshake.len() as u32).to_be_bytes())?;
    sock.write_all(handshake.as_bytes())?;
    Ok(sock.flush()?)
}

impl Producer {
    pub fn new(broker: impl ToSocketAddrs, topic: &str) -> Result<Self, FranzClientError> {
        let sock = TcpStream::connect(broker)?;
        let mut sock = BufWriter::new(sock);

        let handshake = format!("version=1,topic={},api=produce", topic);
        send_handshake(&mut sock, &handshake)?;

        Ok(Producer { inner: sock })
    }

    pub fn send<D: AsRef<[u8]>>(&mut self, msg: D) -> Result<(), FranzClientError> {
        // IF WE SEND MESSAGE THAT DOES NOT MATCH
        // EXPECTED BYTES, WARN
        self.inner.write_all(msg.as_ref())?;
        self.inner.write_all(b"\n")?;

        Ok(())
    }

    pub fn send_unbuffered<D: AsRef<[u8]>>(&mut self, msg: D) -> Result<(), FranzClientError> {
        self.send(msg)?;
        self.inner.flush()?;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), FranzClientError> {
        Ok(self.inner.flush()?)
    }
}

/// A simple Franz Consumer that receives messages from the broker
///
/// ```rust,ignore
/// use franz_client::{Consumer, FranzClientError};
///
/// fn main() -> Result<(), FranzClientError> {
///     let mut consumer = Consumer::new("127.0.0.1:8085", "test", None)?;
///     let msg = consumer.recv()?;
///     Ok(println!("{}", String::from_utf8_lossy(&msg)))
/// }
/// ```
pub struct Consumer {
    inner: BufReader<TcpStream>,
}

impl Consumer {
    pub fn new(
        broker: impl ToSocketAddrs,
        topic: &str,
        group: Option<u16>,
    ) -> Result<Self, FranzClientError> {
        let sock = TcpStream::connect(broker)?;
        let sock_c = sock.try_clone()?;
        let mut sock = BufWriter::new(sock);

        let handshake = match group {
            Some(g) => format!("version=1,topic={},group={},api=consume", topic, g),
            None => format!("version=1,topic={},api=consume", topic),
        };
        send_handshake(&mut sock, &handshake)?;

        // KEEPALIVE
        thread::spawn(move || loop {
            sock.write_all(b"PING\n").unwrap();
            sock.flush().unwrap();
            thread::sleep(Duration::from_secs(60));
        });

        let inner = BufReader::new(sock_c);

        Ok(Consumer { inner })
    }

    pub fn recv(&mut self) -> Result<Vec<u8>, FranzClientError> {
        // WE CAN READ EXPECTED BYTES FROM FRANZ
        // AND ALLOCATE OUR BUFFER WITH THE EXPECTED BYTES
        let mut buf = Vec::new();
        self.inner.read_until(b'\n', &mut buf)?;
        Ok(buf)
    }
}
