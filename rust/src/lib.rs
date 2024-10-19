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
/// ```rust
/// use franz_client::{ FranzClientError, FranzProducer };
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), FranzClientError> {
/// let mut p = FranzProducer::new("127.0.0.1:8085", "test").await?;
/// p.send("i was here! :3").await?;
///
/// # Ok(())
/// # }
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

impl Producer {
    pub fn new<S: AsRef<str>>(broker: S, topic: S) -> Result<Self, FranzClientError> {
        let addr = broker
            .as_ref()
            .to_socket_addrs()?
            // .filter(|a| a.is_ipv4() || a.is_ipv6())
            .next()
            .ok_or(std::io::Error::other("can't resolve socket addr"))?;

        let sock = TcpStream::connect(addr)?;
        let mut sock = BufWriter::new(sock);

        let handshake = format!("version=1,topic={},api=produce", topic.as_ref());

        sock.write_all(&(handshake.len() as u32).to_be_bytes())?;
        sock.write_all(handshake.as_bytes())?;
        sock.flush()?;

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
/// ```rust
/// use franz_client::{ FranzClientError, FranzConsumer };
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), FranzClientError> {
/// let mut c = FranzConsumer::new("127.0.0.1:8085", "test").await?;
/// // returns None if there are no new messages
/// // and errors on incorrectly formatted message
/// let msg = c.recv().await.unwrap()?;
///
/// # Ok(())
/// # }
/// ```
pub struct Consumer {
    inner: BufReader<TcpStream>,
}

impl Consumer {
    pub fn new<S: AsRef<str>>(
        broker: S,
        topic: S,
        group: Option<u16>,
    ) -> Result<Self, FranzClientError> {
        let addr = broker
            .as_ref()
            .to_socket_addrs()?
            // .filter(|a| a.is_ipv4() || a.is_ipv6())
            .next()
            .ok_or(std::io::Error::other("can't resolve socket addr"))?;

        let sock = TcpStream::connect(addr)?;
        let sock_c = sock.try_clone()?;
        let mut sock = BufWriter::new(sock);

        let handshake = match group {
            Some(g) => format!("version=1,topic={},group={},api=consume", topic.as_ref(), g),
            None => format!("version=1,topic={},api=consume", topic.as_ref()),
        };

        sock.write_all(&(handshake.len() as u32).to_be_bytes())?;
        sock.write_all(handshake.as_bytes())?;
        sock.flush()?;

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
