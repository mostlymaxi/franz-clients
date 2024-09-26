use futures::{SinkExt, StreamExt};
use tokio::net::{TcpSocket, TcpStream};
use tokio_util::codec::{Framed, LinesCodec};

#[derive(Copy, Clone)]
#[repr(u8)]
pub enum FranzClientKind {
    Producer = 0,
    Consumer = 1,
}

impl FranzClientKind {
    const fn as_str(self) -> &'static str {
        match self {
            Self::Producer => "0",
            Self::Consumer => "1",
        }
    }
}

pub struct FranzProducer {
    raw: Framed<TcpStream, LinesCodec>,
}

#[derive(thiserror::Error, Debug)]
pub enum FranzClientError {
    #[error(transparent)]
    AddrParseError(#[from] std::net::AddrParseError),
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    CodecError(#[from] tokio_util::codec::LinesCodecError),
}

impl FranzProducer {
    pub async fn new<S: AsRef<str>>(broker: S, topic: S) -> Result<Self, FranzClientError> {
        let s = TcpSocket::new_v4()?;
        let raw = s.connect(broker.as_ref().parse().unwrap()).await?;
        let encoder = LinesCodec::new();
        let mut raw = Framed::new(raw, encoder);

        raw.feed(FranzClientKind::Producer.as_str()).await?;
        raw.send(topic).await?;

        Ok(FranzProducer { raw })
    }

    pub async fn send<S: AsRef<str>>(&mut self, msg: S) -> Result<(), FranzClientError> {
        // Send str input + '\n'
        Ok(self.raw.send(msg).await?)
    }
}

pub struct FranzConsumer {
    raw: Framed<TcpStream, LinesCodec>,
}

impl FranzConsumer {
    pub async fn new<S: AsRef<str>>(broker: S, topic: S) -> Result<Self, FranzClientError> {
        let s = TcpSocket::new_v4()?;
        let raw = s.connect(broker.as_ref().parse()?).await?;
        let encoder = LinesCodec::new();
        let mut raw = Framed::new(raw, encoder);

        raw.feed(FranzClientKind::Consumer.as_str()).await?;
        raw.send(topic).await?;

        Ok(FranzConsumer { raw })
    }

    pub async fn recv(&mut self) -> Option<Result<String, FranzClientError>> {
        self.raw.next().await.map(|n| n.map_err(Into::into))
    }
}
