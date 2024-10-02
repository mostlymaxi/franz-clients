# Franz Client

## Example

This example assumes you have a franz server running on port 8085 bound to localhost.

You can spin up a test server with ```franz --path /tmp/franz-test```

```rust
use franz_client::{ FranzClientError, FranzConsumer, FranzProducer };

#[tokio::main]
async fn main() -> Result<(), FranzClientError> {

    let mut p = FranzProducer::new("127.0.0.1:8085", "test").await?;
    p.send("i was here! :3").await?;

    let mut c = FranzConsumer::new("127.0.0.1:8085", "test").await?;
    // returns None if there are no new messages
    // and errors on incorrectly formatted message
    let msg = c.recv().await.unwrap()?;

    Ok(())
}
```
