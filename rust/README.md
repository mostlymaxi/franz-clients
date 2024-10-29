# Franz Client

## Protocol
[message length : u32]([key]=[value] : utf8)

#### mandatory keys
- version
- topic
- api

#### example key-values
```version=1,topic=test_topic_name,api=produce```

```msg1\nmsg2\nmsg3\n```

## Example
void franz_send(franz_producer_t tx, char *data, size_t len);

This example assumes you have a franz server running on port 8085 bound to localhost.

You can spin up a test server with ```franz --path /tmp/franz-test```
then test the client with ```cargo run --example client```
