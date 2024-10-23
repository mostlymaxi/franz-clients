use franz_client::{Consumer, FranzClientError, Producer};
use rand::Rng;
use std::thread;

const BROKER: &str = "127.0.0.1:8085";
const TOPIC: &str = "test_topic";

fn main() -> Result<(), FranzClientError> {
    const NUM_MESSAGES: usize = 3;

    let producer_handle = thread::spawn(move || {
        let mut producer = Producer::new(BROKER, TOPIC)?;
        let mut rng = rand::thread_rng();

        for i in 0..NUM_MESSAGES {
            let random_num: u32 = rng.gen_range(1..=100);
            let msg = format!("Hello, Franz! {i} - Random: {random_num}");
            producer.send_unbuffered(&msg)?;
            println!("Producer sent: \"{msg}\"");
        }
        producer.flush()
    });

    let consumer_handle = thread::spawn(move || -> Result<(), FranzClientError> {
        let mut consumer = Consumer::new(BROKER, TOPIC, Some(0))?;
        for _ in 0..NUM_MESSAGES {
            let msg = consumer.recv()?;
            let msg = String::from_utf8_lossy(&msg);
            println!("Consumer received: {msg}");
        }
        Ok(())
    });

    producer_handle.join().unwrap()?;
    println!("--------------------");
    consumer_handle.join().unwrap()?;

    Ok(())
}
