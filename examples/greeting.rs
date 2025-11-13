use log::info;
use redis_serde::{Deserializer, Serializer};
use redis_stream_bus::{client::RedisClient, entry::Entry, error::Result};
use serde::{Deserialize, Serialize};

#[derive(Debug, Serialize, Deserialize)]
struct Greeting {
    message: String,
}

#[tokio::main]
async fn main() -> Result<()> {
    simple_logger::init_with_level(log::Level::Info).ok();

    let client = RedisClient::new("redis://127.0.0.1:6379", "examples-group", "consumer-1")?;
    client.create_groups(&["examples"]).await;

    let payload = Greeting {
        message: "hello world!".into(),
    };
    let fields = payload.serialize(Serializer).expect("serialize payload");

    info!("Publishing message: {:?}", payload);
    let entry = Entry::new("examples", fields);
    let id = client.xadd(entry).await?;
    info!("Published message id: {}", id);

    let message = client.xread_one("examples").await?;
    info!("Received message with id {:?}", message.id);

    let greeting: Greeting = Deserialize::deserialize(Deserializer::new(message.fields.clone()))
        .expect("deserialize payload");
    info!("Decoded payload: {:?}", greeting);

    let message_id = message.id.clone().unwrap_or_default();
    client.xack(&message).await?;
    info!("Acknowledged message {}", message_id);

    Ok(())
}
