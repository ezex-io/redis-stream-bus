pub use crate::{bus::StreamBus, stream::Stream};
use async_trait::async_trait;
use futures::channel::mpsc::{channel, Receiver, Sender};
use futures::{select, SinkExt};
use futures_util::StreamExt;
use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

pub struct MockRedisClient {
    add_ch: (Sender<Stream>, Receiver<Stream>),
    ack_ch: (Sender<Stream>, Receiver<Stream>),
}

impl MockRedisClient {
    pub fn new() -> Self {
        MockRedisClient {
            add_ch: channel(100),
            ack_ch: channel(100),
        }
    }
}

#[async_trait]
impl StreamBus for MockRedisClient {
    fn xadd_sender(&self) -> Sender<Stream> {
        self.add_ch.0.clone()
    }
    fn xack_sender(&self) -> Sender<Stream> {
        self.ack_ch.0.clone()
    }

    async fn run<'a, 'b>(
        &mut self,
        keys: &[&'a str],
        read_tx: &'b mut Sender<Stream>,
    ) -> anyhow::Result<()> {
        let mut streams: HashMap<String, Stream> = HashMap::new();
        loop {
            select! {
                add_option = self.add_ch.1.next() => if let Some(stream) = add_option {
                    if !keys.contains(&stream.key.as_ref()){
                        continue;
                    }

                    let id=format!("{}-0",
                        SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .expect("Time went backwards")
                        .as_millis()
                    );

                    let mut stored_stream=stream.clone();
                    stored_stream.id=Some(id.clone());

                    streams.insert(id.clone(),stored_stream.clone());

                    read_tx.send(stored_stream.clone()).await.expect("read channel receiving");

                    log::trace!("Stream added: {:?}, id: {:?}", stream, id);
                },
                ack_option = self.ack_ch.1.next() => if let Some(stream) = ack_option {
                    streams.remove(&stream.id.expect("id should exist"));
                },
            }
        }
    }
}

impl Default for MockRedisClient {
    fn default() -> Self {
        Self::new()
    }
}
