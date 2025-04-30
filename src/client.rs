use super::config::Config;
use super::{bus::StreamBus, stream::Stream};
use crate::error::{RedisBusError, Result};
use async_trait::async_trait;
use futures::FutureExt;
use futures::channel::mpsc::{Receiver, Sender, channel};
use futures::{SinkExt, select};
use futures_util::StreamExt;
#[cfg(not(test))]
use log::{error, info, trace, warn};
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, RedisResult};
use std::collections::{BTreeMap, HashMap};
use std::pin::Pin;

#[cfg(test)]
use std::{println as trace, println as info, println as warn, println as error};

/// **RedisClient** will keep connection and internal options of redis client
///
///
/// # Fields
/// - `client`: redis-rs client keeping connection and utility
/// - `group_name`: name of group will be joining
/// - `consumer_name`: the name of consumer node
/// - `timeout`: how much block(wait) per event request (ms)
/// - `count`: maximum events per request
/// - `start_id`: ID to use when creating consumer groups ("0" for all messages, "$" for new messages only)
///
pub struct RedisClient {
    client: redis::Client,
    group_name: String,
    consumer_name: String,
    timeout: usize,
    count: usize,
    start_id: String,
    add_ch: (Sender<Stream>, Receiver<Stream>),
    ack_ch: (Sender<Stream>, Receiver<Stream>),
}

impl RedisClient {
    pub fn new(connection_string: &str, group_name: &str, consumer_name: &str) -> Result<Self> {
        let client = redis::Client::open(connection_string)?;
        Ok(RedisClient {
            client,
            group_name: group_name.to_owned(),
            consumer_name: consumer_name.to_owned(),
            timeout: 5_000,
            count: 5,
            start_id: "0".to_string(), // Default to "0" to process all messages
            add_ch: channel(100),
            ack_ch: channel(100),
        })
    }

    pub fn with_timeout(mut self, timeout: usize) -> Self {
        self.timeout = timeout;
        self
    }

    pub fn with_count(mut self, count: usize) -> Self {
        self.count = count;
        self
    }

    /// Set the starting ID for consumer groups
    ///
    /// # Arguments
    /// * `start_id` - The ID to use when creating consumer groups:
    ///   - "0": Process all messages in the stream (default)
    ///   - "$": Process only new messages that arrive after group creation
    ///   - Any valid ID: Start processing from that specific ID
    pub fn with_start_id(mut self, start_id: &str) -> Self {
        self.start_id = start_id.to_string();
        self
    }

    pub fn from_config(config: &Config) -> Result<Self> {
        Self::new(
            &config.connection_string,
            &config.group_name,
            &config.consumer_name,
        )
    }

    async fn create_groups<'a>(
        group_name: &str,
        keys: &[&'a str],
        con_read: &mut MultiplexedConnection,
        start_id: &str,
    ) -> Vec<&'a str> {
        let mut ids = vec![];
        for key in keys {
            let created: RedisResult<()> = con_read
                .xgroup_create_mkstream(*key, group_name, start_id)
                .await;

            match created {
                Ok(_) => trace!("Group created successfully: {}", group_name),
                Err(err) => warn!(
                    "An error occurred when creating a group {:?}: {:?}",
                    group_name, err
                ),
            }
            ids.push(">");
        }
        ids
    }

    pub(super) fn map_to_value(map: HashMap<String, redis::Value>) -> redis::Value {
        let mut values = Vec::with_capacity(map.len() * 2);
        for (key, val) in map {
            values.push(redis::Value::BulkString(key.into_bytes()));
            values.push(val);
        }
        redis::Value::Array(values)
    }

    pub(super) fn value_to_map(val: redis::Value) -> Result<BTreeMap<String, Vec<u8>>> {
        match val {
            redis::Value::Array(arr) => {
                let mut map = BTreeMap::new();
                let mut iter = arr.into_iter();
                while let Some(redis::Value::BulkString(key_bytes)) = iter.next() {
                    let key = String::from_utf8(key_bytes)?;

                    match iter.next() {
                        Some(value) => {
                            let value_bytes = match value {
                                redis::Value::BulkString(data) => data,
                                _ => {
                                    return Err(RedisBusError::InvalidData(
                                        "Unsupported Redis Value Type".to_string(),
                                    ));
                                }
                            };

                            map.insert(key, value_bytes);
                        }
                        None => {
                            return Err(RedisBusError::InvalidData(format!(
                                "Missing Redis Value For {key}"
                            )));
                        }
                    }
                }
                Ok(map)
            }
            _ => Err(RedisBusError::InvalidData(
                "Unsupported Redis Key Type".to_string(),
            )),
        }
    }
}

#[async_trait]
impl StreamBus for RedisClient {
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
    ) -> Result<()> {
        let mut con_read = self.client.get_multiplexed_async_connection().await?;
        let mut con_add = self.client.get_multiplexed_async_connection().await?;
        let mut con_ack = self.client.get_multiplexed_async_connection().await?;

        let opts = StreamReadOptions::default()
            .group(&self.group_name, &self.consumer_name)
            .block(self.timeout)
            .count(self.count);

        let ids = Self::create_groups(&self.group_name, keys, &mut con_read, &self.start_id).await;

        info!("Started listening on events. on keys: {:?}", keys);

        loop {
            let mut read_stream: futures_util::future::IntoStream<
                Pin<
                    Box<
                        dyn futures_util::Future<
                                Output = std::result::Result<StreamReadReply, redis::RedisError>,
                            > + std::marker::Send,
                    >,
                >,
            > = con_read.xread_options(keys, &ids, &opts).into_stream();

            select! {
                read_option = read_stream.next() => if let Some(stream_result) = read_option {
                    match stream_result {
                        Ok(stream) =>{
                            for stream_key in stream.keys {
                                for stream_id in stream_key.ids {
                                    trace!("[>][{}]: {}", &stream_key.key, stream_id.id);

                                    let fields = Self::map_to_value(stream_id.map);
                                    let stream = Stream::new(&stream_key.key, Some(stream_id.id), fields);
                                    if let Err(err) = read_tx.send(stream).await {
                                        error!("[!]: {:?}", err);
                                        break;
                                    };
                                }
                            }
                        },
                        Err (err) => {
                            warn!("[!]: {:?} , CODE:'{:?}'", err,err.code());
                        }
                    }
                },
                add_option = self.add_ch.1.next() => {
                    if let Some(stream) = add_option {
                        let stream_id = match stream.id {
                            Some(id) => id,
                            None => "*".to_owned(),
                        };

                       let map = Self::value_to_map(stream.fields)?;
                       match con_add.xadd_map::<_, _, BTreeMap<String, Vec<u8>>, String>(
                            stream.key.clone(),
                            stream_id,
                            map,
                        ).await {
                            Ok(id) => {
                                trace!("[<][{}]: {}", &stream.key, &id);
                            }
                            Err(e)=> {
                                warn!("[!][{}]: {}", &stream.key, e);
                            }
                        }
                    }
                },
                ack_option = self.ack_ch.1.next() => if let Some(stream) = ack_option {
                    match stream.id {
                        Some(id) => {
                            trace!("[^][{}]: {}", &stream.key, &id);
                            if let Err(err) = con_ack.xack::<_, _, _, i32>(&stream.key, &self.group_name, &[id]).await{
                                error!("[!][{}]: {}", &stream.key, err);
                            }
                        }
                        None => {
                            warn!("[!][{}]: Stream ID is not set for the acknowledgment: {:?}", &stream.key, stream);
                        }
                    }
                },
            }
        }
    }
}

#[cfg(test)]
pub(crate) mod test_utils {
    use super::*;

    /// Resets the Redis database by flushing all data
    ///
    /// This is useful for ensuring test isolation.
    /// Call this at the beginning of each test that interacts with Redis.
    ///
    /// # Example
    /// ```
    /// #[tokio::test]
    /// async fn my_test() {
    ///     client::test_utils::reset_redis("redis://localhost:6379").await.unwrap();
    ///     // Test code...
    /// }
    /// ```
    pub async fn reset_redis(connection_string: &str) -> Result<()> {
        match tokio::time::timeout(tokio::time::Duration::from_secs(5), async {
            let client = redis::Client::open(connection_string)?;
            let mut con = client.get_multiplexed_async_connection().await?;

            // FLUSHALL removes all keys from all databases
            // redis::cmd("FLUSHALL").query_async::<()>(&mut con).await?;

            // Delete all consumer groups for extra cleanup
            let keys: Vec<String> = redis::cmd("KEYS").arg("*").query_async(&mut con).await?;

            for key in keys {
                // Check if the key is a stream
                let key_type: String = redis::cmd("TYPE").arg(&key).query_async(&mut con).await?;

                if key_type == "stream" {
                    // Try to get all consumer groups for this stream
                    let groups_result: RedisResult<Vec<redis::Value>> = redis::cmd("XINFO")
                        .arg("GROUPS")
                        .arg(&key)
                        .query_async(&mut con)
                        .await;

                    if let Ok(groups) = groups_result {
                        for group in groups {
                            if let redis::Value::Array(group_info) = group {
                                if group_info.len() > 1 {
                                    if let redis::Value::BulkString(name_bytes) = &group_info[1] {
                                        if let Ok(group_name) =
                                            String::from_utf8(name_bytes.clone())
                                        {
                                            println!(
                                                "Destroying group {} on stream {}",
                                                group_name, &key
                                            );
                                            let _: RedisResult<()> = redis::cmd("XGROUP")
                                                .arg("DESTROY")
                                                .arg(&key)
                                                .arg(&group_name)
                                                .query_async(&mut con)
                                                .await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
            }

            // Now flush all keys
            println!("Flushing all Redis keys");
            redis::cmd("FLUSHALL").query_async::<()>(&mut con).await?;

            // Verify that all keys are gone
            let remaining_keys: Vec<String> =
                redis::cmd("KEYS").arg("*").query_async(&mut con).await?;
            if !remaining_keys.is_empty() {
                println!(
                    "Warning: Some keys remain after FLUSHALL: {:?}",
                    remaining_keys
                );
            } else {
                println!("Redis successfully reset - no keys remain");
            }

            Ok(())
        })
        .await
        {
            Ok(result) => result,
            Err(_) => Err(RedisBusError::Timeout(
                "Redis reset operation timed out".into(),
            )),
        }
    }
    /// Creates a test client with default settings
    pub fn create_test_client(
        connection_string: &str,
        group_name: &str,
        consumer_name: &str,
    ) -> Result<RedisClient> {
        RedisClient::new(connection_string, group_name, consumer_name)
            .map(|client| client.with_timeout(1000).with_count(1))
    }

    /// Waits for Redis to be available
    ///
    /// Useful in CI environments where Redis might take time to start
    pub async fn wait_for_redis(connection_string: &str, max_attempts: usize) -> Result<()> {
        let client = redis::Client::open(connection_string)?;

        for attempt in 1..=max_attempts {
            match client.get_multiplexed_async_connection().await {
                Ok(mut con) => {
                    // Try a simple PING command to verify connection is working
                    match redis::cmd("PING").query_async::<String>(&mut con).await {
                        Ok(_) => return Ok(()),
                        Err(e) => {
                            if attempt == max_attempts {
                                return Err(e.into());
                            }
                        }
                    }
                }
                Err(e) => {
                    if attempt == max_attempts {
                        return Err(e.into());
                    }
                }
            }

            // Wait before retrying
            tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        }

        Ok(())
    }
}
