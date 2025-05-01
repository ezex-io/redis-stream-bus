use super::config::Config;
use super::{bus::StreamBus, entry::Entry};
use crate::error::{RedisBusError, Result};
use async_trait::async_trait;
use futures::SinkExt;
use futures::channel::mpsc::Sender;
use log::{error, trace, warn};
use redis::aio::MultiplexedConnection;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::{AsyncCommands, RedisResult};
use std::collections::{BTreeMap, HashMap};

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
    pub(crate) client: redis::Client,
    group_name: String,
    consumer_name: String,
    timeout: usize,
    count: usize,
    start_id: String,
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

    pub fn get_sync_connection(&self) -> Result<redis::Connection> {
        Ok(self.client.get_connection()?)
    }

    pub async fn get_async_connection(&self) -> Result<redis::aio::MultiplexedConnection> {
        Ok(self.client.get_multiplexed_tokio_connection().await?)
    }

    pub fn get_read_options(&self) -> StreamReadOptions {
        StreamReadOptions::default()
            .group(&self.group_name, &self.consumer_name)
            .block(self.timeout)
            .count(self.count)
    }

    pub async fn create_groups(&self, keys: &[&str]) {
        let mut conn = self.get_async_connection().await.unwrap();
        for key in keys {
            let created: RedisResult<()> = conn
                .xgroup_create_mkstream(*key, self.group_name.clone(), self.start_id.clone())
                .await;

            match created {
                Ok(_) => trace!("Group created successfully: {}", self.group_name),
                Err(err) => warn!(
                    "An error occurred when creating a group {:?}: {:?}",
                    self.group_name, err
                ),
            }
        }
    }

    /// Appends a new stream entry to a Redis stream using the `XADD` command.
    ///
    /// # Arguments
    /// * `entry` - The entry object containing the key, optional ID, and field-value pairs to append.
    ///
    /// # Returns
    /// * `Ok(String)` - The ID assigned to the newly added entry.
    /// * `Err` - If the operation fails (e.g., due to a Redis error or invalid input).
    ///
    /// # Notes
    /// * If Entry `id is `None`, Redis will auto-generate an ID (using "*").
    ///
    /// [Redis XADD command documentation](https://redis.io/docs/latest/commands/xadd/)
    pub async fn xadd(&mut self, entry: Entry) -> Result<String> {
        let mut conn = self.get_async_connection().await?;

        let entry_id = match entry.id {
            Some(id) => id,
            None => "*".to_owned(),
        };

        let map = Self::value_to_map(entry.fields)?;

        let id = conn
            .xadd_map::<_, _, BTreeMap<String, Vec<u8>>, String>(
                entry.key.clone(),
                entry_id,
                map.clone(),
            )
            .await?;

        Ok(id)
    }

    /// Acknowledges a processed stream entry within a Redis consumer group using `XACK`.
    ///
    /// # Arguments
    /// * `entry` - The stream entry object containing the key and the entry ID to acknowledge.
    ///
    /// # Returns
    /// * `Ok(String)` - The acknowledged entry's ID if the acknowledgment succeeded.
    /// * `Err` - If the ID is missing or Redis did not acknowledge the entry.
    ///
    /// # Notes
    /// * Uses the `XACK` command to mark the message as processed.
    /// * The acknowledgment is scoped to this client's `group_name`.
    ///
    /// [Redis XACK command documentation](https://redis.io/docs/latest/commands/xack/)
    pub async fn xack(&mut self, entry: &Entry) -> Result<String> {
        match &entry.id {
            Some(id) => {
                let mut conn = self.get_async_connection().await?;

                let no = conn
                    .xack::<_, _, _, i32>(&entry.key, &self.group_name, &[&entry.id])
                    .await?;

                if no == 1 {
                    Ok(id.clone())
                } else {
                    Err(RedisBusError::InvalidData(format!(
                        "Failed to acknowledge entry with ID: {}",
                        id
                    )))
                }
            }
            None => Err(RedisBusError::InvalidData(format!(
                "Entry ID is not set for the acknowledgment: {}",
                entry.key
            ))),
        }
    }

    pub async fn xread_one(&mut self, key: &str) -> Result<Entry> {
        let mut conn = self.get_async_connection().await?;
        let read_reply: StreamReadReply = conn
            .xread_options(&[key], &[">"], &self.get_read_options())
            .await?;

        let stream_key = read_reply.keys.first().unwrap();
        let steam_id = stream_key.ids.first().unwrap();
        let fields = Self::map_to_value(steam_id.clone().map);

        Ok(Entry::new(&stream_key.key, fields).with_id(steam_id.clone().id))
    }

    pub async fn read_loop(
        mut conn: MultiplexedConnection,
        opts: StreamReadOptions,
        keys: &[&str],
        read_tx: &mut Sender<Entry>,
    ) -> Result<()> {
        // Set the stream ID to ">", and it means: messages never delivered to other consumers so far.
        let ids = [">"];
        loop {
            let read_option: std::result::Result<StreamReadReply, redis::RedisError> =
                conn.xread_options(keys, &ids, &opts).await;

            match read_option {
                Ok(stream) => {
                    for stream_key in stream.keys {
                        for entry in stream_key.ids {
                            trace!("[>][{}]: {}", &stream_key.key, entry.id);

                            let fields = Self::map_to_value(entry.map);
                            let entry = Entry::new(&stream_key.key, fields).with_id(entry.id);
                            if let Err(err) = read_tx.send(entry).await {
                                error!("[!]: {:?}", err);
                                break;
                            };
                        }
                    }
                }
                Err(err) => {
                    warn!("[!]: {:?} , CODE:'{:?}'", err, err.code());

                    return Err(RedisBusError::Redis(err));
                }
            }
        }
    }
}

#[async_trait]
impl StreamBus for RedisClient {
    async fn xadd(&mut self, entry: Entry) -> Result<String> {
        self.xadd(entry).await
    }

    async fn xack(&mut self, entry: &Entry) -> Result<String> {
        self.xack(entry).await
    }
}
