#[cfg(test)]
mod tests {
    use crate::client::RedisClient;
    use crate::entry::Entry;
    use crate::error::{RedisBusError, Result};
    use futures::StreamExt;
    use futures::channel::mpsc::channel;
    use redis::Value;
    use serde::{Deserialize, Serialize};
    use serial_test::serial;
    use std::collections::{BTreeMap, HashMap};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::select;
    use tokio::task;

    const REDIS_CON: &str = "redis://localhost:6379";

    pub(super) struct Testsuite {
        pub(super) client: RedisClient,
    }

    impl Testsuite {
        pub fn create_test_client(group_name: &str, consumer_name: &str) -> Result<Testsuite> {
            let connection_string = format!("{}/15", REDIS_CON); // Use DB 15 for testing

            Ok(Testsuite {
                client: RedisClient::new(&connection_string, group_name, consumer_name)?,
            })
        }
    }

    impl Drop for Testsuite {
        fn drop(&mut self) {
            if let Ok(mut con) = self.client.get_sync_connection() {
                let _: redis::RedisResult<()> = redis::cmd("FLUSHDB").query(&mut con);
            }
        }
    }

    #[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
    struct TestData {
        field: String,
    }

    impl TestData {
        fn new(msg: &str) -> Self {
            TestData {
                field: msg.to_owned(),
            }
        }

        fn from_value(value: Value) -> Self {
            let de = redis_serde::Deserializer::new(value);
            let decoded: TestData = Deserialize::deserialize(de).unwrap();

            decoded
        }

        fn to_value(&self) -> Value {
            self.serialize(redis_serde::Serializer).unwrap()
        }
    }

    #[test]
    fn test_value_to_map_table() {
        #[derive(Debug)]
        struct TestCase {
            name: &'static str,
            input: Value,
            expected: Result<BTreeMap<String, Vec<u8>>>,
        }

        let mut expected_map = BTreeMap::new();
        expected_map.insert("foo".to_string(), b"1".to_vec());
        expected_map.insert("bar".to_string(), b"2".to_vec());

        let test_cases = vec![
            TestCase {
                name: "valid input",
                input: Value::Array(vec![
                    Value::BulkString(b"foo".to_vec()),
                    Value::BulkString(b"1".to_vec()),
                    Value::BulkString(b"bar".to_vec()),
                    Value::BulkString(b"2".to_vec()),
                ]),
                expected: Ok(expected_map.clone()),
            },
            TestCase {
                name: "invalid UTF-8 key",
                input: Value::Array(vec![
                    Value::BulkString(vec![0xff, 0xfe]),
                    Value::BulkString(vec![1]),
                ]),
                expected: Err(RedisBusError::InvalidData(
                    "invalid utf-8 sequence of 1 bytes from index 0".into(),
                )),
            },
            TestCase {
                name: "missing value for key",
                input: Value::Array(vec![
                    Value::BulkString(b"key".to_vec()),
                    // missing value
                ]),
                expected: Err(RedisBusError::InvalidData(
                    "Missing Redis Value For key".into(),
                )),
            },
            TestCase {
                name: "non-array input",
                input: Value::Int(123),
                expected: Err(RedisBusError::InvalidData(
                    "Unsupported Redis Key Type".into(),
                )),
            },
        ];

        for case in test_cases {
            let result = RedisClient::value_to_map(case.input);
            assert_eq!(result, case.expected, "Test failed: {}", case.name);
        }
    }

    #[test]
    fn test_convert_map_to_value() {
        let mut map = HashMap::new();
        map.insert("a".to_string(), Value::Int(1));
        map.insert("b".to_string(), Value::Boolean(true));

        let result = RedisClient::map_to_value(map);

        if let Value::Array(vec) = result {
            assert!(vec.contains(&Value::BulkString(b"a".to_vec())));
            assert!(vec.contains(&Value::Int(1)));
            assert!(vec.contains(&Value::BulkString(b"b".to_vec())));
            assert!(vec.contains(&Value::Boolean(true)));
        } else {
            panic!("Expected Value::Array");
        }
    }

    #[tokio::test]
    #[serial]
    async fn test_xadd() {
        let ts = Testsuite::create_test_client("group_1", "consumer_1").unwrap();

        let sent = TestData::new("hello world");
        let entry = Entry::new("test_key", sent.to_value());
        let id = ts.client.xadd(entry).await.unwrap();

        assert!(!id.is_empty());
    }

    #[tokio::test]
    #[serial]
    async fn test_xadd_with_id() {
        let ts = Testsuite::create_test_client("group_1", "consumer_1").unwrap();

        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        let entry_id = format!("{}-0", since_the_epoch.as_millis());

        let sent = TestData::new("hello world");
        let entry = Entry::new("test_key", sent.to_value()).with_id(entry_id.clone());
        let id = ts.client.xadd(entry).await.unwrap();

        assert_eq!(id, entry_id);
    }

    #[tokio::test]
    #[serial]
    async fn test_xack_unknown_id() {
        let ts = Testsuite::create_test_client("group_1", "consumer_1").unwrap();

        let sent = TestData::new("hello world");
        let entry = Entry::new("test_key", sent.to_value());

        let ret = ts.client.xack(&entry).await;
        assert!(ret.is_err());
    }

    #[tokio::test]
    #[serial]
    async fn test_xack_undelivered_id() {
        let ts = Testsuite::create_test_client("group_1", "consumer_1").unwrap();

        let sent = TestData::new("hello world");
        let mut entry = Entry::new("test_key", sent.to_value());
        let id = ts.client.xadd(entry.clone()).await.unwrap();

        entry.id = Some(id.clone());

        let ret = ts.client.xack(&entry).await;
        assert!(ret.is_err());
    }

    #[tokio::test]
    #[serial]
    async fn test_xack_ok() {
        let ts = Testsuite::create_test_client("group_1", "consumer_1").unwrap();

        let test_key = "key_foo";
        ts.client.create_groups(&[test_key]).await;

        let sent = TestData::new("hello world");
        let entry_in = Entry::new(test_key, sent.to_value());
        let id = ts.client.xadd(entry_in.clone()).await.unwrap();

        let entry_out = ts.client.xread_one(test_key).await.unwrap();

        let ack_id = ts.client.xack(&entry_out).await.unwrap();
        assert_eq!(ack_id, id);
    }

    // This test verifies that all messages sent to a single consumer group
    // are delivered in exactly the same order they were published.
    #[tokio::test]
    #[serial]
    async fn test_read_one_group() {
        let ts = Testsuite::create_test_client("group_1", "consumer_1").unwrap();

        let test_key = "key_foo";
        ts.client.create_groups(&[test_key]).await;

        // TODO: Can be simpler??
        let (mut read_tx, mut read_rx) = channel(100);
        let conn = ts.client.get_async_connection().await.unwrap();
        let opts = ts.client.get_read_options();
        task::spawn(async move {
            RedisClient::read_loop(conn, opts, &[test_key], &mut read_tx)
                .await
                .unwrap();
        });

        // Create and send test data
        let test_messages = vec!["test1", "test2", "test3"];
        for msg in &test_messages {
            let entry = Entry::new(test_key, TestData::new(msg).to_value());
            ts.client.xadd(entry).await.unwrap();
        }

        // Verify received messages
        for expected_msg in test_messages {
            let received = read_rx.next().await.unwrap();
            let received_data = TestData::from_value(received.fields);
            assert_eq!(TestData::new(expected_msg), received_data);
        }
    }

    // This test verifies that all messages sent to two separate consumer groups
    // are delivered in the correct order within each group.
    #[tokio::test]
    #[serial]
    async fn test_read_two_groups() {
        let ts_1 = Testsuite::create_test_client("group_1", "consumer_1").unwrap();
        let ts_2 = Testsuite::create_test_client("group_2", "consumer_1").unwrap();

        let test_key = "key_foo";
        ts_1.client.create_groups(&[test_key]).await;
        ts_2.client.create_groups(&[test_key]).await;

        let (mut read_tx_1, mut read_rx_1) = channel(100);
        let conn_1 = ts_1.client.get_async_connection().await.unwrap();
        let opts_1 = ts_1.client.get_read_options();
        task::spawn(async move {
            RedisClient::read_loop(conn_1, opts_1, &[test_key], &mut read_tx_1)
                .await
                .unwrap();
        });

        let (mut read_tx_2, mut read_rx_2) = channel(100);
        let conn_2 = ts_2.client.get_async_connection().await.unwrap();
        let opts_2 = ts_2.client.get_read_options();
        task::spawn(async move {
            RedisClient::read_loop(conn_2, opts_2, &[test_key], &mut read_tx_2)
                .await
                .unwrap();
        });

        // Create and send test data
        let test_messages = vec!["test1", "test2", "test3"];
        for msg in &test_messages {
            let entry = Entry::new(test_key, TestData::new(msg).to_value());
            ts_1.client.xadd(entry).await.unwrap();
        }

        // Verify received messages
        for expected_msg in test_messages {
            let received_1 = read_rx_1.next().await.unwrap();
            let received_data_1 = TestData::from_value(received_1.fields);
            assert_eq!(TestData::new(expected_msg), received_data_1);

            let received_2 = read_rx_2.next().await.unwrap();
            let received_data_2 = TestData::from_value(received_2.fields);
            assert_eq!(TestData::new(expected_msg), received_data_2);
        }
    }

    // This test verifies that all messages sent to two consumers
    // are delivered only once to each consumer, and all messages are received.
    #[tokio::test]
    #[serial]
    async fn test_two_consumers() {
        let ts_1 = Testsuite::create_test_client("group_1", "consumer_1").unwrap();
        let ts_2 = Testsuite::create_test_client("group_1", "consumer_2").unwrap();

        let test_key = "key_foo";
        ts_1.client.create_groups(&[test_key]).await;
        ts_2.client.create_groups(&[test_key]).await;

        let (mut read_tx_1, mut read_rx_1) = channel(100);
        let conn_1 = ts_1.client.get_async_connection().await.unwrap();
        let opts_1 = ts_1.client.get_read_options();
        task::spawn(async move {
            RedisClient::read_loop(conn_1, opts_1, &[test_key], &mut read_tx_1)
                .await
                .unwrap();
        });

        let (mut read_tx_2, mut read_rx_2) = channel(100);
        let conn_2 = ts_2.client.get_async_connection().await.unwrap();
        let opts_2 = ts_2.client.get_read_options();
        task::spawn(async move {
            RedisClient::read_loop(conn_2, opts_2, &[test_key], &mut read_tx_2)
                .await
                .unwrap();
        });

        // Create and send test data
        let test_messages = vec!["test1", "test2", "test3"];
        for msg in &test_messages {
            let entry = Entry::new(test_key, TestData::new(msg).to_value());
            ts_1.client.xadd(entry).await.unwrap();
        }

        // Verify received messages
        let mut received = Vec::new();

        for _ in 0..3 {
            select! {
                read_option = read_rx_1.next() => if let Some(stream_in) = read_option {
                    received.push(TestData::from_value(stream_in.fields))
                },
                read_option = read_rx_2.next() => if let Some(stream_in) = read_option {
                    received.push(TestData::from_value(stream_in.fields))
                },
            }
        }

        for _ in 0..3 {
            let received_data = received.pop().unwrap();
            assert!(test_messages.contains(&received_data.field.as_str()));
        }
    }
}
