#[cfg(test)]
mod tests {
    use crate::bus::StreamBus;
    use crate::client::RedisClient;
    use crate::error::{RedisBusError, Result};
    use crate::stream::Stream;
    use futures::channel::mpsc::channel;
    use futures::channel::oneshot::Receiver;
    use futures::{SinkExt, StreamExt};
    use redis::Value;
    use serde::{Deserialize, Serialize};
    use std::collections::{BTreeMap, HashMap};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::{select, task};

    const REDIS_CON: &str = "redis://localhost:6379";

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

    #[derive(Serialize, Deserialize, Debug, Eq, PartialEq)]
    struct TestData {
        f1: String,
    }

    impl TestData {
        fn new(msg: &str) -> Self {
            TestData { f1: msg.to_owned() }
        }

        fn from_value(value: Value) -> Self {
            let de = serde_redis::Deserializer::new(value);
            let decoded: TestData = Deserialize::deserialize(de).unwrap();

            decoded
        }

        fn to_value(&self) -> Value {
            self.serialize(serde_redis::Serializer).unwrap()
        }
    }

    #[tokio::test]
    async fn test_add_stream() {
        let mut client = RedisClient::new(REDIS_CON, "group_1", "consumer_1").unwrap();

        let mut add_tx = client.xadd_sender();
        let (mut read_tx, mut read_rx) = channel(100);

        task::spawn(async move {
            client.run(&["key_read_id"], &mut read_tx).await.unwrap();
        });

        let sent = TestData::new("hello world");
        let stream_out = Stream::new("key_read_id", None, sent.to_value());

        add_tx.send(stream_out.clone()).await.unwrap();

        let stream_in = read_rx.next().await.unwrap();
        let received = TestData::from_value(stream_in.fields);

        assert_eq!(sent, received);
    }

    #[tokio::test]
    async fn test_add_stream_with_id() {
        let mut client = RedisClient::new(REDIS_CON, "group_1", "consumer_1").unwrap();

        let mut add_tx = client.xadd_sender();
        let (mut read_tx, mut read_rx) = channel(100);

        task::spawn(async move {
            client.run(&["key_read_id"], &mut read_tx).await.unwrap();
        });

        let sent = TestData::new("hello world");
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        let stream_id = Some(format!("{}-0", since_the_epoch.as_millis()));
        let stream_out = Stream::new("key_read_id", stream_id.clone(), sent.to_value());

        add_tx.send(stream_out.clone()).await.unwrap();

        let stream_in = read_rx.next().await.unwrap();
        let received = TestData::from_value(stream_in.fields);

        assert_eq!(sent, received);
        assert_eq!(stream_id, stream_out.id);
    }

    #[tokio::test]
    async fn test_read_one_group() {
        let mut client = RedisClient::new(REDIS_CON, "group_1", "consumer_1").unwrap();

        let mut add_tx = client.xadd_sender();
        let (mut read_tx, mut read_rx) = channel(100);

        task::spawn(async move {
            client
                .run(&["key_read_one_group"], &mut read_tx)
                .await
                .unwrap();
        });

        let sent = TestData::new("hello world");
        let stream_out = Stream::new("key_read_one_group", None, sent.to_value());

        add_tx.send(stream_out.clone()).await.unwrap();

        let stream_in = read_rx.next().await.unwrap();
        let received = TestData::from_value(stream_in.fields);

        assert_eq!(sent, received);
        assert_eq!(stream_in.key, stream_out.key);
    }

    #[tokio::test]
    async fn test_read_two_groups() {
        let mut client_1 = RedisClient::new(REDIS_CON, "group_1", "consumer_1").unwrap();
        let mut client_2 = RedisClient::new(REDIS_CON, "group_2", "consumer_1").unwrap();

        let mut add_tx_1 = client_1.xadd_sender();
        let (mut read_tx_1, mut read_rx_1) = channel(100);
        let (mut read_tx_2, mut read_rx_2) = channel(100);

        task::spawn(async move {
            client_1.run(&["foo"], &mut read_tx_1).await.unwrap();
        });

        task::spawn(async move {
            client_2.run(&["foo", "bar"], &mut read_tx_2).await.unwrap();
        });

        let sent_1 = TestData::new("test1");
        let sent_2 = TestData::new("test2");
        let sent_3 = TestData::new("test3");

        let stream_out_1 = Stream::new("key_1", None, sent_1.to_value());
        let stream_out_2 = Stream::new("key_2", None, sent_2.to_value());
        let stream_out_3 = Stream::new("key_3", None, sent_3.to_value());

        add_tx_1.send(stream_out_1.clone()).await.unwrap();
        add_tx_1.send(stream_out_2.clone()).await.unwrap();
        add_tx_1.send(stream_out_3.clone()).await.unwrap();

        let stream_in_1 = read_rx_1.next().await.unwrap();
        let stream_in_2 = read_rx_2.next().await.unwrap();
        let stream_in_3 = read_rx_2.next().await.unwrap();

        let received_1 = TestData::from_value(stream_in_1.fields);
        let received_2 = TestData::from_value(stream_in_2.fields);
        let received_3 = TestData::from_value(stream_in_3.fields);

        assert_eq!(sent_1, received_1);
        assert_eq!(sent_2, received_2);
        assert_eq!(sent_3, received_3);
    }

    #[tokio::test]
    async fn test_two_consumers() {
        let mut consumer_1 = RedisClient::new(REDIS_CON, "group_1", "consumer_1").unwrap();
        let mut consumer_2 = RedisClient::new(REDIS_CON, "group_1", "consumer_2").unwrap();

        let mut add_tx_1 = consumer_1.xadd_sender();
        let (mut read_tx_1, mut read_rx_1) = channel(100);
        let (mut read_tx_2, mut read_rx_2) = channel(100);

        task::spawn(async move {
            consumer_1
                .run(&["key_2_consumers"], &mut read_tx_1)
                .await
                .unwrap();
        });

        task::spawn(async move {
            consumer_2
                .run(&["key_2_consumers"], &mut read_tx_2)
                .await
                .unwrap();
        });

        let sent = [
            TestData::new("test1"),
            TestData::new("test2"),
            TestData::new("test3"),
            TestData::new("test4"),
        ];

        let streams_out = [
            Stream::new("key_2_consumers", None, sent[0].to_value()),
            Stream::new("key_2_consumers", None, sent[1].to_value()),
            Stream::new("key_2_consumers", None, sent[2].to_value()),
            Stream::new("key_2_consumers", None, sent[3].to_value()),
        ];

        add_tx_1.send(streams_out[0].clone()).await.unwrap();
        add_tx_1.send(streams_out[1].clone()).await.unwrap();
        add_tx_1.send(streams_out[2].clone()).await.unwrap();
        add_tx_1.send(streams_out[3].clone()).await.unwrap();

        let mut received = Vec::new();

        for _ in 0..4 {
            select! {
                read_option = read_rx_1.next() => if let Some(stream_in) = read_option {
                    received.push(TestData::from_value(stream_in.fields))
                },
                read_option = read_rx_2.next() => if let Some(stream_in) = read_option {
                    received.push(TestData::from_value(stream_in.fields))
                },
            }
        }

        assert!(received.contains(&sent[0]));
        assert!(received.contains(&sent[1]));
        assert!(received.contains(&sent[2]));
        assert!(received.contains(&sent[3]));
    }

    #[tokio::test]
    async fn test_ack() {
        let mut client_1 = RedisClient::new(REDIS_CON, "group_1", "consumer_1").unwrap();

        let mut add_tx = client_1.xadd_sender();
        let mut ack_tx = client_1.xack_sender();
        let (mut read_tx_1, mut read_rx_1) = channel(100);

        task::spawn(async move {
            client_1.run(&["key_ack"], &mut read_tx_1).await.unwrap();
        });

        let sent_1 = TestData::new("test1");
        let sent_2 = TestData::new("test2");

        let stream_out_1 = Stream::new("key_1", None, sent_1.to_value());
        let stream_out_2 = Stream::new("key_2", None, sent_2.to_value());

        add_tx.send(stream_out_1.clone()).await.unwrap();
        let stream_in_1 = read_rx_1.next().await.unwrap();
        ack_tx.send(stream_in_1.clone()).await.unwrap();

        add_tx.send(stream_out_2.clone()).await.unwrap();
        read_rx_1.next().await.unwrap();
    }
}
