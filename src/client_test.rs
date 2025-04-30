#[cfg(test)]
mod tests {
    use crate::bus::StreamBus;
    use crate::client::{RedisClient, test_utils};
    use crate::error::{RedisBusError, Result};
    use crate::stream::Stream;
    use futures::channel::mpsc::channel;
    use futures::{SinkExt, StreamExt};
    use redis::Value;
    // use redis_test::server::RedisServer;
    use serde::{Deserialize, Serialize};
    use std::collections::{BTreeMap, HashMap};
    use std::sync::atomic::{AtomicU64, Ordering};
    use std::time::{SystemTime, UNIX_EPOCH};
    use tokio::{select, task};

    const REDIS_CON: &str = "redis://localhost:6379";

    // Use a counter to generate unique keys for each test
    static TEST_COUNTER: AtomicU64 = AtomicU64::new(0);

    fn get_unique_id() -> u64 {
        TEST_COUNTER.fetch_add(1, Ordering::SeqCst)
    }

    // Generate unique keys for each test to avoid conflicts
    fn unique_key(prefix: &str) -> String {
        format!("{}_{}", prefix, get_unique_id())
    }

    // Generate unique group names for each test
    fn unique_group(prefix: &str) -> String {
        format!("{}_{}", prefix, get_unique_id())
    }

    // call it at the beginning of each test
    async fn setup() -> Result<()> {
        // Wait for Redis to be available
        test_utils::wait_for_redis(REDIS_CON, 5).await?;

        // Reset Redis to ensure a clean state
        test_utils::reset_redis(REDIS_CON).await?;

        Ok(())
    }

    //  run at the end of each test
    async fn cleanup() -> Result<()> {
        println!("Running cleanup...");
        match test_utils::reset_redis(REDIS_CON).await {
            Ok(_) => {
                println!("Successfully cleaned up Redis after test");
                Ok(())
            }
            Err(e) => {
                println!("Failed to clean up Redis after test: {:?}", e);
                Err(e)
            }
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
        // let _cleanup_guard = CleanupGuard;
        setup().await.expect("Failed to setup test env");

        let key = unique_key("key_read_id");
        let group = unique_group("group");
        let key_clone = key.clone();
        println!("Using key: {}, group: {}", key, group);

        let mut client = test_utils::create_test_client(REDIS_CON, &group, "consumer_1").unwrap();

        let mut add_tx = client.xadd_sender();
        let (mut read_tx, mut read_rx) = channel(100);

        task::spawn(async move {
            client.run(&[&key], &mut read_tx).await.unwrap();
        });

        // Add a small delay to ensure the client is ready
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let sent = TestData::new("hello world");
        let stream_out = Stream::new(&key_clone, None, sent.to_value());
        println!(
            "Created stream to send: key={}, id={:?}",
            stream_out.key, stream_out.id
        );

        add_tx.send(stream_out.clone()).await.unwrap();

        // let stream_in = read_rx.next().await.unwrap();
        let stream_in = tokio::time::timeout(tokio::time::Duration::from_secs(5), read_rx.next())
            .await
            .expect("Timeout waiting for message")
            .expect("No message received");
        let received = TestData::from_value(stream_in.fields);

        println!("Deserialized received message: {:?}", received);

        assert_eq!(sent, received);

        // Clean up at the end of the test
        cleanup().await.expect("Failed to clean up after test");
    }

    #[tokio::test]
    async fn test_add_stream_with_id() {
        // let _cleanup_guard = CleanupGuard;
        setup().await.expect("Failed to setup test env");

        let key = unique_key("key_read_id");
        let group = unique_group("group");
        let key_cloned = key.clone();

        let mut client = test_utils::create_test_client(REDIS_CON, &group, "consumer_1").unwrap();

        let mut add_tx = client.xadd_sender();
        let (mut read_tx, mut read_rx) = channel(100);

        task::spawn(async move {
            client.run(&[&key], &mut read_tx).await.unwrap();
        });
        // Add a small delay to ensure the client is ready
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

        let sent = TestData::new("hello world");
        let start = SystemTime::now();
        let since_the_epoch = start
            .duration_since(UNIX_EPOCH)
            .expect("Time went backwards");

        let stream_id = Some(format!(
            "{}-{}",
            since_the_epoch.as_millis(),
            get_unique_id()
        ));
        let stream_out = Stream::new(&key_cloned, stream_id.clone(), sent.to_value());

        add_tx.send(stream_out.clone()).await.unwrap();

        // let stream_in = read_rx.next().await.unwrap();
        // Use a timeout to prevent the test from hanging
        let stream_in = tokio::time::timeout(tokio::time::Duration::from_secs(5), read_rx.next())
            .await
            .expect("Timeout waiting for message")
            .expect("No message received");

        let received = TestData::from_value(stream_in.fields);

        assert_eq!(sent, received);
        assert_eq!(stream_id, stream_out.id);

        cleanup().await.expect("Failed to clean up after test");
    }

    // #[tokio::test]
    // async fn test_read_one_group() {
    //     setup().await.expect("Failed to setup test env");
    //     let mut client = test_utils::create_test_client(REDIS_CON, "group_1", "consumer_1").unwrap();

    //     let mut add_tx = client.xadd_sender();
    //     let (mut read_tx, mut read_rx) = channel(100);

    //     task::spawn(async move {
    //         client
    //             .run(&["key_read_one_group"], &mut read_tx)
    //             .await
    //             .unwrap();
    //     });

    //     // Add a small delay to ensure the client is ready
    //     tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    //     let sent = TestData::new("hello world");
    //     let stream_out = Stream::new("key_read_one_group", None, sent.to_value());

    //     add_tx.send(stream_out.clone()).await.unwrap();

    //     // let stream_in = read_rx.next().await.unwrap();
    //     // Use a timeout to prevent the test from hanging
    //     let stream_in = tokio::time::timeout(
    //         tokio::time::Duration::from_secs(5),
    //         read_rx.next()
    //     ).await.expect("Timeout waiting for message").expect("No message received");

    //     let received = TestData::from_value(stream_in.fields);

    //     assert_eq!(sent, received);
    //     assert_eq!(stream_in.key, stream_out.key);
    // }

    // #[tokio::test]
    // async fn test_read_two_groups() {
    //     setup().await.expect("Failed to setup test env");
    //     let mut client_1 = test_utils::create_test_client(REDIS_CON, "group_1", "consumer_1").unwrap();
    //     let mut client_2 = test_utils::create_test_client(REDIS_CON,"group_2", "consumer_1").unwrap();

    //     let mut add_tx_1 = client_1.xadd_sender();
    //     let (mut read_tx_1, mut read_rx_1) = channel(100);
    //     let (mut read_tx_2, mut read_rx_2) = channel(100);

    //     task::spawn(async move {
    //         client_1.run(&["foo"], &mut read_tx_1).await.unwrap();
    //     });

    //     task::spawn(async move {
    //         client_2.run(&["foo", "bar"], &mut read_tx_2).await.unwrap();
    //     });

    //     // Add a small delay to ensure both clients are ready
    //     tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    //     let sent_1 = TestData::new("test1");
    //     let sent_2 = TestData::new("test2");
    //     let sent_3 = TestData::new("test3");

    //     let stream_out_1 = Stream::new("key_1", None, sent_1.to_value());
    //     let stream_out_2 = Stream::new("key_2", None, sent_2.to_value());
    //     let stream_out_3 = Stream::new("key_3", None, sent_3.to_value());

    //     add_tx_1.send(stream_out_1.clone()).await.unwrap();
    //     add_tx_1.send(stream_out_2.clone()).await.unwrap();
    //     add_tx_1.send(stream_out_3.clone()).await.unwrap();

    //     // let stream_in_1 = read_rx_1.next().await.unwrap();
    //     // let stream_in_2 = read_rx_2.next().await.unwrap();
    //     // let stream_in_3 = read_rx_2.next().await.unwrap();

    //     // let received_1 = TestData::from_value(stream_in_1.fields);
    //     // let received_2 = TestData::from_value(stream_in_2.fields);
    //     // let received_3 = TestData::from_value(stream_in_3.fields);

    //     // We need to collect 3 messages - 2 from group_1 and 1 from group_2
    //     let mut received_messages = 0;
    //     let mut received_values = Vec::new();

    //     // Use a timeout to prevent the test from hanging
    //     let timeout = tokio::time::Duration::from_secs(5);

    //     let result = tokio::time::timeout(timeout, async {
    //         while received_messages < 3 {
    //             select! {
    //                 read_option = read_rx_1.next() => if let Some(stream_in) = read_option {
    //                     received_values.push(TestData::from_value(stream_in.fields));
    //                     received_messages += 1;
    //                 },
    //                 read_option = read_rx_2.next() => if let Some(stream_in) = read_option {
    //                     received_values.push(TestData::from_value(stream_in.fields));
    //                     received_messages += 1;
    //                 },
    //             }
    //         }
    //     }).await;

    //     // Check if we timed out
    //     assert!(result.is_ok(), "Test timed out waiting for messages");

    //     // Check that we received all the expected values
    //     assert!(received_values.contains(&sent_1));
    //     assert!(received_values.contains(&sent_2));
    //     assert!(received_values.contains(&sent_3));

    //     // assert_eq!(sent_1, received_1);
    //     // assert_eq!(sent_2, received_2);
    //     // assert_eq!(sent_3, received_3);
    // }

    // #[tokio::test]
    // async fn test_two_consumers() {
    //     setup().await.expect("Failed to setup test env");
    //     let mut consumer_1 = test_utils::create_test_client(REDIS_CON, "group_1", "consumer_1").unwrap();
    //     let mut consumer_2 = test_utils::create_test_client(REDIS_CON, "group_1", "consumer_2").unwrap();

    //     let mut add_tx_1 = consumer_1.xadd_sender();
    //     let (mut read_tx_1, mut read_rx_1) = channel(100);
    //     let (mut read_tx_2, mut read_rx_2) = channel(100);

    //     task::spawn(async move {
    //         consumer_1
    //             .run(&["key_2_consumers"], &mut read_tx_1)
    //             .await
    //             .unwrap();
    //     });

    //     task::spawn(async move {
    //         consumer_2
    //             .run(&["key_2_consumers"], &mut read_tx_2)
    //             .await
    //             .unwrap();
    //     });

    //     // Add a small delay to ensure both consumers are ready
    //     tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    //     let sent = [
    //         TestData::new("test1"),
    //         TestData::new("test2"),
    //         TestData::new("test3"),
    //         TestData::new("test4"),
    //     ];

    //     let streams_out = [
    //         Stream::new("key_2_consumers", None, sent[0].to_value()),
    //         Stream::new("key_2_consumers", None, sent[1].to_value()),
    //         Stream::new("key_2_consumers", None, sent[2].to_value()),
    //         Stream::new("key_2_consumers", None, sent[3].to_value()),
    //     ];

    //     // add_tx_1.send(streams_out[0].clone()).await.unwrap();
    //     // add_tx_1.send(streams_out[1].clone()).await.unwrap();
    //     // add_tx_1.send(streams_out[2].clone()).await.unwrap();
    //     // add_tx_1.send(streams_out[3].clone()).await.unwrap();

    //     // Send all messages
    //     for stream in &streams_out {
    //         add_tx_1.send(stream.clone()).await.unwrap();
    //     }

    //     let mut received = Vec::new();

    //     let timeout = tokio::time::Duration::from_secs(5);

    //     // Collect all 4 messages with a timeout
    //     let result = tokio::time::timeout(timeout, async {
    //         for _ in 0..4 {
    //             select! {
    //                 read_option = read_rx_1.next() => if let Some(stream_in) = read_option {
    //                     received.push(TestData::from_value(stream_in.fields))
    //                 },
    //                 read_option = read_rx_2.next() => if let Some(stream_in) = read_option {
    //                     received.push(TestData::from_value(stream_in.fields))
    //                 },
    //             }
    //         }
    //     }).await;

    //     // Check if we timed out
    //     assert!(result.is_ok(), "Test timed out waiting for messages");

    //     // Verify we received all messages
    //     assert!(received.contains(&sent[0]));
    //     assert!(received.contains(&sent[1]));
    //     assert!(received.contains(&sent[2]));
    //     assert!(received.contains(&sent[3]));
    // }

    // #[tokio::test]
    // async fn test_ack() {
    //     setup().await.expect("Failed to setup test env");
    //     let mut client_1 = test_utils::create_test_client(REDIS_CON, "group_1", "consumer_1").unwrap();

    //     let mut add_tx = client_1.xadd_sender();
    //     let mut ack_tx = client_1.xack_sender();
    //     let (mut read_tx_1, mut read_rx_1) = channel(100);

    //     task::spawn(async move {
    //         client_1.run(&["key_ack"], &mut read_tx_1).await.unwrap();
    //     });

    //     // Add a small delay to ensure the client is ready
    //     tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    //     let sent_1 = TestData::new("test1");
    //     let sent_2 = TestData::new("test2");

    //     let stream_out_1 = Stream::new("key_1", None, sent_1.to_value());
    //     let stream_out_2 = Stream::new("key_2", None, sent_2.to_value());

    //     // Send first message
    //     add_tx.send(stream_out_1.clone()).await.unwrap();

    //     // let stream_in_1 = read_rx_1.next().await.unwrap();
    //     // Receive and acknowledge first message
    //     let stream_in_1 = tokio::time::timeout(
    //         tokio::time::Duration::from_secs(5),
    //         read_rx_1.next()
    //     ).await.expect("Timeout waiting for message").expect("No message received");

    //     ack_tx.send(stream_in_1.clone()).await.unwrap();

    //     // Send 2nd mesg
    //     add_tx.send(stream_out_2.clone()).await.unwrap();

    //     // read_rx_1.next().await.unwrap();
    //     // Receive second message
    //     let stream_in_2 = tokio::time::timeout(
    //         tokio::time::Duration::from_secs(5),
    //         read_rx_1.next()
    //     ).await.expect("Timeout waiting for message").expect("No message received");
    //     // Verify messages
    //     let received_1 = TestData::from_value(stream_in_1.fields);
    //     let received_2 = TestData::from_value(stream_in_2.fields);

    //     assert_eq!(sent_1, received_1);
    //     assert_eq!(sent_2, received_2);
    // }

    // // Add a new test to verify start_id functionality
    // #[tokio::test]
    // async fn test_start_id_dollar_sign() {
    //     setup().await.expect("Failed to setup test env");

    //     // First, add some messages to the stream
    //     let mut pre_client = test_utils::create_test_client(REDIS_CON, "pre_group", "pre_consumer").unwrap();
    //     let mut pre_add_tx = pre_client.xadd_sender();

    //     // Send a message that should NOT be processed by the "$" start_id client
    //     let pre_message = TestData::new("pre-existing message");
    //     let pre_stream = Stream::new("start_id_test", None, pre_message.to_value());

    //     // Create a channel just to keep the pre_client running
    //     let (mut pre_read_tx, _) = channel(100);

    //     task::spawn(async move {
    //         pre_client.run(&["start_id_test"], &mut pre_read_tx).await.unwrap();
    //     });

    //     // Add a small delay to ensure the client is ready
    //     tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    //     // Send the pre-existing message
    //     pre_add_tx.send(pre_stream).await.unwrap();

    //     // Wait a moment to ensure the message is stored
    //     tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    //     // Now create a client with "$" start_id that should only see new messages
    //     let mut dollar_client = test_utils::create_test_client(REDIS_CON, "dollar_group", "dollar_consumer")
    //         .unwrap()
    //         .with_start_id("$");

    //     let mut dollar_add_tx = dollar_client.xadd_sender();
    //     let (mut dollar_read_tx, mut dollar_read_rx) = channel(100);

    //     task::spawn(async move {
    //         dollar_client.run(&["start_id_test"], &mut dollar_read_tx).await.unwrap();
    //     });

    //     // Wait for the client to initialize
    //     tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    //     // Send a new message that SHOULD be processed
    //     let new_message = TestData::new("new message");
    //     let new_stream = Stream::new("start_id_test", None, new_message.to_value());

    //     dollar_add_tx.send(new_stream).await.unwrap();

    //     // We should receive only the new message, not the pre-existing one
    //     let received_stream = tokio::time::timeout(
    //         tokio::time::Duration::from_secs(5),
    //         dollar_read_rx.next()
    //     ).await.unwrap().unwrap();

    //     let received_data = TestData::from_value(received_stream.fields);

    //     // Verify we got the new message
    //     assert_eq!(received_data, new_message);

    //     // Try to receive another message - this should time out because the pre-existing
    //     // message should not be delivered to this consumer group
    //     let timeout_result = tokio::time::timeout(
    //         tokio::time::Duration::from_secs(1),
    //         dollar_read_rx.next()
    //     ).await;

    //     assert!(timeout_result.is_err(), "Should not receive the pre-existing message");
    // }

    // // Add a test for the zero start_id
    // #[tokio::test]
    // async fn test_start_id_zero() {
    //     setup().await.expect("Failed to setup test env");

    //     // First, add some messages to the stream
    //     let mut pre_client = test_utils::create_test_client(REDIS_CON, "pre_group", "pre_consumer").unwrap();
    //     let mut pre_add_tx = pre_client.xadd_sender();

    //     // Send a message that SHOULD be processed by the "0" start_id client
    //     let pre_message = TestData::new("pre-existing message");
    //     let pre_stream = Stream::new("start_id_zero_test", None, pre_message.to_value());

    //     // Create a channel just to keep the pre_client running
    //     let (mut pre_read_tx, _) = channel(100);

    //     task::spawn(async move {
    //         pre_client.run(&["start_id_zero_test"], &mut pre_read_tx).await.unwrap();
    //     });

    //     // Add a small delay to ensure the client is ready
    //     tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    //     // Send the pre-existing message
    //     pre_add_tx.send(pre_stream).await.unwrap();

    //     // Wait a moment to ensure the message is stored
    //     tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    //     // Now create a client with "0" start_id that should see all messages
    //     let mut zero_client =test_utils::create_test_client(REDIS_CON, "zero_group", "zero_consumer")
    //         .unwrap()
    //         .with_start_id("0");  // This is the default, but we're explicit here

    //     let mut zero_add_tx = zero_client.xadd_sender();
    //     let (mut zero_read_tx, mut zero_read_rx) = channel(100);

    //     task::spawn(async move {
    //         zero_client.run(&["start_id_zero_test"], &mut zero_read_tx).await.unwrap();
    //     });

    //     // Wait for the client to initialize
    //     tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;

    //     // Send a new message
    //     let new_message = TestData::new("new message");
    //     let new_stream = Stream::new("start_id_zero_test", None, new_message.to_value());

    //     zero_add_tx.send(new_stream).await.unwrap();

    //     // We should receive both messages - collect them
    //     let mut received_messages = Vec::new();

    //     for _ in 0..2 {
    //         let received_stream = tokio::time::timeout(
    //             tokio::time::Duration::from_secs(5),
    //             zero_read_rx.next()
    //         ).await.unwrap().unwrap();

    //         received_messages.push(TestData::from_value(received_stream.fields));
    //     }

    //     // Verify we got both messages
    //     assert!(received_messages.contains(&pre_message));
    //     assert!(received_messages.contains(&new_message));
    // }
}
