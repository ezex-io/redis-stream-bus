use super::bus::StreamBus;
use crate::entry::Entry;
use crate::error::Result;
use mockall::mock;

mock! {
    pub StreamBus1 {}

    #[async_trait::async_trait]
    impl StreamBus for StreamBus1{
        async fn xadd(&mut self, entry: Entry) -> Result<String>;
        async fn xack(&mut self, entry: &Entry) -> Result<String>;
    }
}
