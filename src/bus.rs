use crate::entry::Entry;
use crate::error::Result;
use async_trait::async_trait;

#[async_trait]
pub trait StreamBus {
    async fn xadd(&mut self, entry: Entry) -> Result<String>;
    async fn xack(&mut self, entry: &Entry) -> Result<String>;
}
