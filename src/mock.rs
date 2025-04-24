use super::bus::StreamBus;
use crate::error::Result;
use crate::stream::Stream;
use futures::channel::mpsc::Sender;
use mockall::mock;

mock! {
    pub StreamBus1 {}

    #[async_trait::async_trait]
    impl StreamBus for StreamBus1{
        fn xadd_sender(&self) -> Sender<Stream>;
        fn xack_sender(&self) -> Sender<Stream>;
        async fn run<'a, 'b>(
            &mut self,
            keys: &[&'a str],
            read_tx: &'b mut Sender<Stream>,
        ) -> Result<()>;
    }
}
