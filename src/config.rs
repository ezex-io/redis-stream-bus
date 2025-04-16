use clap::Args;

#[derive(Debug, Clone, Args)]
#[group(id = "redis")]
pub struct Config {
    #[arg(long = "redis-connection-string", env = "REDIS_CONNECTION_STRING")]
    pub connection_string: String,
    #[arg(long = "redis-group-name", env = "REDIS_GROUP_NAME")]
    pub group_name: String,
    #[arg(long = "redis-consumer-name", env = "REDIS_CONSUMER_NAME")]
    pub consumer_name: String,
}
