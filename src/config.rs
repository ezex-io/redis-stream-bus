use serde::{Deserialize, Serialize};
use structopt::StructOpt;

#[derive(Debug, StructOpt, Clone, Serialize, Deserialize)]
pub struct Config {
    #[structopt(long = "redis-connection-string", env = "REDIS_CONNECTION_STRING")]
    pub connection_string: String,
    #[structopt(long = "redis-group-name", env = "REDIS_GROUP_NAME")]
    pub group_name: String,
    #[structopt(long = "redis-consumer", env = "REDIS_CONSUMER")]
    pub consumer_name: String,
}
