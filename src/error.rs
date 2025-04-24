use std::string::FromUtf8Error;

use redis::RedisError;

#[derive(Debug, PartialEq)]
pub enum RedisBusError {
    Redis(RedisError),
    InvalidData(String),
}

impl From<FromUtf8Error> for RedisBusError {
    fn from(err: FromUtf8Error) -> Self {
        RedisBusError::InvalidData(err.to_string())
    }
}

impl From<RedisError> for RedisBusError {
    fn from(err: RedisError) -> Self {
        RedisBusError::Redis(err)
    }
}

pub type Result<T> = std::result::Result<T, RedisBusError>;
