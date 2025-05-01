use std::string::FromUtf8Error;

use redis::RedisError;

#[derive(Debug, PartialEq)]
pub enum RedisBusError {
    Redis(RedisError),
    InvalidData(String),
    Timeout(String),
    InternalError(String),
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

impl std::fmt::Display for RedisBusError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::result::Result<(), std::fmt::Error> {
        match self {
            RedisBusError::Redis(err) => {
                f.write_str("Redis data: ")?;
                err.fmt(f)
            }
            RedisBusError::InvalidData(err) => {
                f.write_str("Invalid data: ")?;
                err.fmt(f)
            }
            RedisBusError::Timeout(err) => {
                f.write_str("Timeout: ")?;
                err.fmt(f)
            }
            RedisBusError::InternalError(err) => {
                f.write_str("Internal error: ")?;
                err.fmt(f)
            }
        }
    }
}

pub type Result<T> = std::result::Result<T, RedisBusError>;
