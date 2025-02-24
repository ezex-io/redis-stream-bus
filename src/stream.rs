use std::collections::HashMap;

///
/// Stream is the data structure to keep streams nice and tidy
#[derive(Debug, Clone)]
pub struct Stream {
    /// redis streams Id auto added via redis node
    pub id: Option<String>,
    /// redis stream this streams belongs to
    pub key: String,
    /// data embedded in the event
    pub fields: HashMap<String, redis::Value>,
}

impl Stream {
    pub fn new(key: &str, id: Option<String>, fields: HashMap<String, redis::Value>) -> Self {
        Stream {
            id,
            key: key.to_owned(),
            fields,
        }
    }
}
