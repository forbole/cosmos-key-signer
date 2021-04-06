//! Transaction Message representation
use prost_types::Any;

/// Transaction message wrapper
pub struct Msg(pub Any);

impl Msg {
    pub fn new(type_url: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        Msg(Any {
            type_url: type_url.into(),
            value: value.into(),
        })
    }
}

impl From<Any> for Msg {
    fn from(any: Any) -> Msg {
        Msg(any)
    }
}

impl From<Msg> for Any {
    fn from(msg: Msg) -> Any {
        msg.0
    }
}
