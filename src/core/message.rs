use crate::proto::msg as message;
use crate::proto::snapchain;

impl message::Message {
    pub fn is_type(&self, message_type: message::MessageType) -> bool {
        self.data.is_some() && self.data.as_ref().unwrap().r#type == message_type as i32
    }

    pub fn fid(&self) -> u32 {
        if self.data.is_some() {
            self.data.as_ref().unwrap().fid as u32
        } else {
            0
        }
    }

    pub fn msg_type(&self) -> u8 {
        if self.data.is_some() {
            self.data.as_ref().unwrap().r#type as u8
        } else {
            0
        }
    }

    pub fn hex_hash(&self) -> String {
        hex::encode(&self.hash)
    }
}

impl snapchain::ValidatorMessage {
    pub fn fid(&self) -> u32 {
        if let Some(fname) = &self.fname_transfer {
            return fname.fid as u32;
        }
        if let Some(event) = &self.on_chain_event {
            return event.fid as u32;
        }
        0
    }
}
