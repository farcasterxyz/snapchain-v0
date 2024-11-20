use crate::proto::msg as message;

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

    pub fn msg_type(&self) -> u32 {
        if self.data.is_some() {
            self.data.as_ref().unwrap().r#type as u32
        } else {
            0
        }
    }
}
