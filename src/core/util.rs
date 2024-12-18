use crate::core::error::HubError;
use crate::core::types::FARCASTER_EPOCH;

#[allow(dead_code)]
pub fn to_farcaster_time(time_ms: u64) -> Result<u64, HubError> {
    if time_ms < FARCASTER_EPOCH {
        return Err(HubError {
            code: "bad_request.invalid_param".to_string(),
            message: format!("time_ms is before the farcaster epoch: {}", time_ms),
        });
    }

    let seconds_since_epoch = (time_ms - FARCASTER_EPOCH) / 1000;
    if seconds_since_epoch > u32::MAX as u64 {
        return Err(HubError {
            code: "bad_request.invalid_param".to_string(),
            message: format!("time too far in future: {}", time_ms),
        });
    }

    Ok(seconds_since_epoch as u64)
}

#[allow(dead_code)]
pub fn from_farcaster_time(time: u64) -> u64 {
    time * 1000 + FARCASTER_EPOCH
}

#[allow(dead_code)]
pub fn get_farcaster_time() -> Result<u64, HubError> {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_err(|e| HubError {
            code: "internal_error".to_string(),
            message: format!("failed to get time: {}", e),
        })?;
    Ok(to_farcaster_time(now.as_millis() as u64)?)
}

pub fn calculate_message_hash(data_bytes: &[u8]) -> Vec<u8> {
    blake3::hash(data_bytes).as_bytes()[0..20].to_vec()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_farcaster_time() {
        let time = get_farcaster_time().unwrap();
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64;
        assert!(time <= now);
    }

    #[test]
    fn to_farcaster_time_test() {
        // It is an error to pass a time before the farcaster epoch
        let time = to_farcaster_time(0);
        assert!(time.is_err());

        let time = to_farcaster_time(FARCASTER_EPOCH - 1);
        assert!(time.is_err());

        let time = to_farcaster_time(FARCASTER_EPOCH).unwrap();
        assert_eq!(time, 0);

        let time = to_farcaster_time(FARCASTER_EPOCH + 1000).unwrap();
        assert_eq!(time, 1);
    }
}
