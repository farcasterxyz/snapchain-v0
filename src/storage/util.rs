use crate::core::error::HubError;

/**
 * The hashes in the sync trie are 20 bytes (160 bits) long, so we use the first 20 bytes of the blake3 hash
 */
const BLAKE3_HASH_LEN: usize = 20;
pub fn blake3_20(input: &[u8]) -> Vec<u8> {
    let mut hasher = blake3::Hasher::new();
    hasher.update(input);
    hasher.finalize().as_bytes()[..BLAKE3_HASH_LEN].to_vec()
}

pub fn bytes_compare(a: &[u8], b: &[u8]) -> i8 {
    let len = a.len().min(b.len());
    for i in 0..len {
        if a[i] < b[i] {
            return -1;
        }
        if a[i] > b[i] {
            return 1;
        }
    }
    if a.len() < b.len() {
        -1
    } else if a.len() > b.len() {
        1
    } else {
        0
    }
}

/** Increment the bytes of a Vec<u8> as if it were a big-endian number */
pub fn increment_vec_u8(vec: &Vec<u8>) -> Vec<u8> {
    let mut result = vec.clone(); // Clone the input vector to create a new one for the result
    let mut carry = true; // Start with a carry to simulate the increment

    // Iterate over the result vector from the least significant byte (end) to the most significant byte (start)
    for byte in result.iter_mut().rev() {
        if carry {
            if *byte == 255 {
                *byte = 0; // Reset and carry over
            } else {
                *byte += 1; // Increment the current byte
                carry = false; // No carry over needed, stop the loop
                break;
            }
        }
    }

    // If after processing all bytes there's still a carry, it means the vector was all 255s
    // and needs to be extended to represent the overflow (e.g., [255, 255] -> [1, 0, 0])
    if carry {
        result.insert(0, 1);
    }

    result
}

/**
 * Helper function to cast a vec into a [u8; 24] for TsHash
 */
pub fn vec_to_u8_24(vec: &Option<Vec<u8>>) -> Result<[u8; 24], HubError> {
    if let Some(vec) = vec {
        if vec.len() == 24 {
            let mut arr = [0u8; 24];
            arr.copy_from_slice(&vec);
            Ok(arr)
        } else {
            Err(HubError {
                code: "bad_request.internal_error".to_string(),
                message: format!("message_ts_hash is not 24 bytes: {:x?}", vec),
            })
        }
    } else {
        Err(HubError {
            code: "bad_request.internal_error".to_string(),
            message: "message_ts_hash is not 24 bytes: None".to_string(),
        })
    }
}

/** Derement the bytes of a Vec<u8> as if it were a big-endian number */
#[allow(dead_code)]
pub fn decrement_vec_u8(vec: &Vec<u8>) -> Vec<u8> {
    let mut result = vec.clone(); // Clone the input vector to create a new one for the result
    let mut borrow = true; // Start with a borrow to simulate the decrement

    // Iterate over the result vector from the least significant byte (end) to the most significant byte (start)
    for byte in result.iter_mut().rev() {
        if borrow {
            if *byte == 0 {
                *byte = 255; // Reset and borrow over
            } else {
                *byte -= 1; // Decrement the current byte
                borrow = false; // No borrow over needed, stop the loop
                break;
            }
        }
    }

    // If after processing all bytes there's still a borrow, it means the vector was all 0s
    // and needs to be extended to represent the underflow (e.g., [0, 0] -> [255])
    if borrow {
        result.pop();
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bytes_compare() {
        let a = vec![0, 0, 0];
        let b = vec![0, 0, 0];
        assert_eq!(bytes_compare(&a, &b), 0);

        let a = vec![0, 0, 0];
        let b = vec![0, 0, 1];
        assert_eq!(bytes_compare(&a, &b), -1);

        let a = vec![0, 0, 1];
        let b = vec![0, 0, 0];
        assert_eq!(bytes_compare(&a, &b), 1);
    }

    #[test]
    fn test_increment_vec_u8() {
        let vec = vec![0, 0, 0];
        let result = increment_vec_u8(&vec);
        assert_eq!(result, vec![0, 0, 1]);

        let vec = vec![0, 0, 255];
        let result = increment_vec_u8(&vec);
        assert_eq!(result, vec![0, 1, 0]);

        let vec = vec![0, 255, 255];
        let result = increment_vec_u8(&vec);
        assert_eq!(result, vec![1, 0, 0]);

        let vec = vec![255, 255, 255];
        let result = increment_vec_u8(&vec);
        assert_eq!(result, vec![1, 0, 0, 0]);

        let vec = vec![255, 255, 255];
        let result = increment_vec_u8(&vec);
        assert_eq!(result, vec![1, 0, 0, 0]);
    }

    #[test]
    fn test_decrement_vec_u8() {
        let vec = vec![0, 0, 1];
        let result = decrement_vec_u8(&vec);
        assert_eq!(result, vec![0, 0, 0]);

        let vec = vec![0, 1, 0];
        let result = decrement_vec_u8(&vec);
        assert_eq!(result, vec![0, 0, 255]);

        let vec = vec![1, 0, 0];
        let result = decrement_vec_u8(&vec);
        assert_eq!(result, vec![0, 255, 255]);

        let vec = vec![1, 0, 0, 0];
        let result = decrement_vec_u8(&vec);
        assert_eq!(result, vec![0, 255, 255, 255]);
    }
}
