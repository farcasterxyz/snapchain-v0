pub const PAGE_SIZE_MAX: usize = 1_000;
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
