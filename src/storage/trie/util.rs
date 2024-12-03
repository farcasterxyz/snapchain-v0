fn expand_nibbles(input: Vec<u8>) -> Vec<u8> {
    let mut result = Vec::with_capacity(input.len() * 2);
    for byte in input {
        let high_nibble = (byte >> 4) & 0x0F;
        let low_nibble = byte & 0x0F;
        result.push(high_nibble);
        result.push(low_nibble);
    }
    result
}

fn combine_nibbles(input: Vec<u8>) -> Vec<u8> {
    assert!(input.len() % 2 == 0, "Input length must be even");
    let mut result = Vec::with_capacity(input.len() / 2);
    for chunk in input.chunks(2) {
        let high_nibble = chunk[0] << 4;
        let low_nibble = chunk[1] & 0x0F;
        result.push(high_nibble | low_nibble);
    }
    result
}

fn expand_quibbles(input: Vec<u8>) -> Vec<u8> {
    let mut result = Vec::with_capacity(input.len() * 4);
    for byte in input {
        let q1 = (byte >> 6) & 0x03;
        let q2 = (byte >> 4) & 0x03;
        let q3 = (byte >> 2) & 0x03;
        let q4 = byte & 0x03;
        result.push(q1);
        result.push(q2);
        result.push(q3);
        result.push(q4);
    }
    result
}

fn combine_quibbles(input: Vec<u8>) -> Vec<u8> {
    assert!(input.len() % 4 == 0, "Input length must be a multiple of 4");
    let mut result = Vec::with_capacity(input.len() / 4);
    for chunk in input.chunks(4) {
        let q1 = (chunk[0] & 0x03) << 6;
        let q2 = (chunk[1] & 0x03) << 4;
        let q3 = (chunk[2] & 0x03) << 2;
        let q4 = chunk[3] & 0x03;
        result.push(q1 | q2 | q3 | q4);
    }
    result
}

fn expand_byte(input: Vec<u8>) -> Vec<u8> {
    input
}

fn combine_byte(input: Vec<u8>) -> Vec<u8> {
    input
}

#[derive(Clone)]
pub struct BranchingFactorTransform {
    pub expand: fn(Vec<u8>) -> Vec<u8>,
    pub combine: fn(Vec<u8>) -> Vec<u8>,
}

pub fn get_transform_functions(branching_factor: u32) -> Option<BranchingFactorTransform> {
    match branching_factor {
        4 => Some(BranchingFactorTransform {
            expand: expand_quibbles,
            combine: combine_quibbles,
        }),
        16 => Some(BranchingFactorTransform {
            expand: expand_nibbles,
            combine: combine_nibbles,
        }),
        256 => Some(BranchingFactorTransform {
            expand: expand_byte,
            combine: combine_byte,
        }),
        _ => None,
    }
}
