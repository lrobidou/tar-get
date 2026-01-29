use serde::{Deserialize, Serialize};
use std::{
    fs::File,
    io::{Read, Seek, SeekFrom, Write},
    path::Path,
};

pub fn serialize<T: Serialize>(path: &str, v: &[T]) {
    let mut file = File::create(path).unwrap();

    let count = v.len() as u64;

    file.write_all(&count.to_le_bytes()).unwrap();
    // reserve space for the header
    for _ in 0..count {
        file.write_all(&0u64.to_le_bytes()).unwrap();
    }

    let payload_start = file.stream_position().unwrap();

    // stream payload and record offsets
    let mut offsets = Vec::with_capacity(v.len());
    for item in v {
        let pos = file.stream_position().unwrap();
        offsets.push(pos - payload_start);
        bincode::serialize_into(&mut file, item).unwrap();
    }

    // rewrite header
    file.seek(SeekFrom::Start(8)).unwrap();
    for off in offsets {
        file.write_all(&off.to_le_bytes()).unwrap();
    }
}

pub fn deserialize<P, T: for<'a> Deserialize<'a>>(path: P, i: usize) -> T
where
    P: AsRef<Path>,
{
    let mut file = File::open(path).unwrap();

    let mut buf = [0u8; 8];
    file.read_exact(&mut buf).unwrap();
    let count = u64::from_le_bytes(buf) as usize;

    let mut offsets = vec![0u64; count + 1];
    for j in 0..count {
        file.read_exact(&mut buf).unwrap();
        offsets[j] = u64::from_le_bytes(buf);
    }

    let data_start = 8 + count * 8;
    let start = data_start as u64 + offsets[i];
    let end = if i + 1 < count {
        data_start as u64 + offsets[i + 1]
    } else {
        file.seek(SeekFrom::End(0)).unwrap()
    };

    let len = (end - start) as usize;
    let mut data = vec![0u8; len];

    file.seek(SeekFrom::Start(start)).unwrap();
    file.read_exact(&mut data).unwrap();

    bincode::deserialize(&data).unwrap()
}

pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

#[cfg(test)]
mod tests {

    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Data {
        u: u16,
        v: Vec<u8>,
        i: i64,
    }

    #[test]
    fn one_struct() {
        let path = "path.batar";
        let data = [Data {
            u: u16::MAX,
            v: vec![8, 98, 9],
            i: 65454254,
        }];
        serialize(path, &data);
        let result = deserialize(path, 0);
        assert_eq!(data[0], result);
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn multiple_structs() {
        let path = "path2.batar";
        let data = [
            Data {
                u: u16::MAX,
                v: vec![8, 98, 9],
                i: 65454254,
            },
            Data {
                u: 0,
                v: vec![8, 6, 200],
                i: 6542345420,
            },
            Data {
                u: u16::MAX,
                v: vec![79, 45, 1],
                i: 2385243420343543114,
            },
        ];
        serialize(path, &data);
        for (i, expected) in data.iter().enumerate() {
            assert_eq!(expected, &deserialize(path, i));
        }
        std::fs::remove_file(path).unwrap();
    }
}
