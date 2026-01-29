use serde::{Deserialize, Serialize};
use std::io::{self, Read, Seek, SeekFrom, Write};

#[derive(thiserror::Error, Debug)]
pub enum SerializationError<T> {
    IO(#[from] io::Error),
    Serialization(T),
}

pub fn serialize<T, F, WS, E>(
    writer: &mut WS,
    v: &[T],
    mut parser: F,
) -> Result<(), SerializationError<E>>
where
    T: Serialize,
    WS: Write + Seek,
    F: for<'w> FnMut(&'w mut WS, &T) -> Result<(), E>,
    E: std::error::Error,
{
    let count = v.len() as u64;
    let start_position = writer.stream_position()?;
    writer.write_all(&count.to_le_bytes())?;
    // reserve space for the header
    for _ in 0..count {
        writer.write_all(&0u64.to_le_bytes())?;
    }

    let payload_start = writer.stream_position()?;

    // stream payload and record offsets
    let mut offsets = Vec::with_capacity(v.len());
    for item in v {
        let pos = writer.stream_position()?;
        offsets.push(pos - payload_start);
        parser(writer, item).map_err(SerializationError::Serialization)?;
    }

    let end_position = writer.stream_position()?;

    // rewrite header
    writer.seek(SeekFrom::Start(start_position + 8))?;
    for off in offsets {
        writer.write_all(&off.to_le_bytes())?;
    }
    writer.seek(SeekFrom::Start(end_position))?;
    Ok(())
}

#[derive(thiserror::Error, Debug)]
pub enum DeserializationError<T> {
    #[error("IO error")]
    IO(#[from] io::Error),
    #[error("Error during deserialization")]
    Deserialization(T),
    #[error("Tried to access index {asked}, but max is {max}")]
    IndexNotFound { asked: usize, max: usize },
}

pub fn deserialize<RS, F, E, T: for<'a> Deserialize<'a>>(
    mut reader: RS,
    i: usize,
    deserializer: F,
) -> Result<T, DeserializationError<E>>
where
    RS: Read + Seek,
    F: Fn(Vec<u8>) -> Result<T, E>,
{
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    let count = u64::from_le_bytes(buf) as usize;

    if i >= count {
        return Err(DeserializationError::IndexNotFound {
            asked: i,
            max: count,
        });
    }

    let mut offsets = vec![0u64; count + 1];
    for j in 0..count {
        reader.read_exact(&mut buf)?;
        offsets[j] = u64::from_le_bytes(buf);
    }

    let data_start = 8 + count * 8;
    let start = data_start as u64 + offsets[i];
    let end = if i + 1 < count {
        data_start as u64 + offsets[i + 1]
    } else {
        reader.seek(SeekFrom::End(0)).unwrap()
    };

    let len = (end - start) as usize;
    let mut data = vec![0u8; len];

    reader.seek(SeekFrom::Start(start))?;
    reader.read_exact(&mut data)?;

    deserializer(data).map_err(DeserializationError::Deserialization)
}

#[cfg(test)]
mod tests {

    use std::{
        fs::File,
        io::{BufReader, BufWriter},
    };

    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct Data {
        u: u16,
        v: Vec<u8>,
        i: i64,
    }

    fn serialize_data(writer: &mut impl Write, value: &Data) -> bincode::Result<()> {
        bincode::serialize_into(writer, value)
    }

    pub fn deserialize_data<'a, T>(bytes: &'a [u8]) -> Result<T, bincode::Error>
    where
        T: serde::de::Deserialize<'a>,
    {
        bincode::deserialize(bytes)
    }

    #[test]
    fn one_struct() {
        let path = "path.batar";
        let data = [Data {
            u: u16::MAX,
            v: vec![8, 98, 9],
            i: 65454254,
        }];

        let file = File::create(path).unwrap();
        let mut buffer = BufWriter::new(file);
        serialize(&mut buffer, &data, serialize_data).unwrap();
        buffer.flush().unwrap();

        let file = File::open(path).unwrap();
        let mut buffer = BufReader::new(file);
        let result = deserialize(&mut buffer, 0, |reader| deserialize_data(&reader)).unwrap();
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
        let file = File::create(path).unwrap();
        let mut buffer = BufWriter::new(file);
        serialize(&mut buffer, &data, serialize_data).unwrap();
        buffer.flush().unwrap();

        for (i, expected) in data.iter().enumerate() {
            let file = File::open(path).unwrap();
            let buffer = BufReader::new(file);
            assert_eq!(
                expected,
                &deserialize(buffer, i, |reader| deserialize_data(&reader)).unwrap()
            );
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn ask_too_much() {
        let path = "path3.batar";
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
        let file = File::create(path).unwrap();
        let mut buffer = BufWriter::new(file);
        serialize(&mut buffer, &data, serialize_data).unwrap();
        buffer.flush().unwrap();

        let file = File::open(path).unwrap();
        let buffer = BufReader::new(file);
        match deserialize(buffer, 8, |reader| deserialize_data::<Data>(&reader)) {
            Err(DeserializationError::IndexNotFound { asked: 8, max: 3 }) => {}
            _ => panic!("test failed"),
        }

        std::fs::remove_file(path).unwrap();
    }
}
