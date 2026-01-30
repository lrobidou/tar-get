use serde::{Deserialize, Serialize};
use std::io::{self, Read, Seek, SeekFrom, Write};

#[derive(thiserror::Error, Debug)]
pub enum SerializationError<T> {
    IO(#[from] io::Error),
    Serialization(T),
}

/// Serializes elements from `v` into `writer` using `serializer`.
/// A header lookup table is inserted at the starting position in the `writer`
/// to allow for constant time computation of the position of any elements of `v` in `writer``.
pub fn serialize<T, F, WS, E>(
    writer: &mut WS,
    v: &[T],
    mut serializer: F,
) -> Result<(), SerializationError<E>>
where
    T: Serialize,
    WS: Write + Seek,
    F: for<'w> FnMut(&'w mut WS, &T) -> Result<(), E>,
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
        serializer(writer, item).map_err(SerializationError::Serialization)?;
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

fn get_offset_of_element_i<RS>(reader: &mut RS, stream_start: u64, i: u64) -> std::io::Result<u64>
where
    RS: Read + Seek,
{
    let mut buf = [0u8; 8];
    reader.seek(SeekFrom::Start(stream_start + 8 + i * 8))?;
    reader.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

/// Deserializes the element `i` of type `T` from `reader` using `deserializer`.
/// Moving `reader` to the correct position is O(1).
pub fn deserialize<RS, F, E, T: for<'a> Deserialize<'a>>(
    mut reader: RS,
    i: usize,
    deserializer: F,
) -> Result<T, DeserializationError<E>>
where
    RS: Read + Seek,
    F: Fn(Vec<u8>) -> Result<T, E>,
{
    let stream_start = reader.stream_position()?;
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    let count = u64::from_le_bytes(buf);

    if i >= count as usize {
        return Err(DeserializationError::IndexNotFound {
            asked: i,
            max: count as usize,
        });
    }
    let i = i as u64;

    let data_start = stream_start + 8 + count * 8;

    reader.seek(SeekFrom::Start(stream_start + 8 + i * 8))?;
    reader.read_exact(&mut buf)?;
    let data_i_start = data_start + get_offset_of_element_i(&mut reader, stream_start, i)?;

    let end = if i + 1 < count {
        data_start + get_offset_of_element_i(&mut reader, stream_start, i + 1)?
    } else {
        reader.seek(SeekFrom::End(0)).unwrap()
    };

    let len = (end - data_i_start) as usize;
    let mut data = vec![0u8; len];

    reader.seek(SeekFrom::Start(data_i_start))?;
    reader.read_exact(&mut data)?;
    reader.seek(SeekFrom::Start(stream_start))?;

    deserializer(data).map_err(DeserializationError::Deserialization)
}

/// Returns the number of objects in `reader`.
pub fn get_num_of_objects<RS>(reader: &mut RS) -> std::io::Result<u64>
where
    RS: Read + Seek,
{
    let stream_start = reader.stream_position()?;
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    let count = u64::from_le_bytes(buf);
    reader.seek(SeekFrom::Start(stream_start))?;
    Ok(count)
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

    fn deserialize_data<'a, T>(bytes: &'a [u8]) -> Result<T, bincode::Error>
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
    fn empty() {
        let path = "path4.batar";
        let data = [];
        let file = File::create(path).unwrap();
        let mut buffer = BufWriter::new(file);
        serialize(&mut buffer, &data, serialize_data).unwrap();
        buffer.flush().unwrap();

        let file = File::open(path).unwrap();
        let mut buffer = BufReader::new(file);
        assert_eq!(get_num_of_objects(&mut buffer).unwrap(), 0);

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
