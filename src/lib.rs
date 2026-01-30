use serde::{Deserialize, Serialize};
use std::io::{self, Read, Seek, SeekFrom, Write};

#[derive(thiserror::Error, Debug)]
pub enum SerializationError<T> {
    IO(#[from] io::Error),
    Serialization(T),
}

/// Serializes `nb_elem` elements from `iter` into `writer` using `serializer`.
/// A header lookup table is inserted at the starting position in the `writer`
/// to allow for constant time computation of the position of any elements of `iter` in `writer`.
/// The writer is left at the end of the written elements.
pub fn serialize_iter<'a, I, T, F, WS, E>(
    writer: &mut WS,
    iterator: I,
    nb_elem: usize,
    mut serializer: F,
) -> Result<(), SerializationError<E>>
where
    T: Serialize + 'a,
    WS: Write + Seek,
    F: for<'w> FnMut(&'w mut WS, &T) -> Result<(), E>,
    I: Iterator<Item = &'a T>,
{
    writer.write_all(&nb_elem.to_le_bytes())?;
    // reserve space for the number of element
    let start_count_position = writer.stream_position()?;
    writer.write_all(&0u64.to_le_bytes())?;
    // reserve space for the header
    for _ in 0..nb_elem {
        writer.write_all(&0u64.to_le_bytes())?;
    }

    let payload_start = writer.stream_position()?;
    dbg!(payload_start);

    // stream payload and record offsets
    let mut offsets = Vec::with_capacity(nb_elem);
    let mut nb_actual_elements: u64 = 0;
    for item in iterator.take(nb_elem) {
        let pos = writer.stream_position()?;
        offsets.push(pos - payload_start);
        serializer(writer, item).map_err(SerializationError::Serialization)?;
        nb_actual_elements += 1;
    }

    let end_position = writer.stream_position()?;

    // rewrite header
    writer.seek(SeekFrom::Start(start_count_position))?;
    writer.write_all(&nb_actual_elements.to_le_bytes())?;
    dbg!(nb_actual_elements);
    dbg!(&offsets);

    for off in offsets {
        writer.write_all(&off.to_le_bytes())?;
        dbg!(writer.stream_position().unwrap());
    }
    writer.seek(SeekFrom::Start(end_position))?;
    Ok(())
}

/// Serializes elements from `v` into `writer` using `serializer`.
/// A header lookup table is inserted at the starting position in the `writer`
/// to allow for constant time computation of the position of any elements of `v` in `writer``.
pub fn serialize_slice<'a, T, F, WS, E>(
    writer: &mut WS,
    v: &[T],
    serializer: F,
) -> Result<(), SerializationError<E>>
where
    T: Serialize + 'a,
    WS: Write + Seek,
    F: for<'w> FnMut(&'w mut WS, &T) -> Result<(), E>,
{
    serialize_iter(writer, v.iter(), v.len(), serializer)
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

fn get_offset_of_element_i<RS>(reader: &mut RS, offsets_start: u64, i: u64) -> std::io::Result<u64>
where
    RS: Read + Seek,
{
    let mut buf = [0u8; 8];
    reader.seek(SeekFrom::Start(offsets_start + i * 8))?;
    reader.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

/// Deserializes the element `i` of type `T` from `reader` using `deserializer`.
/// Moving `reader` to the correct position is O(1).
/// The reader is moved back to where it was before the call fo the function.
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
    let capacity = u64::from_le_bytes(buf);

    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    let count = u64::from_le_bytes(buf);

    let offset_start = reader.stream_position()?;

    if i >= count as usize {
        return Err(DeserializationError::IndexNotFound {
            asked: i,
            max: count as usize,
        });
    }
    let i = i as u64;

    let payload_start = offset_start + capacity * 8;

    reader.seek(SeekFrom::Start(offset_start + i * 8))?;
    reader.read_exact(&mut buf)?;
    let data_i_start = payload_start + get_offset_of_element_i(&mut reader, offset_start, i)?;
    dbg!(data_i_start);

    let end = if i + 1 < count {
        payload_start + get_offset_of_element_i(&mut reader, offset_start, i + 1)?
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
pub fn get_capacity_and_num_of_objects<RS>(reader: &mut RS) -> std::io::Result<(u64, u64)>
where
    RS: Read + Seek,
{
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    let capacity = u64::from_le_bytes(buf);

    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    let count = u64::from_le_bytes(buf);
    Ok((capacity, count))
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

    fn generate_random_filename() -> String {
        uuid::Uuid::new_v4().to_string()
    }

    #[test]
    fn serialize_slice_no_elem_one_struct() {
        let path = generate_random_filename();
        let data = [Data {
            u: u16::MAX,
            v: vec![8, 98, 9],
            i: 65454254,
        }];

        let file = File::create(&path).unwrap();
        let mut buffer = BufWriter::new(file);
        serialize_slice(&mut buffer, &data, serialize_data).unwrap();
        buffer.flush().unwrap();

        let file = File::open(&path).unwrap();
        let mut buffer = BufReader::new(file);
        let result = deserialize(&mut buffer, 0, |reader| deserialize_data(&reader)).unwrap();
        assert_eq!(data[0], result);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn serialize_slice_multiple_structs() {
        let path = generate_random_filename();
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
        let file = File::create(&path).unwrap();
        let mut buffer = BufWriter::new(file);
        serialize_slice(&mut buffer, &data, serialize_data).unwrap();
        buffer.flush().unwrap();

        for (i, expected) in data.iter().enumerate() {
            let file = File::open(&path).unwrap();
            let buffer = BufReader::new(file);
            assert_eq!(
                expected,
                &deserialize(buffer, i, |reader| deserialize_data(&reader)).unwrap()
            );
        }
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn serialize_slice_empty() {
        let path = generate_random_filename();
        let data = [];
        let file = File::create(&path).unwrap();
        let mut buffer = BufWriter::new(file);
        serialize_slice(&mut buffer, &data, serialize_data).unwrap();
        buffer.flush().unwrap();

        let file = File::open(&path).unwrap();
        let mut buffer = BufReader::new(file);
        assert_eq!(
            get_capacity_and_num_of_objects(&mut buffer).unwrap(),
            (0, 0)
        );

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn serialize_slice_ask_too_much() {
        let path = generate_random_filename();
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
        let file = File::create(&path).unwrap();
        let mut buffer = BufWriter::new(file);
        serialize_slice(&mut buffer, &data, serialize_data).unwrap();
        buffer.flush().unwrap();

        let file = File::open(&path).unwrap();
        let buffer = BufReader::new(file);
        match deserialize(buffer, 8, |reader| deserialize_data::<Data>(&reader)) {
            Err(DeserializationError::IndexNotFound { asked: 8, max: 3 }) => {}
            _ => panic!("test failed"),
        }

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn serialize_iter_all_elem_one_struct() {
        let path = generate_random_filename();
        let data = [Data {
            u: u16::MAX,
            v: vec![8, 98, 9],
            i: 65454254,
        }];

        let file = File::create(&path).unwrap();
        let mut buffer = BufWriter::new(file);
        serialize_iter(&mut buffer, data.iter(), data.len(), serialize_data).unwrap();
        buffer.flush().unwrap();

        let file = File::open(&path).unwrap();
        let mut buffer = BufReader::new(file);
        let result = deserialize(&mut buffer, 0, |reader| deserialize_data(&reader)).unwrap();
        assert_eq!(data[0], result);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn serialize_iter_no_elem_one_struct() {
        let path = generate_random_filename();
        let data = [Data {
            u: u16::MAX,
            v: vec![8, 98, 9],
            i: 65454254,
        }];

        let file = File::create(&path).unwrap();
        let mut buffer = BufWriter::new(file);
        serialize_iter(&mut buffer, data.iter(), 0, serialize_data).unwrap();
        buffer.flush().unwrap();

        let file = File::open(&path).unwrap();
        let mut buffer = BufReader::new(file);
        match deserialize(&mut buffer, 0, |reader| deserialize_data::<Data>(&reader)) {
            Err(DeserializationError::IndexNotFound { asked: 0, max: 0 }) => {}
            _ => panic!("test failed"),
        };
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn serialize_iter_too_many_elem_one_struct() {
        let path = generate_random_filename();
        let data = [Data {
            u: u16::MAX,
            v: vec![8, 98, 9],
            i: 65454254,
        }];

        let file = File::create(&path).unwrap();
        let mut buffer = BufWriter::new(file);
        serialize_iter(&mut buffer, data.iter(), 100, serialize_data).unwrap();
        buffer.flush().unwrap();
        // TODO test writer position

        let file = File::open(&path).unwrap();
        let mut buffer = BufReader::new(file);
        let result = deserialize(&mut buffer, 0, |reader| deserialize_data(&reader)).unwrap();
        assert_eq!(data[0], result);
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn serialize_iter_all_elem_multiple_structs() {
        let path = generate_random_filename();
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
        let file = File::create(&path).unwrap();
        let mut buffer = BufWriter::new(file);
        serialize_iter(&mut buffer, data.iter(), data.len(), serialize_data).unwrap();
        buffer.flush().unwrap();

        for (i, expected) in data.iter().enumerate() {
            let file = File::open(&path).unwrap();
            let buffer = BufReader::new(file);
            assert_eq!(
                expected,
                &deserialize(buffer, i, |reader| deserialize_data(&reader)).unwrap()
            );
        }
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn serialize_iter_no_elem_multiple_structs() {
        let path = generate_random_filename();
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
        let file = File::create(&path).unwrap();
        let mut buffer = BufWriter::new(file);
        serialize_iter(&mut buffer, data.iter(), 0, serialize_data).unwrap();
        buffer.flush().unwrap();

        for (i, _expected) in data.iter().enumerate() {
            let file = File::open(&path).unwrap();
            let buffer = BufReader::new(file);
            match deserialize(buffer, i, |reader| deserialize_data::<Data>(&reader)) {
                Err(DeserializationError::IndexNotFound { asked, max: 0 }) if asked == i => {}
                _ => panic!("test failed"),
            };
        }
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn serialize_iter_too_many_elem_multiple_structs() {
        let path = generate_random_filename();
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
        let file = File::create(&path).unwrap();
        let mut buffer = BufWriter::new(file);
        serialize_iter(&mut buffer, data.iter(), 100, serialize_data).unwrap();
        buffer.flush().unwrap();

        for (i, expected) in data.iter().enumerate() {
            let file = File::open(&path).unwrap();
            let buffer = BufReader::new(file);
            assert_eq!(
                expected,
                &deserialize(buffer, i, |reader| deserialize_data(&reader)).unwrap()
            );
        }
        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn serialize_iter_empty() {
        let path = generate_random_filename();
        let data = [];
        let file = File::create(&path).unwrap();
        let mut buffer = BufWriter::new(file);
        serialize_iter(&mut buffer, data.iter(), data.len(), serialize_data).unwrap();
        buffer.flush().unwrap();

        let file = File::open(&path).unwrap();
        let mut buffer = BufReader::new(file);
        assert_eq!(
            get_capacity_and_num_of_objects(&mut buffer).unwrap(),
            (0, 0)
        );

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn serialize_iter_too_mush_empty() {
        let path = generate_random_filename();
        let data = [];
        let file = File::create(&path).unwrap();
        let mut buffer = BufWriter::new(file);
        serialize_iter(&mut buffer, data.iter(), 100, serialize_data).unwrap();
        buffer.flush().unwrap();

        let file = File::open(&path).unwrap();
        let mut buffer = BufReader::new(file);
        assert_eq!(
            get_capacity_and_num_of_objects(&mut buffer).unwrap(),
            (100, 0)
        );

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn serialize_iter_ask_too_much() {
        let path = generate_random_filename();
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
        let file = File::create(&path).unwrap();
        let mut buffer = BufWriter::new(file);
        serialize_iter(&mut buffer, data.iter(), data.len(), serialize_data).unwrap();
        buffer.flush().unwrap();

        let file = File::open(&path).unwrap();
        let buffer = BufReader::new(file);
        match deserialize(buffer, 8, |reader| deserialize_data::<Data>(&reader)) {
            Err(DeserializationError::IndexNotFound { asked: 8, max: 3 }) => {}
            _ => panic!("test failed"),
        }

        std::fs::remove_file(&path).unwrap();
    }
}
