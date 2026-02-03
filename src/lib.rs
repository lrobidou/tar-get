use std::{
    error::Error,
    fs::File,
    io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
};

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
    T: 'a,
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

    for off in offsets {
        writer.write_all(&off.to_le_bytes())?;
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
    T: 'a,
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
    IndexNotFound { asked: u64, max: u64 },
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
pub fn deserialize<RS, F, E, T>(
    mut reader: RS,
    i: u64,
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

    if i >= count {
        return Err(DeserializationError::IndexNotFound {
            asked: i,
            max: count,
        });
    }

    let payload_start = offset_start + capacity * 8;

    reader.seek(SeekFrom::Start(offset_start + i * 8))?;
    let data_i_start = payload_start + get_offset_of_element_i(&mut reader, offset_start, i)?;

    let end = if i + 1 < count {
        payload_start + get_offset_of_element_i(&mut reader, offset_start, i + 1)?
    } else {
        reader.seek(SeekFrom::End(0))?
    };

    let len = (end - data_i_start) as usize;
    let mut data = vec![0u8; len];

    reader.seek(SeekFrom::Start(data_i_start))?;
    reader.read_exact(&mut data)?;
    reader.seek(SeekFrom::Start(stream_start))?;

    deserializer(data).map_err(DeserializationError::Deserialization)
}

/// Returns the position of the count, and the (start, end) of the last element.
/// The reader is returned in its previous state.
pub fn get_positions_of_count_start_end_of_last_obejct<RS>(
    reader: &mut RS,
) -> Result<(u64, (u64, u64)), io::Error>
where
    RS: Read + Seek,
{
    // OPTIMIZE the whole function
    let stream_start = reader.stream_position()?;

    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    let capacity = u64::from_le_bytes(buf);

    let count_start = reader.stream_position()?;

    reader.read_exact(&mut buf)?;
    let count = u64::from_le_bytes(buf);

    let offset_start = reader.stream_position()?;

    assert!(count > 0); // TODO
    let i = count - 1;

    let payload_start = offset_start + capacity * 8;

    reader.seek(SeekFrom::Start(offset_start + i * 8))?;
    reader.read_exact(&mut buf)?;

    let data_i_start = payload_start + get_offset_of_element_i(reader, offset_start, i)?;
    let end = if i + 1 < count {
        payload_start + get_offset_of_element_i(reader, offset_start, i + 1)?
    } else {
        reader.seek(SeekFrom::End(0))?
    };

    reader.seek(SeekFrom::Start(stream_start))?;
    Ok((count_start, (data_i_start, end)))
}

#[derive(thiserror::Error, Debug)]
pub enum RemoveElementError<T, W: Write> {
    #[error("IO error")]
    IO(#[from] io::Error),
    #[error("Error during deserialization")]
    Deserialization(T),
    #[error(
        "Error when converting the witer buffer back into a file. Likely, the buffer couldn't be flushed."
    )]
    ConversionError(#[from] std::io::IntoInnerError<BufWriter<W>>),
}

/// Deserializes and removes the last element of type `T` from `reader` using `deserializer`.
/// Moving `reader` to the correct position is O(1).
/// The reader is moved back to where it was before the call fo the function.
/// Returns the obect and the number of element left.
pub fn remove_last_element<F, E, T>(
    file: &mut File,
    deserializer: F,
) -> Result<T, RemoveElementError<E, &mut File>>
where
    F: Fn(Vec<u8>) -> Result<T, E>,
{
    let file_start = file.stream_position()?;
    let mut reader = BufReader::new(file);

    let (count_start, (start, end)) = get_positions_of_count_start_end_of_last_obejct(&mut reader)?;

    // read the count
    reader.seek(SeekFrom::Start(count_start))?;
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    let count = u64::from_le_bytes(buf);

    // read the last element
    reader.seek(SeekFrom::Start(start))?;
    let len = (end - start) as usize;
    let mut data = vec![0u8; len];
    reader.read_exact(&mut data)?;
    let last_object = deserializer(data).map_err(RemoveElementError::Deserialization)?;

    // Read the remaining bytes
    reader.seek(SeekFrom::Start(end))?;
    let mut remaining_bytes = Vec::new();
    reader.read_to_end(&mut remaining_bytes)?;
    let file = reader.into_inner();
    let mut writer = BufWriter::new(file);

    // rewrite count
    writer.seek(SeekFrom::Start(count_start))?;
    writer.write_all(&(count - 1).to_le_bytes())?;

    // Wwrite the remaining bytes to erase the last element
    writer.seek(SeekFrom::Start(end))?;
    writer.write_all(&remaining_bytes)?;

    let file = writer.into_inner()?;

    // Truncate the file to the new length
    file.set_len(start + remaining_bytes.len() as u64)?;

    file.seek(SeekFrom::Start(file_start))?;

    Ok(last_object)
}

#[derive(thiserror::Error, Debug)]
pub enum AppendError<T, W: Write> {
    #[error("IO error")]
    IO(#[from] io::Error),
    #[error("Error during serialization")]
    Serialization(T),
    #[error(
        "Error when converting the witer buffer back into a file. Likely, the buffer couldn't be flushed."
    )]
    ConversionError(#[from] std::io::IntoInnerError<BufWriter<W>>),
}

pub fn reserve_capacity<WS>(writer: &mut WS, capacity: u64) -> Result<(), std::io::Error>
where
    WS: Write + Seek,
{
    let start = writer.stream_position()?;

    // write capacity
    writer.write_all(&capacity.to_le_bytes())?;
    // reserve space for the number of element
    writer.write_all(&0u64.to_le_bytes())?;
    // reserve space for the header
    for _ in 0..capacity {
        writer.write_all(&0u64.to_le_bytes())?;
    }

    writer.seek(SeekFrom::Start(start))?;

    Ok(())
}

#[derive(PartialEq, Debug)]
pub struct Metadata {
    capacity: u64,
    nb_objects: u64,
    offsets: Vec<u64>,
}

pub fn get_metadata<RS>(reader: &mut RS) -> std::io::Result<Metadata>
where
    RS: Read + Seek,
{
    let start = reader.stream_position()?;

    let (capacity, nb_objects) = get_capacity_and_num_of_objects(reader)?;
    let mut v = vec![];
    for i in 0..nb_objects {
        let mut buf = [0u8; 8];
        reader.seek(SeekFrom::Start(start + 16 + i * 8))?;
        reader.read_exact(&mut buf)?;
        v.push(u64::from_le_bytes(buf));
    }

    reader.seek(SeekFrom::Start(start))?;

    Ok(Metadata {
        capacity,
        nb_objects,
        offsets: v,
    })
}

pub fn append_element<'b, F, E, T>(
    file: &'b mut File,
    item: &T,
    mut serializer: F,
) -> Result<(), AppendError<E, &'b mut File>>
where
    F: for<'w> FnMut(&mut BufWriter<&mut File>, &T) -> Result<(), E>,
    E: Error,
{
    let file_start = file.stream_position()?;
    let mut reader = BufReader::new(file);

    let (capacity, nb_objects) = get_capacity_and_num_of_objects(&mut reader)?;
    assert!(nb_objects < capacity); // TODO exceptions
    reader.seek_relative(8)?;
    let count_start = reader.stream_position()?;
    // read the count
    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    let count = u64::from_le_bytes(buf);

    assert_eq!(count, nb_objects);

    let file = reader.into_inner();
    let mut writer = BufWriter::new(file);

    let start_element = writer.seek(SeekFrom::End(0))?; // TODO might not be the end in practice
    serializer(&mut writer, item).map_err(AppendError::Serialization)?;
    // rewrite count
    writer.seek(SeekFrom::Start(count_start))?;
    writer.write_all(&(count + 1).to_le_bytes())?;

    writer.seek(SeekFrom::Start(file_start + 8 + 8 + count * 8))?;
    writer.write_all(&(start_element - (16 + capacity * 8)).to_le_bytes())?;

    let file = writer.into_inner()?;
    file.seek(SeekFrom::Start(file_start))?;

    Ok(())
}

/// Returns the number of objects in `reader`.
/// The reader is set back to its previous position.
pub fn get_capacity_and_num_of_objects<RS>(reader: &mut RS) -> std::io::Result<(u64, u64)>
where
    RS: Read + Seek,
{
    let stream_start = reader.stream_position()?;

    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    let capacity = u64::from_le_bytes(buf);

    let mut buf = [0u8; 8];
    reader.read_exact(&mut buf)?;
    let count = u64::from_le_bytes(buf);

    reader.seek(SeekFrom::Start(stream_start))?;
    Ok((capacity, count))
}

#[cfg(test)]
mod tests {

    use std::{
        fs::{File, OpenOptions},
        io::{BufReader, BufWriter},
        os::unix::fs::MetadataExt,
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
                &deserialize(buffer, i as u64, |reader| deserialize_data(&reader)).unwrap()
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
                &deserialize(buffer, i as u64, |reader| deserialize_data(&reader)).unwrap()
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
            match deserialize(buffer, i as u64, |reader| deserialize_data::<Data>(&reader)) {
                Err(DeserializationError::IndexNotFound { asked, max: 0 }) if asked == i as u64 => {
                }
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
                &deserialize(buffer, i as u64, |reader| deserialize_data(&reader)).unwrap()
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

    #[test]
    fn test_remove_last_element() {
        let path = generate_random_filename();
        let data = [
            Data {
                u: u16::MAX,
                v: vec![5; 585845],
                i: 65454254,
            },
            Data {
                u: 0,
                v: vec![9; 55],
                i: 6542345420,
            },
            Data {
                u: u16::MAX,
                v: vec![78; 100],
                i: 2385243420343543114,
            },
        ];

        let file = File::create(&path).unwrap();

        assert_eq!(file.metadata().unwrap().size(), 0);

        let mut buffer = BufWriter::new(file);
        serialize_iter(&mut buffer, data.iter(), data.len(), serialize_data).unwrap();
        buffer.flush().unwrap();

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();
        assert_eq!(file.metadata().unwrap().len(), 586094);

        let x = remove_last_element(&mut file, |reader| deserialize_data::<Data>(&reader)).unwrap();
        assert_eq!(file.metadata().unwrap().len(), 585976);
        assert_eq!(x, data[2]);

        let x = remove_last_element(&mut file, |reader| deserialize_data::<Data>(&reader)).unwrap();
        assert_eq!(file.metadata().unwrap().len(), 585903);
        assert_eq!(x, data[1]);

        let x = remove_last_element(&mut file, |reader| deserialize_data::<Data>(&reader)).unwrap();
        assert_eq!(file.metadata().unwrap().size(), 40);
        assert_eq!(x, data[0]);

        std::fs::remove_file(&path).unwrap();
    }

    #[test]
    fn reserve_and_use() {
        let path = generate_random_filename();
        let data = [
            Data {
                u: u16::MAX,
                v: vec![5; 10],
                i: 65454254,
            },
            Data {
                u: 0,
                v: vec![9; 10],
                i: 6542345420,
            },
            Data {
                u: u16::MAX,
                v: vec![78; 10],
                i: 2385243420343543114,
            },
        ];

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(&path)
            .unwrap();
        assert_eq!(file.metadata().unwrap().size(), 0);

        let mut buffer = BufWriter::new(file);
        reserve_capacity(&mut buffer, data.len() as u64).unwrap();
        buffer.flush().unwrap();

        let mut file = buffer.into_inner().unwrap();
        for x in &data {
            append_element(&mut file, x, |a, b| serialize_data(a, b)).unwrap();
        }

        let mut file = OpenOptions::new()
            .read(true)
            .write(true)
            .open(&path)
            .unwrap();

        assert_eq!(file.metadata().unwrap().len(), 124);

        let x = remove_last_element(&mut file, |reader| deserialize_data::<Data>(&reader)).unwrap();
        assert_eq!(file.metadata().unwrap().len(), 96);
        assert_eq!(x, data[2]);

        let x = remove_last_element(&mut file, |reader| deserialize_data::<Data>(&reader)).unwrap();
        assert_eq!(file.metadata().unwrap().len(), 68);
        assert_eq!(x, data[1]);

        let x = remove_last_element(&mut file, |reader| deserialize_data::<Data>(&reader)).unwrap();
        assert_eq!(file.metadata().unwrap().size(), 40);
        assert_eq!(x, data[0]);

        std::fs::remove_file(&path).unwrap();
    }
}
