# TAR-get: start deserializing in O(1)

TAR-get lets you serialize a vector of Rust structs to a writer of your choice (e.g. a file on your disk). IN the end, it's like if you serializzed them on disk and `tar`ed them together. But contrary to `tar`, TAR-get lets you start deserializing a struct in O(1).

## Example
```rust

#[derive(Serialize, Deserialize, PartialEq, Debug)]
struct MyStruct {
    /* */
}

// The library requires a function with these parameters.
// This allows for compatibility with multiple serializers.
// This example uses `bincode`.
fn serialize_data(writer: &mut impl Write, value: &Data) -> bincode::Result<()> {
    bincode::serialize_into(writer, value)
}

// Using `bincode` to deserailize.
pub fn deserialize_data<'a, T>(bytes: &'a [u8]) -> Result<T, bincode::Error>
where
    T: serde::de::Deserialize<'a>,
{
    bincode::deserialize(bytes)
}

fn serialize_and_deserialize() {
    let data: Vec<MyStruct> = vec![/* ... */];

    // we need a reader to write the vector in
    let file = File::create(/* path */).unwrap();
    let mut buffer = BufWriter::new(file);

    // let's serialize the vector
    serialize_slice(&mut buffer, &data, serialize_data).unwrap();
    buffer.flush().unwrap();

    // let's get a struct back
    let index = 2;
    let file = File::open(path).unwrap();
    let buffer = BufReader::new(file);
    // deserilization starts in O(1)
    let value = deserialize(buffer, index, |reader| deserialize_data(&reader)).unwrap();
    assert_eq!(data[index], value);

    std::fs::remove_file(path).unwrap();
 }
```
If you only have an iterator over your data, instead of
```rust
serialize_slice(&mut buffer, &data, serialize_data).unwrap();
```
You can use 
```rust
let iterator = data.iter();
let nb_elem = data.len();
serialize_iter(&mut buffer, iterator, nb_elem, serialize_data).unwrap();
```
If the `iterator` contains less than `nb_elem`, some space will be lost in the buffer.

If the `iterator` contains more than `nb_elem` elements, iteration will stop after `nb_elem` elements.