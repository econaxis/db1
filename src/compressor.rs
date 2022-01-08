
use crate::SuitableDataType;

fn shuffle_bytes(bytes: &[u8], type_len: usize) -> Vec<u8> {
    assert_eq!(bytes.len() % type_len, 0);
    let tuples = bytes.len() / type_len;
    let mut buffer = Vec::with_capacity(bytes.len());

    for i in 0..type_len {
        for tup in 0..tuples {
            buffer.push(bytes[tup * type_len + i]);
        }
    };

    buffer
}

fn reassemble_bytes(bytes: &[u8], type_len: usize) -> Vec<u8> {
    assert_eq!(bytes.len() % type_len, 0);
    let chunk_size = bytes.len() / type_len;

    let mut buffer = Vec::with_capacity(bytes.len());
    for index in 0..chunk_size {
        for struct_pos in 0..type_len {
            buffer.push(bytes[struct_pos * chunk_size + index]);
        }
    }
    buffer
}




fn shuffle_struct<T: SuitableDataType>(structs: &[u8]) -> Vec<u8> {
    let type_len = T::TYPE_SIZE as usize;

    shuffle_bytes(structs, type_len)
}

fn recover_structs<T: SuitableDataType>(bytes: &[u8]) -> Vec<u8> {
    reassemble_bytes(bytes, T::TYPE_SIZE as usize)
}

pub fn compress<T: SuitableDataType>(structs: &[u8]) -> Vec<u8> {
    let shuffled = shuffle_struct::<T>(structs);
    
    zstd::stream::encode_all(&*shuffled, 0).unwrap()
}
pub fn compress_heap(data: &[u8]) -> Vec<u8> {
    zstd::stream::encode_all(data, 0).unwrap()
}

pub fn decompress_heap(data: &[u8]) -> Vec<u8> {
    zstd::stream::decode_all(data).unwrap()
}
pub fn decompress<T: SuitableDataType>(bytes: &[u8]) -> Vec<u8> {
    let decompressed = zstd::stream::decode_all(bytes).unwrap();
    
    recover_structs::<T>(&decompressed)
}

#[test]
fn test_reassembly_works() {
    
    let rand_str: String = " fdsafd;salkf dsa08hf d [sahdsa;ofjs afdhsa [ufdsafd;sa fkdsa ;flsaj ;dlka jfdsa".to_string();

    let shuffled = shuffle_bytes(rand_str.as_bytes(), 8);

    assert_ne!(shuffled.as_slice(), rand_str.as_bytes());

    let reassembled = reassemble_bytes(&shuffled, 8);

    assert_eq!(reassembled.as_slice(), rand_str.as_bytes());
}