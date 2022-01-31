use std::io::{Cursor, Write};

const CHECK_SEQUENCE: u16 = 54593;

pub fn default_heap_writer() -> Cursor<Vec<u8>> {
    let mut s = Cursor::new(Vec::new());
    s.write_all(&CHECK_SEQUENCE.to_le_bytes()).unwrap();
    s
}
pub fn default_mem_writer() -> Cursor<Vec<u8>> {
    Cursor::new(Vec::new())
}

pub fn check(unchecked: &[u8]) -> &[u8] {
    assert_eq!(unchecked[0..2], CHECK_SEQUENCE.to_le_bytes());
    unchecked
}
