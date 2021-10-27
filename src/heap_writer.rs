use std::io::{Cursor, Seek, SeekFrom, Write};

const CHECK_SEQUENCE: u16 = 54593;

pub fn heap_writer() -> Cursor<Vec<u8>> {
    let mut s = Cursor::new(Vec::new());
    s.write(&CHECK_SEQUENCE.to_le_bytes());
    s
}

pub fn check(unchecked: &[u8]) -> &[u8] {
    assert_eq!(unchecked[0..2], CHECK_SEQUENCE.to_le_bytes());
    unchecked
}

