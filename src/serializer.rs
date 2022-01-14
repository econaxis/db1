use std::fmt::Arguments;
use std::io::{Cursor, IoSlice, Read, Seek, SeekFrom, Write};
use std::io::SeekFrom::Current;
use std::mem::size_of;
use std::ops::RangeBounds;

use ::{ChunkHeader, FromReader};
use chunk_header::ChunkHeaderIndex;
use serializer::PageResult::Good;
use table_base::read_to_buf;

pub trait DbPageManager {
    type WriterType;
    type ReaderType;
    fn add_page<R: Read>(self, buf: R, len: u64, _: ChunkHeader) -> u64;
    fn get_page(self, position: u64) -> Self::ReaderType;
}

#[derive(Default)]
pub struct PageSerializer<W> {
    pub(crate) file: W,
    pub previous_headers: ChunkHeaderIndex,

}

pub struct LimitedReader<W>(W, usize);

impl<W> LimitedReader<W> {
    pub(crate) fn size(&self) -> usize {
        self.1
    }
}

enum PageResult<'a, W> {
    Good(&'a mut W, u64),
    Deleted(&'a mut W, u64),
    Eof,
}

impl<W: Write + Read + Seek> PageSerializer<W> {
    const CHECK_SEQ: u64 = 3180343028731803290;
    const WORKING_PAGE: u16 = 31920;
    const WORKING_PAGE_SIZE: u64 = 2;
    const DELETED_PAGE: u16 = 21923;
    pub fn replace_inner(&mut self, w: W) -> W {
        std::mem::replace(&mut self.file, w)
    }

    pub fn free_page(&mut self, p: u64) {
        // Check that page is still valid
        if let PageResult::Good(page, _) = Self::page_checked(&mut self.file, Some(p)) {
            page.seek(SeekFrom::Current(Self::WORKING_PAGE_SIZE as i64 * -1)).unwrap();
            page.write_all(&Self::DELETED_PAGE.to_le_bytes()).unwrap();
            self.previous_headers.0.retain(|a| a.0 != p);
        } else {
            panic!()
        }
    }

    pub fn flush(&mut self) {
        self.file.flush().unwrap();
    }

    fn iter_pages<Handler: FnMut(LimitedReader<&mut W>, u64) -> u64>(mut r: &mut W, mut header_handler: Handler) {
        assert_eq!(r.stream_position().unwrap(), 0);
        let check_seq = u64::from_le_bytes(read_to_buf(&mut r));

        assert_eq!(check_seq, Self::CHECK_SEQ);

        loop {
            match Self::page_checked(&mut r, None) {
                PageResult::Good(mut pr, pos) => {
                    let real_position = pr.stream_position().unwrap();
                    let len = u32::from_le_bytes(read_to_buf(&mut pr));
                    let skip = header_handler(LimitedReader(pr, len as usize), pos);

                    r.seek(SeekFrom::Current(skip as i64)).unwrap();
                }
                PageResult::Deleted(mut pr, pos) => {
                    let skip = u32::from_le_bytes(read_to_buf(&mut pr));
                    r.seek(SeekFrom::Current(skip as i64)).unwrap();
                }
                PageResult::Eof => break
            };
        }
    }
    pub fn create_from_reader(mut w: W) -> Self {
        let mut v = Vec::new();
        PageSerializer::iter_pages(&mut w, |mut reader, position| {
            let ch = Option::<ChunkHeader>::from_reader_and_heap(&mut reader, &[]);
            if let Some(ch) = ch {
                let size = ch.calculate_total_size() as u64;
                log::debug!("Chunk header detected {:?}", ch);
                v.push((position, ch));
                size
            } else {
                log::debug!("Skip");
                reader.size() as u64
            }
        });
        let ch = ChunkHeaderIndex(v);
        Self {
            file: w,
            previous_headers: ch,
        }
    }

    fn page_checked(file: &mut W, position: Option<u64>) -> PageResult<'_, W> {
        let pos = if let Some(pos) = position {
            file.seek(SeekFrom::Start(pos)).unwrap()
        } else {
            file.seek(SeekFrom::Current(0)).unwrap()
        };
        let mut u16_bytes = [0u8; 2];
        match file.read_exact(&mut u16_bytes) {
            Ok(_) => {
                let check_val = u16::from_le_bytes(u16_bytes);
                match check_val {
                    PageSerializer::<W>::WORKING_PAGE => {
                        PageResult::Good(file, pos)
                    }
                    PageSerializer::<W>::DELETED_PAGE => {
                        println!("Encountered deleted page at {:?}", position);
                        PageResult::Deleted(file, pos)
                    }
                    _ => panic!("Tried to load page incorrectly at {:?}", position)
                }
            }
            Err(e) => {
                log::debug!("Load page wrong {:?} {:?}", position, e);
                println!("Load page wrong {:?} {:?}", position, e);
                PageResult::Eof
            }
        }
    }
    pub fn create(mut w: W) -> Self {
        w.write(&Self::CHECK_SEQ.to_le_bytes());
        Self {
            file: w,
            previous_headers: ChunkHeaderIndex::default(),
        }
    }

    pub fn get_in_all<RB: RangeBounds<u64>>(&self, ty: u8, range: &RB) -> Vec<u64> {
        self.previous_headers.get_in_all(ty, range)
    }
}

impl<W: Read> Read for LimitedReader<W> {
    fn read(&mut self, buf: &mut [u8]) -> std::io::Result<usize> {
        assert!(self.1 >= buf.len());
        self.1 -= buf.len();
        self.0.read(buf)
    }
}


impl<'a, W: Write + Seek + Read> DbPageManager for &'a mut PageSerializer<W> {
    type WriterType = Cursor<Vec<u8>>;
    type ReaderType = LimitedReader<&'a mut W>;
    fn add_page<R: Read>(self, mut buf: R, size: u64, ch: ChunkHeader) -> u64 {
        // Unchecked give out page
        let new_pos = self.file.seek(SeekFrom::End(0)).unwrap();
        self.file.write(&PageSerializer::<W>::WORKING_PAGE.to_le_bytes()).unwrap();
        self.file.write(&(size as u32).to_le_bytes()).unwrap();
        let _length_added = std::io::copy(&mut buf, &mut self.file).unwrap();

        self.previous_headers.push(new_pos, ch);
        new_pos
    }

    fn get_page(self, position: u64) -> Self::ReaderType {
        if let PageResult::Good(mut page, _) = PageSerializer::<W>::page_checked(&mut self.file, Some(position)) {
            let size = u32::from_le_bytes(read_to_buf(&mut page));

            if size == 0 {
                log::info!("Tried to load deleted page")
            }
            LimitedReader(page, size as usize)
        } else {
            panic!()
        }
    }
}
