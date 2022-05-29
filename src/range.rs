use std::collections::Bound;
use std::fmt::{Debug, Formatter};
use std::io::{Read, Seek, Write};
use std::ops::RangeBounds;
use dynamic_tuple::TypeData;

use table_base::read_to_buf;

use crate::bytes_serializer::{BytesSerialize, FromReader};
use crate::chunk_header::slice_from_type;

const CHECK_SEQUENCE: u16 = 22859;

#[derive(Copy, Clone)]
enum OptionState {
    None = 0,
    Some = 1,
}

impl BytesSerialize for OptionState {
    fn serialize_with_heap<W: Write, W1: Write + Seek>(&self, mut data: W, _heap: W1) {
        data.write_all(&(*self as i8).to_le_bytes()).unwrap();
    }
}

impl FromReader for OptionState {
    fn from_reader_and_heap<R: Read>(mut r: R, _heap: &[u8]) -> Self {
        let mut buf = [0u8; 1];
        r.read_exact(&mut buf).unwrap();
        match buf[0] {
            0 => OptionState::None,
            1 => OptionState::Some,
            _ => panic!(),
        }
    }
}

impl BytesSerialize for Range<u64> {
    fn serialize_with_heap<W: Write, W1: Write + Seek>(&self, mut w: W, mut _heap: W1) {
        w.write_all(&CHECK_SEQUENCE.to_le_bytes()).unwrap();
        if self.min.is_some() && self.max.is_some() {
            OptionState::Some.serialize_with_heap(&mut w, &mut _heap);
            w.write_all(&self.min.unwrap().to_le_bytes()).unwrap();
            w.write_all(&self.max.unwrap().to_le_bytes()).unwrap();
        } else {
            OptionState::None.serialize_with_heap(&mut w, &mut _heap);
        }
    }
}

#[derive(Clone, PartialEq)]
pub struct Range<T> {
    pub(crate) min: Option<T>,
    pub(crate) max: Option<T>,
}

impl<T> Default for Range<T> {
    fn default() -> Self {
        Range::<T> {
            min: None,
            max: None,
        }
    }
}

impl Range<TypeData> {
    pub fn new(init: Option<TypeData>, init1: Option<TypeData>) -> Self {
        Self {
            min: init,
            max: init1,
        }
    }
    // Returns comp(lhs, rhs) if both are defined value, else returns True
    fn check_else_true<A, F: FnOnce(A, A) -> bool>(
        lhs: Option<A>,
        rhs: Option<A>,
        comp: F,
    ) -> bool {
        match lhs.is_some() && rhs.is_some() {
            false => true,
            true => comp(lhs.unwrap(), rhs.unwrap()),
        }
    }

    // Adds new element to the range, potentially expanding the min and max extrema
    pub fn add(&mut self, new_elt: &TypeData) {
        if Self::check_else_true(Some(new_elt), self.min.as_ref(), |new, min| new < min) {
            self.min = Some(new_elt.clone());
        }
        if Self::check_else_true(Some(new_elt), self.max.as_ref(), |new, max| new > max) {
            self.max = Some(new_elt.clone());
        }
    }
    pub fn resolve(&mut self, heap: &[u8]) {
        match self.min.as_mut() {
            Some(TypeData::String(x)) => x.resolve_item(heap)
            ,_ => {}
        };
        match self.max.as_mut() {
            Some(TypeData::String(x)) => x.resolve_item(heap)
            ,_ => {}
        };
    }
}
impl FromReader for Range<u64> {
    fn from_reader_and_heap<R: Read>(mut r: R, heap: &[u8]) -> Self {
        let mut check: u16 = 0;
        r.read_exact(slice_from_type(&mut check)).unwrap();
        assert_eq!(check, CHECK_SEQUENCE);

        match OptionState::from_reader_and_heap(&mut r, heap) {
            OptionState::Some => {
                let min = u64::from_le_bytes(read_to_buf(&mut r));
                let max = u64::from_le_bytes(read_to_buf(&mut r));
                Self {
                    min: Some(min),
                    max: Some(max),
                }
            }
            OptionState::None => {
                panic!("Option state is none");
                Self::default()
            },
        }
    }
}

impl<T: PartialEq + PartialOrd> Range<T> {
    pub fn overlaps<RB: RangeBounds<T>>(&self, rb: &RB) -> bool {
        match (&self.min, &self.max) {
            (Some(min), Some(max)) => {
                let min_in = match rb.start_bound() {
                    Bound::Included(start) => max >= start,
                    Bound::Excluded(start) => max > start,
                    Bound::Unbounded => true,
                };
                let max_in = match rb.end_bound() {
                    Bound::Included(end) => min <= end,
                    Bound::Excluded(end) => min < end,
                    Bound::Unbounded => true,
                };

                max_in && min_in
            }
            _ => true,
        }
    }
}
impl RangeBounds<TypeData> for Range<TypeData> {
    fn start_bound(&self) -> Bound<&TypeData> {
        match &self.min {
            None => Bound::Unbounded,
            Some(x) => Bound::Included(x),
        }
    }

    fn end_bound(&self) -> Bound<&TypeData> {
        match &self.max {
            None => Bound::Unbounded,
            Some(x) => Bound::Included(x),
        }
    }
}

impl<T: Debug> Debug for Range<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.write_fmt(format_args!("Range({:?} {:?})", self.min, self.max))
    }
}


impl FromReader for Range<TypeData> {
    fn from_reader_and_heap<R: Read>(mut r: R, heap: &[u8]) -> Self {
        let min = Some(TypeData::from_reader_and_heap(&mut r, heap));
        let max = Some(TypeData::from_reader_and_heap(&mut r, heap));
        Self {
            min, max
        }
    }
}

impl BytesSerialize for Range<TypeData> {
    fn serialize_with_heap<W: Write, W1: Write + Seek>(&self, mut data: W, mut heap: W1) {
        self.min.as_ref().unwrap().serialize_with_heap(&mut data, &mut heap);
        self.max.as_ref().unwrap().serialize_with_heap(&mut data, &mut heap);
    }
}