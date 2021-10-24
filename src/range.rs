use std::collections::Bound;
use std::fmt::{Debug, Formatter};
use std::io::{Read, Write};
use std::ops::RangeBounds;

use crate::bytes_serializer::{BytesSerialize, FromReader};
use crate::suitable_data_type::SuitableDataType;

const CHECK_SEQUENCE: u8 = 98;

impl<T: SuitableDataType> BytesSerialize for Range<T> {
    fn serialize<W: Write>(&self, mut w: W) {
        w.write_all(&CHECK_SEQUENCE.to_le_bytes()).unwrap();
        self.min.as_ref().unwrap().serialize(&mut w);
        self.max.as_ref().unwrap().serialize(w);
    }
}


#[derive(Clone)]
pub struct Range<T> {
    pub(crate) min: Option<T>,
    pub(crate) max: Option<T>,
}

impl<T: Ord + Clone> Range<T> {
    pub fn new(init: Option<T>) -> Self {
        Self { min: init.clone(), max: init }
    }
    // Returns comp(lhs, rhs) if both are defined value, else returns True
    fn check_else_true<A, F: FnOnce(A, A) -> bool>(lhs: Option<A>, rhs: Option<A>, comp: F) -> bool {
        match lhs.is_some() && rhs.is_some() {
            false => true,
            true => comp(lhs.unwrap(), rhs.unwrap())
        }
    }

    // Adds new element to the range, potentially expanding the min and max extrema
    pub fn add(&mut self, new_elt: &T) {
        if Self::check_else_true(Some(new_elt), self.min.as_ref(), |new, min| new < min) {
            self.min = Some(new_elt.clone());
        }
        if Self::check_else_true(Some(new_elt), self.max.as_ref(), |new, max| new > max) {
            self.max = Some(new_elt.clone());
        }
    }
}

impl<T: SuitableDataType> FromReader for Range<T> {
    fn from_reader<R: Read>(r: &mut R) -> Self {
        let mut check = [0u8; 1];
        r.read_exact(&mut check).unwrap();
        assert_eq!(check[0], CHECK_SEQUENCE);
        let min = T::from_reader(r);
        let max = T::from_reader(r);
        Self { min: Some(min), max: Some(max) }
    }
}


impl<T: SuitableDataType> Range<T> {
    pub fn overlaps<RB: RangeBounds<u64>>(&self, rb: &RB) -> bool {
        match (&self.min, &self.max) {
            (Some(min), Some(max)) => {
                let min_in = match rb.start_bound() {
                    Bound::Included(start) => max >= start,
                    Bound::Excluded(start) => max > start,
                    Bound::Unbounded => true
                };
                let max_in = match rb.end_bound() {
                    Bound::Included(end) => min <= end,
                    Bound::Excluded(end) => min < end,
                    Bound::Unbounded => true
                };

                max_in && min_in
            }
            _ => panic!("Must not be None to compare")
        }
    }
}

impl<T: SuitableDataType> Debug for Range<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_tuple("Range")
            .field(&self.min)
            .field(&self.max)
            .finish()
    }
}
