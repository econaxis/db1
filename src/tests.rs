use rand::distributions::Alphanumeric;
use rand::prelude::SliceRandom;
use rand::Rng;



use std::ops::Range as stdRange;





use crate::*;

pub fn rand_string(len: usize) -> String {
    rand::thread_rng()
        .sample_iter(Alphanumeric)
        .take(len)
        .map(char::from)
        .collect()
}




#[test]
fn test_range() {
    let test_range = Range {
        min: Some(3),
        max: Some(13),
    };
    assert!(!test_range.overlaps(&(15..20)));
    assert!(test_range.overlaps(&(7..20)));
}


// Generate Vec of unique, random integers in range [min, max)
fn generate_int_range<T>(min: T, max: T) -> Vec<T>
where
    stdRange<T>: Iterator<Item = T>,
{
    let mut vec: Vec<_> = (min..max).collect();
    vec.shuffle(&mut rand::thread_rng());
    vec
}
