#![feature(cursor_remaining)]
#![feature(is_sorted)]

mod bytes_serializer;
mod chunk_header;
mod suitable_data_type;
mod tests;
mod main_db;

pub use main_db::{DbManager, Range};
use cpython;
use cpython::{PyBytes, PyResult, Python};
use std::cmp::Ordering;
use crate::suitable_data_type::SuitableDataType;
use crate::bytes_serializer::{BytesSerialize, FromReader};
use std::io::Read;
use std::mem::MaybeUninit;
use crate::chunk_header::slice_from_type;
use crate::main_db::DbBase;

use cpython::{py_module_initializer, py_fn};
use std::convert::TryInto;

#[repr(C)]
#[derive(Debug, Clone)]
struct BusStruct {
    timestamp: u64,
    trip_id: u32,
    start_date: [u8;8],
    route_id: [u8;5],
    latitude: f64,
    longitude: f64,
    current_stop_sequence: u8,
    stop_id: u16,
    vehicle_id: u16,
    direction_id: bool,
}

impl SuitableDataType for BusStruct {
    fn first(&self) -> u64 {
        self.timestamp
    }
}
impl BytesSerialize for BusStruct {}
gen_suitable_data_type_impls!(BusStruct);
unsafe fn raw_ptr_to_slice<'a, T, A: 'a>(ptr: *mut T, _lifetime: &A) -> &'a mut [u8] {
    std::slice::from_raw_parts_mut(ptr as *mut u8, std::mem::size_of::<T>())
}
impl FromReader for BusStruct {
    fn from_reader<R: Read>(r: &mut R) -> Self {
        let mut buf = MaybeUninit::<BusStruct>::uninit();
        let buf_u8 = unsafe { raw_ptr_to_slice(buf.as_mut_ptr(), &buf) };
        r.read_exact(buf_u8).unwrap();
        unsafe { buf.assume_init() }
    }
}

static mut DBPTR: *mut DbManager<BusStruct> = std::ptr::null::<DbManager<BusStruct>>() as *mut _;


unsafe fn init_dbptr() -> &'static mut DbManager<BusStruct> {
    if DBPTR as *const _ == std::ptr::null() {
        let db = Box::new(DbManager::new(DbBase::default()));
        let dbptr = Box::leak(db) as *mut DbManager<BusStruct>;
        DBPTR = dbptr;
    }
    &mut *DBPTR
}


fn store(_p: Python, timestamp: u64,
         trip_id: u32,
         start_date: &str,
         route_id: &str,
         latitude: f64,
         longitude: f64,
         current_stop_sequence: u8,
         stop_id: u16,
         vehicle_id: u16,
         direction_id: bool,) -> PyResult<cpython::NoArgs>{
    let start_date: [u8; 8] = start_date.as_bytes().try_into().unwrap();
    let route_id: [u8; 5] = route_id.as_bytes().try_into().unwrap();
    let bus = BusStruct {
        timestamp,
        trip_id,
        start_date,
        route_id,
        direction_id,
        latitude,
        longitude,
        current_stop_sequence,
        stop_id,
        vehicle_id,
    };
    unsafe {init_dbptr()}.store(bus);
    Ok(cpython::NoArgs)
}

fn debug_dump(_p: Python) -> PyResult<cpython::NoArgs> {
    let db = unsafe {init_dbptr()};
    println!("{:?}", db);
    Ok(cpython::NoArgs)
}

py_module_initializer!(libpythonlib, |py, m| {
    m.add(py, "store", py_fn!(py, store(timestamp: u64,
    trip_id: u32,
    start_date: &str,
    route_id: &str,
    latitude: f64,
    longitude: f64,
    current_stop_sequence: u8,
    stop_id: u16,
    vehicle_id: u16,
    direction_id: bool)))?;
    m.add(py, "debug_dump", py_fn!(py, debug_dump()))?;
    Ok(())
});