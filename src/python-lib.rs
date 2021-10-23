#![feature(cursor_remaining)]
#![feature(is_sorted)]

mod bytes_serializer;
mod chunk_header;
mod suitable_data_type;
mod tests;
mod main_db;

pub use main_db::{DbManager, Range};
use cpython;
use cpython::{PyBytes, PyResult, Python, PyObject, PyNone, ToPyObject, ObjectProtocol, PyDict, PythonObject, PyList};
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
    start_date: [u8; 8],
    route_id: [u8; 5],
    latitude: f64,
    longitude: f64,
    current_stop_sequence: u8,
    stop_id: u16,
    vehicle_id: u16,
    direction_id: bool,
}

impl BusStruct {
    fn kv_iter<F: Fn(&str, PyObject)>(&self,_p: Python, callable: F) {
        callable("timestamp", ToPyObject::to_py_object(&self.timestamp, _p).into_object());
        callable("trip_id", ToPyObject::to_py_object(&self.trip_id, _p).into_object());
        callable("start_date", PyBytes::new(_p, &self.start_date).into_object());
        callable("route_id", PyBytes::new(_p, &self.route_id).into_object());
        callable("latitude", ToPyObject::to_py_object(&self.latitude, _p).into_object());
        callable("longitude", ToPyObject::to_py_object(&self.longitude, _p).into_object());
        callable("current_stop_sequence", ToPyObject::to_py_object(&self.current_stop_sequence, _p).into_object());
        callable("stop_id", ToPyObject::to_py_object(&self.stop_id, _p).into_object());
        callable("vehicle_id", ToPyObject::to_py_object(&self.vehicle_id, _p).into_object());
        callable("direction_id", ToPyObject::to_py_object(&self.direction_id, _p).into_object());
    }
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


fn str_to_slice<const T: usize>(a: &str) -> [u8; T] {
    if a.len() > T {
        panic!("Passed length exceeds allocated buffer");
    }

    let mut buf = [0u8; T];
    let buf_same_len = &mut buf[0..a.len()];
    buf_same_len.copy_from_slice(a.as_bytes());
    buf
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
         direction_id: bool, ) -> PyResult<cpython::NoArgs> {
    let start_date: [u8; 8] = str_to_slice(start_date);
    let route_id: [u8; 5] = str_to_slice(route_id);
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
    unsafe { init_dbptr() }.store(bus);
    Ok(cpython::NoArgs)
}


fn get(_p: Python, pkey: u64) -> PyResult<PyList> {
    let dbm = unsafe { init_dbptr() };
    let result = dbm.get_in_all(pkey..=pkey);

    let py_result: Vec<_> = result.into_iter().map(|a| {
        let dict = PyDict::new(_p);
        a.kv_iter(_p, |name, value| { dict.set_item(_p, name, value); });
        dict.into_object()
    }).collect();

    Ok(PyList::new(_p, py_result.as_slice()))
}



fn debug_dump(_p: Python) -> PyResult<cpython::NoArgs> {
    let db = unsafe { init_dbptr() };
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
    m.add(py, "get", py_fn!(py, get(pkey: u64)))?;
    Ok(())
});