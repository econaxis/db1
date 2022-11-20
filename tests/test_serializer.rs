extern crate db2;

use db2::TableType;

#[test]
fn serializer_works() {
    use db2::{ChunkHeader, PageSerializer, Range, TypeData};

    let default_ch = ChunkHeader {
        ty: 0,
        tot_len: 0,
        type_size: 0,
        tuple_count: 0,
        heap_size: 0,
        limits: Range {
            min: Some(TypeData::Int(0)),
            max: Some(TypeData::Int(0)),
        },
        compressed_size: 0,
        table_type: TableType::Data,
    };
    let mut ps = PageSerializer::default();
    ps.add_page(vec![0, 1, 2, 3, 4, 5], default_ch.clone());
    ps.add_page(vec![5, 6, 9, 1, 2, 3], default_ch);

    let mut f = std::mem::take(&mut ps.file);
    f.set_position(0);
    let ps1 = PageSerializer::create_from_reader(f, None);
    dbg!(&ps1.previous_headers);
}

#[test]
fn delete_works() {
    use db2::{ChunkHeader, PageSerializer, Range, TypeData};
    let mut ps = PageSerializer::default();
    let loc = ps.add_page(
        vec![1u8; 100],
        ChunkHeader {
            ty: 0,
            tot_len: 0,
            type_size: 0,
            tuple_count: 0,
            heap_size: 0,
            limits: Range {
                min: Some(TypeData::Int(3)),
                max: Some(TypeData::Int(3)),
            },
            compressed_size: 0,
            table_type: TableType::Data,
        },
    );

    assert_eq!(ps.get_in_all(0, Some(TypeData::Int(3))).first(), Some(&loc));
    ps.free_page(0, TypeData::Int(3));
    assert_eq!(ps.get_in_all(0, Some(TypeData::Int(3))).first(), None);

    let ps1 = PageSerializer::create_from_reader(std::mem::take(&mut ps.file), None);
    assert_eq!(ps1.get_in_all(0, Some(TypeData::Int(3))).first(), None);
}
