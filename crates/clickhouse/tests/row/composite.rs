use super::*;

struct LengthTrap;

impl Field for LengthTrap {
    type Input<'a> = u8;
    type Buffer = Vec<u8>;
    type View<'a> = &'a [u8];

    fn reserve(buffer: &mut Self::Buffer, rows: usize) {
        buffer.reserve(rows);
    }
    fn push(buffer: &mut Self::Buffer, value: u8) {
        buffer.push(value);
    }
    fn len(buffer: &Self::Buffer) -> usize {
        buffer.len()
    }
    fn capacity(buffer: &Self::Buffer) -> usize {
        buffer.capacity()
    }
    fn empty() -> u8 {
        0
    }
    fn borrow(&self) -> u8 {
        0
    }
    fn view(column: &Column) -> Result<Self::View<'_>> {
        <u8 as Field>::view(column)
    }
    fn view_len(_: Self::View<'_>) -> usize {
        panic!("view_len must follow nullable validation");
    }
    fn get<'a>(view: Self::View<'a>, row: usize) -> Option<Self::Input<'a>> {
        view.get(row).copied()
    }
    fn column(buffer: &Self::Buffer, name: &'static str) -> Result<Column> {
        <u8 as Field>::column(buffer, name)
    }
}

#[test]
fn invalid_map_skips_custom_length() {
    for null_map in [vec![0, 2], vec![0]] {
        let values = <u8 as Field>::column(&vec![7, 9], "values").unwrap();
        let column = Column {
            name: "nullable".into(),
            type_name: "Nullable(UInt8)".into(),
            rows: 2,
            data: ColumnData::Nullable {
                null_map,
                values: Box::new(values),
            },
        };
        let result = <Option<LengthTrap> as Field>::view(&column);
        let Err(error) = result else {
            panic!("malformed null map was accepted");
        };
        assert_eq!(
            error.to_string(),
            "schema mismatch: invalid nullable column"
        );
    }
}
