//! E2E lifecycle coverage over a real isolated ClickHouse 26.8.2.7 server.
//! The controlled HTTP fixture uses RowBinary; row APIs and column projections are real.
#[path = "support/docker.rs"]
mod docker;
use std::{borrow::Cow, collections::BTreeMap};

use clickhouse::{Block, Error, Result, Row, column::ColumnData, row::RowClient};

#[derive(Row)]
#[row(table = "events", rename_all = "camelCase")]
struct LiveRow {
    event_id: u64,
    #[row(column = "symbol")]
    name: String,
    #[row(bytes)]
    payload: Vec<u8>,
    prices: Vec<f64>,
    note: Option<String>,
    active: bool,
    attributes: BTreeMap<String, i64>,
}
#[derive(Row)]
#[row(table = "events", rename_all = "camelCase")]
struct BorrowedRow<'a> {
    event_id: u64,
    #[row(column = "symbol")]
    name: Cow<'a, str>,
    payload: Cow<'a, [u8]>,
    prices: Vec<f64>,
    note: Option<Cow<'a, str>>,
    active: bool,
    attributes: BTreeMap<String, i64>,
}
struct HttpClient {
    address: String,
}
fn err(reason: &'static str) -> Error {
    Error::Config {
        reason: reason.into(),
    }
}
/// The test transport exercises the production typed extension through real HTTP.
impl RowClient for HttpClient {
    fn insert_block(&mut self, table: &str, block: &Block) -> Result<()> {
        if table != "events" {
            return Err(err("unexpected fixture table"));
        }
        let mut body = b"INSERT INTO events FORMAT RowBinary\n".to_vec();
        for row in 0..block.row_count() {
            for col in 0..block.column_count() {
                encode(block.column(col)?, row, &mut body)?;
            }
        }
        docker::request(&self.address, &body)?;
        Ok(())
    }
    fn query_blocks(&mut self, sql: &str) -> Result<Vec<Block>> {
        let bytes = docker::request(&self.address, sql.as_bytes())?;
        let mut input = bytes.as_slice();
        let mut batch = LiveRowBatch::default();
        while !input.is_empty() {
            let event_id = u64::from_le_bytes(
                take(&mut input, 8)?
                    .try_into()
                    .map_err(|_| err("invalid id"))?,
            );
            let name = read_string(&mut input)?;
            let payload = read_bytes(&mut input)?;
            let count = read_varint(&mut input)?;
            let mut prices = Vec::with_capacity(count);
            for _ in 0..count {
                prices.push(f64::from_le_bytes(
                    take(&mut input, 8)?
                        .try_into()
                        .map_err(|_| err("invalid price"))?,
                ));
            }
            let flag = take(&mut input, 1)?[0];
            let note = match flag {
                0 => Some(read_string(&mut input)?),
                1 => None,
                _ => return Err(err("invalid null flag")),
            };
            let active = match take(&mut input, 1)?[0] {
                0 => false,
                1 => true,
                _ => return Err(err("invalid bool")),
            };
            let count = read_varint(&mut input)?;
            let mut attributes = BTreeMap::new();
            for _ in 0..count {
                let key = String::from(read_string(&mut input)?);
                let value = i64::from_le_bytes(
                    take(&mut input, 8)?
                        .try_into()
                        .map_err(|_| err("invalid map value"))?,
                );
                attributes.insert(key, value);
            }
            batch.push_ref(LiveRowRef {
                event_id,
                name,
                payload,
                prices: &prices,
                note,
                active,
                attributes: clickhouse::row::MapRef::BTree(&attributes),
            });
        }
        Ok(vec![batch.to_block()?])
    }
}
/// Encode only the in-memory column families exercised by this fixture.
fn encode(c: &clickhouse::Column, row: usize, out: &mut Vec<u8>) -> Result<()> {
    match &c.data {
        ColumnData::Fixed(bytes) => {
            let width = match c.type_name.as_ref() {
                "UInt64" | "Int64" | "Float64" => 8,
                "Bool" | "UInt8" => 1,
                _ => return Err(err("unsupported fixture numeric type")),
            };
            out.extend_from_slice(
                bytes
                    .as_bytes()
                    .get(row * width..(row + 1) * width)
                    .ok_or_else(|| err("invalid fixed row"))?,
            );
        }
        ColumnData::Strings { offsets, bytes } => {
            let (start, end) = bounds(offsets, row)?;
            write_varint(end - start, out);
            out.extend_from_slice(&bytes[start..end]);
        }
        ColumnData::Array { offsets, values } => {
            let (start, end) = bounds(offsets, row)?;
            write_varint(end - start, out);
            for i in start..end {
                encode(values, i, out)?;
            }
        }
        ColumnData::Nullable { null_map, values } => {
            let flag = *null_map
                .get(row)
                .ok_or_else(|| err("invalid nullable row"))?;
            out.push(flag);
            if flag == 0 {
                encode(values, row, out)?;
            }
        }
        ColumnData::Map {
            offsets,
            keys,
            values,
        } => {
            let (start, end) = bounds(offsets, row)?;
            write_varint(end - start, out);
            for i in start..end {
                encode(keys, i, out)?;
                encode(values, i, out)?;
            }
        }
    }
    Ok(())
}
fn bounds(offsets: &[u64], row: usize) -> Result<(usize, usize)> {
    let end = *offsets.get(row).ok_or_else(|| err("invalid offsets"))? as usize;
    let start = if row == 0 {
        0
    } else {
        offsets[row - 1] as usize
    };
    Ok((start, end))
}
fn write_varint(mut n: usize, out: &mut Vec<u8>) {
    while n >= 128 {
        out.push((n as u8) | 128);
        n >>= 7;
    }
    out.push(n as u8);
}
fn read_varint(input: &mut &[u8]) -> Result<usize> {
    let mut value = 0usize;
    for shift in (0..usize::BITS).step_by(7) {
        let b = take(input, 1)?[0];
        let part = usize::from(b & 127)
            .checked_shl(shift)
            .ok_or_else(|| err("varint overflow"))?;
        value = value
            .checked_add(part)
            .ok_or_else(|| err("varint overflow"))?;
        if b & 128 == 0 {
            return Ok(value);
        }
    }
    Err(err("varint overflow"))
}
fn take<'a>(input: &mut &'a [u8], n: usize) -> Result<&'a [u8]> {
    let (head, tail) = input
        .split_at_checked(n)
        .ok_or_else(|| err("truncated RowBinary"))?;
    *input = tail;
    Ok(head)
}
fn read_bytes<'a>(input: &mut &'a [u8]) -> Result<&'a [u8]> {
    let n = read_varint(input)?;
    take(input, n)
}
fn read_string<'a>(input: &mut &'a [u8]) -> Result<&'a str> {
    std::str::from_utf8(read_bytes(input)?).map_err(|_| err("invalid UTF-8"))
}

/// Borrowed writes, owned reads, projections, and capacity reuse cross a live server.
#[test]
fn real_server_row_roundtrip() {
    let server = docker::Server::start();
    docker::request(&server.address,b"CREATE TABLE events(eventId UInt64,symbol String,payload String,prices Array(Float64),note Nullable(String),active Bool,attributes Map(String,Int64)) ENGINE=Memory").unwrap();
    assert_eq!(
        docker::request(&server.address, b"SELECT version()").unwrap(),
        b"26.8.2.7\n"
    );
    let mut client = HttpClient {
        address: server.address.clone(),
    };
    let mut batch = LiveRowBatch::with_capacity(4);
    let attrs = BTreeMap::from([(String::from("venue"), -7), (String::from("weight"), 9)]);
    batch.push_ref(LiveRowRef {
        event_id: 1,
        name: "BTC",
        payload: &[0, 255, 3],
        prices: &[1.25, -0.0],
        note: Some("hello"),
        active: true,
        attributes: clickhouse::row::MapRef::BTree(&attrs),
    });
    client.insert_batch(&batch).unwrap();
    let capacity = (
        batch.name.bytes.capacity(),
        batch.prices.values.capacity(),
        batch.event_id.capacity(),
    );
    batch.clear();
    batch.push_ref(LiveRowRef {
        event_id: 3,
        name: "ETH",
        payload: &[],
        prices: &[],
        note: None,
        active: false,
        attributes: clickhouse::row::MapRef::Empty,
    });
    assert_eq!(
        capacity,
        (
            batch.name.bytes.capacity(),
            batch.prices.values.capacity(),
            batch.event_id.capacity()
        )
    );
    client
        .insert_rows(&[BorrowedRow {
            event_id: 2,
            name: Cow::Borrowed("ETH"),
            payload: Cow::Borrowed(&[255]),
            prices: vec![3.5],
            note: None,
            active: false,
            attributes: BTreeMap::new(),
        }])
        .unwrap();
    client.insert_batch(&batch).unwrap();
    let query = "SELECT eventId,symbol,payload,prices,note,active,attributes FROM events ORDER BY \
                 eventId FORMAT RowBinary";
    let rows = client.query_rows::<LiveRow>(query).unwrap();
    assert_eq!(rows.len(), 3);
    assert_eq!(rows[0].event_id, 1);
    assert_eq!(rows[0].name, "BTC");
    assert_eq!(rows[0].payload, [0, 255, 3]);
    assert_eq!(
        rows[0]
            .prices
            .iter()
            .map(|v| v.to_bits())
            .collect::<Vec<_>>(),
        [1.25_f64.to_bits(), (-0.0_f64).to_bits()]
    );
    assert_eq!(rows[0].note.as_deref(), Some("hello"));
    assert!(rows[0].active);
    assert_eq!(rows[0].attributes, attrs);
    assert_eq!(rows[1].name, "ETH");
    assert_eq!(rows[1].payload, [255]);
    assert!(rows[1].note.is_none());
    assert!(!rows[1].active);
    assert!(rows[1].attributes.is_empty());
    assert!(rows[2].prices.is_empty());
    let mut visited = 0;
    client
        .query_views::<LiveRow, _>(query, |view| {
            assert_eq!(view.len(), 3);
            for row in view.iter() {
                assert_eq!(row.event_id, view.event_id[visited]);
                visited += 1;
            }
            assert!(view.get(3).is_none());
            Ok(())
        })
        .unwrap();
    assert_eq!(visited, 3);
    drop(client);
    let id = String::from(server.id());
    drop(server);
    let removed = std::process::Command::new("docker")
        .args(["inspect", &id])
        .output()
        .unwrap();
    assert!(
        !removed.status.success(),
        "the owned server container must be removed"
    );
}
