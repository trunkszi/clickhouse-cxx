//! Integration scenarios for callback dispatch and failure-safe batch replacement.
use clickhouse::{Block, Error, Result, Row, row::RowClient};

#[derive(Row, serde::Deserialize)]
struct Record {
    value: u64,
}

struct Incremental {
    blocks: Vec<Block>,
    delivered: usize,
}

impl RowClient for Incremental {
    fn insert_block(&mut self, _: &str, _: &Block) -> Result<()> {
        unreachable!("read-only test transport")
    }
    fn query_blocks(&mut self, _: &str) -> Result<Vec<Block>> {
        panic!("callback reads must use the streaming override")
    }
    fn query_blocks_for_each(
        &mut self,
        _: &str,
        mut visit: impl FnMut(Block) -> Result<()>,
    ) -> Result<()> {
        for block in self.blocks.drain(..) {
            self.delivered += 1;
            visit(block)?;
        }
        Ok(())
    }
}

fn incremental() -> Incremental {
    let mut blocks = Vec::new();
    for value in [11, 19] {
        let mut batch = RecordBatch::default();
        batch.push(&Record { value });
        blocks.push(batch.to_block().unwrap());
    }
    Incremental {
        blocks,
        delivered: 0,
    }
}

/// Every callback facade dispatches through the transport's incremental hook.
#[test]
fn integration_callback_dispatch() {
    let mut values = Vec::new();
    incremental()
        .query_views::<Record, _>("SELECT", |view| {
            values.extend(view.iter().map(|row| row.value));
            Ok(())
        })
        .unwrap();
    assert_eq!(values, [11, 19]);
    let mut values = Vec::new();
    assert_eq!(
        incremental()
            .query_for_each::<Record, _>("SELECT", |row| {
                values.push(row.value);
                Ok(())
            })
            .unwrap(),
        2
    );
    assert_eq!(values, [11, 19]);
    let mut values = Vec::new();
    incremental()
        .query_row_refs::<Record, _>("SELECT", |row| {
            values.push(row.value);
            Ok(())
        })
        .unwrap();
    assert_eq!(values, [11, 19]);
    let mut values = Vec::new();
    incremental()
        .query_serde_for_each::<Record, _>("SELECT", |row| {
            values.push(row.value);
            Ok(())
        })
        .unwrap();
    assert_eq!(values, [11, 19]);
}

/// Callback rejection stops delivery before fetching another block.
#[test]
fn scenario_callback_stops_delivery() {
    let mut source = incremental();
    assert!(
        source
            .query_for_each::<Record, _>("SELECT", |_| {
                Err(Error::Protocol("consumer rejected row".into()))
            })
            .is_err()
    );
    assert_eq!(source.delivered, 1);
}

struct Failing {
    blocks: Option<Vec<Block>>,
}
impl RowClient for Failing {
    fn insert_block(&mut self, _: &str, _: &Block) -> Result<()> {
        unreachable!("read-only test transport")
    }
    fn query_blocks(&mut self, _: &str) -> Result<Vec<Block>> {
        self.blocks
            .take()
            .ok_or_else(|| Error::Protocol("transport failed".into()))
    }
}

/// Transport errors and invalid later blocks preserve all existing batch rows.
#[test]
fn scenario_batch_failure_preserves_rows() {
    let mut destination = RecordBatch::default();
    destination.push(&Record { value: 42 });
    let mut transport = Failing { blocks: None };
    assert!(
        transport
            .query_batch_into::<Record>("SELECT", &mut destination)
            .is_err()
    );
    let mut blocks = incremental().blocks;
    blocks.push(Block::new());
    let mut schema = Failing {
        blocks: Some(blocks),
    };
    assert!(
        schema
            .query_batch_into::<Record>("SELECT", &mut destination)
            .is_err()
    );
    assert_eq!(destination.len(), 1);
    let block = destination.to_block().unwrap();
    assert_eq!(
        RecordBlockView::try_from_block(&block)
            .unwrap()
            .get(0)
            .unwrap()
            .value,
        42
    );
}
