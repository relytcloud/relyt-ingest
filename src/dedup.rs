//! Intra-file primary-key dedup, last write wins.
//!
//! Rationale: duplicate PKs *within one staged file* make the Relyt master
//! reject the whole load and roll it back. Without this pass, an update stream would produce a
//! poison file that parks its partition's serial queue. Duplicates *across*
//! files are fine (different statements, group serialized by seq).

use std::collections::HashMap;

use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{Array, RecordBatch};
use arrow_schema::{DataType, TimeUnit};

use crate::error::{Error, Result};

/// Selects, per input batch, the rows that survive last-wins dedup over the
/// given PK columns. Each surviving key keeps its LAST occurrence, and those
/// survivors are emitted in ascending (batch_index, row_index) order — i.e.
/// the slot of the final version, not of the first sighting. Output pairs are
/// grouped back into per-batch row lists ready for
/// [`crate::csv::CsvFormatter::format_rows`].
pub fn dedup_last_wins(batches: &[RecordBatch], pk_columns: &[usize]) -> Result<Vec<Vec<usize>>> {
    // key -> (batch_idx, row_idx) of the latest occurrence.
    let mut latest: HashMap<Vec<u8>, (usize, usize)> = HashMap::new();
    for (bi, batch) in batches.iter().enumerate() {
        for row in 0..batch.num_rows() {
            let key = pk_key(batch, pk_columns, row)?;
            latest.insert(key, (bi, row));
        }
    }
    let mut keep: Vec<Vec<usize>> = vec![Vec::new(); batches.len()];
    let mut winners: Vec<(usize, usize)> = latest.into_values().collect();
    // Deterministic output order within the file (order inside one load does
    // not matter semantically, but stable bytes keep retries byte-identical).
    winners.sort_unstable();
    for (bi, row) in winners {
        keep[bi].push(row);
    }
    Ok(keep)
}

/// Binary key for the PK columns of one row. NULL PK parts are encoded with a
/// distinct tag so (NULL) != ("").
fn pk_key(batch: &RecordBatch, pk_columns: &[usize], row: usize) -> Result<Vec<u8>> {
    let mut key = Vec::with_capacity(16);
    for &col in pk_columns {
        let array = batch.column(col);
        if array.is_null(row) {
            key.push(0u8); // NULL tag
            continue;
        }
        key.push(1u8);
        append_value_bytes(array.as_ref(), row, &mut key).map_err(|_| {
            Error::Schema(format!(
                "unsupported PK column type {:?} (column #{col})",
                array.data_type()
            ))
        })?;
        // Field separator. 0xff can occur inside a fixed-width encoding (-1i64
        // is all-0xff bytes), so this is not a "byte that cannot appear" trick:
        // the encoding is unambiguous because every element is either
        // fixed-width or length-prefixed, which makes field boundaries
        // recoverable regardless of the separator's value.
        key.push(0xff);
    }
    Ok(key)
}

fn append_value_bytes(array: &dyn Array, row: usize, key: &mut Vec<u8>) -> Result<()> {
    match array.data_type() {
        DataType::Boolean => key.push(array.as_boolean().value(row) as u8),
        DataType::Int8 => {
            key.extend((array.as_primitive::<Int8Type>().value(row) as i64).to_be_bytes())
        }
        DataType::Int16 => {
            key.extend((array.as_primitive::<Int16Type>().value(row) as i64).to_be_bytes())
        }
        DataType::Int32 => {
            key.extend((array.as_primitive::<Int32Type>().value(row) as i64).to_be_bytes())
        }
        DataType::Int64 => key.extend(array.as_primitive::<Int64Type>().value(row).to_be_bytes()),
        DataType::Date32 => {
            key.extend((array.as_primitive::<Date32Type>().value(row) as i64).to_be_bytes())
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => key.extend(
            array
                .as_primitive::<TimestampMicrosecondType>()
                .value(row)
                .to_be_bytes(),
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => key.extend(
            array
                .as_primitive::<TimestampMillisecondType>()
                .value(row)
                .to_be_bytes(),
        ),
        DataType::Decimal128(_, _) => key.extend(
            array
                .as_primitive::<Decimal128Type>()
                .value(row)
                .to_be_bytes(),
        ),
        DataType::Utf8 => {
            let v = array.as_string::<i32>().value(row).as_bytes();
            key.extend((v.len() as u32).to_be_bytes());
            key.extend(v);
        }
        DataType::LargeUtf8 => {
            let v = array.as_string::<i64>().value(row).as_bytes();
            key.extend((v.len() as u32).to_be_bytes());
            key.extend(v);
        }
        other => {
            return Err(Error::Schema(format!("unsupported PK type {other:?}")));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, StringArray};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn batch(ids: Vec<i64>, vals: Vec<&str>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("v", DataType::Utf8, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(ids)),
                Arc::new(StringArray::from(vals)),
            ],
        )
        .unwrap()
    }

    #[test]
    fn last_wins_within_batch() {
        let b = batch(vec![1, 2, 1], vec!["old", "x", "new"]);
        let keep = dedup_last_wins(&[b], &[0]).unwrap();
        // id=1 keeps row 2 (last), id=2 keeps row 1.
        assert_eq!(keep, vec![vec![1, 2]]);
    }

    #[test]
    fn last_wins_across_batches() {
        let b1 = batch(vec![1, 2], vec!["a", "b"]);
        let b2 = batch(vec![2, 3], vec!["b2", "c"]);
        let keep = dedup_last_wins(&[b1, b2], &[0]).unwrap();
        assert_eq!(keep[0], vec![0]); // id=1 from batch 0
        assert_eq!(keep[1], vec![0, 1]); // id=2 (updated), id=3
    }

    #[test]
    fn no_duplicates_is_identity() {
        let b = batch(vec![1, 2, 3], vec!["a", "b", "c"]);
        let keep = dedup_last_wins(&[b], &[0]).unwrap();
        assert_eq!(keep, vec![vec![0, 1, 2]]);
    }
}
