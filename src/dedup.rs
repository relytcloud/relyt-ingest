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

/// Whether a column of this type can take part in a primary key.
///
/// The one truth for it is `append_value_bytes` below, which turns a cell
/// into key bytes; a type it cannot encode cannot be deduplicated. Kept
/// beside it so the two never drift, and called from `TableSchema::validate`
/// (for upsert streams only) so an unusable key fails at `open_table` rather
/// than at the first rotation -- the encoder runs inside the pipeline, where
/// a failure is permanent and the rows are already sealed.
///
/// Only floats are excluded, and on purpose rather than by omission:
/// equality on NaN and on ±0.0 does not agree with the server's, so a float
/// key could deduplicate differently on the two sides. Every other column
/// type this crate accepts can be a key.
///
/// Derived from the column whitelist rather than restating it, so the two
/// cannot disagree about which types exist. The direction that costs
/// something is a type added to `csv::is_supported_type` without an arm in
/// `append_value_bytes`: this would then let it through as a key and the
/// failure would land in the pipeline, where the rows are already sealed.
/// `every_supported_column_type_can_be_a_key` below fails the moment that
/// happens.
pub fn is_supported_pk_type(dt: &DataType) -> bool {
    !matches!(dt, DataType::Float32 | DataType::Float64) && crate::csv::is_supported_type(dt)
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
        // Same shape as the string arms: a length prefix keeps `a|bc` and
        // `ab|c` apart in a composite key.
        DataType::Binary => {
            let v = array.as_binary::<i32>().value(row);
            key.extend((v.len() as u32).to_be_bytes());
            key.extend(v);
        }
        DataType::LargeBinary => {
            let v = array.as_binary::<i64>().value(row);
            key.extend((v.len() as u32).to_be_bytes());
            key.extend(v);
        }
        // Unreachable through the public API: `TableSchema::validate`
        // refuses these at open_table via `is_supported_pk_type`. Kept as a
        // hard error rather than a panic in case a caller builds a writer
        // some other way.
        other => {
            return Err(Error::Schema(format!(
                "column type {other:?} cannot be part of a primary key"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{ArrayRef, BinaryArray, Int64Array, StringArray};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    /// Review finding on the eighth round: `is_supported_pk_type` derives
    /// from the column whitelist, so a column type added there without an
    /// arm in `append_value_bytes` would be accepted as a key and then fail
    /// inside the pipeline, where the rows are already sealed.
    ///
    /// The guard is `sample_of` below: its match over `DataType` is
    /// exhaustive, so a new variant reaching the whitelist stops compiling
    /// until someone decides what it does here. This test then walks every
    /// type the whitelist accepts and asserts the encoder agrees with
    /// `is_supported_pk_type` about it.
    #[test]
    fn every_supported_column_type_can_be_a_key() {
        for dt in all_data_types() {
            if !crate::csv::is_supported_type(&dt) {
                continue;
            }
            let array = sample_of(&dt)
                .unwrap_or_else(|| panic!("{dt:?} is a supported column type with no sample"));
            let mut key = Vec::new();
            let encoded = append_value_bytes(array.as_ref(), 0, &mut key).is_ok();
            assert_eq!(
                encoded,
                is_supported_pk_type(&dt),
                "{dt:?}: is_supported_pk_type and the encoder disagree"
            );
            // Floats are the one deliberate exclusion; everything else the
            // whitelist accepts must be usable as a key.
            assert_eq!(
                encoded,
                !matches!(dt, DataType::Float32 | DataType::Float64),
                "{dt:?}"
            );
        }
    }

    /// Every `DataType` the whitelist could plausibly name, so the loop above
    /// covers the whitelist rather than a hand-kept subset of it.
    fn all_data_types() -> Vec<DataType> {
        vec![
            DataType::Boolean,
            DataType::Int8,
            DataType::Int16,
            DataType::Int32,
            DataType::Int64,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
            DataType::UInt64,
            DataType::Float16,
            DataType::Float32,
            DataType::Float64,
            DataType::Utf8,
            DataType::LargeUtf8,
            DataType::Binary,
            DataType::LargeBinary,
            DataType::Date32,
            DataType::Date64,
            DataType::Time32(TimeUnit::Second),
            DataType::Time64(TimeUnit::Microsecond),
            DataType::Timestamp(TimeUnit::Second, None),
            DataType::Timestamp(TimeUnit::Millisecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, None),
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            DataType::Timestamp(TimeUnit::Nanosecond, None),
            DataType::Decimal128(18, 5),
            DataType::Decimal256(40, 5),
            DataType::Null,
        ]
    }

    /// A one-row array of `dt`, for the types this crate can hold.
    ///
    /// The match is deliberately exhaustive over `DataType`: when arrow adds
    /// a variant, or when someone widens `csv::is_supported_type`, this stops
    /// compiling and the decision — can it be a key, and how is it encoded —
    /// has to be made here rather than discovered in production.
    fn sample_of(dt: &DataType) -> Option<ArrayRef> {
        use arrow_array::{
            BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array, Int16Array,
            Int32Array, Int8Array, LargeBinaryArray, LargeStringArray, TimestampMicrosecondArray,
            TimestampMillisecondArray,
        };
        Some(match dt {
            DataType::Boolean => Arc::new(BooleanArray::from(vec![true])),
            DataType::Int8 => Arc::new(Int8Array::from(vec![1i8])),
            DataType::Int16 => Arc::new(Int16Array::from(vec![1i16])),
            DataType::Int32 => Arc::new(Int32Array::from(vec![1i32])),
            DataType::Int64 => Arc::new(Int64Array::from(vec![1i64])),
            DataType::Float32 => Arc::new(Float32Array::from(vec![1.0f32])),
            DataType::Float64 => Arc::new(Float64Array::from(vec![1.0f64])),
            DataType::Utf8 => Arc::new(StringArray::from(vec!["a"])),
            DataType::LargeUtf8 => Arc::new(LargeStringArray::from(vec!["a"])),
            DataType::Binary => Arc::new(BinaryArray::from(vec![b"a".as_ref()])),
            DataType::LargeBinary => Arc::new(LargeBinaryArray::from(vec![b"a".as_ref()])),
            DataType::Date32 => Arc::new(Date32Array::from(vec![1i32])),
            DataType::Timestamp(TimeUnit::Microsecond, _) => {
                Arc::new(TimestampMicrosecondArray::from(vec![1i64]))
            }
            DataType::Timestamp(TimeUnit::Millisecond, _) => {
                Arc::new(TimestampMillisecondArray::from(vec![1i64]))
            }
            DataType::Decimal128(p, s) => Arc::new(
                Decimal128Array::from(vec![1i128])
                    .with_precision_and_scale(*p, *s)
                    .unwrap(),
            ),
            // Not held by this crate: no sample, and the loop above skips
            // them because the whitelist rejects them. If one ever reaches
            // the whitelist, the assertion there turns this None into a
            // failure naming the type.
            DataType::Null
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64
            | DataType::Float16
            | DataType::Date64
            | DataType::Time32(_)
            | DataType::Time64(_)
            | DataType::Timestamp(_, _)
            | DataType::Duration(_)
            | DataType::Interval(_)
            | DataType::FixedSizeBinary(_)
            | DataType::BinaryView
            | DataType::Utf8View
            | DataType::List(_)
            | DataType::ListView(_)
            | DataType::FixedSizeList(_, _)
            | DataType::LargeList(_)
            | DataType::LargeListView(_)
            | DataType::Struct(_)
            | DataType::Union(_, _)
            | DataType::Dictionary(_, _)
            | DataType::Decimal32(_, _)
            | DataType::Decimal64(_, _)
            | DataType::Decimal256(_, _)
            | DataType::Map(_, _)
            | DataType::RunEndEncoded(_, _) => return None,
        })
    }

    /// Review finding on the seventh round: bytea keys were refused as
    /// "unsupported" when the encoder simply had no arm for them.
    #[test]
    fn binary_keys_deduplicate_and_stay_unambiguous() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Binary, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BinaryArray::from(vec![
                    b"\x00\xff".as_ref(),
                    b"ab".as_ref(),
                    b"\x00\xff".as_ref(),
                ])),
                Arc::new(Int64Array::from(vec![1, 2, 3])),
            ],
        )
        .unwrap();
        // Last write wins within the file: row 0 is superseded by row 2.
        assert_eq!(dedup_last_wins(&[batch], &[0]).unwrap(), vec![vec![1, 2]]);

        // The length prefix keeps composite keys apart: ("a","bc") and
        // ("ab","c") must not collide.
        let mut left = Vec::new();
        let mut right = Vec::new();
        let two = |a: &[u8], b: &[u8], out: &mut Vec<u8>| {
            let arr = BinaryArray::from(vec![a]);
            append_value_bytes(&arr, 0, out).unwrap();
            let arr = BinaryArray::from(vec![b]);
            append_value_bytes(&arr, 0, out).unwrap();
        };
        two(b"a", b"bc", &mut left);
        two(b"ab", b"c", &mut right);
        assert_ne!(left, right);
    }

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
