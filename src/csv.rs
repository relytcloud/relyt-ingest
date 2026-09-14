//! CSV serialization, in the PostgreSQL-CSV dialect the Relyt master's loader
//! expects:
//!
//! - every non-NULL value is double-quoted (even numbers);
//! - NULL is a bare empty field (no quotes);
//! - the empty string is `""`;
//! - embedded `"` doubles to `""`;
//! - NUL bytes (\0) are stripped (the loader rejects them);
//! - decimals render plain (no scientific notation);
//! - header line with column names is always written (the load options
//!   declare `header=true`).
//!
//! Only the delimiter is configurable, and it must round-trip into the job's
//! load options (see [`crate::notify`]).

use std::fmt::Write as _;

use arrow_array::cast::AsArray;
use arrow_array::types::*;
use arrow_array::{
    Array, BinaryArray, BooleanArray, Date32Array, Decimal128Array, Float32Array, Float64Array,
    Int16Array, Int32Array, Int64Array, Int8Array, LargeBinaryArray, LargeStringArray, RecordBatch,
    StringArray, TimestampMicrosecondArray, TimestampMillisecondArray,
};
use arrow_schema::{DataType, TimeUnit};

use crate::error::{Error, Result};

/// Column types the SDK accepts. Anything else fails loudly at `open_table`.
pub fn is_supported_type(dt: &DataType) -> bool {
    matches!(
        dt,
        DataType::Boolean
            | DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::Float32
            | DataType::Float64
            | DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Date32
            | DataType::Timestamp(TimeUnit::Microsecond, _)
            | DataType::Timestamp(TimeUnit::Millisecond, _)
            | DataType::Decimal128(_, _)
            | DataType::Binary
            | DataType::LargeBinary
    )
}

pub struct CsvFormatter {
    delimiter: char,
}

impl CsvFormatter {
    pub fn new(delimiter: char) -> Self {
        Self { delimiter }
    }

    /// Header line (always present; the load options declare `header=true`).
    pub fn header(&self, column_names: &[&str]) -> String {
        let mut out = String::new();
        for (i, name) in column_names.iter().enumerate() {
            if i > 0 {
                out.push(self.delimiter);
            }
            quote_into(&mut out, name);
        }
        out.push('\n');
        out
    }

    /// Serialize the rows of `batch` selected by `rows` (in the given order)
    /// into `out`. `rows` lets the PK dedup pass drop superseded rows without
    /// copying the batch.
    ///
    /// Every column is downcast once per batch and each cell is written
    /// straight into `out` (quoted, NUL-stripped): no per-cell String, no
    /// per-cell schema lookup, no per-cell `DataType` dispatch -- this loop
    /// runs under the writer's state lock for every rotation.
    /// Callers reserve `out` up front; see `StageStats::bytes` for the
    /// calibration this makes possible.
    pub fn format_rows(&self, batch: &RecordBatch, rows: &[usize], out: &mut String) -> Result<()> {
        let schema = batch.schema();
        let cols = batch
            .columns()
            .iter()
            .zip(schema.fields().iter())
            .map(|(array, field)| Col::bind(array.as_ref(), field.name()))
            .collect::<Result<Vec<Col<'_>>>>()?;
        for &row in rows {
            for (i, col) in cols.iter().enumerate() {
                if i > 0 {
                    out.push(self.delimiter);
                }
                col.write_cell(row, out);
            }
            out.push('\n');
        }
        Ok(())
    }
}

/// One column of a batch, downcast once so the row loop neither matches on
/// `DataType` nor allocates per cell.
enum Col<'a> {
    Bool(&'a BooleanArray),
    I8(&'a Int8Array),
    I16(&'a Int16Array),
    I32(&'a Int32Array),
    I64(&'a Int64Array),
    F32(&'a Float32Array),
    F64(&'a Float64Array),
    Str(&'a StringArray),
    LargeStr(&'a LargeStringArray),
    Date(&'a Date32Array),
    /// (values, tz-aware)
    TsMicros(&'a TimestampMicrosecondArray, bool),
    TsMillis(&'a TimestampMillisecondArray, bool),
    /// (values, scale)
    Decimal(&'a Decimal128Array, i8),
    Bin(&'a BinaryArray),
    LargeBin(&'a LargeBinaryArray),
}

impl<'a> Col<'a> {
    fn bind(array: &'a dyn Array, column: &str) -> Result<Self> {
        Ok(match array.data_type() {
            DataType::Boolean => Col::Bool(array.as_boolean()),
            DataType::Int8 => Col::I8(array.as_primitive::<Int8Type>()),
            DataType::Int16 => Col::I16(array.as_primitive::<Int16Type>()),
            DataType::Int32 => Col::I32(array.as_primitive::<Int32Type>()),
            DataType::Int64 => Col::I64(array.as_primitive::<Int64Type>()),
            DataType::Float32 => Col::F32(array.as_primitive::<Float32Type>()),
            DataType::Float64 => Col::F64(array.as_primitive::<Float64Type>()),
            DataType::Utf8 => Col::Str(array.as_string::<i32>()),
            DataType::LargeUtf8 => Col::LargeStr(array.as_string::<i64>()),
            DataType::Date32 => Col::Date(array.as_primitive::<Date32Type>()),
            DataType::Timestamp(TimeUnit::Microsecond, tz) => Col::TsMicros(
                array.as_primitive::<TimestampMicrosecondType>(),
                tz.is_some(),
            ),
            DataType::Timestamp(TimeUnit::Millisecond, tz) => Col::TsMillis(
                array.as_primitive::<TimestampMillisecondType>(),
                tz.is_some(),
            ),
            DataType::Decimal128(_, scale) => {
                Col::Decimal(array.as_primitive::<Decimal128Type>(), *scale)
            }
            DataType::Binary => Col::Bin(array.as_binary::<i32>()),
            DataType::LargeBinary => Col::LargeBin(array.as_binary::<i64>()),
            other => {
                return Err(Error::UnsupportedType {
                    column: column.to_string(),
                    data_type: format!("{other:?}"),
                })
            }
        })
    }

    /// Append one cell: a bare empty field for NULL, otherwise the quoted
    /// text. Numbers, dates and hex never contain `"` or NUL, so they are
    /// written between two quotes directly; text goes through `quote_into`.
    fn write_cell(&self, row: usize, out: &mut String) {
        macro_rules! null_is_empty {
            ($a:expr) => {
                if $a.is_null(row) {
                    return;
                }
            };
        }
        match self {
            // PostgreSQL CSV accepts t/f; keep the canonical short form.
            Col::Bool(a) => {
                null_is_empty!(a);
                out.push_str(if a.value(row) { "\"t\"" } else { "\"f\"" });
            }
            Col::I8(a) => {
                null_is_empty!(a);
                write_quoted(out, a.value(row));
            }
            Col::I16(a) => {
                null_is_empty!(a);
                write_quoted(out, a.value(row));
            }
            Col::I32(a) => {
                null_is_empty!(a);
                write_quoted(out, a.value(row));
            }
            Col::I64(a) => {
                null_is_empty!(a);
                write_quoted(out, a.value(row));
            }
            // Rust's Display for f32/f64 never switches to scientific notation
            // (1e21f64 renders as "1000000000000000000000"), so "decimal plain"
            // holds. Non-finite values render as "NaN" / "inf" / "-inf", and
            // the server-side load accepts that spelling as-is: the kernel's
            // float4in/float8in take "inf" case-insensitively, and
            // all_supported_types_round_trip proves it end to end with two
            // non-finite rows that read back as NaN / Infinity / -Infinity.
            Col::F32(a) => {
                null_is_empty!(a);
                write_quoted(out, a.value(row));
            }
            Col::F64(a) => {
                null_is_empty!(a);
                write_quoted(out, a.value(row));
            }
            Col::Str(a) => {
                null_is_empty!(a);
                quote_into(out, a.value(row));
            }
            Col::LargeStr(a) => {
                null_is_empty!(a);
                quote_into(out, a.value(row));
            }
            Col::Date(a) => {
                null_is_empty!(a);
                out.push('"');
                write_date32(out, a.value(row));
                out.push('"');
            }
            // tz-aware columns carry an explicit `+00` offset: the values are UTC
            // instants (schema.rs maps `timestamp with time zone` to UTC), and
            // without the offset the Relyt master would reinterpret the text in
            // the session TimeZone and shift the whole column.
            Col::TsMicros(a, tz) => {
                null_is_empty!(a);
                out.push('"');
                write_timestamp_micros(out, a.value(row), *tz);
                out.push('"');
            }
            Col::TsMillis(a, tz) => {
                null_is_empty!(a);
                out.push('"');
                write_timestamp_micros(out, a.value(row) * 1000, *tz);
                out.push('"');
            }
            Col::Decimal(a, scale) => {
                null_is_empty!(a);
                out.push('"');
                write_decimal128(out, a.value(row), *scale);
                out.push('"');
            }
            // bytea hex input: `\x` + two hex digits per byte. Inside a quoted
            // CSV field the backslash is literal, so this reaches the bytea
            // input function untouched.
            Col::Bin(a) => {
                null_is_empty!(a);
                out.push('"');
                write_hex_bytea(out, a.value(row));
                out.push('"');
            }
            Col::LargeBin(a) => {
                null_is_empty!(a);
                out.push('"');
                write_hex_bytea(out, a.value(row));
                out.push('"');
            }
        }
    }
}

/// `"<Display>"` for values that can contain neither `"` nor NUL. Writing
/// through `fmt::Write` into the output String allocates nothing.
fn write_quoted<T: std::fmt::Display>(out: &mut String, v: T) {
    out.push('"');
    let _ = write!(out, "{v}");
    out.push('"');
}

/// Two hex digits per byte from a nibble table (a `format!` per byte
/// allocated a String per byte).
fn write_hex_bytea(out: &mut String, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    out.push_str("\\x");
    for &b in bytes {
        out.push(HEX[(b >> 4) as usize] as char);
        out.push(HEX[(b & 0x0f) as usize] as char);
    }
}

/// Quote-always, doubling embedded `"` and dropping NUL (the loader rejects \0
/// anywhere in the input): `abc` -> `"abc"`, `a"b` -> `"a""b"`, `` -> `""`.
fn quote_into(out: &mut String, value: &str) {
    out.push('"');
    if !value.contains(['"', '\0']) {
        out.push_str(value);
    } else {
        for c in value.chars() {
            match c {
                '\0' => {}
                '"' => out.push_str("\"\""),
                c => out.push(c),
            }
        }
    }
    out.push('"');
}

/// Plain (non-scientific) decimal rendering for Decimal128, digits produced
/// into a stack buffer (an i128 has at most 39).
fn write_decimal128(out: &mut String, raw: i128, scale: i8) {
    if scale <= 0 {
        // Negative scale multiplies by 10^-scale; rare, render exactly.
        let _ = write!(out, "{raw}");
        for _ in 0..(-scale) {
            out.push('0');
        }
        return;
    }
    let scale = scale as usize;
    if raw < 0 {
        out.push('-');
    }
    let mut buf = [b'0'; 40];
    let mut n = raw.unsigned_abs();
    let mut start = buf.len() - 1; // "0" when raw == 0
    if n > 0 {
        start = buf.len();
        while n > 0 {
            start -= 1;
            buf[start] = b'0' + (n % 10) as u8;
            n /= 10;
        }
    }
    let digits = &buf[start..];
    let push_ascii = |out: &mut String, bytes: &[u8]| {
        for &b in bytes {
            out.push(b as char);
        }
    };
    if digits.len() > scale {
        push_ascii(out, &digits[..digits.len() - scale]);
        out.push('.');
        push_ascii(out, &digits[digits.len() - scale..]);
    } else {
        out.push_str("0.");
        for _ in 0..(scale - digits.len()) {
            out.push('0');
        }
        push_ascii(out, digits);
    }
}

/// Days-since-epoch -> `YYYY-MM-DD` (proleptic Gregorian, no chrono dep).
fn write_date32(out: &mut String, days: i32) {
    // Civil-from-days algorithm (Howard Hinnant), valid for the whole i32 range.
    let z = days as i64 + 719_468;
    let era = if z >= 0 { z } else { z - 146_096 } / 146_097;
    let doe = (z - era * 146_097) as u64;
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146096) / 365;
    let y = yoe as i64 + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    let _ = write!(out, "{y:04}-{m:02}-{d:02}");
}

/// Microseconds-since-epoch -> `YYYY-MM-DD HH:MM:SS[.ffffff][+00]` (UTC),
/// fractional seconds with trailing zeros trimmed.
fn write_timestamp_micros(out: &mut String, micros: i64, with_offset: bool) {
    let days = micros.div_euclid(86_400_000_000);
    let in_day = micros.rem_euclid(86_400_000_000);
    write_date32(out, days as i32);
    let secs = in_day / 1_000_000;
    let mut frac = in_day % 1_000_000;
    let (h, m, s) = (secs / 3600, (secs % 3600) / 60, secs % 60);
    let _ = write!(out, " {h:02}:{m:02}:{s:02}");
    if frac != 0 {
        let mut width = 6;
        while frac % 10 == 0 {
            frac /= 10;
            width -= 1;
        }
        let _ = write!(out, ".{frac:0width$}");
    }
    if with_offset {
        out.push_str("+00");
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Decimal128Array, Int64Array, StringArray};
    use arrow_schema::{Field, Schema};
    use std::sync::Arc;

    fn batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int64, false),
            Field::new("name", DataType::Utf8, true),
            Field::new("amount", DataType::Decimal128(20, 4), true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a\"b"), None, Some("")])),
                Arc::new(
                    Decimal128Array::from(vec![Some(12345678901234_i128), None, Some(-50)])
                        .with_precision_and_scale(20, 4)
                        .unwrap(),
                ),
            ],
        )
        .unwrap()
    }

    #[test]
    fn quoting_null_and_empty() {
        let f = CsvFormatter::new(',');
        let mut out = String::new();
        f.format_rows(&batch(), &[0, 1, 2], &mut out).unwrap();
        let lines: Vec<&str> = out.lines().collect();
        // Non-NULL always quoted; embedded quote doubled; decimal plain.
        assert_eq!(lines[0], r#""1","a""b","1234567890.1234""#);
        // NULL = bare empty field.
        assert_eq!(lines[1], r#""2",,"#);
        // Empty string = "" ; small negative decimal zero-padded.
        assert_eq!(lines[2], r#""3","","-0.0050""#);
    }

    #[test]
    fn header_quoted() {
        let f = CsvFormatter::new(',');
        assert_eq!(f.header(&["id", "name"]), "\"id\",\"name\"\n");
    }

    #[test]
    fn row_subset_preserves_order() {
        let f = CsvFormatter::new(',');
        let mut out = String::new();
        f.format_rows(&batch(), &[2, 0], &mut out).unwrap();
        let lines: Vec<&str> = out.lines().collect();
        assert!(lines[0].starts_with("\"3\""));
        assert!(lines[1].starts_with("\"1\""));
    }

    #[test]
    fn nul_stripped() {
        let schema = Arc::new(Schema::new(vec![Field::new("s", DataType::Utf8, true)]));
        let b = RecordBatch::try_new(
            schema,
            vec![Arc::new(StringArray::from(vec![Some("a\0b")]))],
        )
        .unwrap();
        let f = CsvFormatter::new(',');
        let mut out = String::new();
        f.format_rows(&b, &[0], &mut out).unwrap();
        assert_eq!(out, "\"ab\"\n");
    }

    fn date32(days: i32) -> String {
        let mut s = String::new();
        write_date32(&mut s, days);
        s
    }

    fn ts_micros(micros: i64, with_offset: bool) -> String {
        let mut s = String::new();
        write_timestamp_micros(&mut s, micros, with_offset);
        s
    }

    fn decimal128(raw: i128, scale: i8) -> String {
        let mut s = String::new();
        write_decimal128(&mut s, raw, scale);
        s
    }

    #[test]
    fn date_and_timestamp() {
        assert_eq!(date32(0), "1970-01-01");
        assert_eq!(date32(19723), "2024-01-01");
        assert_eq!(ts_micros(0, false), "1970-01-01 00:00:00");
        assert_eq!(
            ts_micros(1_704_067_200_500_000, false),
            "2024-01-01 00:00:00.5"
        );
        // Fractional trimming keeps the leading zeros of the fraction.
        assert_eq!(
            ts_micros(1_704_067_200_000_001, false),
            "2024-01-01 00:00:00.000001"
        );
        assert_eq!(
            ts_micros(1_704_067_200_010_000, false),
            "2024-01-01 00:00:00.01"
        );
        // Pre-epoch instants (negative micros) still render calendar-correct.
        assert_eq!(ts_micros(-1, false), "1969-12-31 23:59:59.999999");
    }

    #[test]
    fn timestamptz_carries_utc_offset() {
        // tz-aware columns must be unambiguous to the Relyt master, whatever
        // the session TimeZone is.
        assert_eq!(ts_micros(0, true), "1970-01-01 00:00:00+00");
        assert_eq!(
            ts_micros(1_704_067_200_500_000, true),
            "2024-01-01 00:00:00.5+00"
        );
    }

    /// The digit rendering behind Decimal128 changed with the buffer rewrite
    /// (stack buffer instead of an intermediate String) -- pin the edges.
    #[test]
    fn decimal_rendering_edges() {
        assert_eq!(decimal128(0, 4), "0.0000");
        assert_eq!(decimal128(1, 4), "0.0001");
        assert_eq!(decimal128(-1, 4), "-0.0001");
        assert_eq!(decimal128(12345678901234, 4), "1234567890.1234");
        assert_eq!(decimal128(-50, 4), "-0.0050");
        assert_eq!(decimal128(1234, 4), "0.1234");
        assert_eq!(decimal128(12345, 4), "1.2345");
        assert_eq!(decimal128(42, 0), "42");
        assert_eq!(decimal128(-42, 0), "-42");
        assert_eq!(decimal128(7, -2), "700"); // negative scale multiplies
        assert_eq!(decimal128(i128::MAX, 0), i128::MAX.to_string());
        assert_eq!(decimal128(i128::MIN, 0), i128::MIN.to_string());
        // i128::MIN with a scale: unsigned_abs must not overflow.
        let min_scaled = decimal128(i128::MIN, 2);
        assert!(min_scaled.starts_with("-1701411834604692317316873037158841057."));
        assert!(min_scaled.ends_with("28"));
    }

    /// Same output as the old per-cell-String renderer, for a batch that mixes
    /// every supported type, NULLs and quote-worthy text.
    #[test]
    fn mixed_types_round_trip_unchanged() {
        use arrow_array::{
            BinaryArray, BooleanArray, Date32Array, Float64Array, Int32Array,
            TimestampMicrosecondArray,
        };
        let schema = Arc::new(Schema::new(vec![
            Field::new("b", DataType::Boolean, true),
            Field::new("i", DataType::Int32, true),
            Field::new("f", DataType::Float64, true),
            Field::new("s", DataType::Utf8, true),
            Field::new("d", DataType::Date32, true),
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
                true,
            ),
            Field::new("n", DataType::Decimal128(20, 4), true),
            Field::new("by", DataType::Binary, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(BooleanArray::from(vec![Some(true), Some(false), None])),
                Arc::new(Int32Array::from(vec![Some(-7), None, Some(0)])),
                Arc::new(Float64Array::from(vec![Some(1.5), None, Some(-0.25)])),
                Arc::new(StringArray::from(vec![
                    Some("a\"b,c\nd"),
                    Some("中文 emoji 🚀"),
                    None,
                ])),
                Arc::new(Date32Array::from(vec![Some(19723), None, Some(0)])),
                Arc::new(
                    TimestampMicrosecondArray::from(vec![
                        Some(1_704_067_200_500_000),
                        None,
                        Some(0),
                    ])
                    .with_timezone("UTC"),
                ),
                Arc::new(
                    Decimal128Array::from(vec![Some(-50_i128), None, Some(12345)])
                        .with_precision_and_scale(20, 4)
                        .unwrap(),
                ),
                Arc::new(BinaryArray::from(vec![
                    Some(&[0xde_u8, 0xad][..]),
                    None,
                    Some(&b""[..]),
                ])),
            ],
        )
        .unwrap();
        let f = CsvFormatter::new(',');
        let mut out = String::new();
        f.format_rows(&batch, &[0, 1, 2], &mut out).unwrap();
        // Row 0 embeds a comma AND a newline inside a quoted field, so the
        // output has more physical lines than rows -- compare the whole blob.
        let expected = concat!(
            // every column present; the quote doubles, comma and newline stay
            "\"t\",\"-7\",\"1.5\",\"a\"\"b,c\nd\",\"2024-01-01\",",
            "\"2024-01-01 00:00:00.5+00\",\"-0.0050\",\"\\xdead\"\n",
            // NULL everywhere but bool and text: bare empty fields
            "\"f\",,,\"中文 emoji 🚀\",,,,\n",
            // NULL bool leads, zero/epoch values render normally, empty bytea
            ",\"0\",\"-0.25\",,\"1970-01-01\",\"1970-01-01 00:00:00+00\",\"1.2345\",\"\\x\"\n",
        );
        assert_eq!(out, expected);
    }

    #[test]
    fn bytea_renders_as_hex_input() {
        use arrow_array::BinaryArray;
        let arr: arrow_array::ArrayRef = Arc::new(BinaryArray::from(vec![
            Some(&[0x00u8, 0xff, 0x41][..]),
            Some(&b""[..]),
            None,
        ]));
        let schema = Arc::new(Schema::new(vec![Field::new("b", DataType::Binary, true)]));
        let batch = RecordBatch::try_new(schema, vec![arr]).unwrap();
        let fmt = CsvFormatter::new(',');
        let mut out = String::new();
        fmt.format_rows(&batch, &(0..3).collect::<Vec<_>>(), &mut out)
            .unwrap();
        // \x00ff41 quoted; empty bytea is `"\x"`; NULL is bare.
        assert_eq!(out, "\"\\x00ff41\"\n\"\\x\"\n\n");
    }
}
