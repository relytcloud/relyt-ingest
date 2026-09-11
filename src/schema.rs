//! Target-table schema handling: fetch from Relyt, PK check, arrow type
//! whitelist validation.

use std::sync::Arc;

use arrow_schema::{DataType, Field, Schema, SchemaRef, TimeUnit};

use crate::csv::is_supported_type;
use crate::error::{Error, Result};

/// Schema of the target heap table as the SDK sees it.
#[derive(Debug, Clone)]
pub struct TableSchema {
    pub arrow: SchemaRef,
    /// Column names of the primary key (mandatory in upsert mode: the Relyt
    /// master upserts on the PK).
    pub pk: Vec<String>,
    /// OID of the database the table lives in — with `rel_oid`, the
    /// rename-immune identity every staging path derives from.
    pub db_oid: u32,
    /// OID of the table itself. Also what the job's `target` is submitted
    /// as (the UDF has a `target oid` overload), replacing name resolution
    /// at notify time.
    pub rel_oid: u32,
}

impl TableSchema {
    /// Indices of the PK columns inside the arrow schema.
    pub fn pk_indices(&self) -> Result<Vec<usize>> {
        self.pk
            .iter()
            .map(|name| {
                self.arrow.index_of(name).map_err(|_| {
                    Error::Schema(format!("primary key column `{name}` not in schema"))
                })
            })
            .collect()
    }

    /// Column-type whitelist check — fail-loud, never silently coerce.
    ///
    /// Deliberately does NOT require a primary key: that is a per-mode rule
    /// (upsert needs one for the ON CONFLICT target, insert-only does not), so
    /// it is enforced by the caller that knows the stream mode.
    pub fn validate(&self) -> Result<()> {
        for field in self.arrow.fields() {
            if !is_supported_type(field.data_type()) {
                return Err(Error::UnsupportedType {
                    column: field.name().clone(),
                    data_type: format!("{:?}", field.data_type()),
                });
            }
        }
        if self.pk.is_empty() {
            return Ok(());
        }
        self.pk_indices().map(|_| ())
    }
}

/// Fetch schema + PK from the control connection.
///
/// TODO: local schema cache fallback, so open_table survives a master outage
/// when the schema is already known.
pub async fn fetch_table_schema(
    client: &tokio_postgres::Client,
    table: &str,
) -> Result<TableSchema> {
    let (schema_name, table_name) = split_qualified(table)?;

    let cols = client
        .query(
            "SELECT a.attname, format_type(a.atttypid, a.atttypmod), a.attnotnull,
                    c.oid,
                    (SELECT d.oid FROM pg_database d WHERE d.datname = current_database())
             FROM pg_attribute a
             JOIN pg_class c ON c.oid = a.attrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             WHERE n.nspname = $1 AND c.relname = $2
               AND a.attnum > 0 AND NOT a.attisdropped
             ORDER BY a.attnum",
            &[&schema_name, &table_name],
        )
        .await?;
    if cols.is_empty() {
        return Err(Error::Schema(format!("table {table} not found")));
    }
    let rel_oid: tokio_postgres::types::Oid = cols[0].get(3);
    let db_oid: tokio_postgres::types::Oid = cols[0].get(4);
    if rel_oid == 0 || db_oid == 0 {
        return Err(Error::Schema(format!(
            "could not resolve OIDs for {table} (rel={rel_oid}, db={db_oid})"
        )));
    }

    let pk_rows = client
        .query(
            "SELECT a.attname
             FROM pg_index i
             JOIN pg_class c ON c.oid = i.indrelid
             JOIN pg_namespace n ON n.oid = c.relnamespace
             JOIN pg_attribute a ON a.attrelid = c.oid AND a.attnum = ANY(i.indkey)
             WHERE n.nspname = $1 AND c.relname = $2 AND i.indisprimary
             ORDER BY array_position(i.indkey, a.attnum)",
            &[&schema_name, &table_name],
        )
        .await?;
    let pk: Vec<String> = pk_rows.iter().map(|r| r.get::<_, String>(0)).collect();

    let mut fields = Vec::with_capacity(cols.len());
    for row in &cols {
        let name: String = row.get(0);
        let type_name: String = row.get(1);
        let not_null: bool = row.get(2);
        let dt = pg_type_to_arrow(&name, &type_name)?;
        fields.push(Field::new(&name, dt, !not_null));
    }

    let schema = TableSchema {
        arrow: Arc::new(Schema::new(fields)),
        pk,
        db_oid,
        rel_oid,
    };
    schema.validate()?;
    Ok(schema)
}

/// Maximum precision an arrow `Decimal128` can carry.
const DECIMAL128_MAX_PRECISION: u8 = 38;

/// PG `format_type` output -> arrow. Whitelist only; anything else is a
/// fail-loud error carrying the column name and the rejected declaration.
///
/// Matching is on the rendered type string rather than (typid, typmod)
/// because that keeps this table readable; the parameterised forms
/// (`numeric(p,s)`, `timestamp(3) with time zone`, `character varying(64)`)
/// are handled explicitly so a typmod never turns a supported type into an
/// "unsupported" rejection.
fn pg_type_to_arrow(column: &str, pg: &str) -> Result<DataType> {
    let reject = |reason: &str| {
        Err(Error::UnsupportedType {
            column: column.to_string(),
            data_type: format!("{pg} ({reason})"),
        })
    };

    // numeric(p,s): arrow's Decimal128 caps precision at 38.
    if let Some(params) = pg
        .strip_prefix("numeric(")
        .and_then(|s| s.strip_suffix(")"))
    {
        let mut it = params.split(',');
        let p = it.next().and_then(|v| v.trim().parse::<u8>().ok());
        // A lone numeric(p) means scale 0.
        let s = match it.next() {
            Some(v) => v.trim().parse::<i8>().ok(),
            None => Some(0),
        };
        return match (p, s) {
            (Some(p), Some(s)) if (1..=DECIMAL128_MAX_PRECISION).contains(&p) => {
                Ok(DataType::Decimal128(p, s))
            }
            (Some(p), Some(_)) => reject(&format!(
                "precision {p} exceeds the Decimal128 maximum of {DECIMAL128_MAX_PRECISION}"
            )),
            _ => reject("unparseable numeric parameters"),
        };
    }
    if pg == "numeric" {
        return reject("numeric without precision has no fixed-width arrow mapping; declare numeric(p,s) with p <= 38");
    }

    // timestamp(N) with/without time zone — strip the typmod, keep the kind.
    if let Some(rest) = pg.strip_prefix("timestamp(") {
        if let Some((_, tail)) = rest.split_once(')') {
            match tail.trim() {
                "without time zone" => return Ok(DataType::Timestamp(TimeUnit::Microsecond, None)),
                "with time zone" => {
                    return Ok(DataType::Timestamp(
                        TimeUnit::Microsecond,
                        Some("UTC".into()),
                    ))
                }
                _ => {}
            }
        }
    }

    Ok(match pg {
        "boolean" => DataType::Boolean,
        "smallint" => DataType::Int16,
        "integer" => DataType::Int32,
        "bigint" => DataType::Int64,
        "real" => DataType::Float32,
        "double precision" => DataType::Float64,
        "text" | "character varying" => DataType::Utf8,
        "date" => DataType::Date32,
        // Rendered as hex input (`\x0aff..`).
        "bytea" => DataType::Binary,
        "timestamp without time zone" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "timestamp with time zone" => {
            DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into()))
        }
        _ if pg.starts_with("character varying(") => DataType::Utf8,
        _ if pg.starts_with("character(") => DataType::Utf8,
        _ => return reject("outside the supported type whitelist"),
    })
}

pub(crate) fn split_qualified(table: &str) -> Result<(String, String)> {
    match table.split_once('.') {
        Some((s, t)) if !s.is_empty() && !t.is_empty() => Ok((s.to_string(), t.to_string())),
        None => Ok(("public".to_string(), table.to_string())),
        _ => Err(Error::Config(format!("bad table name `{table}`"))),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn qualified_names() {
        assert_eq!(
            split_qualified("public.t1").unwrap(),
            ("public".into(), "t1".into())
        );
        assert_eq!(
            split_qualified("t1").unwrap(),
            ("public".into(), "t1".into())
        );
        assert!(split_qualified(".t1").is_err());
    }

    fn map(pg: &str) -> Option<DataType> {
        pg_type_to_arrow("c", pg).ok()
    }

    #[test]
    fn type_mapping() {
        assert_eq!(map("bigint"), Some(DataType::Int64));
        assert_eq!(map("numeric(20,4)"), Some(DataType::Decimal128(20, 4)));
        assert_eq!(map("character varying(64)"), Some(DataType::Utf8));
        assert_eq!(map("jsonb"), None); // outside the whitelist
    }

    #[test]
    fn type_mapping_parameterised_forms() {
        // typmod must not turn a supported type into a rejection.
        assert_eq!(
            map("timestamp(3) without time zone"),
            Some(DataType::Timestamp(TimeUnit::Microsecond, None))
        );
        assert_eq!(
            map("timestamp(6) with time zone"),
            Some(DataType::Timestamp(
                TimeUnit::Microsecond,
                Some("UTC".into())
            ))
        );
        // numeric(p) means scale 0.
        assert_eq!(map("numeric(10)"), Some(DataType::Decimal128(10, 0)));
        // Rejections carry a reason rather than a bare "unsupported".
        assert!(map("numeric").is_none());
        assert!(map("numeric(40,2)").is_none()); // > Decimal128 max precision
        let err = pg_type_to_arrow("amount", "numeric")
            .unwrap_err()
            .to_string();
        assert!(err.contains("numeric(p,s)"), "{err}");
    }
}
