//! Parquet reader/writer for contract-award rows.
//!
//! # File layout
//!
//! One row per award. Columns, in order:
//!
//! ```text
//! action_date Int32(YYYYMMDD), award_id Utf8, recipient_name Utf8,
//! recipient_uei Utf8, ticker Utf8, parent_recipient Utf8,
//! awarding_agency Utf8, amount_usd Int64, award_type Utf8,
//! naics_code Utf8, description Utf8
//! ```
//!
//! `action_date` is a plain `i32` `YYYYMMDD` integer, not Arrow `Date32`, so a
//! consumer never needs a calendar library to compare or bucket awards.

use std::fs;
use std::path::Path;
use std::sync::Arc;

use arrow::array::{Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;

use crate::error::{Error, Result};
use crate::record::Award;

const ROW_GROUP_ROWS: usize = 50_000;

// ---------------------------------------------------------------------------
// Schema
// ---------------------------------------------------------------------------

/// The bundled-parquet schema, bound field by field. Every column non-null; the
/// writer fills empty strings rather than nulls so the read path can reject any
/// unexpected null as corruption.
fn award_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("action_date", DataType::Int32, false),
        Field::new("award_id", DataType::Utf8, false),
        Field::new("recipient_name", DataType::Utf8, false),
        Field::new("recipient_uei", DataType::Utf8, false),
        Field::new("ticker", DataType::Utf8, false),
        Field::new("parent_recipient", DataType::Utf8, false),
        Field::new("awarding_agency", DataType::Utf8, false),
        Field::new("amount_usd", DataType::Int64, false),
        Field::new("award_type", DataType::Utf8, false),
        Field::new("naics_code", DataType::Utf8, false),
        Field::new("description", DataType::Utf8, false),
    ]))
}

fn writer_props() -> WriterProperties {
    WriterProperties::builder()
        .set_compression(Compression::ZSTD(
            ZstdLevel::try_new(3).expect("valid zstd level"),
        ))
        .set_max_row_group_row_count(Some(ROW_GROUP_ROWS))
        .build()
}

// ---------------------------------------------------------------------------
// Write
// ---------------------------------------------------------------------------

/// Write `rows` to a parquet file at `path` (creates or overwrites).
pub fn write_awards(path: &Path, rows: &[Award]) -> Result<()> {
    let schema = award_schema();
    let file = fs::File::create(path)?;
    let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(writer_props()))?;
    for chunk in rows.chunks(ROW_GROUP_ROWS) {
        writer.write(&batch_of(&schema, chunk)?)?;
    }
    writer.close()?;
    Ok(())
}

fn batch_of(schema: &Arc<Schema>, rows: &[Award]) -> Result<RecordBatch> {
    let action_date: Int32Array = rows.iter().map(|r| Some(r.action_date)).collect();
    let award_id: StringArray = rows.iter().map(|r| Some(r.award_id.as_str())).collect();
    let recipient_name: StringArray = rows
        .iter()
        .map(|r| Some(r.recipient_name.as_str()))
        .collect();
    let recipient_uei: StringArray = rows
        .iter()
        .map(|r| Some(r.recipient_uei.as_str()))
        .collect();
    let ticker: StringArray = rows.iter().map(|r| Some(r.ticker.as_str())).collect();
    let parent_recipient: StringArray = rows
        .iter()
        .map(|r| Some(r.parent_recipient.as_str()))
        .collect();
    let awarding_agency: StringArray = rows
        .iter()
        .map(|r| Some(r.awarding_agency.as_str()))
        .collect();
    let amount_usd: Int64Array = rows.iter().map(|r| Some(r.amount_usd)).collect();
    let award_type: StringArray = rows.iter().map(|r| Some(r.award_type.as_str())).collect();
    let naics_code: StringArray = rows.iter().map(|r| Some(r.naics_code.as_str())).collect();
    let description: StringArray = rows.iter().map(|r| Some(r.description.as_str())).collect();

    RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(action_date),
            Arc::new(award_id),
            Arc::new(recipient_name),
            Arc::new(recipient_uei),
            Arc::new(ticker),
            Arc::new(parent_recipient),
            Arc::new(awarding_agency),
            Arc::new(amount_usd),
            Arc::new(award_type),
            Arc::new(naics_code),
            Arc::new(description),
        ],
    )
    .map_err(Error::Arrow)
}

// ---------------------------------------------------------------------------
// Read
// ---------------------------------------------------------------------------

fn column_as<'a, A: Array + 'static>(batch: &'a RecordBatch, name: &str) -> Result<&'a A> {
    let idx = batch
        .schema()
        .index_of(name)
        .map_err(|_| Error::Parquet(format!("missing column: {name}")))?;
    batch
        .column(idx)
        .as_any()
        .downcast_ref::<A>()
        .ok_or_else(|| Error::Parquet(format!("{name} column type mismatch")))
}

#[inline]
fn require_non_null(col: &dyn Array, field: &str, i: usize) -> Result<()> {
    if col.is_null(i) {
        Err(Error::Parquet(format!("null {field} at row {i}")))
    } else {
        Ok(())
    }
}

/// Parse a parquet file (in-memory bytes) into [`Award`] records.
pub fn read_awards(bytes: &[u8]) -> Result<Vec<Award>> {
    let owned: bytes::Bytes = bytes::Bytes::copy_from_slice(bytes);
    let reader = ParquetRecordBatchReaderBuilder::try_new(owned)?.build()?;

    let mut rows = Vec::new();
    for batch in reader {
        let batch = batch?;
        let action_date = column_as::<Int32Array>(&batch, "action_date")?;
        let award_id = column_as::<StringArray>(&batch, "award_id")?;
        let recipient_name = column_as::<StringArray>(&batch, "recipient_name")?;
        let recipient_uei = column_as::<StringArray>(&batch, "recipient_uei")?;
        let ticker = column_as::<StringArray>(&batch, "ticker")?;
        let parent_recipient = column_as::<StringArray>(&batch, "parent_recipient")?;
        let awarding_agency = column_as::<StringArray>(&batch, "awarding_agency")?;
        let amount_usd = column_as::<Int64Array>(&batch, "amount_usd")?;
        let award_type = column_as::<StringArray>(&batch, "award_type")?;
        let naics_code = column_as::<StringArray>(&batch, "naics_code")?;
        let description = column_as::<StringArray>(&batch, "description")?;

        for i in 0..batch.num_rows() {
            require_non_null(action_date, "action_date", i)?;
            require_non_null(award_id, "award_id", i)?;
            require_non_null(amount_usd, "amount_usd", i)?;

            rows.push(Award {
                action_date: action_date.value(i),
                award_id: award_id.value(i).to_owned(),
                recipient_name: recipient_name.value(i).to_owned(),
                recipient_uei: recipient_uei.value(i).to_owned(),
                ticker: ticker.value(i).to_owned(),
                parent_recipient: parent_recipient.value(i).to_owned(),
                awarding_agency: awarding_agency.value(i).to_owned(),
                amount_usd: amount_usd.value(i),
                award_type: award_type.value(i).to_owned(),
                naics_code: naics_code.value(i).to_owned(),
                description: description.value(i).to_owned(),
            });
        }
    }
    Ok(rows)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> Award {
        Award {
            action_date: 20240115,
            award_id: "HT940216C0001".into(),
            recipient_name: "LOCKHEED MARTIN CORP".into(),
            recipient_uei: "FYHNA5WC8XD7".into(),
            ticker: "LMT".into(),
            parent_recipient: String::new(),
            awarding_agency: "Department of Defense".into(),
            amount_usd: 51269205263,
            award_type: "DEFINITIVE CONTRACT".into(),
            naics_code: "336411".into(),
            description: "AIRCRAFT".into(),
        }
    }

    #[test]
    fn round_trips_rows() {
        let dir = std::env::temp_dir().join("contractskit_pq_test");
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("contracts-2024.parquet");
        let rows = vec![sample()];
        write_awards(&path, &rows).unwrap();
        let bytes = std::fs::read(&path).unwrap();
        let back = read_awards(&bytes).unwrap();
        assert_eq!(back, rows);
    }

    #[test]
    fn rejects_null_in_non_nullable_action_date() {
        // A nullable action_date column with a null value must be rejected on read.
        let schema = Arc::new(Schema::new(vec![
            Field::new("action_date", DataType::Int32, true), // nullable — the bad case
            Field::new("award_id", DataType::Utf8, false),
            Field::new("recipient_name", DataType::Utf8, false),
            Field::new("recipient_uei", DataType::Utf8, false),
            Field::new("ticker", DataType::Utf8, false),
            Field::new("parent_recipient", DataType::Utf8, false),
            Field::new("awarding_agency", DataType::Utf8, false),
            Field::new("amount_usd", DataType::Int64, false),
            Field::new("award_type", DataType::Utf8, false),
            Field::new("naics_code", DataType::Utf8, false),
            Field::new("description", DataType::Utf8, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![None])),
                Arc::new(StringArray::from(vec!["A1"])),
                Arc::new(StringArray::from(vec!["ACME"])),
                Arc::new(StringArray::from(vec![""])),
                Arc::new(StringArray::from(vec![""])),
                Arc::new(StringArray::from(vec![""])),
                Arc::new(StringArray::from(vec!["DoD"])),
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(StringArray::from(vec!["DEFINITIVE CONTRACT"])),
                Arc::new(StringArray::from(vec!["336411"])),
                Arc::new(StringArray::from(vec![""])),
            ],
        )
        .unwrap();
        let mut buf = Vec::new();
        {
            let mut w = ArrowWriter::try_new(&mut buf, schema, None).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        }
        let err = read_awards(&buf).unwrap_err().to_string();
        assert!(err.contains("null action_date"), "got: {err}");
    }
}
