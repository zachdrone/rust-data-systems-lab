use arrow_array::builder::Float64Builder;
use arrow_array::{Array, RecordBatch};
use arrow_array::{ArrayRef, Float64Array, Int64Array};
use arrow_schema::{DataType, Field, Schema};
use std::sync::Arc;

pub fn normalize_i64(col: &Int64Array) -> Float64Array {
    let mut sum: f64 = 0.0;
    let mut count: f64 = 0.0;

    for i in 0..col.len() {
        if !col.is_null(i) {
            sum += col.value(i) as f64;
            count += 1.0;
        }
    }

    let mean = sum / count;

    let mut sumsq: f64 = 0.0;
    for i in 0..col.len() {
        if !col.is_null(i) {
            let v = col.value(i) as f64;
            let diff = v - mean;
            sumsq += diff * diff
        }
    }

    let variance = sumsq / count;
    let std = variance.sqrt();

    let mut builder = Float64Builder::with_capacity(col.len());

    for i in 0..col.len() {
        if col.is_null(i) {
            builder.append_null();
        } else {
            let v = col.value(i) as f64;
            builder.append_value((v - mean) / std)
        }
    }
    builder.finish()
}

pub fn add_normalized_col(
    batch: &RecordBatch,
    normalized: Float64Array,
    new_name: &str,
) -> RecordBatch {
    let old_schema = batch.schema();
    let old_fields = old_schema.fields();
    let old_columns = batch.columns();

    let new_field = Arc::new(Field::new(new_name, DataType::Float64, true));
    let mut new_fields = old_fields.to_vec();
    new_fields.push(new_field);

    let new_schema = Arc::new(Schema::new(new_fields));

    let mut new_columns: Vec<ArrayRef> = old_columns.to_vec();
    new_columns.push(Arc::new(normalized) as ArrayRef);

    RecordBatch::try_new(new_schema, new_columns).unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, record_batch};

    #[test]
    fn test_normalize_i64() {
        let col = Int64Array::from(vec![1, 2, 3]);
        let expected = Float64Array::from(vec![-1.224744871391589, 0.0, 1.224744871391589]);
        let res = normalize_i64(&col);
        assert_eq!(expected, res);
    }

    #[test]
    fn test_add_normed_col() {
        let batch = record_batch!(
            ("a", Int64, [1, 2, 3]),
            ("b", Float64, [Some(4.0), None, Some(5.0)]),
            ("c", Utf8, ["alpha", "beta", "gamma"])
        )
        .unwrap();

        let idx = batch.schema().index_of("a").unwrap();
        let col = batch
            .column(idx)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        let normed_col = normalize_i64(&col);

        let res = add_normalized_col(&batch, normed_col, "normed_a");

        let expected = record_batch!(
            ("a", Int64, [1, 2, 3]),
            ("b", Float64, [Some(4.0), None, Some(5.0)]),
            ("c", Utf8, ["alpha", "beta", "gamma"]),
            (
                "normed_a",
                Float64,
                [-1.224744871391589, 0.0, 1.224744871391589]
            )
        )
        .unwrap();

        assert_eq!(expected, res);
    }
}
