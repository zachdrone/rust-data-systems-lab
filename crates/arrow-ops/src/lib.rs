pub mod kernels;

use arrow_array::{Array, RecordBatch};
use arrow_array::{Int64Array, record_batch};

pub fn testing() {
    let batch = record_batch!(
        ("a", Int64, [1, 2, 3]),
        ("b", Float64, [Some(4.0), None, Some(5.0)]),
        ("c", Utf8, ["alpha", "beta", "gamma"])
    )
    .unwrap();
    let m = mean(&batch, "a");
    dbg!(m);
}

pub fn mean(batch: &RecordBatch, col_name: &str) -> f64 {
    let col = batch
        .column_by_name(&col_name)
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();

    let mut sum: f64 = 0.0;
    let mut count: f64 = 0.0;

    for i in 0..col.len() {
        if !col.is_null(i) {
            sum += col.value(i) as f64;
            count += 1.0;
        }
    }

    sum / count
}
