use arrow_ops::transforms::normalize_i64;
use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::datatypes::DataType;
use datafusion::common::Result;
use datafusion::common::cast::as_int64_array;
use datafusion::logical_expr::ColumnarValue;
use datafusion::logical_expr::{ScalarUDF, Volatility, create_udf};
use datafusion::logical_expr::{col, lit};
use std::sync::Arc;

pub fn normalize(args: &[ColumnarValue]) -> Result<ColumnarValue> {
    let args = ColumnarValue::values_to_arrays(args)?;
    let i64s = as_int64_array(&args[0])?;
    let new_array = normalize_i64(&i64s);
    Ok(ColumnarValue::from(Arc::new(new_array) as ArrayRef))
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, record_batch};

    #[test]
    fn test_normalize() {
        let input = vec![Some(1), Some(2), Some(3)];
        let input = ColumnarValue::from(Arc::new(Int64Array::from(input)) as ArrayRef);

        let result = normalize(&[input]).unwrap();
        let binding = result.into_array(1).unwrap();
        let result = binding.as_any().downcast_ref::<Float64Array>().unwrap();
        let expected = Float64Array::from(vec![
            Some(-1.224744871391589),
            Some(0.0),
            Some(1.224744871391589),
        ]);
        assert_eq!(result, &expected);
    }
}
