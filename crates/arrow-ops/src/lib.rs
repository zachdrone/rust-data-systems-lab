use arrow::array::{
    Array, BooleanArray, Float64Array, Float64Builder, Int16Array, Int32Array, Int32Builder,
    Int64Array, Int64Builder, StringArray, StringBuilder,
};

pub fn filter_i64(values: &Int64Array, mask: &BooleanArray) -> Int64Array {
    let mut builder = Int64Builder::new();

    for i in 0..values.len() {
        if values.is_null(i) {
            builder.append_null();
        } else if mask.value(i) {
            builder.append_value(values.value(i));
        }
    }

    builder.finish()
}

pub fn add_i64(lhs: &Int64Array, rhs: &Int64Array) -> Int64Array {
    assert_eq!(lhs.len(), rhs.len());

    let mut builder = Int64Builder::with_capacity(lhs.len());

    for i in 0..lhs.len() {
        if lhs.is_null(i) || rhs.is_null(i) {
            builder.append_null();
        } else {
            let sum = lhs.value(i) + rhs.value(i);
            builder.append_value(sum);
        }
    }
    builder.finish()
}

pub fn add_i64_simd(a: &Int64Array, b: &Int64Array) -> Int64Array {
    let av = a.values();
    let bv = b.values();

    let mut out = Vec::with_capacity(a.len());
    unsafe {
        out.set_len(a.len());
    }

    for i in 0..a.len() {
        out[i] = av[i].wrapping_add(bv[i]);
    }

    Int64Array::from(out)
}

pub fn add_string(lhs: &StringArray, rhs: &StringArray) -> StringArray {
    assert_eq!(lhs.len(), rhs.len());
    let mut builder = StringBuilder::new();

    for i in 0..lhs.len() {
        if lhs.is_null(i) || rhs.is_null(i) {
            builder.append_null();
        } else {
            let sum = lhs.value(i).to_owned() + rhs.value(i);
            builder.append_value(sum);
        }
    }
    builder.finish()
}

pub fn string_length(a: &StringArray) -> Int32Array {
    let mut builder = Int32Builder::with_capacity(a.len());

    let offsets = a.value_offsets();

    for i in 0..a.len() {
        if a.is_null(i) {
            builder.append_null();
        } else {
            let start = offsets[i] as i32;
            let end = offsets[i + 1] as i32;
            builder.append_value(end - start);
        }
    }

    builder.finish()
}

pub fn int_to_float(a: &Int16Array) -> Float64Array {
    let mut builder = Float64Builder::with_capacity(a.len());

    for i in 0..a.len() {
        if a.is_null(i) {
            builder.append_null();
        } else {
            builder.append_value(a.value(i).into());
        }
    }
    builder.finish()
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{
        BooleanArray, Float64Array, Int16Array, Int32Array, Int64Array, StringArray,
    };
    use arrow::buffer::NullBuffer;

    #[test]
    fn test_add_i64() {
        let a = Int64Array::new(vec![1, 1, 1, 1].into(), None);
        let b = Int64Array::new(vec![2, 2, 2, 2].into(), None);

        let expected = Int64Array::from(vec![3, 3, 3, 3]);
        assert_eq!(add_i64(&a, &b), expected);
    }

    #[test]
    fn test_add_string() {
        let s1 = StringArray::from(vec!["hey", "hi", "yo"]);
        let s2 = StringArray::from(vec![" ", " ", " "]);
        let s3 = StringArray::from(vec!["dude", "guy", "bro"]);

        let res = add_string(&add_string(&s1, &s2), &s3);

        let expected = StringArray::from(vec!["hey dude", "hi guy", "yo bro"]);
        assert_eq!(res, expected);
    }

    #[test]
    fn test_filter_i64() {
        let nulls = NullBuffer::from(vec![true, false, true, false, true]);
        let nums = Int64Array::new(vec![1, 2, 3, 4, 5].into(), Some(nulls));
        let mask = BooleanArray::from(vec![true, true, false, true, true]);
        let res = filter_i64(&nums, &mask);

        let expected_nulls = NullBuffer::from(vec![true, false, false, true]);
        let expected = Int64Array::new(vec![1, 2, 4, 5].into(), Some(expected_nulls));
        assert_eq!(res, expected);
    }

    #[test]
    fn test_string_length() {
        let s1 = StringArray::from(vec!["hey", "yo", "hello"]);
        let res = string_length(&s1);

        let expected = Int32Array::from(vec![3, 2, 5]);
        assert_eq!(res, expected);
    }

    #[test]
    fn test_int_to_float() {
        let int_arr = Int16Array::from(vec![1, 2, 3, 4]);
        let res = int_to_float(&int_arr);

        let expected = Float64Array::from(vec![1.0, 2.0, 3.0, 4.0]);
        assert_eq!(res, expected);
    }

    #[test]
    fn add_i64_simd() {
        let a = Int64Array::new(vec![1, 1, 1, 1].into(), None);
        let b = Int64Array::new(vec![2, 2, 2, 2].into(), None);

        let expected = Int64Array::from(vec![3, 3, 3, 3]);
        assert_eq!(add_i64(&a, &b), expected);
    }
}
