use datafusion::arrow::datatypes::DataType;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::logical_expr::{Volatility, create_udf};
use std::sync::Arc;

use datafusion::prelude::*;
use fusion_extensions::udfs::normalize;

pub async fn pipeline() -> datafusion::error::Result<()> {
    let ctx = SessionContext::new();

    let udf = create_udf(
        "normalize",
        vec![DataType::Int64],
        DataType::Float64,
        Volatility::Immutable,
        Arc::new(normalize),
    );
    ctx.register_udf(udf);

    ctx.register_csv("running_data", "running_data.csv", CsvReadOptions::new())
        .await?;

    let df = ctx
        .sql(
            r#"
            SELECT
                heart_rate_bpm as hr,
                normalize(heart_rate_bpm) AS hr_norm
            FROM running_data
            "#,
        )
        .await?;

    df.write_parquet(
        "normed_hr_datafusion.parquet",
        DataFrameWriteOptions::new(),
        None,
    )
    .await?;

    Ok(())
}
