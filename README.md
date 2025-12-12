# rust-data-systems-lab

This repository is a small collection of Rust crates focused on exploring how data systems are built in rust. The code here isn't meant to be a framework or a polished product -- it's a set of experiments around columnar data handling, pipelines, and query execution using **Apache Arrow** and **Apache DataFusion**.

The workspace is organized into a few focused crates, each targeting one part of the stack. Together, they form a lightweight playground for testing ideas related to data movement, transformation, and analytical execution.

---

## Crate Overview

### **arrow-ops**
Utilities and small abstractions built directly on top of Apache Arrow.  
This crate contains:

- simple compute kernels
- array/record batch transforms
- helpers for manipulating columnar data

It's mostly low-level experimentation with Arrow's memory model.

---

### **etl-engine**

This crate contains small ETL-style experiments. The main example is the
`csv_to_arrow` binary, which implements a simple streaming pipeline using Arrow
arrays and async tasks. The flow looks like this:

1. **Read CSV into Arrow batches**  
   The reader loads `running_data.csv` from a local object store and parses it
   into Arrow `RecordBatch`es using a fixed schema. CSV parsing runs inside a
   blocking task so the async runtime stays responsive.

2. **Normalize a column**  
   Each batch is passed through a CPU-bound transform:
   - downcast the `heart_rate_bpm` column  
   - compute normalized values  
   - append a new column, `normed_heart_rate_bpm`  
   These operations use helper functions from the `arrow-ops` crate.

3. **Write the transformed stream to Parquet**  
   Batches are streamed into an async Parquet writer backed by an object store.

Tasks are connected with `mpsc` channels so reading, transforming, and writing
can run concurrently.

---

### **fusion-extensions**

This crate holds small extensions for [DataFusion](https://github.com/apache/datafusion),
starting with a single scalar UDF that reuses logic from `arrow-ops`.

Right now it exposes a `normalize` function that:

- takes an `Int64` column as input (`ColumnarValue`)
- converts it into an Arrow `Int64Array`
- runs `arrow_ops::transforms::normalize_i64` on the array
- returns the result as a new Arrow array wrapped in `ColumnarValue`

The output is a `Float64` array containing the normalized values (e.g. z-scores).

The intent is for this crate to be the place where custom UDFs and other
DataFusion-specific helpers live, so they stay separate from the lower-level
Arrow logic in `arrow-ops`.

---

### **pipeline-core**

This crate contains a few small, focused pipeline experiments. Each one is wired
with channels and async tasks, but they target different shapes of work.

#### `datafusion_pipeline.rs`

An end-to-end example using DataFusion plus the custom UDF from
`fusion-extensions`:

- registers a `normalize` scalar UDF that wraps the Arrow-based normalization
  logic from fusion_extensions::udfs::normalize
- registers `running_data.csv` as a CSV table
- runs a SQL query
- writes the results to `normed_hr_datafusion.parquet`

This shows how the lower-level pieces (Arrow transforms, UDF wiring, async IO)
can be pulled together into a simple pipeline.

#### `chunked_file_pipeline.rs`

A streaming file pipeline built around CSV text:

1. **Read bytes in chunks**  
   `stream_file_bytes` reads a file (`examples/customers-10000.csv`) in 16 KB
   chunks and sends raw `Vec<u8>` buffers over a channel.

2. **Convert bytes to lines**  
   `bytes_to_lines` assembles UTF-8 text, keeps track of partial lines across
   chunk boundaries, and emits complete lines one by one.

3. **Transform CSV rows**  
   `transform_csv_lines` uppercases the first and last name columns while
   passing the header through unchanged.

4. **Write out a new CSV**  
   `write_csv_lines` writes the transformed lines to
   `transformed-customers-10000.csv`.

All stages run as separate async tasks connected by channels, which makes it
easy to see how backpressure and buffering behave when streaming a file.

#### `basic_pipeline.rs`

A numeric pipeline to show the structure:

- multiple producers send ranges of `u32` into a channel
- a transformer task squares each value and forwards `u64`s downstream
- a sink task prints the results

It uses `tokio::sync::mpsc` and `join_all` to fan out producers, run a single
transformer, and drain everything in a sink. 

---

## Purpose

This repository exists as a practical sandbox for exploring:

- Arrow's columnar memory structures  
- DataFusion’s execution engine and extension points  
- How data flows through pipelines in a Rust-based system  
