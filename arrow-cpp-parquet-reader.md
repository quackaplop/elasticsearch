# Apache Arrow C++ Parquet Reader: Deep Dive

## Table of Contents
1. [Architecture Overview](#architecture-overview)
2. [Full Read Pipeline](#full-read-pipeline)
3. [Class Hierarchy](#class-hierarchy)
4. [Pushdown Mechanisms](#pushdown-mechanisms)
5. [Optimizations](#optimizations)
6. [Arrow Dataset API Wrapper](#arrow-dataset-api-wrapper)
7. [Java JNI Bridge (arrow-dataset)](#java-jni-bridge)
8. [Comparison with Competitors](#comparison-with-competitors)
9. [Known Limitations and Bottlenecks](#known-limitations-and-bottlenecks)
10. [Recent Improvements (2024-2025)](#recent-improvements)

---

## Architecture Overview

The Apache Arrow C++ Parquet reader is the **reference implementation** for reading Parquet files and is widely considered the performance benchmark for columnar file reading. It lives in two layers within the Arrow codebase:

- **Low-level Parquet library** (`cpp/src/parquet/`): Handles raw Parquet format concerns -- file metadata parsing, page reading, decompression, and encoding/decoding. Key files include `file_reader.h`, `column_reader.h`, `page_reader.h`, and the encoding implementations.

- **Arrow-Parquet bridge** (`cpp/src/parquet/arrow/`): Bridges Parquet's internal representation into Arrow columnar arrays. Key files include `reader.h`, `reader.cc`, and the column reader implementations that produce `arrow::ChunkedArray` objects.

- **Dataset layer** (`cpp/src/arrow/dataset/`): Wraps the Parquet reader in a higher-level API that adds partition pruning, predicate pushdown via filter expressions, projection, and multi-file scanning with `Scanner`, `Fragment`, and `Dataset` abstractions.

Sources:
- [Arrow C++ Parquet Docs](https://arrow.apache.org/docs/cpp/parquet.html)
- [reader.h on GitHub](https://github.com/apache/arrow/blob/main/cpp/src/parquet/arrow/reader.h)
- [file_reader.h on GitHub](https://github.com/apache/arrow/blob/main/cpp/src/parquet/file_reader.h)

---

## Full Read Pipeline

The complete read pipeline from file open to Arrow arrays:

```
File Open (RandomAccessFile / MemoryMappedFile)
    |
    v
[1] Footer/Metadata Read
    - Read last 8 bytes to find footer offset
    - Deserialize Thrift-encoded FileMetaData
    - Parse schema, row group metadata, column chunk metadata
    - Extract statistics (min/max), page index, bloom filter offsets
    |
    v
[2] Row Group Selection
    - Filter row groups using column statistics (min/max)
    - (Dataset layer) Apply partition pruning from directory structure
    - (Dataset layer) Evaluate filter expressions against row group stats
    - Select which row groups to actually read
    |
    v
[3] Column Selection (Projection Pushdown)
    - Only read column chunks for requested columns
    - Skip entire column chunks for non-projected columns
    - Reduces both I/O and CPU
    |
    v
[4] Pre-buffering / IO Coalescing (optional)
    - PreBuffer() caches needed byte ranges asynchronously
    - Coalesces nearby small reads into larger sequential reads
    - Critical for high-latency filesystems (S3, GCS, HDFS)
    |
    v
[5] Page Reading (SerializedPageReader)
    - Read dictionary pages first (if present)
    - Read data pages sequentially within each column chunk
    - Decompress pages (Snappy, Zstd, Gzip, LZ4, etc.)
    - Reuses decompression buffers across pages
    |
    v
[6] Decoding
    - Decode repetition and definition levels (RLE/Bit-packed)
    - Decode values using appropriate encoding:
      * PLAIN encoding
      * DICTIONARY (RLE-encoded indices + dictionary page)
      * DELTA_BINARY_PACKED (for INT32/INT64)
      * DELTA_BYTE_ARRAY / DELTA_LENGTH_BYTE_ARRAY
      * BYTE_STREAM_SPLIT (with SIMD: SSE2/AVX2)
      * RLE (for BOOLEAN)
    - Definition levels decoded to 16-bit integers, then re-encoded to bitmap
    |
    v
[7] Arrow Array Assembly
    - Decoded values assembled into Arrow arrays
    - Dictionary-encoded columns can be kept as DictionaryArray (configurable)
    - Null bitmaps constructed from definition levels
    - Nested types assembled from repetition/definition levels
    - Result: arrow::ChunkedArray per column
    |
    v
[8] RecordBatch / Table Construction
    - Combine column arrays into RecordBatch or Table
    - Optional: parallel column decoding (if use_threads=true)
    - Optional: streaming via RecordBatchReader
```

Sources:
- [Arrow C++ Parquet Docs](https://arrow.apache.org/docs/cpp/parquet.html)
- [reader.cc on GitHub](https://github.com/apache/arrow/blob/main/cpp/src/parquet/arrow/reader.cc)

---

## Class Hierarchy

### Low-Level Parquet Classes

```
parquet::ParquetFileReader
    |-- Opens file, reads footer/metadata
    |-- metadata() -> FileMetaData (schema, row group info, key-value metadata)
    |-- RowGroup(i) -> RowGroupReader
            |-- Column(j) -> ColumnReader (typed: Int32Reader, BoolReader, etc.)
            |-- metadata() -> RowGroupMetaData
```

### Arrow-Parquet Bridge Classes

```
parquet::arrow::FileReader (public interface)
    |-- Constructed from ParquetFileReader + ArrowReaderProperties
    |-- ReadTable() -> arrow::Table (entire file)
    |-- ReadRowGroup(i) -> arrow::Table (single row group)
    |-- ReadRowGroups(indices) -> arrow::Table (multiple row groups)
    |-- RowGroup(i) -> RowGroupReader
    |       |-- ReadTable() -> arrow::Table
    |       |-- Column(j) -> ColumnChunkReader
    |               |-- Read() -> arrow::ChunkedArray
    |-- GetRecordBatchReader(row_groups, columns) -> RecordBatchReader
    |       (streaming interface, supports parallel column decoding)
    |
    FileReaderImpl (implementation)
        |-- pool_: MemoryPool*
        |-- reader_: unique_ptr<ParquetFileReader>
        |-- reader_properties_: ArrowReaderProperties
        |-- manifest_: SchemaManifest (maps Parquet schema to Arrow schema)
```

### Configuration Classes

```
parquet::ReaderProperties
    |-- buffer_size, memory_pool, thrift limits, file decryption

parquet::ArrowReaderProperties (extends ReaderProperties)
    |-- use_threads: bool (parallel column decoding)
    |-- batch_size: int64_t
    |-- read_dictionary: set<int> (which columns to keep as DictionaryArray)
    |-- enable_read_coalescing: bool (pre-buffering for remote FS)
    |-- coerce_int96_timestamp_unit
```

Sources:
- [reader.h on GitHub](https://github.com/apache/arrow/blob/main/cpp/src/parquet/arrow/reader.h)
- [file_reader.h on GitHub](https://github.com/apache/arrow/blob/main/cpp/src/parquet/file_reader.h)

---

## Pushdown Mechanisms

### 1. Projection Pushdown

**Where applied:** Step 3 (Column Selection)

The most fundamental optimization. Only columns listed in the projection are read from disk. Since Parquet stores columns independently, skipping a column means zero I/O and zero CPU for that column.

- At the `FileReader` level: `GetRecordBatchReader({row_groups}, {column_indices})`
- At the Dataset level: `ScannerBuilder::Project(column_names)`

### 2. Row Group Pruning (Statistics-Based)

**Where applied:** Step 2 (Row Group Selection)

Each row group stores per-column statistics (min, max, null count, distinct count). The reader can skip entire row groups whose statistics prove they cannot contain matching rows.

- At the Dataset level: `ScannerBuilder::Filter(expression)` evaluates expressions against row group statistics
- The C++ Dataset Scanner automatically evaluates filter predicates against RowGroup metadata
- Supported for numeric and string types (with caveats on ordering)

**Current status (C++):** Row group statistics are accessible and used by the Dataset Scanner for filtering. However, the low-level `parquet::arrow::FileReader` does NOT automatically apply predicate filters -- the caller must select row groups manually.

### 3. Page Index Pruning

**Where applied:** Between Steps 2 and 5

The Parquet Page Index (Column Index + Offset Index) provides per-page min/max statistics, enabling skipping individual data pages within a column chunk. This is more granular than row group pruning.

**Current status (C++):** Access to Column Index and Offset Index structures IS provided in Arrow C++, but the **data read APIs do not currently make use of them for optimization purposes**. The Rust implementation (arrow-rs) is ahead here with `RowSelection` that can skip pages based on page index evaluation.

Sources:
- [C++ predicate pushdown issue #35305](https://github.com/apache/arrow/issues/35305)
- [Querying Parquet with Millisecond Latency](https://arrow.apache.org/blog/2022/12/26/querying-parquet-with-millisecond-latency/)

### 4. Bloom Filter Pruning

**Where applied:** Step 2 (Row Group Selection), potentially per-page

Parquet Bloom Filters (Split Block Bloom Filter) enable probabilistic filtering, especially useful for high-cardinality columns (like IDs) where min/max statistics are ineffective.

**Current status (C++):** APIs are provided for creating, serializing, and deserializing Bloom Filters. You CAN read a bloom filter from a Parquet file. However, bloom filters are **NOT integrated into the data read APIs** -- they are not automatically used for row group pruning. Users must manually read bloom filters and make pruning decisions.

Sources:
- [Bloom filter support issue #40548](https://github.com/apache/arrow/issues/40548)
- [Parquet Bloom Filter spec](https://parquet.apache.org/docs/file-format/bloomfilter/)

### 5. Partition Pruning

**Where applied:** Before Step 1 (file-level)

Only available through the Dataset API. When Parquet files are organized in a partitioned directory structure (e.g., Hive-style `year=2024/month=01/`), the scanner can eliminate entire files based on partition values.

- `ScannerBuilder::Filter()` with partition column expressions
- Files whose partition values contradict the filter are never opened

### Summary: Where Pushdowns Are Applied

| Pushdown Type | Pipeline Stage | Granularity | Auto in C++ Reader? | Auto in Dataset API? |
|---|---|---|---|---|
| Projection | Column selection | Column | Yes (caller specifies) | Yes |
| Row Group Stats | Row group selection | Row group (~128MB) | No (manual) | Yes |
| Page Index | Page selection | Page (~1MB) | No | No (access only) |
| Bloom Filter | Row group selection | Row group | No (API only) | No |
| Partition | File selection | File | N/A | Yes |
| Late Materialization | Decoding | Row-level | No (C++) | No (C++) |

---

## Optimizations

### 1. SIMD Decoding

Arrow C++ uses SIMD instructions for performance-critical encoding/decoding:

- **BYTE_STREAM_SPLIT**: Has SSE2 and AVX2 implementations. SSE2/AVX2 achieve 5-10+ GB/s throughput. AVX512 variants were removed because they performed equal or worse than AVX2.
- **RLE/Bit-Packing**: Used for definition/repetition levels. The RLE decoder is being refactored (issue #47112) to extract an RLE parser for further optimization.
- **Dynamic dispatch**: Added for BYTE_STREAM_SPLIT (Arrow 22.0.0), selecting SSE2/AVX2 at runtime based on CPU capabilities.

Sources:
- [ByteStreamSplit AVX2 PR #6899](https://github.com/apache/arrow/pull/6899)
- [SIMD dynamic dispatch issue #46962](https://github.com/apache/arrow/issues/46962)
- [byte_stream_split.h on GitHub](https://github.com/apache/arrow/blob/main/cpp/src/arrow/util/byte_stream_split.h)

### 2. Pre-buffering and IO Coalescing

For remote/high-latency filesystems:

- **`PreBuffer()`**: Asynchronously pre-buffers specified column indices across all row groups. Caches needed byte ranges in memory before deserialization begins.
- **IO Coalescing**: Combines nearby small byte range requests into larger sequential reads, reducing the number of API calls to remote storage (S3, GCS).
- **`AsyncContext`**: PreBuffer accepts an AsyncContext parameter, enabling fully asynchronous prefetching.
- Enabled via `ArrowReaderProperties::set_pre_buffer(true)` / `set_cache_options()`.

Sources:
- [PARQUET-1820: Column filter hint for prefetching](https://issues.apache.org/jira/browse/PARQUET-1820)
- [ARROW-7995: IO coalescing and caching](https://github.com/apache/arrow/issues/24212)

### 3. Parallel Column Reading

- Controlled by `ArrowReaderProperties::set_use_threads(true)` (off by default)
- When enabled, columns within a row group are decoded in parallel using Arrow's thread pool
- `GetRecordBatchReader()` can internally parallelize column decoding
- Depending on I/O speed and decoding expense, can yield significantly higher throughput

### 4. Memory Mapping

- Arrow provides `MemoryMappedFile` for zero-copy local file access
- `ParquetFileReader` accepts any `::arrow::io::RandomAccessFile`, including memory-mapped files
- For local files, memory mapping avoids explicit read() syscalls
- Zero-copy buffer slicing via `arrow::SliceBuffer()`

**Caveat:** Memory mapping does NOT help with Parquet's compressed data since decompression requires copying. It helps most with uncompressed Parquet files or metadata access.

Sources:
- [Arrow Memory Management](https://arrow.apache.org/docs/cpp/memory.html)

### 5. Dictionary Encoding Preservation

One of the most impactful optimizations:

- By default, Parquet dictionary-encoded data is "materialized" (expanded) into plain arrays, duplicating strings in memory
- With `ArrowReaderProperties::set_read_dictionary(column_index)`, columns are read directly as `arrow::DictionaryArray`
- **Performance impact**: Up to 60x faster reads and dramatically lower memory usage
- **Memory impact**: Peak memory dropped from 1.94 GB to 405 MB in benchmarks for a 152 MB dataset
- Dictionary indices from Parquet are written directly into Arrow DictionaryBuilder without rehashing

Sources:
- [Faster strings blog post (2019)](https://arrow.apache.org/blog/2019/09/05/faster-strings-cpp-parquet/)
- [DictionaryArray issue #20110](https://github.com/apache/arrow/issues/20110)

### 6. Direct Arrow Decoding

- Removed abstraction layer between low-level Parquet decoders and Arrow builders
- `ByteArrayDecoder::DecodeArrow` decodes directly into Arrow builders
- `ColumnWriter::WriteArrow` writes directly from Arrow arrays
- Eliminates intermediate copies between Parquet's internal representation and Arrow format

### 7. Buffer Reuse

- `SerializedPageReader` reuses decompression buffers across pages (does not resize down for smaller pages)
- Reduces allocation overhead for sequential page reads within a column chunk

---

## Arrow Dataset API Wrapper

The Dataset API (`cpp/src/arrow/dataset/`) provides a higher-level abstraction over the Parquet reader:

### Key Components

```
Dataset
    |-- FileSystemDataset: represents files on disk/cloud storage
    |-- InMemoryDataset: represents in-memory data
    |
    |-- GetFragments(filter) -> Iterator<Fragment>

Fragment
    |-- ParquetFileFragment: wraps a single Parquet file (or subset of row groups)
    |-- Scan(options) -> RecordBatchGenerator
    |-- SplitByRowGroup() -> vector<ParquetFileFragment> (one per row group)

Scanner
    |-- ScannerBuilder::Project(columns): projection pushdown
    |-- ScannerBuilder::Filter(expression): predicate pushdown
    |-- ScannerBuilder::BatchSize(n): control batch sizes
    |-- Scan() -> RecordBatchIterator
    |-- ScanBatchesAsync() -> RecordBatchGenerator (async version)
    |-- ToTable() -> Table
```

### How the Dataset API Applies Pushdowns

1. **Partition Pruning**: Filter expressions on partition columns eliminate entire files before opening them
2. **Row Group Pruning**: Filter expressions are evaluated against Parquet RowGroup statistics. Row groups whose metadata contradicts the filter are excluded
3. **Column Projection**: Only requested columns are read from each file
4. **Fragment Granularity**: A single Parquet file can export multiple fragments based on row groups, enabling parallel processing of row groups across threads

### What the Dataset API Adds Over Raw FileReader

| Feature | Raw FileReader | Dataset API |
|---|---|---|
| Multi-file scanning | No | Yes |
| Partition pruning | No | Yes |
| Automatic row group stats filtering | No | Yes |
| Filter expression evaluation | No | Yes |
| Async scanning | Limited (PreBuffer) | Full (ScanBatchesAsync) |
| Format abstraction | Parquet only | Parquet, CSV, IPC, ORC |

Sources:
- [Dataset API docs](https://arrow.apache.org/docs/cpp/api/dataset.html)
- [Tabular Datasets](https://arrow.apache.org/docs/cpp/dataset.html)
- [Dataset tutorial](https://arrow.apache.org/docs/cpp/tutorials/datasets_tutorial.html)
- [scanner.cc on GitHub](https://github.com/apache/arrow/blob/main/cpp/src/arrow/dataset/scanner.cc)

---

## Java JNI Bridge (arrow-dataset)

### What Is Exposed

The Java `arrow-dataset` module bridges Java to the C++ Dataset API via JNI (`NativeDatasetFactory`, `JniWrapper`):

**Exposed features:**
- `FileSystemDatasetFactory`: Create datasets from file URIs (local, HDFS, S3)
- `DatasetFactory.inspect()`: Inspect schema without reading data
- `ScanOptions.Builder`:
  - `.columns(String[])`: Column projection (e.g., `{"id", "name"}`)
  - `.substraitFilter(ByteBuffer)`: Predicate filter via Substrait expression
  - `.substraitProjection(ByteBuffer)`: Computed projection via Substrait expression
  - `.batchSize(long)`: Control record batch size
  - `.fileFormat(FileFormat)`: Specify Parquet, CSV, etc.
- Scanning returns `ArrowReader` which yields `VectorSchemaRoot` batches

### What Is NOT Exposed

The Java JNI bridge has significant limitations compared to the full C++ API:

1. **No direct Fragment access**: Fragment, ScanTask, and RecordBatchIterator are NOT JNI-mapped (kept simple for initial implementation)
2. **No page index pruning**: Page-level filtering is not exposed
3. **No bloom filter access**: Bloom filter APIs are not bridged to Java
4. **No PreBuffer/IO coalescing control**: Cannot configure async pre-buffering from Java
5. **No parallel column decoding control**: Cannot set `use_threads` on ArrowReaderProperties from Java
6. **No dictionary read mode control**: Cannot configure which columns to read as DictionaryArray
7. **Substrait-only filtering**: Filter expressions must be provided as Substrait binary, but there are no user-friendly tools to create Substrait expressions in Java yet
8. **Resource management burden**: All JNI components must be manually closed or use try-with-resources to release native objects
9. **Early development status**: The Java Dataset module is explicitly documented as "under early development" with API instability warnings

### Performance Overhead

- JNI bridge overhead is the largest source of constant overhead when bridging Arrow between Java and C++
- Thread attachment and memory pool reservation can cause issues in certain scenarios
- Data crossing the JNI boundary incurs serialization costs unless zero-copy Arrow IPC is used

Sources:
- [Java Dataset docs](https://arrow.apache.org/docs/java/dataset.html)
- [Java Dataset Cookbook](https://arrow.apache.org/cookbook/java/dataset.html)
- [JNI implementation PR #7030](https://github.com/apache/arrow/pull/7030)
- [Push-down filtering in Java issue #14782](https://github.com/apache/arrow/issues/14782)
- [Java push-down filtering issue #227](https://github.com/apache/arrow-java/issues/227)

---

## Comparison with Competitors

### Arrow C++ vs parquet-mr (Java)

| Aspect | Arrow C++ | parquet-mr (parquet-java) |
|---|---|---|
| **Language** | C++ (native) | Java (JVM) |
| **SIMD** | SSE2/AVX2 for BYTE_STREAM_SPLIT, runtime dispatch | Experimental Java Vector API (incubating), not guaranteed |
| **Dictionary handling** | Direct DictionaryArray (zero-copy indices) | Materializes dictionaries by default |
| **Memory model** | Direct memory management, zero-copy slicing | JVM heap + GC overhead |
| **Vectorized decoding** | Native vectorization, SIMD intrinsics | JIT-compiled vectorization, not always guaranteed |
| **Pre-buffering** | Async PreBuffer + IO coalescing | Vectored IO (issue #2703, in progress) |
| **Performance** | >2x faster than parquet-mr row-based reading | 9x improvement with vectorized reader (vs non-vectorized) |
| **Page index** | Accessible but not used in read APIs | Supported in Spark's vectorized reader |
| **Bloom filters** | Read API exists, not integrated in scans | Supported in parquet-java |
| **Nested types** | Full support | Full support |

**Key advantage of Arrow C++**: Native code with SIMD, zero-copy dictionary preservation, direct Arrow format output, async I/O coalescing. These compound into significant performance wins, especially for string-heavy and dictionary-encoded data.

**Where parquet-mr is ahead**: Bloom filter integration in reads, page index filtering (via Spark integration), mature ecosystem integration with Hadoop/Spark.

Sources:
- [parquet-java on GitHub](https://github.com/apache/parquet-java)
- [Faster strings blog](https://arrow.apache.org/blog/2019/09/05/faster-strings-cpp-parquet/)

### Arrow C++ vs DuckDB Parquet Reader

| Aspect | Arrow C++ | DuckDB |
|---|---|---|
| **Architecture** | Library (decoupled from query engine) | Tightly integrated with vectorized query engine |
| **Memory model** | Materializes into Arrow arrays | Streaming with strict buffer manager limits |
| **Predicate pushdown** | Dataset API: row group stats only | Row group zonemaps + filter pushdown into scans |
| **Page-level pruning** | API access only, not used | Row group level only (no page-level) |
| **Parallelism** | Optional parallel column decoding | Parallel row group processing, single thread per row group |
| **Streaming** | RecordBatchReader (optional) | Always streaming, bounded memory |
| **Zero-dependency** | Requires Arrow libraries | Self-contained, zero dependencies |
| **Late materialization** | Not in C++ (Rust has it) | Integrated in execution engine |
| **Filter types** | Expression-based via Dataset API | Full SQL predicate pushdown |

**Why DuckDB built its own reader** (from [GitHub Discussion #2762](https://github.com/duckdb/duckdb/discussions/2762)):
- Deep integration with DuckDB's vectorized execution engine
- Zero external dependencies
- Custom memory management with strict bounds
- Streaming-first design that never materializes entire datasets
- Ability to push SQL predicates directly into the scan operator

**Where DuckDB wins**: Tighter integration means filters and projections flow naturally from SQL to scan. Streaming model prevents memory blowups on large files. Parallel row group processing is automatic.

**Where Arrow C++ wins**: More flexible as a library (can be embedded in any application). Better async I/O with PreBuffer. Dictionary preservation to DictionaryArray. SIMD-accelerated decoders. Broader format support (encryption, all encodings).

Sources:
- [DuckDB Parquet docs](https://duckdb.org/docs/stable/data/parquet/overview)
- [DuckDB custom reader discussion](https://github.com/duckdb/duckdb/discussions/2762)
- [DuckDB benchmarks over time](https://duckdb.org/2024/06/26/benchmarks-over-time)

### Arrow C++ vs Arrow Rust (arrow-rs)

The Rust implementation has surpassed C++ in several areas:

| Feature | Arrow C++ | Arrow Rust |
|---|---|---|
| **Late materialization** | Not implemented | Full pipeline: filter, then project |
| **Page index pruning** | API access only | RowSelection skips pages |
| **Adaptive row selection** | Not implemented | RLE vs bitmask, auto-selects best |
| **CachedArrayReader** | Not implemented | Dual-layer cache prevents double-decode |
| **Bloom filter in reads** | API only | Integrated in DataFusion |
| **Custom Thrift parser** | Standard Thrift | Custom parser: 3-9x faster metadata |

Sources:
- [Late materialization deep dive](https://arrow.apache.org/blog/2025/12/11/parquet-late-materialization-deep-dive/)
- [Querying Parquet with Millisecond Latency](https://arrow.apache.org/blog/2022/12/26/querying-parquet-with-millisecond-latency/)
- [Custom Thrift parser blog](https://arrow.apache.org/blog/2025/10/23/rust-parquet-metadata/)

---

## Known Limitations and Bottlenecks

### Memory Consumption

- `ReadTable()` materializes the complete dataset in memory. A 23GB Parquet file can exceed 64GB RAM during reading.
- Large text/binary columns are the primary bottleneck, as they cannot be efficiently compressed in memory after decoding.
- Source: [Issue #44890](https://github.com/apache/arrow/issues/44890)

### Footer/Metadata Parsing

- Footer parsing scales linearly with number of columns and row groups
- For wide tables (thousands of columns) or files with many row groups, metadata parsing becomes a bottleneck
- The Rust implementation addressed this with a custom Thrift parser (3-9x faster)
- Source: [Rust Parquet metadata blog](https://arrow.apache.org/blog/2025/10/23/rust-parquet-metadata/)

### No Late Materialization in C++

- The C++ reader decodes ALL selected columns for ALL selected row groups
- There is no filter-then-project pipeline where filter columns are decoded first, rows are selected, and then remaining columns are decoded only for matching rows
- This means predicate pushdown in C++ only works at row group granularity, not row-level
- The Rust implementation (arrow-rs) has full late materialization with CachedArrayReader

### Page Index Not Used in Read APIs

- Column Index and Offset Index structures are accessible in C++
- But the read APIs do not automatically use them to skip pages
- This is a significant gap compared to DuckDB and arrow-rs

### Bloom Filters Not Integrated

- APIs exist for reading bloom filters from Parquet files
- But they are not integrated into the scan/read pipeline
- Writing bloom filters to Parquet from C++ was only recently added
- Source: [Issue #40548](https://github.com/apache/arrow/issues/40548)

### Performance Regressions

- Small regressions observed in Parquet read benchmarks over time
- Source: [Issue #38432](https://github.com/apache/arrow/issues/38432)

### Slow Multi-Column Reading

- Reading many columns from a file with many columns can be slow due to metadata overhead
- Source: [Issue #38149](https://github.com/apache/arrow/issues/38149)

### Binary Data Type Performance

- Performance for reading binary/string data types differs significantly from integral types
- Large binary values cause disproportionate memory allocation overhead
- Source: [Issue #41224](https://github.com/apache/arrow/issues/41224)

---

## Recent Improvements (2024-2025)

### Arrow 15.0.0 (January 2024)
- Various Parquet reader stability improvements

### Arrow 19.0.0 (Early 2025)
- Bug fix for reading Parquet files created by Arrow Rust v53.0.0+

### Arrow 21.0.0 (July 2025)
- **LargeBinary and BinaryView support**: Read BYTE_ARRAY columns directly as LargeBinary or BinaryView without intermediate conversion
- **LargeList support**: Read LIST columns directly as LargeList, bypassing 2^31 values per chunk limitation
- **Content-Defined Chunking**: Improved deduplication via rolling hash-based page boundaries
- **New logical types**: VARIANT, UUID (auto-converted to arrow.uuid), GEOMETRY, GEOGRAPHY
- **SecureString for encryption**: Memory-wiping string class for encryption APIs
- Source: [Arrow 21.0.0 Release](https://arrow.apache.org/blog/2025/07/17/21.0.0-release/)

### Arrow 22.0.0 (October 2025)
- **Half Float (Float16) support** improvements
- **RLE Decoder refactoring**: Extracted RLE parser for further optimization (issue #47112)
- **Dynamic dispatch for BYTE_STREAM_SPLIT**: Runtime CPU feature detection for SIMD paths
- **Statistics preservation**: Null count statistics no longer discarded when sort order is unknown
- **Memory optimization**: Reduced memory for decryption buffers in encrypted Parquet
- Source: [Arrow 22.0.0 Release](https://arrow.apache.org/blog/2025/10/24/22.0.0-release/)

---

## Key Takeaways for Elasticsearch Context

1. **The C++ reader is fast but incomplete on pushdowns**: While the raw decoding is highly optimized (SIMD, dictionary preservation, buffer reuse), the C++ reader lacks late materialization, page-level pruning, and bloom filter integration that arrow-rs and DuckDB have. The Dataset API adds row group pruning but nothing more granular.

2. **The Java JNI bridge is thin and fragile**: The arrow-dataset Java module exposes basic scanning with projection and Substrait-based filtering, but lacks control over pre-buffering, parallel decoding, dictionary modes, page pruning, and bloom filters. It is explicitly "early development" with unstable APIs.

3. **DuckDB's advantage is integration, not raw speed**: DuckDB's custom Parquet reader wins because it streams data through the query engine with bounded memory, automatic predicate pushdown, and tight vectorized execution integration. Arrow C++ is a library that requires the caller to orchestrate these optimizations.

4. **Arrow Rust is the new performance frontier**: With late materialization, page index pruning, adaptive row selection, and a 3-9x faster metadata parser, arrow-rs has surpassed the C++ implementation in several key areas relevant to query engines.

5. **For Elasticsearch**: If considering Arrow-based Parquet reading, the key gaps are (a) no late materialization in C++, (b) limited Java JNI surface, and (c) Substrait-only filtering from Java. A custom integration would need to either use the C++ reader directly (bypassing Java JNI limitations) or implement pushdown logic at the Elasticsearch level on top of the basic scanning primitives the JNI bridge provides.

---

## Source Citations

### Official Documentation
- [Arrow C++ Parquet Reading and Writing](https://arrow.apache.org/docs/cpp/parquet.html)
- [Arrow C++ Dataset API](https://arrow.apache.org/docs/cpp/api/dataset.html)
- [Arrow C++ Tabular Datasets](https://arrow.apache.org/docs/cpp/dataset.html)
- [Arrow Java Dataset](https://arrow.apache.org/docs/java/dataset.html)
- [Arrow C++ Memory Management](https://arrow.apache.org/docs/cpp/memory.html)
- [Arrow C++ File Formats API](https://arrow.apache.org/docs/cpp/api/formats.html)

### Blog Posts
- [Querying Parquet with Millisecond Latency (2022)](https://arrow.apache.org/blog/2022/12/26/querying-parquet-with-millisecond-latency/)
- [Faster C++ Parquet on Dictionary-Encoded Strings (2019)](https://arrow.apache.org/blog/2019/09/05/faster-strings-cpp-parquet/)
- [Late Materialization Deep Dive in arrow-rs (2025)](https://arrow.apache.org/blog/2025/12/11/parquet-late-materialization-deep-dive/)
- [3-9x Faster Parquet Metadata with Custom Thrift Parser (2025)](https://arrow.apache.org/blog/2025/10/23/rust-parquet-metadata/)
- [Parquet Pruning in DataFusion (2025)](https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/)

### GitHub Issues and PRs
- [ByteStreamSplit AVX2/AVX512 PR #6899](https://github.com/apache/arrow/pull/6899)
- [SIMD dynamic dispatch for BYTE_STREAM_SPLIT #46962](https://github.com/apache/arrow/issues/46962)
- [RLE decoder refactoring #47112](https://github.com/apache/arrow/issues/47112)
- [IO coalescing and caching #24212](https://github.com/apache/arrow/issues/24212)
- [Column filter hint for prefetching PARQUET-1820](https://issues.apache.org/jira/browse/PARQUET-1820)
- [Pre-buffer columns PR #6744](https://github.com/apache/arrow/pull/6744/files)
- [DictionaryArray direct read #20110](https://github.com/apache/arrow/issues/20110)
- [Bloom filter writing #40548](https://github.com/apache/arrow/issues/40548)
- [Row group filtering for nested paths #39064](https://github.com/apache/arrow/issues/39064)
- [Predicate pushdown for timestamp/string #35305](https://github.com/apache/arrow/issues/35305)
- [Memory consumption issue #44890](https://github.com/apache/arrow/issues/44890)
- [Performance regressions #38432](https://github.com/apache/arrow/issues/38432)
- [Binary data type memory #41224](https://github.com/apache/arrow/issues/41224)
- [Java Dataset JNI PR #7030](https://github.com/apache/arrow/pull/7030)
- [Java push-down filtering #14782](https://github.com/apache/arrow/issues/14782)
- [Java push-down filtering #227 (arrow-java)](https://github.com/apache/arrow-java/issues/227)
- [DuckDB custom reader discussion #2762](https://github.com/duckdb/duckdb/discussions/2762)

### Release Notes
- [Arrow 21.0.0 Release](https://arrow.apache.org/blog/2025/07/17/21.0.0-release/)
- [Arrow 22.0.0 Release](https://arrow.apache.org/blog/2025/10/24/22.0.0-release/)
- [Arrow 15.0.0 Release](https://arrow.apache.org/blog/2024/01/21/15.0.0-release/)
- [Parquet C++ Changelog](https://github.com/apache/arrow/blob/main/cpp/CHANGELOG_PARQUET.md)
