# Arrow Parquet Reader: Critical Analysis

A performance-focused critique of Apache Arrow's Parquet reading capabilities across all implementations (C++, Java/JNI, Rust), with competitive comparison against parquet-mr and DuckDB.

---

## 1. Executive Summary

Arrow's Parquet reader exists as **three distinct implementations** with vastly different maturity levels:

| Implementation | Maturity | Performance Tier | Pushdown Depth |
|---|---|---|---|
| **Arrow C++** | Production-ready | High (SIMD, zero-copy) | Row-group only |
| **Arrow Rust (arrow-rs)** | Production-ready | Highest (late materialization) | Page-level + row-level |
| **Arrow Java (JNI to C++)** | Early development | Medium (JNI overhead) | Row-group only (Substrait) |

**The core finding**: Arrow's C++ Parquet reader excels at raw decoding speed but has significant gaps in higher-level query optimizations. The Rust implementation has surpassed C++ in several critical areas. The Java path is a thin JNI wrapper that exposes a fraction of C++ capabilities and adds its own limitations.

---

## 2. Strengths

### 2.1 Raw Decoding Performance (C++)

The C++ reader is highly optimized at the byte-decoding level:

- **SIMD-accelerated BYTE_STREAM_SPLIT**: SSE2 and AVX2 implementations achieve 5-10+ GB/s throughput with runtime CPU feature detection (Arrow 22.0.0+) [[1]](#ref-1)
- **Direct Arrow decoding**: `ByteArrayDecoder::DecodeArrow` writes directly into Arrow builders, eliminating intermediate copies between Parquet's internal format and Arrow columnar format [[2]](#ref-2)
- **Buffer reuse**: `SerializedPageReader` reuses decompression buffers across pages within a column chunk, reducing allocation churn [[3]](#ref-3)
- **Dictionary preservation**: Reading dictionary-encoded columns directly as `DictionaryArray` yields up to **60x faster reads** and peak memory drops from 1.94 GB to 405 MB for a 152 MB dataset [[4]](#ref-4)

These optimizations compound for string-heavy and dictionary-encoded data, which is common in log analytics and observability workloads relevant to Elasticsearch.

### 2.2 Async I/O Pipeline (C++)

The pre-buffering system is well-designed for cloud storage:

- **`PreBuffer()`** asynchronously caches needed byte ranges before decoding begins [[5]](#ref-5)
- **IO coalescing** combines nearby small range requests into larger sequential reads, reducing S3/GCS API calls [[6]](#ref-6)
- **`AsyncContext`** enables fully asynchronous prefetching

This is a meaningful advantage over parquet-mr, which has no async I/O support.

### 2.3 C Data Interface for Java (Near-Zero-Copy JNI)

The Java Dataset module uses the Arrow C Data Interface for batch transfer:

- JNI boundary crossed only once per batch (typically 32,768 rows), not per row or column [[7]](#ref-7)
- `ArrowArray` structs pass buffer pointers, not buffer contents
- Buffer ownership transfers cleanly from C++ to Java `BufferAllocator`

This design amortizes JNI overhead effectively. The bottleneck is the C++ reader itself, not the bridge.

### 2.4 Late Materialization (Rust Only)

Arrow Rust has implemented a full filter-then-project pipeline [[8]](#ref-8):

1. Decode only filter columns
2. Evaluate predicates to produce a `RowSelection` (RLE-encoded bitmask)
3. Decode remaining columns only for matching rows
4. `CachedArrayReader` prevents double-decoding when a column is used in both filter and projection

This is the single most impactful optimization for selective queries and is **not available in C++ or Java**.

### 2.5 Comprehensive Format Support

Arrow supports the full Parquet specification:
- All encodings: PLAIN, RLE, DELTA_BINARY_PACKED, DELTA_BYTE_ARRAY, BYTE_STREAM_SPLIT
- All compression codecs: Snappy, ZSTD, GZIP, LZ4, Brotli
- Nested types (structs, lists, maps)
- Parquet encryption (with recent memory optimizations in Arrow 22.0.0)
- New logical types: VARIANT, UUID, GEOMETRY, GEOGRAPHY (Arrow 21.0.0+) [[9]](#ref-9)

---

## 3. Weaknesses

### 3.1 No Late Materialization in C++ (Critical)

The C++ reader decodes **ALL selected columns for ALL selected row groups**. There is no mechanism to:
- Decode filter columns first, evaluate predicates, then decode remaining columns only for matching rows
- Skip individual pages within a column chunk based on filter results
- Build a `RowSelection` from page-level statistics

**Impact**: For selective queries (e.g., "WHERE status = 'error'" on a dataset where 1% of rows match), the C++ reader reads and decodes 100x more data than necessary for non-filter columns.

**Competitive gap**: Both arrow-rs and DuckDB have this capability. DuckDB integrates it into its vectorized execution engine; arrow-rs provides it as a library feature. The C++ issue tracker acknowledges this gap [[10]](#ref-10) but there is no implementation timeline.

### 3.2 Page Index and Bloom Filters Not Integrated (Critical)

The C++ reader provides **API access** to Column Index, Offset Index, and Bloom Filters, but does **not use them in the read pipeline**:

| Feature | C++ Status | Rust Status | parquet-mr Status |
|---|---|---|---|
| Page Index pruning | API only, not used | Integrated in reads | Integrated (via Spark) |
| Bloom filter pruning | API only, not used | Integrated in DataFusion | Integrated in reads |

This means the C++ reader cannot skip individual data pages (~1 MB granularity) even when page-level statistics prove they contain no matching rows. For sorted data, this can mean reading 128x more data than necessary (entire row group vs. matching page).

**Source**: [[11]](#ref-11), [[12]](#ref-12)

### 3.3 Java JNI Bridge Is Thin and Fragile (Major)

The `arrow-dataset` Java module exposes a small fraction of C++ capabilities:

**Not exposed via JNI:**
- Pre-buffering / IO coalescing configuration
- Parallel column decoding (`use_threads`)
- Dictionary read mode (`read_dictionary`)
- Page index access
- Bloom filter access
- Fine-grained row group selection
- Fragment-level control

**Substrait-only filtering**: Filter predicates must be serialized as Substrait binary expressions. There are no user-friendly Java tools to construct these; the recommended path is `SqlExpressionToSubstrait` from the Isthmus library, which adds another dependency and complexity layer [[13]](#ref-13).

**"Early development" stability**: The module's own documentation warns of API instability [[14]](#ref-14). `NativeScanner.NativeReader.loadRecordBatch()` and `loadDictionary()` both throw `UnsupportedOperationException` — only the C Data Interface path works.

**Native library dependency**: Requires `arrow_dataset_jni` compiled for each target OS/architecture, with no pure-Java fallback.

### 3.4 Memory Consumption (Major)

`ReadTable()` materializes complete datasets in memory. A 23 GB Parquet file can exceed 64 GB RAM during reading because:
- Decompressed data is typically 2-4x larger than compressed
- Arrow's fixed-width arrays and string offsets add overhead
- No streaming backpressure mechanism in the core reader API

`GetRecordBatchReader()` provides streaming but the caller must manage memory bounds externally — there is no built-in memory budget mechanism like DuckDB's buffer manager.

**Source**: [[15]](#ref-15)

### 3.5 Footer/Metadata Parsing Bottleneck (Moderate)

Footer parsing scales linearly with `columns * row_groups`. For wide tables (thousands of columns) or files with many row groups:
- Standard Thrift deserialization becomes a bottleneck
- The Rust implementation addressed this with a custom Thrift parser achieving **3-9x faster metadata parsing** [[16]](#ref-16)
- The C++ implementation still uses standard Thrift
- For multi-file datasets, this cost multiplies

### 3.6 No Streaming with Bounded Memory (Moderate)

DuckDB's reader operates within a buffer manager that enforces strict memory limits. Arrow C++ has no equivalent:
- `ReadTable()` is unbounded
- `GetRecordBatchReader()` streams but doesn't enforce memory limits
- The Dataset Scanner has no memory budget concept
- The Java JNI bridge has no memory limiting beyond the `BufferAllocator` tree (which limits Java-side only; C++ allocations during decoding are separate)

### 3.7 Binary/String Data Performance (Moderate)

Large binary and string values cause disproportionate memory allocation overhead [[17]](#ref-17). This is inherent to Arrow's format: strings require offset arrays plus data buffers, and variable-length data cannot be efficiently pre-allocated. The BinaryView type (Arrow 21.0.0+) partially addresses this but is not yet the default path.

---

## 4. Competitive Analysis

### 4.1 Arrow C++ vs DuckDB

| Dimension | Arrow C++ | DuckDB | Winner |
|---|---|---|---|
| **Raw decode speed** | SIMD-accelerated, highly optimized | Good, but no explicit SIMD for Parquet encodings | Arrow |
| **Predicate pushdown depth** | Row group only (Dataset API) | Row group zonemaps + in-scan filtering | DuckDB |
| **Late materialization** | No | Integrated in execution engine | DuckDB |
| **Memory management** | Unbounded or manually managed | Buffer manager with strict limits | DuckDB |
| **Streaming** | Optional via RecordBatchReader | Always streaming, bounded memory | DuckDB |
| **Dictionary handling** | DictionaryArray preservation (60x speedup) | Internal dictionary optimization | Arrow |
| **Async I/O** | PreBuffer + IO coalescing | Parallel row group processing | Tie |
| **Library flexibility** | Embeddable in any application | Tightly coupled to DuckDB engine | Arrow |
| **Format breadth** | Full spec (encryption, all encodings, new types) | Subset (no encryption, limited encodings) | Arrow |

**DuckDB's key advantage**: Deep integration between query engine and reader means filters, projections, and memory limits flow naturally from SQL to I/O. DuckDB built its own reader specifically because Arrow's library model couldn't provide this integration [[18]](#ref-18).

**Arrow's key advantage**: Flexibility as a library component. Any application can embed Arrow's reader and get high-performance Parquet access. Arrow also supports the full Parquet specification including encryption and all encoding types.

### 4.2 Arrow C++ vs parquet-mr (parquet-java)

| Dimension | Arrow C++ | parquet-mr | Winner |
|---|---|---|---|
| **Decode throughput** | >2x faster (SIMD, vectorized) | JIT-compiled, value-at-a-time | Arrow |
| **Dictionary handling** | DictionaryArray (zero-copy indices) | Materializes by default | Arrow |
| **Page Index integration** | API only, not used in reads | Integrated via `readNextFilteredRowGroup()` | parquet-mr |
| **Bloom filter integration** | API only, not used in reads | Integrated in read pipeline | parquet-mr |
| **Dictionary filter** | Not integrated | Integrated (reads dict page, evaluates predicate) | parquet-mr |
| **Multi-threading** | Optional parallel column decoding | Single-threaded | Arrow |
| **Async I/O** | PreBuffer + IO coalescing | Synchronous only | Arrow |
| **Ecosystem** | Arrow-native applications | Hadoop/Spark/Iceberg ecosystem | parquet-mr |
| **Memory model** | Off-heap, zero-copy slicing | JVM heap + off-heap codec buffers | Arrow |

**Surprising finding**: parquet-mr has **more complete pushdown integration** than Arrow C++. While Arrow C++ decodes faster at the byte level, parquet-mr integrates bloom filters, dictionary filters, and page-level statistics directly into its read pipeline. For selective queries on well-indexed Parquet files, parquet-mr may skip more data than Arrow C++.

**However**: parquet-mr's record-oriented assembly (`GroupRecordConverter`) negates much of this advantage by converting columnar data to rows. Systems like Spark and Iceberg bypass this with custom vectorized readers.

### 4.3 Arrow C++ vs Arrow Rust (arrow-rs)

| Dimension | Arrow C++ | Arrow Rust | Winner |
|---|---|---|---|
| **Late materialization** | Not implemented | Full pipeline (filter → select → project) | Rust |
| **Page index pruning** | API only | `RowSelection` skips pages | Rust |
| **Adaptive row selection** | Not implemented | Auto-selects RLE vs bitmask based on selectivity | Rust |
| **CachedArrayReader** | Not implemented | Prevents double-decode for filter+project columns | Rust |
| **Metadata parsing** | Standard Thrift | Custom parser (3-9x faster) | Rust |
| **Bloom filter in reads** | API only | Integrated in DataFusion | Rust |
| **SIMD** | SSE2/AVX2 with runtime dispatch | LLVM auto-vectorization | Tie |
| **Ecosystem maturity** | Broader C++ ecosystem | Growing rapidly | C++ |
| **Multi-language reach** | JNI, Python (PyArrow), R | Rust only (+ Python via PyArrow) | C++ |

**The Rust implementation has surpassed C++ in query-relevant optimizations.** This is significant because it demonstrates what Arrow's Parquet reader *could* be — the C++ implementation has the same architectural opportunity but hasn't executed on it.

### 4.4 Competitive Position Summary

```
                    Raw Decode Speed
                         |
                    Arrow C++  ★★★★★
                    Arrow Rust ★★★★☆
                    DuckDB     ★★★☆☆
                    parquet-mr ★★☆☆☆

               Pushdown Integration
                         |
                    Arrow Rust ★★★★★
                    DuckDB     ★★★★☆
                    parquet-mr ★★★★☆
                    Arrow C++  ★★☆☆☆

              Query Engine Integration
                         |
                    DuckDB     ★★★★★
                    Arrow Rust ★★★★☆ (via DataFusion)
                    Arrow C++  ★★☆☆☆ (Dataset API)
                    parquet-mr ★★★☆☆ (via Spark)

                  Java Accessibility
                         |
                    parquet-mr ★★★★★ (native Java)
                    Arrow C++  ★★☆☆☆ (JNI, early dev)
                    Arrow Rust ★☆☆☆☆ (no Java bridge)
                    DuckDB     ★★★☆☆ (JDBC)
```

---

## 5. Implications for Elasticsearch

### 5.1 If Using Arrow Java JNI (`arrow-dataset`)

**Pros:**
- C++ decoding speed, near-zero-copy batch transfer
- Projection pushdown works well
- Single-dependency native library

**Cons:**
- Early development, API instability
- Substrait-only filtering is awkward from Java
- No control over dictionary modes, pre-buffering, parallel decoding
- Native library deployment and platform support burden
- No page-level or bloom filter pushdowns reachable from Java
- No late materialization (C++ backend lacks it)

### 5.2 If Using parquet-mr (Current ES Path)

**Pros:**
- Pure Java, no native dependencies
- Full pushdown stack available (stats, dictionary, bloom filter, page index)
- Battle-tested at massive scale (Hadoop/Spark ecosystem)
- Already in ES's dependency tree

**Cons:**
- Record-oriented assembly forces columnar→row→columnar conversion
- Single-threaded I/O
- No async I/O or pre-buffering
- ES's current implementation uses none of the available pushdowns

### 5.3 Recommended Path

The biggest performance wins for ES's Parquet reading come from **fixing the current parquet-mr integration** rather than switching to Arrow:

1. **Projection pushdown** (easy, high impact): Pass `requestedSchema` to `GroupRecordConverter` so only needed columns are read from disk
2. **Predicate pushdown** (medium, high impact): Convert ESQL filter expressions to parquet-mr `FilterPredicate` objects (similar to `IcebergPushdownFilters`)
3. **Column index filtering** (medium, high impact for sorted data): Enable `readNextFilteredRowGroup()` with page-level pruning
4. **Vectorized reading** (hard, highest impact): Bypass `GroupRecordConverter` entirely; read column chunks directly into ESQL blocks using parquet-mr's `ColumnReader` API, or adopt Iceberg's `ArrowReader` pattern

Switching to Arrow Java JNI only makes sense if:
- ES already needs the native library for other Arrow functionality (e.g., Flight)
- The JNI bridge matures significantly (exposes dictionary modes, pre-buffering, parallel decoding)
- Late materialization is added to Arrow C++

---

## 6. Open Questions

1. **Will Arrow C++ implement late materialization?** Issue [[10]](#ref-10) exists but no implementation timeline. This is the single biggest gap.
2. **Will the Java JNI bridge expose more C++ features?** Issues [[13]](#ref-13) and [[19]](#ref-19) track this but progress is slow.
3. **When will Arrow C++ integrate page index and bloom filters into read APIs?** These are the second and third biggest gaps.
4. **Is Hardwood (Java 21+ Parquet parser) a viable alternative?** Multi-threaded page-level parallelism in pure Java could outperform parquet-mr without native dependencies.

---

## References

<a id="ref-1"></a>**[1]** Arrow SIMD BYTE_STREAM_SPLIT — [byte_stream_split.h](https://github.com/apache/arrow/blob/main/cpp/src/arrow/util/byte_stream_split.h), [PR #6899](https://github.com/apache/arrow/pull/6899), [Issue #46962](https://github.com/apache/arrow/issues/46962)

<a id="ref-2"></a>**[2]** Direct Arrow decoding — [reader.cc](https://github.com/apache/arrow/blob/main/cpp/src/parquet/arrow/reader.cc), `ByteArrayDecoder::DecodeArrow` method

<a id="ref-3"></a>**[3]** Buffer reuse — `SerializedPageReader` in [column_reader.cc](https://github.com/apache/arrow/blob/main/cpp/src/parquet/column_reader.cc)

<a id="ref-4"></a>**[4]** Dictionary preservation benchmarks — [Faster C++ Parquet on Dictionary-Encoded Strings (2019)](https://arrow.apache.org/blog/2019/09/05/faster-strings-cpp-parquet/), [Issue #20110](https://github.com/apache/arrow/issues/20110)

<a id="ref-5"></a>**[5]** PreBuffer — [PARQUET-1820](https://issues.apache.org/jira/browse/PARQUET-1820), `parquet::arrow::FileReader::PreBuffer()`

<a id="ref-6"></a>**[6]** IO coalescing — [ARROW-7995 / Issue #24212](https://github.com/apache/arrow/issues/24212)

<a id="ref-7"></a>**[7]** JNI batch transfer — [NativeScanner.java](https://fossies.org/linux/apache-arrow/java/dataset/src/main/java/org/apache/arrow/dataset/jni/NativeScanner.java), `NativeReader.loadNextBatch()`

<a id="ref-8"></a>**[8]** Late materialization in arrow-rs — [Deep Dive (2025)](https://arrow.apache.org/blog/2025/12/11/parquet-late-materialization-deep-dive/), [Querying Parquet with Millisecond Latency (2022)](https://arrow.apache.org/blog/2022/12/26/querying-parquet-with-millisecond-latency/)

<a id="ref-9"></a>**[9]** Arrow 21.0.0 new types — [Release Notes](https://arrow.apache.org/blog/2025/07/17/21.0.0-release/)

<a id="ref-10"></a>**[10]** C++ predicate pushdown gap — [Issue #35305](https://github.com/apache/arrow/issues/35305)

<a id="ref-11"></a>**[11]** Bloom filter gap — [Issue #40548](https://github.com/apache/arrow/issues/40548)

<a id="ref-12"></a>**[12]** Page index not used — Arrow C++ Parquet docs: "access to Column Index and Offset Index structures IS provided... but the data read APIs do not currently make use of them"

<a id="ref-13"></a>**[13]** Java pushdown limitations — [Issue #14782](https://github.com/apache/arrow/issues/14782), [Issue #227 (arrow-java)](https://github.com/apache/arrow-java/issues/227)

<a id="ref-14"></a>**[14]** Java Dataset "early development" — [Arrow Java Dataset docs](https://arrow.apache.org/docs/java/dataset.html)

<a id="ref-15"></a>**[15]** Memory consumption — [Issue #44890](https://github.com/apache/arrow/issues/44890)

<a id="ref-16"></a>**[16]** Rust custom Thrift parser — [3-9x Faster Parquet Metadata (2025)](https://arrow.apache.org/blog/2025/10/23/rust-parquet-metadata/)

<a id="ref-17"></a>**[17]** Binary data overhead — [Issue #41224](https://github.com/apache/arrow/issues/41224)

<a id="ref-18"></a>**[18]** DuckDB custom reader rationale — [GitHub Discussion #2762](https://github.com/duckdb/duckdb/discussions/2762)

<a id="ref-19"></a>**[19]** Java Dataset JNI — [PR #7030](https://github.com/apache/arrow/pull/7030)
