# Apache Arrow: A Comprehensive Overview

**Purpose:** Understanding Arrow's full ecosystem, capabilities, and relevance for ES|QL external data sources.

---

## 1. What Arrow Is — And What It Isn't

Apache Arrow is a **language-independent columnar memory format** specification and multi-language toolbox. It is NOT a storage format, NOT a database, and NOT a query engine. Arrow defines how structured data should be laid out in memory so that multiple systems can share data without serialization.

The core insight: before Arrow, every analytics system (Spark, Pandas, R, databases) had its own in-memory format. Moving data between systems required serialization → transfer → deserialization, which consumed **80%+ of total data access time** [[1]](#ref-1). Arrow eliminates this by providing one format everyone agrees on.

Arrow turned 10 years old in February 2026 [[2]](#ref-2) and is now the de facto standard for in-memory columnar data, adopted by DuckDB, Polars, Spark, DataFusion, InfluxDB, and dozens of other systems.

---

## 2. The Columnar Format Specification

### 2.1 Memory Layout
Arrow organizes data in **columns**, not rows. Each column is a contiguous buffer of typed values with:
- **Validity bitmap** — one bit per value indicating null/not-null
- **Data buffer(s)** — the actual values, type-dependent layout
- **Offset buffer** — for variable-length types (strings, binary), stores byte offsets

All buffers are **64-byte aligned**, matching Intel AVX-512 SIMD register width. This enables processing entire columns with single SIMD instructions — the core performance advantage of the format [[3]](#ref-3).

### 2.2 Type System
Arrow supports a rich type system: integers (8/16/32/64-bit, signed/unsigned), floats (16/32/64-bit), decimals, booleans, UTF-8 strings, binary, dates, timestamps (second through nanosecond), durations, intervals, lists (variable and fixed-size), structs, maps, unions (dense and sparse), and dictionary-encoded types.

### 2.3 Recent Format Additions
- **Run-End Encoded (REE) layouts** — efficiently represent columns with many repeated values
- **Variable-size Binary Views** — flexible, scalable storage for string/binary data
- **TimestampWithOffset** — timezone encoding directly with each value

### 2.4 Verified Against ES Codebase
Elasticsearch uses Arrow v18.3.0 and maps 20 ES|QL types to Arrow types. The type mapping is bidirectional — `BlockConverter` handles output and `ArrowToBlockConverter` handles input. Each Arrow field carries `elastic:type` metadata preserving the original ES|QL type name. *[All verified against codebase — see reconciliation.md]*

---

## 3. Protocols: IPC and Flight

### 3.1 Arrow IPC (Inter-Process Communication)
The IPC format enables sharing Arrow data between processes:

**Streaming format:** Schema header → RecordBatch messages → End-of-stream marker. Used when the receiver processes data sequentially.

**File format:** Schema + RecordBatch messages + footer with offsets. Used when random access to batches is needed.

**Zero-copy:** Reading is inherently zero-copy when the source supports it (memory-mapped files, shared memory). The data in memory IS the Arrow format — no deserialization step needed [[4]](#ref-4).

**ES|QL implementation:** `ArrowResponse` implements the IPC streaming format for query results. Content type: `application/vnd.apache.arrow.stream`. Cleverly bypasses Arrow's memory manager by creating dummy `ArrowBuf` objects for size tracking and writing directly from ES|QL Block data. *[Verified: ArrowResponse.java:267-269, BlockConverter.java:417-419]*

### 3.2 Arrow Flight
Flight is a high-performance **gRPC-based protocol** for streaming Arrow record batches over networks:

- **GetFlightInfo** — discover available datasets and their partitions (FlightEndpoints)
- **GetStream** — retrieve data as a stream of Arrow batches using a Ticket
- **DoPut** — upload data to a server
- **DoAction** — execute arbitrary server actions

Key properties:
- **Parallel transfer** via multiple FlightEndpoints (different servers/partitions)
- **Zero serialization cost** — Arrow batches ARE the wire format
- **Flow control** via gRPC backpressure
- **Near wire-speed** data transfer — academic benchmarking confirms 20-50x over ODBC for analytical workloads [[5]](#ref-5)

**ES|QL implementation:** The `esql-datasource-grpc` module implements a Flight client connector. `FlightSplitProvider` maps Flight endpoints to ES|QL splits for parallel execution. Uses real `RootAllocator` (unlike the output path which shims it). Handles `flight://` and `grpc://` URI schemes. *[Verified: FlightConnector.java:55, GrpcDataSourcePlugin.java:27-29]*

### 3.3 Flight SQL and ADBC
**Flight SQL** extends Flight with SQL semantics (ExecuteQuery, GetCatalogs, GetSchemas, PreparedStatement). A JDBC driver ships with Arrow since v10.0.0. Implementing a Flight SQL server gives you ADBC, JDBC, and ODBC drivers essentially for free [[6]](#ref-6).

**ADBC (Arrow Database Connectivity)** is a modern API standard for database access, designed as the Arrow-native replacement for JDBC/ODBC. Available for C, Go, Java. Drivers exist for Flight SQL, PostgreSQL, SQLite, Snowflake, DuckDB.

**ES|QL does NOT currently use Flight SQL or ADBC** — only `flight-core` for raw Flight protocol. *[Verified: zero matches for flight-sql in build files]*

---

## 4. Format Readers: CSV, JSON, Parquet

### 4.1 Parquet (Best-in-Class for Arrow)
Parquet is Arrow's natural complement — a columnar STORAGE format to Arrow's columnar MEMORY format. Key capabilities:
- **Projection pushdown**: Only read columns needed by the query — dramatic I/O reduction for wide tables
- **Predicate pushdown**: Use row group statistics (min/max), bloom filters, and page-level stats to skip entire row groups or pages
- **Multi-threaded column reading**: Parallel decompression/decoding of columns
- **Compression**: Snappy, ZSTD, LZ4, Gzip — all transparent to the reader
- **Memory mapping**: mmap support for local files

Arrow C++/Rust have the most mature Parquet readers. Java uses `parquet-hadoop` (separate Apache project), not Arrow's own reader.

**ES|QL approach:** `ParquetFormatReader` reads Parquet natively via `parquet-hadoop` and produces ES|QL Pages directly — **no Arrow involved**. The Iceberg path uses `iceberg-arrow` (ArrowReader) for vectorized reading. *[Verified: ParquetFormatReader.java has zero Arrow imports]*

### 4.2 CSV
- Row-oriented source — must parse entire rows
- **Projection pushdown**: Column selection during parsing
- **No predicate pushdown**: Must read all rows
- Streaming batch conversion to columnar Arrow format
- Arrow's CSV reader supports multi-threaded parsing

### 4.3 JSON (NDJSON)
- Row-oriented, similar limitations to CSV
- Schema inference or explicit schema
- No predicate pushdown

### 4.4 Performance Hierarchy
```
Parquet >> ORC > CSV ≈ JSON  (for analytical reads from blob stores)
```
Parquet wins because: columnar storage matches columnar memory format, rich metadata enables pushdown, compression ratio is better on columnar data.

---

## 5. Blob Store Access

### 5.1 Filesystem Abstraction
Arrow C++ provides `arrow::fs::FileSystem` with implementations for local, S3, GCS, and HDFS. The Rust `object_store` crate (in `arrow-rs`) provides S3, GCS, Azure Blob, local, and in-memory backends.

**Key optimization**: Range reads — only fetch byte ranges needed. Critical for Parquet where you can read individual column chunks without downloading the full file.

### 5.2 Performance Reality
S3 latency is **significantly higher** than local filesystem even with prebuffering. Arrow's issue tracker documents this gap (issue #39899). Mitigations:
- Larger batch sizes to amortize round-trip latency
- Aggressive metadata caching (Parquet footers)
- Parallel range requests for different column chunks
- `WhenBuffered()` to fully buffer before processing

### 5.3 Java Limitations
Arrow Java does NOT have a pure-Java filesystem abstraction. The Dataset API wraps C++ via JNI. For Java systems (like ES), blob store access typically comes from:
- Hadoop's S3A connector
- AWS SDK directly
- Iceberg's own S3FileIO

**ES|QL approach:** Uses its own `StorageProvider` SPI with S3 support via AWS SDK, not Arrow's filesystem abstraction.

---

## 6. Depth Optimizations

### 6.1 SIMD Vectorization
The 64-byte alignment enables SIMD processing:
- C++ kernels explicitly use SIMD intrinsics (AVX2, AVX-512, NEON)
- Validity bitmap operations benefit from bitwise SIMD
- Java benefits from memory layout (JIT may auto-vectorize aligned operations)
- Alignment comes from Intel's performance guide recommendation [[3]](#ref-3)

### 6.2 Zero-Copy Patterns
- **IPC file reading**: mmap → ready to use, no copies
- **Shared memory IPC**: processes share Arrow buffers directly
- **Flight**: Arrow batches cross process boundaries without transformation
- **C Data Interface**: share Arrow data across FFI boundaries without copies

### 6.3 Dictionary Encoding
Low-cardinality columns stored as integer indices into a dictionary:
- Fixed-width array operations on variable-width data
- Compute kernels can operate on encoded data directly
- Particularly effective for string columns with repeated values (status codes, categories, country codes)

### 6.4 Late Materialization
- Only decode columns actually needed (projection pushdown)
- For Parquet: only decode row groups/pages that pass predicates
- Arrow's columnar layout makes column-level I/O natural

### 6.5 Memory Management
- Off-heap allocation avoids JVM GC pressure
- Hierarchical allocators for isolation and accounting
- Reference counting for buffer lifecycle
- Circuit breaker integration via allocation listeners

---

## 7. Arrow as Unified Multi-Source Framework

### 7.1 The Vision
Arrow enables a single query engine to access multiple data sources without format conversion:
```
  S3 Parquet ──┐
  GCS CSV     ─┤
  HDFS ORC    ─┼──→ Arrow RecordBatches ──→ Query Engine ──→ Results
  Flight SQL  ─┤
  JDBC/ADBC   ─┘
```

### 7.2 Who Uses Arrow This Way
| Engine | How It Uses Arrow |
|--------|-------------------|
| **DuckDB** | Arrow as exchange format, direct Arrow I/O |
| **DataFusion** | Built entirely on Arrow (RecordBatch = unit of work) |
| **Polars** | Arrow as core memory model |
| **Spark** | Arrow for Python interop (PySpark pandas UDFs) |
| **InfluxDB 3.0** | Built on DataFusion/Arrow |
| **Dremio** | Arrow Flight for distributed queries |
| **OpenSearch** | Exploring Arrow integration [[7]](#ref-7) |

### 7.3 Practical Limitations for Java
- Arrow Java's Dataset API (JNI to C++) is marked "early development, API might change"
- No pure-Java compute kernels — compute from application or via JNI
- JNI boundary adds complexity (memory management, resource lifecycle, native lib distribution)
- Smaller community contribution compared to C++/Python/Rust

---

## 8. Roadmap & Direction

### 8.1 Project-Wide
- Language-specific repos (Java, Rust split out) for independent release cycles
- ADBC adoption growing as JDBC/ODBC successor
- Flight SQL becoming standard query protocol for analytics
- Format evolution: REE, binary views, timestamp-with-offset
- nanoarrow for embedded/constrained environments

### 8.2 Java-Specific
- v18.3.0 released May 2025; v18.2.0 (Feb 2025) was first from standalone repo
- Discussion on decoupling Java versioning (v20.0.0+)
- Trend toward Unsafe as default allocator, simplifying Netty dependency
- Flight SQL JDBC driver increasingly production-ready
- **No native Java compute kernels planned** — compute expected from application layer *[Verified]*

### 8.3 Community Health
- 10 years old, top-level Apache project
- Multiple releases per month (especially arrow-rs)
- 339 issues resolved by 82 contributors in v21.0.0 alone
- Strong momentum in Rust ecosystem (DataFusion, Ballista)

---

## References

<a id="ref-1"></a>**[1]** Arrow overview: "more than 80% of the total time spent in accessing data is often elapsed in the serialization/deserialization step" — https://arrow.apache.org/overview/

<a id="ref-2"></a>**[2]** "Apache Arrow is 10 years old" — https://arrow.apache.org/blog/2026/02/12/arrow-anniversary/

<a id="ref-3"></a>**[3]** Arrow Columnar Format: "64-byte alignment... matching the largest SIMD instruction registers on widely deployed x86 architecture (Intel AVX-512)" — https://arrow.apache.org/docs/format/Columnar.html

<a id="ref-4"></a>**[4]** Arrow IPC: "Reading Arrow IPC data is inherently zero-copy if the source allows it" — https://arrow.apache.org/docs/python/ipc.html

<a id="ref-5"></a>**[5]** "Benchmarking Apache Arrow Flight — A wire-speed protocol for data transfer, querying and microservices" — https://arxiv.org/abs/2204.03032

<a id="ref-6"></a>**[6]** "Database developers can just provide a Flight SQL service, which will give them ADBC, JDBC, and ODBC drivers for free" — https://dipankar-tnt.medium.com/what-is-apache-arrow-flight-flight-sql-adbc-a076511122ac

<a id="ref-7"></a>**[7]** "OpenSearch and Apache Arrow: A tour of the archery range" — https://opensearch.org/blog/opensearch-and-apache-arrow-a-tour-of-the-archery-range/
