# Apache Arrow Deep Dive: Codebase & Ecosystem

**Date:** 2026-03-04
**Scope:** Apache Arrow GitHub repos, Java distribution, Maven artifacts, capabilities, roadmap.

---

## 1. Repository Ecosystem

### 1.1 Main Mono-Repo: `apache/arrow`
- **URL:** https://github.com/apache/arrow
- C++ core (reference implementation), Python (PyArrow), Go, C#/.NET, JavaScript, Ruby, Julia, MATLAB, C GLib
- Format specification (FlatBuffers `.fbs` files)
- Integration testing infrastructure
- C++ and Python most mature; Go maturing; others community-driven

### 1.2 `apache/arrow-java`
- **URL:** https://github.com/apache/arrow-java
- Dedicated Java repo (split from mono-repo ~2024-2025)
- Maven-based build
- Key modules:
  - `arrow-format` — FlatBuffers-generated schema classes
  - `arrow-vector` — Vector (column) implementations for all Arrow types
  - `arrow-memory-core` — Memory management abstractions (BufferAllocator, ArrowBuf)
  - `arrow-memory-unsafe` — Allocator using sun.misc.Unsafe (off-heap)
  - `arrow-memory-netty` — Allocator using Netty buffer pools
  - `arrow-memory-netty-buffer-patch` — Netty buffer compat patch
  - `flight-core` — Arrow Flight (gRPC data transport) client/server
  - `flight-sql` — Flight SQL protocol
  - `arrow-dataset` — Dataset API (JNI to C++)
  - `arrow-jdbc` — JDBC adapter
  - `arrow-compression` — LZ4, ZSTD codecs
  - `arrow-c-data` — C Data Interface JNI bridge
  - `arrow-gandiva` — LLVM expression compiler JNI bridge

### 1.3 `apache/arrow-rs`
- **URL:** https://github.com/apache/arrow-rs
- Rust implementation — foundation for DataFusion and Ballista
- Crates: `arrow`, `arrow-array`, `arrow-buffer`, `arrow-schema`, `arrow-flight`, `arrow-ipc`, `arrow-json`, `arrow-csv`, `parquet`, `object_store`

### 1.4 Other Repos
| Repository | Description |
|-----------|-------------|
| `apache/arrow-adbc` | Arrow Database Connectivity standard (JDBC/ODBC successor) |
| `apache/arrow-nanoarrow` | Minimal C Arrow implementation |
| `apache/arrow-flight-sql-postgresql` | Flight SQL adapter for PostgreSQL |
| `apache/arrow-cookbook` | Code examples/recipes |
| `apache/arrow-testing` | Shared test data for cross-implementation testing |

---

## 2. Java Maven Central Artifacts (v18.3.0)

All published under `org.apache.arrow` at https://repo1.maven.org/maven2/org/apache/arrow/

### 2.1 Artifacts Used by Elasticsearch

| Artifact | Purpose |
|----------|---------|
| `arrow-format:18.3.0` | FlatBuffers schema/metadata classes |
| `arrow-vector:18.3.0` | All vector type implementations (IntVector, VarCharVector, Float8Vector, etc.) |
| `arrow-memory-core:18.3.0` | BufferAllocator, ArrowBuf, AllocationManager abstractions |
| `arrow-memory-unsafe:18.3.0` | Off-heap allocator via sun.misc.Unsafe |
| `arrow-memory-netty:18.3.0` | Netty-based allocator (used by Iceberg) |
| `flight-core:18.3.0` | FlightClient, FlightStream, FlightInfo for gRPC transport |

**Source:** `gradle/verification-metadata.xml` lines 2640-2677

### 2.2 Dependency Tree
```
arrow-vector
  ├── arrow-format (FlatBuffers schema)
  ├── arrow-memory-core (memory abstractions)
  ├── com.google.flatbuffers:flatbuffers-java
  └── com.fasterxml.jackson.core:jackson-*

flight-core
  ├── arrow-vector
  ├── arrow-memory-core
  ├── io.grpc:grpc-* (gRPC transport)
  ├── com.google.protobuf:protobuf-java
  └── io.netty:netty-*
```

### 2.3 JNI / Native Code
- **arrow-memory-unsafe**: NO native code — uses sun.misc.Unsafe (Java internal API). Requires `--add-opens=java.base/java.nio=ALL-UNNAMED`.
- **arrow-memory-netty**: NO native code per se — relies on Netty which may load native transports.
- **arrow-gandiva**: YES — JNI to LLVM. Not used by ES.
- **arrow-dataset**: YES — JNI to C++ Dataset lib. Not used by ES.
- **arrow-c-data**: YES — JNI for C Data Interface. Not used by ES.

### 2.4 ES Dependency Management
- All Arrow transitives excluded via `ExcludeAllTransitivesRule` (ComponentMetadataRulesPlugin.java:192-194)
- Jackson versions aligned with ES's own
- Memory manager shimmed to no-op in production (AllocationManagerShim)

---

## 3. Key Capabilities by Module

### 3.1 Columnar Format Specification
- Language-agnostic in-memory columnar format
- Rich type system (nested, user-defined types)
- 64-byte alignment for SIMD (AVX-512) optimization
- Validity bitmaps for null tracking
- Dictionary encoding for repeated values
- Variable-size binary views (recent addition)
- Run-End Encoded layouts for repeated values (recent)

### 3.2 IPC (Inter-Process Communication)
- **Streaming format**: Schema header → RecordBatch messages → EOS marker
- **File format**: Schema + RecordBatch messages + footer with random access
- Zero-copy when source supports it (memory-mapped files, shared memory)
- All buffers 64-byte aligned
- FlatBuffers-based message metadata

### 3.3 Arrow Flight (gRPC Data Transport)
- High-performance gRPC-based protocol for streaming Arrow batches
- Key operations: GetFlightInfo, GetStream, DoPut, DoAction
- FlightEndpoint allows parallel reads across multiple servers
- FlightDescriptor + Ticket for discovery and retrieval
- Built-in flow control via gRPC backpressure
- Zero serialization cost — Arrow batches are the wire format

### 3.4 Arrow Flight SQL
- SQL query protocol built on Flight
- Supports: ExecuteQuery, GetCatalogs, GetSchemas, GetTables, GetPrimaryKeys
- JDBC driver available (since Arrow 10.0.0)
- ADBC (Arrow Database Connectivity) provides modern API alternative
- "Implement Flight SQL server → get ADBC, JDBC, ODBC drivers for free"

### 3.5 Dataset API
- Unified reading from files/blob stores (Parquet, CSV, JSON, ORC, IPC)
- Java version uses JNI to C++ (early development, API may change)
- Supports projection pushdown and filter pushdown (for Parquet)
- Filesystem abstraction for S3, GCS, HDFS, local
- **Limitation in Java**: JNI bridge requires native library, memory is off-JVM-heap

### 3.6 Compute Kernels
- Aggregation: sum, mean, min, max, count, variance, mode, quantile
- Scalar: arithmetic, string ops, casting, comparison
- Vector: sort, filter, take, dictionary encode, unique, partition
- **Java has NO native compute kernel library** — must use JNI to C++ (Gandiva) or application-level compute
- SIMD vectorization in C++ kernels via 64-byte aligned buffers

### 3.7 Memory Management
- BufferAllocator hierarchy with accounting
- RootAllocator → child allocators for isolation
- ArrowBuf wraps off-heap memory
- Two backends: Unsafe (lightweight) and Netty (pooled)
- Reference counting for buffer lifecycle

---

## 4. Format Readers (Arrow Ecosystem)

### 4.1 Parquet
- Primary columnar storage format in the ecosystem
- Arrow C++ has full reader/writer; Java uses `parquet-hadoop` (separate Apache project)
- Supports: projection pushdown, predicate pushdown (row group stats, bloom filters, page stats)
- Iceberg uses `iceberg-arrow` to bridge Parquet → Arrow vectors

### 4.2 CSV
- Arrow C++/Python have CSV readers producing Arrow batches
- Java: basic CSV reader exists but not commonly used in production
- No predicate pushdown possible (row-oriented format)
- Projection pushdown: column selection only

### 4.3 JSON
- Arrow C++/Python have JSON readers
- Java: basic capabilities
- No predicate pushdown possible
- Limited optimization potential vs columnar formats

### 4.4 ORC
- Separate Apache project, not part of Arrow
- Arrow has adapters for reading ORC into Arrow format

---

## 5. Blob Store Access

### 5.1 Filesystem Abstraction
- Arrow C++ provides `arrow::fs::FileSystem` interface
- Implementations: LocalFileSystem, S3FileSystem, GcsFileSystem, HdfsFileSystem
- S3: auto-discovers credentials (env vars, config files, EC2 metadata)
- Supports range reads, parallel fetches

### 5.2 Performance Considerations
- S3 latency significantly higher than local FS even with prebuffering
- `WhenBuffered()` call needed to achieve equivalent performance to local
- Multi-threaded column reading for Parquet
- Prefetching and caching needed for remote storage

### 5.3 Java/Rust Object Store
- Rust `object_store` crate (in arrow-rs): S3, GCS, Azure, in-memory, local
- Java Dataset API: JNI to C++ filesystem abstraction
- No pure-Java filesystem abstraction in Arrow Java itself

---

## 6. On-the-Wire Performance

### 6.1 Arrow IPC
- Zero-copy when source supports it (mmap, shared memory)
- No serialization/deserialization needed — format IS the wire format
- "80%+ of data access time is typically spent in serialization/deserialization" — Arrow eliminates this
- 64-byte aligned buffers enable direct SIMD processing after receipt

### 6.2 Arrow Flight
- gRPC streaming with Arrow batches as payload
- Platform and language independent
- Parallel RecordBatch transfer via multiple FlightEndpoints
- Flow control via gRPC backpressure
- Benchmarking paper: https://arxiv.org/abs/2204.03032

---

## 7. Depth Optimizations

### 7.1 SIMD Vectorization
- 64-byte buffer alignment matches AVX-512 register width
- Enables batch processing of entire columns with SIMD instructions
- Validity bitmap operations benefit from bitwise SIMD
- C++ kernels explicitly use SIMD; Java benefits implicitly through memory layout

### 7.2 Zero-Copy
- IPC format designed for zero-copy reads from mmap or shared memory
- No deserialization step — data in memory IS the Arrow format
- Flight: no ser/deser when crossing process boundaries
- ArrowBuf: off-heap memory accessible without JVM object overhead

### 7.3 Dictionary Encoding
- Repeated values stored as integer indices into a dictionary
- Enables fixed-width dense array processing on variable-width data
- Particularly effective for low-cardinality string columns
- Compute kernels can operate directly on dictionary-encoded data

### 7.4 Late Materialization
- Only decode/access columns actually needed by the query
- Parquet projection pushdown is the classic example
- Arrow's columnar layout enables column-level I/O

### 7.5 Buffer Pooling
- Netty-based allocator provides buffer pooling
- Child allocators provide isolation with shared pool
- Circuit breaker integration possible via allocation listeners

---

## 8. Arrow as Unified Multi-Source Framework

### 8.1 The Vision
Arrow serves as a universal in-memory format enabling zero-copy data interchange between:
- Different programming languages (same process)
- Different processes (IPC)
- Different machines (Flight)
- Different storage systems (Dataset API + filesystem abstraction)

### 8.2 Who Uses Arrow This Way
- **DuckDB**: Direct Arrow input/output, uses Arrow as exchange format
- **Polars**: Arrow as core memory model
- **Spark**: Arrow for Python interop (PySpark)
- **DataFusion**: Built entirely on Arrow (RecordBatch is the unit of work)
- **InfluxDB 3.0**: Built on DataFusion/Arrow

### 8.3 Limitations
- Java Dataset API (JNI to C++) is less mature than C++/Rust
- No pure-Java compute kernel library
- Memory management complexity (JNI boundary, off-heap tracking)
- Arrow Java community smaller than C++/Python/Rust

---

## 9. Roadmap & Direction

### 9.1 Project-Wide
- Language-specific repos for independent release cycles
- ADBC as modern JDBC/ODBC successor
- Flight SQL as query protocol standard
- Format evolution: RunEndEncoded, VariableSizeBinaryView
- nanoarrow for embedded/constrained environments
- 10th anniversary in Feb 2026

### 9.2 Java-Specific
- v18.3.0 released May 2025 (first from standalone repo was v18.2.0 Feb 2025)
- Discussion on v20.0.0 potentially decoupling versioning
- TimestampWithOffset canonical type for timezone handling
- Trend toward Unsafe as default allocator, simplifying Netty dependency
- Flight SQL JDBC driver increasingly production-ready
- No native Java compute kernels planned — compute from application or JNI

### 9.3 Rust Ecosystem (relevant context)
- arrow-rs and DataFusion seeing massive growth
- object_store crate becoming standard for cloud storage access
- Parquet reader/writer in pure Rust — no JNI needed
