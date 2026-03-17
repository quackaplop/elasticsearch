# Apache Arrow Java: Parquet Reading Architecture -- Comprehensive Report

## Table of Contents

1. [Module Structure](#1-module-structure)
2. [Class-by-Class Analysis of the Reader Pipeline](#2-class-by-class-analysis-of-the-reader-pipeline)
3. [Component Interaction Diagram](#3-component-interaction-diagram)
4. [Configuration and Extension Points](#4-configuration-and-extension-points)
5. [Pushdown Support](#5-pushdown-support)
6. [Memory Management](#6-memory-management)
7. [Performance-Relevant Observations](#7-performance-relevant-observations)
8. [The parquet-java / parquet-arrow Module (Apache Parquet Project)](#8-the-parquet-java--parquet-arrow-module)

---

## 1. Module Structure

### 1.1 Repository Overview

The Apache Arrow Java project lives at `https://github.com/apache/arrow-java` (split from the monorepo `apache/arrow` starting around v18). It is a Maven multi-module project. The latest release at time of writing is **v18.3.0**.

### 1.2 Complete Module Listing (Maven Artifacts)

The following Maven artifacts are published under `org.apache.arrow`:

| Artifact ID | Purpose |
|---|---|
| `arrow-java-root` | Parent POM |
| `arrow-format` | FlatBuffers format definitions (IPC schema, message envelopes) |
| `arrow-memory-core` | Memory management interfaces (`BufferAllocator`, `ArrowBuf`) |
| `arrow-memory-netty` | Memory implementation backed by Netty's `PooledByteBufAllocator` |
| `arrow-memory-unsafe` | Memory implementation backed by `sun.misc.Unsafe` |
| `arrow-memory-netty-buffer-patch` | Compatibility shim for Netty buffer API changes |
| `arrow-vector` | Core Arrow vector types, `VectorSchemaRoot`, `ArrowReader` |
| `arrow-algorithm` | Algorithms on Arrow vectors (search, sort, deduplicate) |
| **`arrow-dataset`** | **Dataset API -- the primary Parquet reader module** |
| **`arrow-c-data`** | **C Data Interface -- used by the Dataset module for JNI data transfer** |
| `arrow-avro` | Avro-to-Arrow converter |
| `arrow-compression` | Compression codec support (LZ4, ZSTD) |
| `arrow-jdbc` | JDBC ResultSet to Arrow converter |
| `arrow-tools` | CLI tools (JSON-to-Arrow, stream-to-file) |
| `arrow-bom` | Bill of Materials for dependency management |
| `arrow-performance` | Performance benchmarks |
| `flight-core` | Arrow Flight RPC core |
| `flight-grpc` | gRPC transport for Flight |
| `flight-sql` | Flight SQL protocol |
| `flight-sql-jdbc-core` | Flight SQL JDBC adapter core |
| `flight-sql-jdbc-driver` | Flight SQL JDBC driver |
| `flight-integration-tests` | Flight integration tests |
| `gandiva` | Gandiva expression compiler (JNI to C++ LLVM) |
| `orc` | ORC format adapter |

### 1.3 Parquet-Relevant Modules

Arrow Java does **not** contain its own pure-Java Parquet reader/writer. Instead, Parquet reading is achieved through **two distinct integration paths**:

#### Path A: `arrow-dataset` (Primary -- JNI to C++)

- **JAR**: `org.apache.arrow:arrow-dataset`
- **Packages**: `org.apache.arrow.dataset.file`, `org.apache.arrow.dataset.jni`, `org.apache.arrow.dataset.scanner`, `org.apache.arrow.dataset.source`, `org.apache.arrow.dataset.substrait`
- **Native library**: `arrow_dataset_jni` (compiled C++ shared library)
- **Mechanism**: Java classes delegate to the C++ Arrow Dataset library via JNI. The C++ side uses `arrow::dataset::FileSystemDataset` which in turn uses `parquet::arrow::FileReader` from the C++ Parquet implementation.
- **Dependencies**: `arrow-vector`, `arrow-memory-core`, `arrow-c-data`

#### Path B: `parquet-arrow` (Apache Parquet Java project)

- **JAR**: `org.apache.parquet:parquet-arrow` (separate project: `apache/parquet-java`)
- **Packages**: `org.apache.parquet.arrow.schema`
- **Mechanism**: Schema conversion utilities between Parquet and Arrow schemas. This module is part of the Apache Parquet project, not the Arrow project. It provides `SchemaConverter`, `SchemaMapping`, and `TypeMapping` classes.
- **Note**: This is primarily a schema-level bridge. The actual Parquet reading is done by `parquet-java`'s own `ParquetReader` infrastructure.

**For production Parquet reading in Arrow Java, Path A (`arrow-dataset`) is the recommended and performant approach.** It leverages the mature C++ Parquet implementation with full column/row group pruning, predicate pushdown, and high-performance I/O.

---

## 2. Class-by-Class Analysis of the Reader Pipeline

### 2.1 The Dataset Module (`arrow-dataset`)

#### 2.1.1 `FileFormat` (Enum)
- **Package**: `org.apache.arrow.dataset.file`
- **Purpose**: Defines supported file formats
- **Values**: `PARQUET`, `ORC`, `CSV`, `JSON`, `ARROW_IPC`
- **Usage**: Passed to `FileSystemDatasetFactory` to indicate the file type

#### 2.1.2 `FileSystemDatasetFactory`
- **Package**: `org.apache.arrow.dataset.file`
- **Purpose**: Factory for creating `Dataset` instances from files on a filesystem
- **Constructor**: Takes a `BufferAllocator`, `NativeMemoryPool`, file URI string, and `FileFormat`
- **Key methods**:
  - `inspect()` -- Returns the inferred `Schema` of the dataset (reads Parquet metadata without scanning data)
  - `finish()` / `finish(Schema)` -- Creates a `NativeDataset` from the factory, optionally with a user-specified schema
  - `close()` -- Releases the native DatasetFactory pointer via JNI
- **Internally**: Calls `JniWrapper.createFileSystemDatasetFactory(...)` which invokes native C++ code to construct an `arrow::dataset::FileSystemDatasetFactory`
- **Lifecycle**: Must be closed (implements `AutoCloseable`). The `BufferAllocator` passed to the constructor becomes the parent allocator for all data produced by this pipeline.

#### 2.1.3 `JniWrapper` (in `org.apache.arrow.dataset.file`)
- **Package**: `org.apache.arrow.dataset.file`
- **Purpose**: JNI bridge for filesystem-based Dataset operations
- **Key native methods**:
  - `createFileSystemDatasetFactory(String uri, int fileFormat, String[] fragmentScanOptions)` -- Creates a native DatasetFactory and returns its pointer
  - Other methods for dataset creation
- **Singleton access**: `JniWrapper.get()`
- **Design**: Loads the `arrow_dataset_jni` shared library via `System.loadLibrary`

#### 2.1.4 `JniWrapper` (in `org.apache.arrow.dataset.jni`)
- **Package**: `org.apache.arrow.dataset.jni`
- **Purpose**: JNI bridge for the core Dataset API (scanner operations)
- **Key native methods**:
  - `createScanner(long datasetId, String[] columns, ByteBuffer substraitProjection, ByteBuffer substraitFilter, long batchSize, int fileFormat, String[] serializedFragmentScanOptions, long memoryPoolId)` -- Creates a native Scanner
  - `getSchemaFromScanner(long scannerId)` -- Returns serialized schema from scanner
  - `nextRecordBatch(long scannerId, long arrowArrayAddress)` -- Fetches the next record batch via C Data Interface, returns boolean indicating availability
  - `closeDataset(long datasetId)` -- Releases native Dataset
  - `closeScanner(long scannerId)` -- Releases native Scanner
  - `releaseBuffer(long bufferAddress)` -- Releases a native buffer

#### 2.1.5 `NativeDatasetFactory`
- **Package**: `org.apache.arrow.dataset.jni`
- **Purpose**: Java binding for C++ `DatasetFactory`
- **Maps to**: C++ `arrow::dataset::DatasetFactory`
- **Key methods**:
  - `inspect()` -- Delegates to JNI to read the dataset schema
  - `finish(Schema)` -- Creates a `NativeDataset` with the given schema
- **Lifecycle**: Wraps a native pointer; `close()` calls `JniWrapper.closeDatasetFactory()`

#### 2.1.6 `NativeDataset`
- **Package**: `org.apache.arrow.dataset.jni`
- **Purpose**: Java binding for C++ `Dataset`
- **Maps to**: C++ `arrow::dataset::Dataset`
- **Key methods**:
  - `newScan(ScanOptions)` -- Creates a `NativeScanner`
  - `close()` -- Releases the native Dataset pointer
- **Holds**: A `NativeContext` containing the `BufferAllocator` and `NativeMemoryPool`

#### 2.1.7 `NativeScanner`
- **Package**: `org.apache.arrow.dataset.jni`
- **Purpose**: Java binding for C++ `Scanner` / `DisposableScannerAdaptor`
- **Maps to**: C++ `arrow::dataset::DisposableScannerAdaptor`
- **Key methods**:
  - `scanBatches()` -- Returns an `ArrowReader` (specifically a `NativeReader` inner class)
  - `schema()` -- Returns the schema from the native scanner
  - `close()` -- Uses a write lock, releases the native scanner pointer
- **Thread safety**: Uses `ReentrantReadWriteLock` for concurrent access control
- **Inner class**: `NativeReader` (see below)

#### 2.1.8 `NativeScanner.NativeReader` (Inner Class)
- **Extends**: `org.apache.arrow.vector.ipc.ArrowReader`
- **Purpose**: The actual class that delivers Arrow record batches from the C++ Parquet reader to Java
- **Critical method -- `loadNextBatch()`**:
  1. Acquires a read lock on the scanner
  2. Allocates an `ArrowArray` (C Data Interface struct)
  3. Calls `JniWrapper.get().nextRecordBatch(scannerId, arrowArray.memoryAddress())`
  4. If the native side populated the array: calls `Data.importIntoVectorSchemaRoot(allocator, arrowArray, vectorSchemaRoot, this)` to import the C Data Interface buffers into the Java `VectorSchemaRoot`
  5. Returns `true` if a batch was loaded, `false` if no more data
- **`loadRecordBatch()` / `loadDictionary()`**: Throw `UnsupportedOperationException` -- all batch loading goes through the JNI C Data Interface path
- **Design implication**: Each batch is transferred from C++ to Java via the **Arrow C Data Interface**, which enables near-zero-copy transfer (the C++ side fills an `ArrowArray` struct, the Java side consumes the buffer pointers)

#### 2.1.9 `ScanOptions`
- **Package**: `org.apache.arrow.dataset.scanner`
- **Purpose**: Configures the scan operation
- **Builder pattern**: `ScanOptions.Builder(long batchSize)`
- **Configuration parameters**:
  - `batchSize` (long) -- Maximum number of rows per `ArrowRecordBatch`. The scanner will split large batches to stay under this limit, but will NOT combine smaller batches to reach it. Common default: `32768`.
  - `columns` (Optional<String[]>) -- Projected column names. `Optional.empty()` means scan all columns. Enables **projection pushdown**.
  - `substraitFilter` (Optional<ByteBuffer>) -- A serialized Substrait `ExtendedExpression` for filter pushdown. Enables **predicate pushdown** down to the C++ Parquet reader (row group statistics, page index).
  - `substraitProjection` (Optional<ByteBuffer>) -- A serialized Substrait expression for computed projections.
- **Key method**: `builder.build()` returns an immutable `ScanOptions`

#### 2.1.10 `Scanner` (Interface)
- **Package**: `org.apache.arrow.dataset.scanner`
- **Purpose**: Interface for dataset scanning
- **Key methods**:
  - `scanBatches()` -- Returns an `ArrowReader`
  - `schema()` -- Returns the scan schema

#### 2.1.11 `NativeMemoryPool`
- **Package**: `org.apache.arrow.dataset.jni`
- **Purpose**: Wrapper for C++ `arrow::MemoryPool`, used for off-heap memory accounting on the native side
- **Key methods**:
  - `getDefaultMemoryPool()` -- Returns the default pool (no-op accounting, suitable for testing)
  - `createListenableMemoryPool()` -- Returns a pool that tracks allocations (recommended for production)
  - `getBytesAllocated()` -- Returns current native memory usage
- **Design note**: This manages memory on the **C++ side** of the JNI boundary. The Java-side `BufferAllocator` manages Java Arrow buffers separately.

#### 2.1.12 `NativeContext`
- **Package**: `org.apache.arrow.dataset.jni`
- **Purpose**: Holds the context for native operations (allocator + memory pool pair)
- **Fields**: `BufferAllocator allocator`, `NativeMemoryPool memoryPool`

### 2.2 The C Data Interface Module (`arrow-c-data`)

#### 2.2.1 `ArrowArray`
- **Package**: `org.apache.arrow.c`
- **Purpose**: Java representation of the C `ArrowArray` struct
- **Key methods**:
  - `allocateNew(BufferAllocator)` -- Allocates memory for the struct
  - `memoryAddress()` -- Returns the native memory address (passed to JNI)
  - `wrap(long address)` -- Wraps an existing native address
- **Role in reader pipeline**: The JNI layer fills this struct with buffer pointers from C++; the Java side then imports the data.

#### 2.2.2 `ArrowSchema`
- **Package**: `org.apache.arrow.c`
- **Purpose**: Java representation of the C `ArrowSchema` struct
- **Role**: Describes the schema/type of data being transferred via C Data Interface

#### 2.2.3 `Data`
- **Package**: `org.apache.arrow.c`
- **Purpose**: Static utility class for import/export via C Data Interface
- **Critical method**: `Data.importIntoVectorSchemaRoot(BufferAllocator, ArrowArray, VectorSchemaRoot, DictionaryProvider)` -- Imports an `ArrowArray` into a `VectorSchemaRoot`, transferring buffer ownership from C++ to Java.
- **Other methods**: `importVector()`, `exportVector()`, `importSchema()`, `exportSchema()`

### 2.3 Core Vector Module (`arrow-vector`)

#### 2.3.1 `ArrowReader` (Abstract Class)
- **Package**: `org.apache.arrow.vector.ipc`
- **Purpose**: Abstract base class for all Arrow readers
- **Key methods**:
  - `loadNextBatch()` -- Template method: loads next batch into `VectorSchemaRoot`
  - `getVectorSchemaRoot()` -- Returns the root containing vectors for the current batch
  - `readSchema()` -- Reads schema; called during initialization
  - `bytesRead()` -- Returns bytes read
  - `close()` -- Releases resources
- **Subclasses relevant to Parquet**: `NativeScanner.NativeReader`

#### 2.3.2 `VectorSchemaRoot`
- **Package**: `org.apache.arrow.vector`
- **Purpose**: Container for a batch of columnar data with a schema
- **Design**: Holds a list of `FieldVector` instances plus a row count
- **Lifecycle**: Reused across `loadNextBatch()` calls -- vectors are cleared and refilled

### 2.4 The Substrait Module (`org.apache.arrow.dataset.substrait`)

#### 2.4.1 `AceroSubstraitConsumer`
- **Package**: `org.apache.arrow.dataset.substrait`
- **Purpose**: Executes Substrait plans using the Acero engine (C++ execution engine)
- **Supported operations**: Consume Substrait plans in JSON or binary format
- **Relation to Parquet**: Can be used for complex query execution over Parquet datasets with full expression pushdown

---

## 3. Component Interaction Diagram

```
User Code
    |
    v
FileSystemDatasetFactory  --------> JniWrapper (file)
    |                                    |
    | .finish(schema)                    | createFileSystemDatasetFactory()
    v                                    | [JNI -> C++]
NativeDatasetFactory                     v
    |                           C++ arrow::dataset::FileSystemDatasetFactory
    | .finish()                          |
    v                                    | .Finish()
NativeDataset  ------------------> JniWrapper (jni)
    |                                    |
    | .newScan(ScanOptions)              | createScanner(columns, filter, projection, batchSize)
    v                                    | [JNI -> C++]
NativeScanner                            v
    |                           C++ arrow::dataset::Scanner
    | .scanBatches()                     |   (internally uses parquet::arrow::FileReader)
    v                                    |   (applies filter pushdown via row group statistics)
NativeReader (ArrowReader)               |   (applies column projection)
    |                                    |
    | .loadNextBatch()                   |
    |   1. ArrowArray.allocateNew()      |
    |   2. JniWrapper.nextRecordBatch()  | nextRecordBatch(scannerId, arrowArrayAddr)
    |      [JNI -> C++]                  |   [C++ fills ArrowArray via C Data Interface]
    |   3. Data.importIntoVectorSchemaRoot()
    |      [C Data Interface import]     |
    v                                    v
VectorSchemaRoot                    C++ ArrowArray struct
    |                               (buffer pointers to columnar data)
    | (contains FieldVectors)
    v
User processes batch data
    |
    | .loadNextBatch() again (loop)
    v
false -> done
```

### Data Flow Summary

1. **Factory creation**: `FileSystemDatasetFactory` is constructed with a file URI and `FileFormat.PARQUET`. The JNI layer creates a C++ `FileSystemDatasetFactory` that reads Parquet metadata (footer, schema).

2. **Schema inspection**: `inspect()` reads the Parquet file metadata and returns the Arrow schema without reading any data.

3. **Dataset creation**: `finish(schema)` creates a `NativeDataset` (C++ `Dataset`).

4. **Scanner creation**: `newScan(ScanOptions)` creates a `NativeScanner` (C++ `Scanner`) configured with batch size, column projection, and optional Substrait filter/projection expressions.

5. **Batch reading**: `scanBatches()` returns a `NativeReader`. Each call to `loadNextBatch()`:
   - Allocates a C `ArrowArray` struct in Java
   - Calls into C++ via JNI to get the next record batch
   - C++ reads from Parquet (decompressing, decoding columns), fills the `ArrowArray`
   - Java imports the `ArrowArray` into the `VectorSchemaRoot` via `Data.importIntoVectorSchemaRoot()`
   - The `VectorSchemaRoot` is reused; previous batch data is released

6. **Cleanup**: All native resources must be explicitly closed (try-with-resources).

---

## 4. Configuration and Extension Points

### 4.1 ScanOptions Configuration

| Parameter | Type | Description |
|---|---|---|
| `batchSize` | `long` | Max rows per batch. Scanner splits (never combines) to honor this. Default: typically 32768. |
| `columns` | `Optional<String[]>` | Column names to project. Empty = all columns. |
| `substraitFilter` | `Optional<ByteBuffer>` | Serialized Substrait expression for predicate pushdown. |
| `substraitProjection` | `Optional<ByteBuffer>` | Serialized Substrait expression for computed columns. |

### 4.2 FileSystemDatasetFactory Configuration

- **File URI**: Supports local files (`file:///path/to/data.parquet`), S3 (`s3://bucket/key`), HDFS (`hdfs://...`), and other URIs depending on the C++ build configuration.
- **FileFormat**: Enum selection (`FileFormat.PARQUET`).
- **Fragment scan options**: Serialized options passed as string arrays for format-specific configuration.

### 4.3 Memory Configuration

- **BufferAllocator**: Controls Java-side memory allocation. Pass a `RootAllocator` with a memory limit to bound total memory usage. Child allocators can be created for isolation.
- **NativeMemoryPool**: Controls C++ side memory tracking. Use `NativeMemoryPool.createListenableMemoryPool()` for production to monitor native allocations.

### 4.4 Extension Points

1. **Custom Substrait expressions**: Users can construct arbitrary Substrait filter and projection expressions, including using `io.substrait.isthmus.SqlExpressionToSubstrait` to convert SQL expressions.

2. **Custom file systems**: The C++ backend supports pluggable filesystem implementations. S3 and HDFS are available when the native library is built with those features enabled.

3. **ArrowReader subclassing**: While `NativeReader` is an inner class, the `ArrowReader` interface is public and can be extended for custom data sources.

4. **Compression codecs**: Parquet compression (Snappy, ZSTD, LZ4, GZIP, Brotli) is handled by the C++ layer. The `arrow-compression` module provides Java-side codec support for IPC format.

---

## 5. Pushdown Support

### 5.1 Projection Pushdown

**Supported**: Yes, via `ScanOptions.Builder.columns(String[])`.

**How it works**:
1. User specifies column names in `ScanOptions`.
2. The column list is passed via JNI to the C++ `Scanner`.
3. C++ `Scanner` configures `parquet::arrow::FileReader` to only read the specified columns.
4. At the Parquet level, only the column chunks for requested columns are read from disk (I/O savings) and decoded.
5. The resulting `ArrowArray` contains only the projected columns.

**Implementation**: Direct column name filtering. The C++ layer maps column names to Parquet column indices and skips all other columns during I/O and decoding.

### 5.2 Predicate Pushdown (Filter Pushdown)

**Supported**: Yes, via `ScanOptions.Builder.substraitFilter(ByteBuffer)`.

**How it works**:
1. User constructs a Substrait `ExtendedExpression` representing the filter predicate.
2. The serialized expression is passed via JNI to the C++ `Scanner`.
3. C++ `Scanner` applies the filter at multiple levels:
   - **Row group level**: Uses Parquet row group statistics (min/max values per column) to skip entire row groups that cannot match the predicate.
   - **Page level**: If page index is available, uses page-level statistics for finer-grained skipping.
   - **Post-scan filtering**: Applies the filter to decoded batches as a final pass.
4. Only matching rows (and their containing row groups/pages) are returned.

**Expression construction example**:
```java
// Using Substrait Isthmus to convert SQL to Substrait
SqlExpressionToSubstrait converter = new SqlExpressionToSubstrait();
ExtendedExpression expression = converter.convert("N_NATIONKEY > 18", schema);
ByteBuffer filter = ByteBuffer.wrap(expression.toByteArray());

ScanOptions options = new ScanOptions.Builder(32768)
    .substraitFilter(filter)
    .build();
```

**Limitations**:
- Filter expressions must be expressible as Substrait `ExtendedExpression`.
- The effectiveness depends on Parquet file statistics quality (whether min/max stats are present, sorted data, etc.).
- Complex expressions may not fully push down.

### 5.3 Computed Projection Pushdown

**Supported**: Yes, via `ScanOptions.Builder.substraitProjection(ByteBuffer)`.

**How it works**: Similar to filter pushdown but for computed columns. Users can specify new columns derived from expressions (e.g., string concatenation, arithmetic) that are computed during scanning.

### 5.4 Partition Pruning

**Supported**: Yes (C++ layer).

When using partitioned datasets (e.g., Hive-style partitioning like `year=2024/month=01/data.parquet`), the C++ `FileSystemDataset` can use partition keys to prune entire files/directories before any data is read.

---

## 6. Memory Management

### 6.1 Dual Memory Model

Arrow Java's Dataset Parquet reader operates with **two separate memory domains**:

#### Java-Side Memory (BufferAllocator)
- Managed by Arrow's `BufferAllocator` tree (typically `RootAllocator` -> child allocators)
- Tracks all Java Arrow buffer allocations (off-JVM-heap, using Netty or Unsafe)
- The `BufferAllocator` passed to `FileSystemDatasetFactory` becomes the parent for all buffers created by the reader
- Supports memory limits: allocations that exceed the allocator's limit throw `OutOfMemoryException`
- **Accounting hierarchy**: Child allocator usage rolls up to parent; `RootAllocator` is the global limit

#### C++ Side Memory (NativeMemoryPool)
- Managed by Arrow C++'s `arrow::MemoryPool`
- Tracks native heap allocations made by the C++ Parquet reader (decompression buffers, decode buffers, etc.)
- `NativeMemoryPool.getDefaultMemoryPool()`: No-op tracking (fine for testing)
- `NativeMemoryPool.createListenableMemoryPool()`: Active tracking with `getBytesAllocated()` (recommended for production)

### 6.2 Buffer Lifecycle During Batch Reading

1. **C++ allocates**: The C++ Parquet reader reads column chunks, decompresses them, and decodes them into Arrow format buffers in C++ memory.
2. **C Data Interface transfer**: The `ArrowArray` struct is populated with pointers to these C++ buffers.
3. **Java import**: `Data.importIntoVectorSchemaRoot()` imports the buffers. The ownership transfers: the Java `BufferAllocator` now owns the buffers, and the C++ side releases its references.
4. **Batch reuse**: On the next `loadNextBatch()`, the previous batch's buffers in the `VectorSchemaRoot` are released before importing new data.
5. **Explicit cleanup**: The user must close the `ArrowReader`, `Scanner`, `Dataset`, and `DatasetFactory` (in reverse order or via try-with-resources).

### 6.3 Memory Layout

- All Arrow data is stored **off the JVM heap** (direct memory)
- This avoids GC pressure for large datasets
- Uses Netty's pooled allocator (`arrow-memory-netty`) or Unsafe-based allocation (`arrow-memory-unsafe`)
- Buffer sizes are tracked precisely; no hidden overhead from Java object headers

### 6.4 Resource Leak Prevention

All native-backed objects implement `AutoCloseable`:
```java
try (
    BufferAllocator allocator = new RootAllocator();
    DatasetFactory factory = new FileSystemDatasetFactory(
        allocator, NativeMemoryPool.getDefaultMemoryPool(),
        FileFormat.PARQUET, uri);
    Dataset dataset = factory.finish();
    Scanner scanner = dataset.newScan(scanOptions);
    ArrowReader reader = scanner.scanBatches()
) {
    while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        // process batch
    }
}
```

Failure to close resources results in native memory leaks (C++ objects not freed) and Java memory leaks (Arrow buffers not released).

---

## 7. Performance-Relevant Observations

### 7.1 JNI Bridge Overhead

- The JNI boundary is crossed once per batch (in `loadNextBatch()`), not once per row or per column.
- The C Data Interface minimizes data copying: buffer pointers are passed, not buffer contents.
- The cost of JNI is amortized over potentially thousands of rows per batch.

### 7.2 Batch Size Tuning

- `ScanOptions.batchSize` controls the maximum rows per batch.
- **Larger batches**: Better throughput (fewer JNI calls, better vectorized processing), but more memory per batch.
- **Smaller batches**: Lower memory footprint, better latency for streaming, more JNI overhead.
- The scanner **splits** large batches but does **not combine** small ones. This means if a Parquet row group has 100 rows and batchSize is 32768, you get a batch of 100 rows, not padded to 32768.
- Typical default: `32768` rows.

### 7.3 Columnar I/O Efficiency

- Projection pushdown eliminates I/O for unneeded columns at the Parquet level.
- The C++ Parquet reader reads column chunks independently, enabling efficient selective reads.
- For wide tables with many columns, specifying only needed columns via `ScanOptions.columns()` can provide dramatic I/O reduction.

### 7.4 Predicate Pushdown Efficiency

- Row group statistics (min/max) enable skipping entire row groups (typically 128MB of uncompressed data).
- For sorted or clustered data, predicate pushdown can eliminate 90%+ of I/O.
- Page-level statistics (Parquet page index) enable even finer-grained skipping within row groups.

### 7.5 Threading Model

- The C++ Scanner supports parallel scanning with threads from CPU and I/O executors.
- **Readahead**: The C++ layer can prefetch fragments (row groups / files) ahead of consumption. Higher readahead improves I/O pipeline utilization but increases memory usage.
- The Java API (`scanBatches()` returning an `ArrowReader`) presents a **sequential interface** -- `loadNextBatch()` is called in a loop. However, the C++ backend may perform I/O and decoding in parallel behind the scenes.
- The `NativeScanner` uses `ReentrantReadWriteLock` for thread safety.

### 7.6 Off-Heap Memory Benefits

- No GC pressure from data buffers (all off-heap via Netty/Unsafe).
- Enables processing datasets larger than the Java heap.
- The C Data Interface transfer avoids serialization/deserialization entirely.

### 7.7 Zero-Copy Considerations

- The C Data Interface achieves **near-zero-copy** transfer from C++ to Java: the Java side wraps the same memory buffers that C++ produced, rather than copying data.
- After import, the Java `BufferAllocator` takes ownership and will free the memory when the batch is released.
- This is a key performance advantage over alternatives that would serialize/deserialize Parquet data through a Java Parquet library.

### 7.8 Memory Fragmentation

- Batches are allocated and freed in a pattern that could cause memory fragmentation.
- Netty's pooled allocator mitigates this with slab allocation and memory pooling.
- The `VectorSchemaRoot` is reused across batches, but the underlying buffers are reallocated (since C++ produces new buffers each time).

### 7.9 Known Issues and Limitations

- **Memory tracking gap**: The NativeMemoryPool tracks C++ allocations, and BufferAllocator tracks Java allocations, but there can be a brief period during `importIntoVectorSchemaRoot` where both sides hold references.
- **Reading multiple files**: When scanning datasets with many Parquet files, memory usage can spike if readahead is high ([Issue #13949](https://github.com/apache/arrow/issues/13949)).
- **JVM crashes**: Incorrect lifecycle management (closing objects in wrong order, using after close) can cause JVM crashes since native pointers become dangling ([Issue #13018](https://github.com/apache/arrow/issues/13018)).
- **S3 support**: Requires the native library to be built with S3 filesystem support enabled ([Issue #13110](https://github.com/apache/arrow/issues/13110)).
- **No pure-Java fallback**: If the native library (`arrow_dataset_jni`) is not available, there is no pure-Java alternative for Parquet reading within the Arrow Java project itself.

---

## 8. The parquet-java / parquet-arrow Module

### 8.1 Module Overview

The `parquet-arrow` module is part of the **Apache Parquet Java** project (`apache/parquet-java`), not the Arrow Java project. It provides schema conversion between Parquet and Arrow type systems.

**Maven coordinates**: `org.apache.parquet:parquet-arrow` (latest: 1.16.0+)

### 8.2 Key Classes

#### `SchemaConverter`
- **Package**: `org.apache.parquet.arrow.schema`
- **Source**: `parquet-java/parquet-arrow/src/main/java/org/apache/parquet/arrow/schema/SchemaConverter.java`
- **Purpose**: Bidirectional conversion between Arrow schemas and Parquet message types
- **Key methods**:
  - `fromParquet(MessageType)` -- Converts Parquet schema to Arrow schema + `SchemaMapping`
  - `fromArrow(Schema)` -- Converts Arrow schema to Parquet schema + `SchemaMapping`
- **Type handling**: Maps Parquet primitive types (INT32, INT64, BINARY, FLOAT, DOUBLE, BOOLEAN) and logical types (STRING, DATE, TIMESTAMP, DECIMAL, LIST, MAP) to corresponding Arrow types

#### `SchemaMapping`
- **Package**: `org.apache.parquet.arrow.schema`
- **Source**: `parquet-java/parquet-arrow/src/main/java/org/apache/parquet/arrow/schema/SchemaMapping.java`
- **Purpose**: Holds the bidirectional mapping between an Arrow schema and a Parquet schema
- **Fields**: `arrowSchema`, `parquetSchema`, `children` (list of `TypeMapping`)
- **Methods**: `getArrowSchema()`, `getParquetSchema()`, `getChildren()`

#### `TypeMapping` (and subclasses)
- **Purpose**: Abstract base for individual field-level type mappings
- **Subclasses**:
  - `PrimitiveTypeMapping` -- Maps primitive Arrow types to Parquet primitives
  - `StructTypeMapping` -- Maps Arrow struct types to Parquet groups
  - `ListTypeMapping` -- Maps Arrow list types to Parquet repeated groups
  - `MapTypeMapping` -- Maps Arrow map types to Parquet MAP logical type (three-level encoding)
  - `UnionTypeMapping` -- Maps Arrow union types
  - `RepeatedTypeMapping` -- Handles Parquet repeated fields

### 8.3 Relationship to Arrow Java

- `parquet-arrow` depends on `org.apache.arrow:arrow-vector` for Arrow type definitions
- It does **not** depend on `arrow-dataset` or provide any reading/writing functionality itself
- It is a **schema bridge** -- useful if you are building your own Parquet-to-Arrow pipeline using `parquet-java`'s `ParquetReader` with custom `ReadSupport` implementations
- The Arrow Java `arrow-dataset` module does NOT use `parquet-arrow` -- it delegates to the C++ implementation which has its own schema conversion

---

## Summary

The Apache Arrow Java Parquet reading architecture is a **JNI-bridged system** where:

1. **Java provides the API surface**: `FileSystemDatasetFactory`, `ScanOptions`, `Scanner`, `ArrowReader`, `VectorSchemaRoot`
2. **C++ provides the execution engine**: Parquet file I/O, metadata parsing, column decoding, decompression, predicate/projection pushdown, row group/page pruning
3. **The C Data Interface bridges the gap**: Record batches flow from C++ to Java via `ArrowArray` structs with near-zero-copy semantics
4. **Memory is managed in two domains**: Java `BufferAllocator` (off-heap, Netty/Unsafe) and C++ `NativeMemoryPool`
5. **Pushdowns are expressed via Substrait**: Filter and projection expressions are serialized as Substrait `ExtendedExpression` messages

This design gives Arrow Java the full performance of the battle-tested C++ Parquet implementation while providing a clean Java API. The main trade-off is the dependency on a platform-specific native library (`arrow_dataset_jni`), which must be compiled for each target OS/architecture.

---

## Sources

- [Apache Arrow Java GitHub Repository](https://github.com/apache/arrow-java)
- [Apache Arrow Java Dataset Documentation](https://arrow.apache.org/docs/java/dataset.html)
- [Apache Arrow Java Cookbook -- Dataset](https://arrow.apache.org/cookbook/java/dataset.html)
- [Arrow Java Memory Management Documentation](https://arrow.apache.org/docs/java/memory.html)
- [Arrow Java C Data Interface Documentation](https://arrow.apache.org/docs/java/cdata.html)
- [ArrowReader JavaDoc (v19-SNAPSHOT)](https://arrow.apache.org/java/main/reference/org.apache.arrow.vector/org/apache/arrow/vector/ipc/ArrowReader.html)
- [ScanOptions JavaDoc (v19-SNAPSHOT)](https://arrow.apache.org/java/main/reference/org.apache.arrow.dataset/org/apache/arrow/dataset/scanner/ScanOptions.html)
- [JniWrapper JavaDoc (v18)](https://arrow.apache.org/java/main/reference/org.apache.arrow.dataset/org/apache/arrow/dataset/file/JniWrapper.html)
- [NativeScanner.java Source (Fossies)](https://fossies.org/linux/apache-arrow/java/dataset/src/main/java/org/apache/arrow/dataset/jni/NativeScanner.java)
- [arrow-dataset JavaDoc (v18.3.0)](https://javadoc.io/doc/org.apache.arrow/arrow-dataset/latest/index.html)
- [Apache Parquet Java -- parquet-arrow SchemaConverter](https://github.com/apache/parquet-java/blob/master/parquet-arrow/src/main/java/org/apache/parquet/arrow/schema/SchemaConverter.java)
- [Apache Parquet Java -- parquet-arrow SchemaMapping](https://github.com/apache/parquet-java/blob/master/parquet-arrow/src/main/java/org/apache/parquet/arrow/schema/SchemaMapping.java)
- [Maven Central -- Arrow Artifacts](https://repo.maven.apache.org/maven2/org/apache/arrow/)
- [Maven Central -- parquet-arrow](https://mvnrepository.com/artifact/org.apache.parquet/parquet-arrow)
- [Push-down filtering in Java (Issue #14782)](https://github.com/apache/arrow/issues/14782)
- [Substrait Filter/Projection Simplification (Issue #40055)](https://github.com/apache/arrow/issues/40055)
- [Java Dataset API by JNI to C++ (PR #7030)](https://github.com/apache/arrow/pull/7030)
- [Arrow Java Building Documentation](https://arrow.apache.org/docs/developers/java/building.html)
- [Apache Arrow Java 18.3.0 Release](https://arrow.apache.org/blog/2025/05/13/arrow-java-18.3.0/)
