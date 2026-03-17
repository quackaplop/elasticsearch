# Apache Parquet-Java (formerly parquet-mr) Deep Dive Analysis

## 1. Architecture and Module Structure

### 1.1 Module / JAR Organization

Apache parquet-java (repo: [apache/parquet-java](https://github.com/apache/parquet-java)) is a multi-module Maven project. The key modules are:

| Module | Artifact ID | Purpose |
|--------|-------------|---------|
| **parquet-common** | `parquet-common` | Shared utilities, base interfaces |
| **parquet-encoding** | `parquet-encoding` | Core encoding/decoding: RLE, DELTA_BINARY_PACKED, DELTA_LENGTH_BYTE_ARRAY, DELTA_BYTE_ARRAY, PLAIN, PLAIN_DICTIONARY |
| **parquet-column** | `parquet-column` | Column-level abstractions: `ColumnReader`, `ColumnWriter`, `MessageColumnIO`, `RecordMaterializer`, `FilterPredicate` API, `ColumnIndex`/`OffsetIndex` |
| **parquet-format-structures** | `parquet-format-structures` | Thrift-generated Java classes from the [parquet-format](https://github.com/apache/parquet-format) spec |
| **parquet-hadoop** | `parquet-hadoop` | The main "batteries-included" module: `ParquetFileReader`, `ParquetFileWriter`, `ParquetReader<T>`, `ParquetReadOptions`, `HadoopReadOptions`, `CodecFactory`, `InternalParquetRecordReader`, `ColumnChunkPageReadStore` |
| **parquet-hadoop-bundle** | `parquet-hadoop-bundle` | Fat/shaded JAR that bundles all `parquet-*` modules into a single artifact using maven-shade-plugin. Used to avoid classpath conflicts. ([Maven Central](https://mvnrepository.com/artifact/org.apache.parquet/parquet-hadoop-bundle)) |
| **parquet-arrow** | `parquet-arrow` | Arrow schema conversion utilities (`SchemaConverter`, `SchemaMapping`). Does NOT provide a full Arrow-based vectorized reader. |
| **parquet-avro** | `parquet-avro` | Avro read/write support |
| **parquet-protobuf** | `parquet-protobuf` | Protobuf read/write support |
| **parquet-thrift** | `parquet-thrift` | Thrift read/write support |
| **parquet-jackson** | `parquet-jackson` | JSON metadata serialization |
| **parquet-cli** | `parquet-cli` | Command-line tools |

**Source**: [parquet-java README](https://github.com/apache/parquet-java/blob/master/README.md), [Maven Central](https://mvnrepository.com/artifact/org.apache.parquet/parquet-hadoop-bundle)

### 1.2 The Read Pipeline: File -> Footer -> Row Groups -> Column Chunks -> Pages -> Values

The read pipeline in parquet-java follows a strict hierarchical decomposition:

```
Parquet File
  |
  +-- Footer (file metadata, Thrift-encoded)
  |     |
  |     +-- FileMetaData
  |     |     +-- MessageType schema (the Parquet schema)
  |     |     +-- Key-Value metadata
  |     |     +-- Created-by string
  |     |
  |     +-- List<BlockMetaData> (row group metadata)
  |           +-- Per-column statistics (min/max/null_count)
  |           +-- Column chunk offsets + sizes
  |           +-- Optional: ColumnIndex, OffsetIndex, BloomFilter metadata
  |
  +-- Row Group 0
  |     +-- Column Chunk 0 (column "id")
  |     |     +-- Optional: Dictionary Page
  |     |     +-- Data Page 0
  |     |     +-- Data Page 1
  |     |     +-- ...
  |     +-- Column Chunk 1 (column "name")
  |     |     +-- ...
  |     +-- ...
  |
  +-- Row Group 1
  |     +-- ...
  +-- ...
```

**Read flow** (from [DeepWiki: Reading Parquet Files](https://deepwiki.com/apache/parquet-java/4.2-reading-parquet-files) and [ParquetFileReader source](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetFileReader.java)):

1. **Open file, read footer**: `ParquetFileReader.open(InputFile, ParquetReadOptions)` reads the 8-byte footer length from the end of the file, then reads and deserializes the Thrift-encoded footer metadata. This gives `FileMetaData` containing the schema and all row group metadata.

2. **Row group pruning** (statistics-based): Before reading any data, row groups can be pruned using:
   - **Statistics filter** (`useStatsFilter`): Uses min/max statistics from `BlockMetaData` per column
   - **Dictionary filter** (`useDictionaryFilter`): Reads dictionary pages to check if any values can match
   - **Bloom filter** (`useBloomFilter`): Reads bloom filter data from the file to check membership
   - These are controlled by `ParquetReadOptions` flags

3. **Read row group**: `readNextRowGroup()` returns a `PageReadStore` (specifically a `ColumnChunkPageReadStore`), which holds all column chunks for that row group. Each column chunk's bytes are read from disk and decompressed.

4. **Read column chunks / pages**: Within a `PageReadStore`, each column is accessed as a `PageReader` which yields individual pages (dictionary pages, data pages v1, data pages v2). Pages are the unit of compression and encoding.

5. **Decode values**: Each page contains encoded values using the column's encoding (PLAIN, RLE, DELTA_BINARY_PACKED, etc.). The `ColumnReader` decodes values from pages, yielding individual typed values. Definition levels and repetition levels handle nested/optional fields.

6. **Record materialization**: A `RecordMaterializer` (or its subclass `GroupRecordConverter`) assembles decoded column values back into records (row objects). The `ColumnIOFactory` creates a `MessageColumnIO` that orchestrates reading across columns and drives the converter.

**Source**: [ParquetFileReader.java](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetFileReader.java), [DeepWiki](https://deepwiki.com/apache/parquet-java/4.2-reading-parquet-files)

### 1.3 Key Reader API Classes

| Class | Module | Role |
|-------|--------|------|
| `ParquetReader<T>` | parquet-hadoop | High-level builder-pattern reader. Wraps `ParquetFileReader` + `InternalParquetRecordReader`. Typed by record materializer. |
| `ParquetFileReader` | parquet-hadoop | Low-level file reader. Opens file, reads metadata, provides `readNextRowGroup()` / `readNextFilteredRowGroup()`. Main class for direct API usage. |
| `InternalParquetRecordReader<T>` | parquet-hadoop | Bridge between `ParquetFileReader` (column-level) and `RecordMaterializer<T>` (record-level). Handles batching, filtering, record assembly. |
| `InputFile` | parquet-common (io package) | Abstraction for a readable file. Must provide `getLength()` and `newStream()` returning `SeekableInputStream`. |
| `SeekableInputStream` | parquet-common (io package) | Abstract input stream with `seek(pos)`, `getPos()`, `readFully()`. |
| `PageReadStore` | parquet-column | Interface for a row group's worth of column data. Provides `PageReader` per column. |
| `ColumnChunkPageReadStore` | parquet-hadoop | Implementation of `PageReadStore`. Stores decompressed pages for all columns in a row group. |
| `ParquetReadOptions` | parquet-hadoop | Configuration: filter flags, codec factory, allocator, max allocation size, metadata filter, record filter. |
| `HadoopReadOptions` | parquet-hadoop | Extends `ParquetReadOptions` with Hadoop `Configuration`-based settings. |
| `CodecFactory` | parquet-hadoop | Manages compression/decompression codecs (SNAPPY, GZIP, ZSTD, LZ4, etc.). |

**Source**: [ParquetFileReader.java](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ParquetFileReader.java), [ParquetReadOptions.java](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/ParquetReadOptions.java)


## 2. Pushdown Capabilities

### 2.1 Predicate Pushdown (Filter API)

Parquet-java has two filter APIs:

- **Old API** (`UnboundRecordFilter`): Record-level filtering only. Deprecated in favor of the new API.
- **New API** (`FilterPredicate`, package `org.apache.parquet.filter2.predicate`): Supports both row-group-level pruning and record-level filtering.

The new API provides:
- `FilterApi.eq(column, value)`, `lt`, `gt`, `ltEq`, `gtEq`, `notEq`, `and`, `or`, `not`, `userDefined`
- `FilterCompat.get(FilterPredicate)` wraps into a common `Filter` interface
- Row-group statistics pruning: the filter is evaluated against min/max statistics in `BlockMetaData` to skip entire row groups
- Record-level filtering: after decoding, individual records can be filtered

**Source**: [FilterCompat.java](https://github.com/apache/parquet-java/blob/master/parquet-column/src/main/java/org/apache/parquet/filter2/compat/FilterCompat.java), [Cloudera Predicate Pushdown Docs](https://docs-archive.cloudera.com/documentation/enterprise/6/6.3/topics/cdh_ig_predicate_pushdown_parquet.html)

### 2.2 Projection (Schema Projection)

Column projection is handled via `MessageType` schema pruning:

- `requestedSchema` is a subset of the file's `MessageType` containing only the columns needed
- `ParquetFileReader` only reads column chunks for columns in the requested schema
- The `ColumnIOFactory.getColumnIO(requestedSchema, fileSchema)` handles the mapping between projected and full schemas
- Unrequested columns are never read from disk or decompressed

This is the most impactful optimization -- for wide tables, projecting a few columns avoids reading the vast majority of data.

**Source**: [parquet-hadoop README](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/README.md)

### 2.3 Bloom Filter Support

Bloom filters in Parquet provide set-membership testing for equality predicates on high-cardinality columns. Key characteristics:

- **Format**: Split Block Bloom Filter (SBBF), stored in the file near the footer
- **Usage**: When `useDictionaryFilter` is insufficient (dictionary too large or not present), bloom filters can answer "definitely not in this row group" without reading data
- **API**: Enabled via `ParquetReadOptions.builder().useBloomFilter(true)`
- **Granularity**: Per column chunk (one bloom filter per column per row group)
- **False positive rate**: Configurable at write time; no false negatives

**Source**: [Parquet Bloom Filter spec](https://parquet.apache.org/docs/file-format/bloomfilter/), [parquet-format BloomFilter.md](https://github.com/apache/parquet-format/blob/master/BloomFilter.md)

### 2.4 Page-Level Statistics (Column Index / Offset Index)

The Page Index feature (added in Parquet 1.11+) enables page-level predicate pushdown:

- **Column Index**: Stores min/max statistics per data page within a column chunk. Allows skipping individual pages that cannot match a filter predicate, rather than only skipping entire row groups.
- **Offset Index**: Stores page offsets and first row indexes. Enables navigating to specific pages and aligning row positions across columns.
- **Row synchronization**: When pages are skipped in some columns but not others, `SynchronizingColumnReader` uses row indexes from the `OffsetIndex` to align rows across columns.
- **API**: `readNextFilteredRowGroup()` uses column indexes to skip pages. Enabled via `ParquetReadOptions.builder().useColumnIndexFilter(true)`.

This is particularly valuable for sorted/clustered data where page-level min/max statistics are tight.

**Source**: [Parquet Page Index spec](https://parquet.apache.org/docs/file-format/pageindex/), [ColumnIndexFilterUtils.java](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/ColumnIndexFilterUtils.java)

### 2.5 Dictionary Filter

- When a column chunk has dictionary encoding, the dictionary page can be read first
- If all dictionary values fail the filter predicate, the entire column chunk can be skipped
- Enabled via `ParquetReadOptions.builder().useDictionaryFilter(true)` (on by default)
- Very fast for low-cardinality columns

**Source**: [ParquetReadOptions.java](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/ParquetReadOptions.java)


## 3. Arrow Integration

### 3.1 The parquet-arrow Module (in parquet-java)

The `parquet-arrow` module exists at [parquet-java/parquet-arrow](https://github.com/apache/parquet-java/tree/master/parquet-arrow) but its scope is **very limited**:

- **`SchemaConverter`**: Converts between Parquet `MessageType` schemas and Arrow `Schema` objects. Supports configuration like `convertInt96ToArrowTimestamp`.
- **`SchemaMapping`**: Maintains the bidirectional mapping between Arrow fields and Parquet fields. Supports primitive, struct, list, map, union, and repeated type mappings via a visitor pattern.

**Crucially, the parquet-arrow module does NOT provide**:
- A vectorized Parquet-to-Arrow reader
- Batch reading of Parquet data into Arrow `VectorSchemaRoot` or `FieldVector`
- Any equivalent to the Arrow C++ `parquet::arrow::FileReader`

The module is purely a schema conversion utility. It does not read or write data.

**Source**: [SchemaConverter.java](https://github.com/apache/parquet-java/blob/master/parquet-arrow/src/main/java/org/apache/parquet/arrow/schema/SchemaConverter.java), [SchemaConverter Javadoc](https://javadoc.io/doc/org.apache.parquet/parquet-arrow/latest/org/apache/parquet/arrow/schema/SchemaConverter.html)

### 3.2 Who Provides Vectorized Arrow Reading in Java?

Since parquet-java does not have a vectorized Arrow reader, other projects fill this gap:

| Project | Approach |
|---------|----------|
| **Apache Iceberg** (`iceberg-arrow`) | `ArrowReader` / `VectorizedArrowReader`: Reads Parquet column chunks directly into Arrow `FieldVector` objects. Bypasses parquet-java's `RecordMaterializer` pipeline. Used by Spark and other engines. ([Iceberg ArrowReader Javadoc](https://iceberg.apache.org/javadoc/1.4.3/org/apache/iceberg/arrow/vectorized/ArrowReader.html)) |
| **Apache Spark** | Has its own vectorized Parquet reader that reads into Spark's `ColumnarBatch` (backed by Arrow or Spark's internal columnar format). Does not use parquet-java's record-oriented path. |
| **Apache Drill / Dremio** | Custom vectorized readers that bypass parquet-java's `GroupRecordConverter`. |
| **Arrow C++ (via JNI)** | `arrow-dataset` Java bindings can use the C++ Parquet reader via JNI for maximum performance. |

### 3.3 Comparison: Arrow C++ Parquet Reader vs. parquet-java

| Aspect | Arrow C++ Parquet Reader | parquet-java |
|--------|------------------------|--------------|
| **Reading model** | Vectorized: reads directly into Arrow columnar arrays | Record-oriented by default: decodes into row objects via `RecordMaterializer` |
| **Multi-threading** | Native multi-threaded: parallel column chunk / row group reading | Single-threaded. Row groups read sequentially. |
| **Memory model** | Arrow memory pool, zero-copy where possible | Java heap-based with optional off-heap for codec buffers. No zero-copy. |
| **Pre-buffering** | Supports async pre-buffering of column chunks (useful for cloud/S3) | No pre-buffering. Reads are synchronous. |
| **Schema projection** | Both support it | Both support it |
| **Predicate pushdown** | Row group + page level + bloom filter | Row group + page level + bloom filter + dictionary |
| **Performance** | Significantly faster for large scans due to vectorization and threading | Slower due to row-oriented record assembly and single-threaded I/O |

**Source**: [Arrow C++ Parquet docs](https://arrow.apache.org/docs/cpp/parquet.html), [Arrow Java Cookbook](https://arrow.apache.org/cookbook/java/), [Arrow dev mailing list](https://www.mail-archive.com/dev@arrow.apache.org/msg12303.html)


## 4. Record Conversion Pipeline

### 4.1 Core Classes

The record conversion pipeline is how parquet-java transforms columnar page data into row-oriented Java objects:

```
PageReadStore (row group data)
    |
    v
ColumnIOFactory.getColumnIO(schema) --> MessageColumnIO
    |
    v
MessageColumnIO.getRecordReader(pageReadStore, recordMaterializer) --> RecordReader<T>
    |
    v
RecordReader.read() --> T (one record at a time)
```

**Key classes**:

- **`RecordMaterializer<T>`** (parquet-column): Abstract class. Provides a root `GroupConverter` that receives events (start group, add int, add binary, end group) as columns are read. Must be subclassed to produce the desired record type `T`.
- **`GroupRecordConverter`** (parquet-column, example package): A concrete `RecordMaterializer<Group>` that produces `Group` objects (Parquet's built-in hierarchical record representation).
- **`MessageColumnIO`** (parquet-column): Orchestrates reading across all columns. Created by `ColumnIOFactory` from the schema. Its `getRecordReader()` method wires up column readers to the converter.
- **`RecordReaderImplementation<T>`** (parquet-column): The actual implementation that iterates through primitive columns, reads definition/repetition levels, and drives the converter hierarchy.

**Source**: [RecordMaterializer.java](https://github.com/apache/parquet-java/blob/master/parquet-column/src/main/java/org/apache/parquet/io/api/RecordMaterializer.java), [MessageColumnIO.java](https://github.com/apache/parquet-java/blob/master/parquet-column/src/main/java/org/apache/parquet/io/MessageColumnIO.java), [RecordReaderImplementation.java](https://github.com/apache/parquet-java/blob/master/parquet-column/src/main/java/org/apache/parquet/io/RecordReaderImplementation.java)

### 4.2 The Performance Cost of Record Assembly

The record-oriented pipeline has inherent overhead:

1. **Per-value dispatch**: Each decoded value triggers a method call on the `Converter` hierarchy (`addInt`, `addBinary`, `addLong`, etc.). For a row with N columns, that is N virtual method calls per row.
2. **Object allocation**: `GroupRecordConverter` allocates a `Group` object per row, plus internal lists for field values.
3. **Row-at-a-time**: Even though data is stored and decoded column-by-column, the converter reassembles it row-by-row, losing the columnar advantage for downstream processing.
4. **No SIMD/vectorization**: Java's record assembly pipeline cannot leverage SIMD instructions the way Arrow C++ can for batch decoding.

This is why systems like Spark, Iceberg, and Dremio bypass this pipeline entirely with vectorized readers that decode directly into columnar arrays.

**Source**: [Fast Parquet Reading: From Java to Rust Columnar Readers](https://baarse.substack.com/p/fast-parquet-reading-from-java-to)


## 5. Memory Management

### 5.1 Page Buffering and Decompression

- **Row group granularity**: `readNextRowGroup()` reads ALL column chunks for the row group into memory as a `ColumnChunkPageReadStore`. Each column chunk's compressed bytes are read, then individual pages are decompressed on demand.
- **CodecFactory**: Manages compression/decompression codecs. Creates `BytesDecompressor` instances that handle SNAPPY, GZIP, ZSTD, LZ4 decompression. Off-heap buffers may be used for some codecs (notably SNAPPY and ZSTD).
- **Memory leaks**: Known issue ([PARQUET-1188](https://issues.apache.org/jira/browse/PARQUET-1188)): SNAPPY codec can allocate significant off-heap memory for decompression buffers. `CodecFactory.release()` must be called to recycle compressors.
- **maxAllocationSize**: `ParquetReadOptions` allows setting a maximum allocation size for page buffers.

### 5.2 Configuration Options (ParquetReadOptions)

Key options available via `ParquetReadOptions.builder()`:

| Option | Default | Description |
|--------|---------|-------------|
| `useSignedStringMinMax` | false | Use signed comparison for binary min/max stats |
| `useStatsFilter` | true | Enable row-group pruning via statistics |
| `useDictionaryFilter` | true | Enable dictionary-based filtering |
| `useRecordFilter` | true | Enable record-level filtering |
| `useColumnIndexFilter` | true | Enable page-level filtering via column index |
| `useBloomFilter` | true | Enable bloom filter checking |
| `metadataFilter` | NO_FILTER | `SKIP_ROW_GROUPS` to only read schema metadata |
| `codecFactory` | default | Custom compression codec factory |
| `allocator` | default | Custom byte buffer allocator |
| `maxAllocationSize` | default | Max size for a single allocation |
| `recordFilter` | null | `FilterPredicate` for filtering |

**Source**: [ParquetReadOptions.java](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/ParquetReadOptions.java), [InternalParquetRecordReader.java](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/hadoop/InternalParquetRecordReader.java)

### 5.3 Non-Hadoop Usage (InputFile API)

Since parquet-java 1.10+, reading without Hadoop is possible via the `InputFile` / `SeekableInputStream` interfaces:

```java
InputFile inputFile = new MyCustomInputFile(...);
ParquetReadOptions options = ParquetReadOptions.builder().build();
ParquetFileReader reader = ParquetFileReader.open(inputFile, options);
```

However, there is a practical limitation: `ParquetReadOptions.Builder()` constructor internally creates a `HadoopParquetConfiguration`, which requires the `org.apache.hadoop.conf.Configuration` class to be on the classpath even when not using Hadoop. This means Hadoop JARs (at least `hadoop-client-api`) must be present as a transitive dependency.

**Source**: [Blake Smith: How to use Parquet Java without Hadoop](https://blakesmith.me/2024/10/05/how-to-use-parquet-java-without-hadoop.html), [PARQUET-1822](https://issues.apache.org/jira/browse/PARQUET-1822)


## 6. Performance Characteristics and Known Bottlenecks

### 6.1 Primary Bottlenecks

1. **Record-oriented assembly**: The default `GroupRecordConverter` path reassembles data row-by-row, which is fundamentally at odds with the columnar storage format. Each value requires a virtual method call on a `Converter` instance, and a `Group` object is allocated per row. This is the single biggest performance limitation.

2. **Single-threaded I/O**: `readNextRowGroup()` is synchronous and single-threaded. No parallel reading of column chunks within a row group. No pre-buffering or async I/O.

3. **No vectorized decoding**: While parquet-java supports the Java Vector API for some encoding operations (added in recent versions), the overall pipeline is still value-at-a-time rather than batch-oriented.

4. **Hadoop dependency overhead**: Even in non-Hadoop deployments, the Hadoop Configuration class and related infrastructure must be present, adding classpath complexity and initialization overhead.

5. **Thrift metadata parsing**: Footer parsing uses auto-generated Thrift code which is not optimized for Parquet's specific metadata structures. For files with many row groups or columns, metadata parsing can be significant. (The Rust Arrow implementation achieved [3-9x faster metadata parsing](https://arrow.apache.org/blog/2025/10/23/rust-parquet-metadata/) with a custom Thrift parser.)

6. **Memory management**: Decompression buffers (especially for SNAPPY/ZSTD) use off-heap memory that must be explicitly released. This has historically caused memory leaks in production systems.

### 6.2 Alternatives and Benchmarks

- **Hardwood** ([github.com/hardwood-hq/hardwood](https://github.com/hardwood-hq/hardwood)): A new Java 21+ Parquet parser by Gunnar Morling. Minimal dependencies, multi-threaded decoding pipeline with page-level parallelism. Reported to be significantly faster than parquet-java. Uses modern Java features and distributes page decoding across CPU cores.

- **Rust columnar reader**: A benchmark from [Art Baarse](https://baarse.substack.com/p/fast-parquet-reading-from-java-to) showed a Rust columnar reader achieving 4.89x faster performance and 10.2x less RAM compared to an optimized Java reader, primarily by avoiding per-value Field enum wrapping and reading in columnar batches.

- **Arrow C++ via JNI**: Arrow's Java bindings can use the C++ Parquet reader via JNI, which is multi-threaded and vectorized. This is the fastest Java-accessible Parquet reading path but requires native library deployment.

**Source**: [Hardwood blog post](https://www.morling.dev/blog/hardwood-new-parser-for-apache-parquet/), [Art Baarse Substack](https://baarse.substack.com/p/fast-parquet-reading-from-java-to), [Arrow Rust metadata blog](https://arrow.apache.org/blog/2025/10/23/rust-parquet-metadata/)


## 7. How Elasticsearch's ParquetFormatReader Uses parquet-java

Elasticsearch's `esql-datasource-parquet` module (version `1.16.0` of parquet-java) provides direct Parquet file reading for ESQL external data sources.

### 7.1 Architecture

The implementation consists of three classes in `org.elasticsearch.xpack.esql.datasource.parquet`:

**`ParquetDataSourcePlugin`**: Registers the "parquet" format reader factory. Handles `.parquet` and `.parq` extensions.

**`ParquetStorageObjectAdapter`**: Adapts Elasticsearch's `StorageObject` (which provides `newStream()` and `newStream(position, length)` for range reads) to Parquet's `InputFile` / `SeekableInputStream` interface. This is the I/O bridge.

Key implementation details:
- Forward seeks within the current stream use `InputStream.skip()`
- Backward seeks or large jumps close the current stream and reopen at the target position via `storageObject.newStream(newPos, remainingBytes)` (range read)
- `readFully()` loops until all requested bytes are read
- ByteBuffer reads copy through an intermediate `byte[]` array (not zero-copy)

**`ParquetFormatReader`**: The main reader that:
1. Opens a `ParquetFileReader` with the adapted `InputFile`
2. For schema discovery: uses `ParquetMetadataConverter.SKIP_ROW_GROUPS` to only read file metadata (no data)
3. For data reading: iterates row groups via `readNextRowGroup()`, creates a `RecordReader<Group>` using `GroupRecordConverter`, and reads records row-by-row
4. Converts Parquet `Group` objects to ESQL `Page` objects with typed `Block` builders

### 7.2 Type Mapping

| Parquet Type | Logical Type | ESQL Type |
|-------------|--------------|-----------|
| BOOLEAN | - | BOOLEAN |
| INT32 | - | INTEGER |
| INT32 | DATE | DATETIME |
| INT64 | - | LONG |
| INT64 | TIMESTAMP | DATETIME |
| FLOAT | - | DOUBLE |
| DOUBLE | - | DOUBLE |
| BINARY | STRING | KEYWORD |
| BINARY | - | KEYWORD |
| FIXED_LEN_BYTE_ARRAY | STRING | KEYWORD |
| Complex/nested | - | UNSUPPORTED |

### 7.3 Current Limitations

1. **No predicate pushdown**: The reader does not pass any `FilterPredicate` to `ParquetFileReader`. All row groups are read, and all records within them are materialized. Filtering happens at the ESQL layer after data is loaded.

2. **No projection pushdown to Parquet**: While the reader accepts `projectedColumns` and only creates `Block`s for requested columns, it still reads ALL columns from Parquet (the full schema is used for `GroupRecordConverter`). The `RecordReader<Group>` decodes all columns in every row group.

3. **Record-oriented reading**: Uses `GroupRecordConverter` which materializes one `Group` per row. Each batch of rows is collected into a `List<Group>`, then converted column-by-column into ESQL blocks. This means data is: columnar (Parquet) -> row-oriented (Group) -> columnar (ESQL Block). Two unnecessary format transitions.

4. **No bloom filter / column index usage**: No filter predicates are set, so bloom filter, dictionary filter, and column index features are unused.

5. **No complex type support**: Nested types (structs, lists, maps) are mapped to `UNSUPPORTED`.

6. **ByteBuffer reads are not zero-copy**: The `SeekableInputStream.read(ByteBuffer)` implementation copies through an intermediate byte array.

### 7.4 Contrast with Iceberg Datasource

The Elasticsearch Iceberg datasource (`esql-datasource-iceberg`) takes a different approach:

- Uses **Iceberg's `ArrowReader`** for vectorized columnar reading (parquet -> Arrow `ColumnarBatch` -> `VectorSchemaRoot`)
- Supports **predicate pushdown** via `IcebergPushdownFilters` which converts ESQL expressions to Iceberg expressions (equality, comparison, IN, IS NULL, range, AND/OR/NOT)
- Supports **column projection** via Iceberg's `scan.select(columnNames)`
- Uses **Arrow memory** (`RootAllocator`) for in-memory columnar data

This is architecturally superior for performance but currently marked as work-in-progress (the `SourceOperator.get()` method throws `UnsupportedOperationException`).

### 7.5 Dependency Strategy

Both plugins use `parquet-hadoop-bundle:1.16.0` (the shaded fat JAR) to avoid classpath conflicts with different Parquet module versions. Both also require `hadoop-client-api` and `hadoop-client-runtime` because `ParquetReadOptions.Builder()` internally references Hadoop's `Configuration` class.

**Source**: All Elasticsearch code paths referenced above are from the local codebase at:
- `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetFormatReader.java`
- `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetStorageObjectAdapter.java`
- `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-parquet/build.gradle`
- `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergSourceOperatorFactory.java`
- `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergPushdownFilters.java`


## 8. Summary of Key Findings

### What parquet-java does well:
- Comprehensive Parquet format support (all encodings, compression codecs, logical types)
- Full predicate pushdown stack (statistics, dictionary, bloom filter, column index)
- Schema projection
- Broad ecosystem compatibility (Hadoop, Avro, Protobuf, Thrift)
- Mature, battle-tested in production at massive scale

### What parquet-java does poorly:
- Record-oriented default reading path (columnar -> row -> columnar transitions)
- Single-threaded I/O and decoding
- No built-in vectorized Arrow reader (only schema conversion)
- Heavy Hadoop dependency even for non-Hadoop usage
- Thrift metadata parsing overhead
- Off-heap memory management complexity

### How Elasticsearch uses it:
- Direct `ParquetFileReader` usage with custom `InputFile` adapter
- `GroupRecordConverter` for record materialization
- No filter pushdown, no real projection pushdown to Parquet level
- Converts row-oriented `Group` objects back to columnar ESQL `Block`/`Page`
- The Iceberg path (which uses Arrow vectorized reading) is architecturally better but not yet fully wired up

### Opportunities for improvement in ES Parquet reading:
1. **Schema projection**: Pass `requestedSchema` (subset `MessageType`) to `GroupRecordConverter` and `readNextRowGroup()` so Parquet skips reading unrequested column chunks
2. **Predicate pushdown**: Convert ESQL filter expressions to Parquet `FilterPredicate` objects (similar to what `IcebergPushdownFilters` does for Iceberg expressions)
3. **Vectorized reading**: Bypass `GroupRecordConverter` entirely; read column chunks directly into ESQL blocks using Parquet's `ColumnReader` API or use Iceberg's `ArrowReader`
4. **Async I/O**: For S3/HTTP storage, use pre-buffering or async reads for column chunks
5. **Multi-threaded decoding**: Decode multiple column chunks in parallel within a row group
