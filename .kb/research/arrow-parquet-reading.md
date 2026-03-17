# Reading Parquet Files with Apache Arrow in Java: A Comprehensive Report

## 1. All Available Approaches to Read Parquet in Java with Arrow

There are four primary approaches to reading Parquet files in the Java/Arrow ecosystem. Each differs in architecture, dependency footprint, and the format of data it produces.

### 1A. parquet-hadoop / parquet-mr (Apache Parquet's Own Java Library)

**Libraries required:** `parquet-hadoop-bundle` (or individual modules: `parquet-hadoop`, `parquet-column`, `parquet-format-structures`, `parquet-encoding`, `parquet-common`), plus `hadoop-client-api` and `hadoop-client-runtime` for the `Configuration` class.

**How it works:** This is the canonical Java implementation of the Parquet format, originally named "parquet-mr" (Map Reduce). The central class is `ParquetFileReader`, which opens a Parquet file via the `org.apache.parquet.io.InputFile` interface, reads file-level metadata (schema, row group statistics, bloom filters), and then reads row groups one at a time as `PageReadStore` objects. Records are materialized via a `RecordReader` using a converter (e.g., `GroupRecordConverter` for the `Group` model, `AvroParquetReader` for Avro GenericRecord, etc.).

Despite its name, the library can be used without a Hadoop cluster by implementing `InputFile` and `SeekableInputStream` directly. However, the `ParquetReadOptions.Builder()` constructor still internally references `HadoopParquetConfiguration`, making the Hadoop Configuration class a compile-time requirement even for non-Hadoop code paths ([Blake Smith, 2024](https://blakesmith.me/2024/10/05/how-to-use-parquet-java-without-hadoop.html); [PARQUET-1822](https://issues.apache.org/jira/browse/PARQUET-1822)).

**Maturity:** Production-grade. This is the most widely deployed Java Parquet reader, used by Spark, Hive, Presto/Trino, Flink, and Elasticsearch. Version 1.16.0 is current; 1.17.0 dropped Java 8 support and set Java 11 as the minimum.

**Output format:** Produces Parquet-native data objects (`Group`, Avro `GenericRecord`, custom ReadSupport materializers). Does NOT produce Arrow vectors natively -- conversion to Arrow requires a separate step.

### 1B. Arrow Dataset API (JNI to C++)

**Libraries required:** `arrow-dataset` (JNI module), `arrow-memory-core`, `arrow-memory-unsafe` (or `arrow-memory-netty`), `arrow-vector`, plus the native `libarrow_dataset_jni` shared library compiled for your platform.

**How it works:** The Java Dataset module is a thin JNI wrapper around the C++ Arrow Dataset library. It creates a `FileSystemDatasetFactory` specifying `FileFormat.PARQUET` and a URI, then uses `ScanOptions` to configure batch size, column projection, and optional Substrait-based filter/projection expressions. The C++ layer handles all Parquet decoding, predicate pushdown, and memory allocation. Data is returned as `ArrowRecordBatch` objects in off-heap (native) memory, which must be loaded into `VectorSchemaRoot` using `VectorLoader` ([Arrow Java Docs](https://arrow.apache.org/docs/java/dataset.html); [Arrow Cookbook](https://arrow.apache.org/cookbook/java/dataset.html)).

**Maturity:** Experimental / early development. The API documentation explicitly states: "module dataset is currently under early development, and API might change in each release." S3 support requires the native library to be compiled with S3 enabled, and users have reported connection and JNI errors in versions up to 15.0.0 ([arrow-java#219](https://github.com/apache/arrow-java/issues/219); [arrow#39919](https://github.com/apache/arrow/issues/39919)).

**Output format:** Produces Arrow vectors natively in off-heap memory. This is the only approach that produces true Arrow `VectorSchemaRoot` without any conversion step.

### 1C. Iceberg ArrowReader (iceberg-arrow)

**Libraries required:** `iceberg-core`, `iceberg-parquet`, `iceberg-arrow`, `arrow-vector`, `arrow-memory-core`, plus `parquet-hadoop-bundle` and Hadoop dependencies (pulled transitively by iceberg-parquet).

**How it works:** Iceberg's `ArrowReader` is a vectorized Parquet reader that decodes Parquet pages directly into Arrow `FieldVector` objects. Internally, it uses `BaseVectorizedParquetValuesReader` (derived from Spark's `VectorizedRleValuesReader`) to batch-decode run-length and dictionary-encoded Parquet data directly into Arrow buffers, avoiding row-by-row materialization. The reader works within Iceberg's scan planning framework: a `TableScan` defines projection, filters, and partition pruning; the `ArrowReader` then opens `CombinedScanTask` objects and returns `ColumnarBatch` iterables, where each `ColumnarBatch` wraps Arrow `FieldVector` instances ([Iceberg ArrowReader Javadoc](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/arrow/vectorized/ArrowReader.html); [Iceberg PR #2286](https://github.com/apache/iceberg/pull/2286)).

**Maturity:** Production-ready for supported types. Used in production by multiple Iceberg-based data platforms. Known limitations: delete files are not supported, and complex types (ListType, MapType, StructType) have had incomplete support historically, though work on these is ongoing ([Iceberg #6003](https://github.com/apache/iceberg/issues/6003)).

**Output format:** Produces Arrow `FieldVector` objects wrapped in `ColumnarBatch`. These can be trivially extracted into `VectorSchemaRoot`.

### 1D. parquet-arrow Module (parquet-java's Own Arrow Adapter)

**Libraries required:** `parquet-arrow` (from the parquet-java project), `parquet-hadoop`, `arrow-vector`, `arrow-memory-core`.

**How it works:** The `parquet-arrow` module within the apache/parquet-java project provides `SchemaConverter` for bidirectional conversion between Parquet `MessageType` and Arrow `Schema`, plus `SchemaMapping` for maintaining the mapping. However, this module is primarily a schema conversion utility and does NOT include a full vectorized reader. There is no `ArrowColumnBatchReader` or equivalent in this module. To read Parquet data into Arrow vectors using parquet-java, you would still read via `ParquetFileReader` and then convert/copy data into Arrow vectors manually ([parquet-arrow on Maven](https://mvnrepository.com/artifact/org.apache.parquet/parquet-arrow); [SchemaConverter source](https://github.com/apache/parquet-java/blob/master/parquet-arrow/src/main/java/org/apache/parquet/arrow/schema/SchemaConverter.java)).

**Maturity:** The schema conversion is stable, but there is no vectorized reader component. This module is not a standalone reading solution.

**Output format:** Schema metadata only; no data reading capability.

### 1E. Arrow C++ JNI Parquet Adapter (ARROW-6720)

**Libraries required:** Native `libarrow_parquet_jni` library, `arrow-vector`, `arrow-memory-core`.

**How it works:** ARROW-6720 added a JNI bridge (`java/adapter/parquet` + `cpp/jni/parquet`) that wraps the C++ Parquet reader for Java. The Java `ParquetReader` and `ParquetWriter` classes call through JNI to `arrow::FileReaderBuilder` in C++, using `FilesystemFactory` to open different storage backends. Performance benchmarks showed >2x improvement over row-based approaches ([ARROW-6720 PR #5522](https://github.com/apache/arrow/pull/5522); [PR #5719](https://github.com/apache/arrow/pull/5719)).

**Maturity:** This work was exploratory (2019-2020) and has been largely subsumed by the Dataset API (1B above). The Dataset API is the recommended path for JNI-based Parquet reading in Arrow Java today.

**Output format:** Arrow vectors via JNI.

---

## 2. Feature Comparison with Code Examples

### 2A. parquet-hadoop (parquet-mr) -- The Foundational Reader

#### a) Basic Reading

```java
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.ParquetReadOptions;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.io.InputFile;
import org.apache.parquet.schema.MessageType;

// InputFile can be LocalInputFile, HadoopInputFile, or custom (like ES's ParquetStorageObjectAdapter)
InputFile inputFile = new LocalInputFile(java.nio.file.Path.of("/data/events.parquet"));

try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
    MessageType schema = reader.getFileMetaData().getSchema();
    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);

    PageReadStore rowGroup;
    while ((rowGroup = reader.readNextRowGroup()) != null) {
        long rowCount = rowGroup.getRowCount();
        RecordReader<Group> recordReader = columnIO.getRecordReader(
            rowGroup, new GroupRecordConverter(schema)
        );
        for (int i = 0; i < rowCount; i++) {
            Group record = recordReader.read();
            System.out.println(record.getLong("id", 0) + ": " + record.getString("name", 0));
        }
    }
}
```

#### b) Projection Pushdown

```java
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;
import java.util.List;

try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
    MessageType fullSchema = reader.getFileMetaData().getSchema();

    // Create a projected schema with only the columns we need
    List<Type> projectedFields = List.of(
        fullSchema.getType("id"),
        fullSchema.getType("name")
    );
    MessageType projectedSchema = new MessageType(fullSchema.getName(), projectedFields);

    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(projectedSchema);

    PageReadStore rowGroup;
    while ((rowGroup = reader.readNextRowGroup()) != null) {
        RecordReader<Group> recordReader = columnIO.getRecordReader(
            rowGroup, new GroupRecordConverter(projectedSchema)
        );
        for (int i = 0; i < rowGroup.getRowCount(); i++) {
            Group record = recordReader.read();
            // Only projected columns are available
        }
    }
}
```

#### c) Predicate/Filter Pushdown

```java
import org.apache.parquet.filter2.predicate.FilterApi;
import org.apache.parquet.filter2.predicate.FilterPredicate;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.compat.RowGroupFilter;
import org.apache.parquet.hadoop.metadata.BlockMetaData;

// Build a filter: age > 25 AND status = "active"
FilterPredicate filter = FilterApi.and(
    FilterApi.gt(FilterApi.intColumn("age"), 25),
    FilterApi.eq(FilterApi.binaryColumn("status"),
                 org.apache.parquet.io.api.Binary.fromString("active"))
);

try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
    MessageType schema = reader.getFileMetaData().getSchema();

    // Row group pruning using statistics (min/max) -- happens automatically via RowGroupFilter
    List<BlockMetaData> filteredBlocks = RowGroupFilter.filterRowGroups(
        FilterCompat.get(filter),
        reader.getRowGroups(),
        schema
    );

    // Read only the row groups that pass the statistics filter
    for (BlockMetaData block : filteredBlocks) {
        // readFilteredRowGroup also applies column-index-level page skipping
        PageReadStore rowGroup = reader.readFilteredRowGroup(block);
        if (rowGroup != null) {
            // The filter is also applied at the record level during reading
            MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
            RecordReader<Group> recordReader = columnIO.getRecordReader(
                rowGroup, new GroupRecordConverter(schema),
                FilterCompat.get(filter)
            );
            for (int i = 0; i < rowGroup.getRowCount(); i++) {
                Group record = recordReader.read();
                if (record != null) { // null means filtered out
                    // Process record
                }
            }
        }
    }
}
```

#### d) Limit (Read Only N Rows)

```java
try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
    MessageType schema = reader.getFileMetaData().getSchema();
    MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);

    int limit = 100;
    int rowsRead = 0;

    PageReadStore rowGroup;
    outer:
    while ((rowGroup = reader.readNextRowGroup()) != null) {
        RecordReader<Group> recordReader = columnIO.getRecordReader(
            rowGroup, new GroupRecordConverter(schema)
        );
        for (int i = 0; i < rowGroup.getRowCount(); i++) {
            Group record = recordReader.read();
            // Process record...
            rowsRead++;
            if (rowsRead >= limit) break outer;
        }
    }
}
```

#### e) Row Group Pruning via Statistics

```java
import org.apache.parquet.hadoop.metadata.BlockMetaData;
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.column.statistics.Statistics;

try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
    for (BlockMetaData block : reader.getRowGroups()) {
        for (ColumnChunkMetaData column : block.getColumns()) {
            Statistics<?> stats = column.getStatistics();
            if (stats.hasNonNullValue()) {
                System.out.printf("Column %s: min=%s, max=%s, nullCount=%d%n",
                    column.getPath(),
                    stats.minAsString(),
                    stats.maxAsString(),
                    stats.getNumNulls()
                );
            }
        }
    }
}
```

#### f) Bloom Filter Usage

```java
import org.apache.parquet.hadoop.metadata.ColumnChunkMetaData;
import org.apache.parquet.hadoop.metadata.ColumnPath;
import org.apache.parquet.column.values.bloomfilter.BloomFilter;

try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
    for (BlockMetaData block : reader.getRowGroups()) {
        for (ColumnChunkMetaData column : block.getColumns()) {
            // Read bloom filter for a specific column chunk
            BloomFilter bloomFilter = reader.readBloomFilter(column);
            if (bloomFilter != null) {
                // Check if value might exist in this row group
                Binary searchValue = Binary.fromString("target_value");
                boolean mightContain = bloomFilter.findHash(
                    bloomFilter.hash(searchValue)
                );
                if (!mightContain) {
                    // Value definitely not in this row group -- skip it entirely
                    System.out.println("Skipping row group: bloom filter says value absent");
                }
            }
        }
    }
}
```

Note: Bloom filter integration with `RowGroupFilter` is automatic when bloom filter-enabled columns are present and `parquet.filter.bloom.enabled` is set to `true` in `ParquetReadOptions` ([parquet-hadoop README](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/README.md)).

#### g) Reading from S3

```java
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.util.HadoopInputFile;

// Configure Hadoop for S3 access
Configuration conf = new Configuration();
conf.set("fs.s3a.access.key", "YOUR_ACCESS_KEY");
conf.set("fs.s3a.secret.key", "YOUR_SECRET_KEY");
conf.set("fs.s3a.endpoint", "s3.amazonaws.com");
conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");

// Open Parquet file from S3 using Hadoop's filesystem abstraction
Path s3Path = new Path("s3a://my-bucket/data/events.parquet");
InputFile inputFile = HadoopInputFile.fromPath(s3Path, conf);

try (ParquetFileReader reader = ParquetFileReader.open(inputFile)) {
    // Read as usual...
}
```

### 2B. Arrow Dataset API (JNI to C++)

#### a) Basic Reading

```java
import org.apache.arrow.dataset.file.FileFormat;
import org.apache.arrow.dataset.file.FileSystemDatasetFactory;
import org.apache.arrow.dataset.jni.NativeMemoryPool;
import org.apache.arrow.dataset.scanner.ScanOptions;
import org.apache.arrow.dataset.scanner.Scanner;
import org.apache.arrow.dataset.source.Dataset;
import org.apache.arrow.dataset.source.DatasetFactory;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowReader;

import java.util.Optional;

String uri = "file:///data/events.parquet";
ScanOptions options = new ScanOptions(/*batchSize*/ 32768, Optional.empty());

try (BufferAllocator allocator = new RootAllocator();
     DatasetFactory factory = new FileSystemDatasetFactory(
         allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
     Dataset dataset = factory.finish();
     Scanner scanner = dataset.newScan(options);
     ArrowReader reader = scanner.scanBatches()) {

    while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        System.out.println("Read batch with " + root.getRowCount() + " rows");
        // Access vectors: root.getVector("id"), root.getVector("name"), etc.
    }
}
```

#### b) Projection Pushdown

```java
// Project only "id" and "name" columns
String[] projection = new String[] {"id", "name"};
ScanOptions options = new ScanOptions(32768, Optional.of(projection));

try (BufferAllocator allocator = new RootAllocator();
     DatasetFactory factory = new FileSystemDatasetFactory(
         allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
     Dataset dataset = factory.finish();
     Scanner scanner = dataset.newScan(options);
     ArrowReader reader = scanner.scanBatches()) {

    while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        // Only "id" and "name" vectors are present
    }
}
```

#### c) Predicate/Filter Pushdown (via Substrait)

```java
import java.nio.ByteBuffer;

// Filter pushdown requires encoding the filter as a Substrait expression.
// This is typically generated programmatically using substrait-java.
ByteBuffer substraitFilter = buildSubstraitFilter(); // e.g., "age > 25"
ByteBuffer substraitProjection = buildSubstraitProjection(); // e.g., project "id", "name"

ScanOptions options = new ScanOptions.Builder(32768)
    .columns(Optional.empty())
    .substraitExpressionFilter(substraitFilter)
    .substraitExpressionProjection(substraitProjection)
    .build();

try (BufferAllocator allocator = new RootAllocator();
     DatasetFactory factory = new FileSystemDatasetFactory(
         allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, uri);
     Dataset dataset = factory.finish();
     Scanner scanner = dataset.newScan(options);
     ArrowReader reader = scanner.scanBatches()) {

    while (reader.loadNextBatch()) {
        VectorSchemaRoot root = reader.getVectorSchemaRoot();
        // Only rows matching the filter and projected columns
    }
}
```

Note: Filter pushdown in Arrow Java is tracked as an open issue ([arrow-java#227](https://github.com/apache/arrow-java/issues/227)). Substrait expression support was added incrementally and requires both the `substrait-java` library and correct Substrait plan serialization.

#### d) Limit

There is no native `LIMIT` parameter in `ScanOptions`. Limiting must be implemented by the caller by stopping iteration after N rows.

#### e-f) Row Group Pruning / Bloom Filters

Row group pruning via statistics is handled internally by the C++ Dataset layer when a Substrait filter is provided. Bloom filter usage depends on the C++ Parquet reader's support and is not directly configurable from Java.

#### g) Reading from S3

```java
// S3 support requires the native Arrow library compiled with -DARROW_S3=ON
String s3Uri = "s3://my-bucket/data/events.parquet";

// The C++ layer handles S3 access natively
try (BufferAllocator allocator = new RootAllocator();
     DatasetFactory factory = new FileSystemDatasetFactory(
         allocator, NativeMemoryPool.getDefault(), FileFormat.PARQUET, s3Uri);
     Dataset dataset = factory.finish();
     Scanner scanner = dataset.newScan(new ScanOptions(32768, Optional.empty()));
     ArrowReader reader = scanner.scanBatches()) {

    while (reader.loadNextBatch()) {
        // Read data from S3
    }
}
```

Caveat: S3 support is compile-time optional and has been reported as unreliable in practice ([arrow-java#219](https://github.com/apache/arrow-java/issues/219)).

### 2C. Iceberg ArrowReader

#### a) Basic Reading

```java
import org.apache.iceberg.Table;
import org.apache.iceberg.TableScan;
import org.apache.iceberg.CombinedScanTask;
import org.apache.iceberg.arrow.vectorized.ArrowReader;
import org.apache.iceberg.arrow.vectorized.ColumnarBatch;
import org.apache.iceberg.arrow.vectorized.ColumnVector;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.CloseableIterator;
import org.apache.arrow.vector.FieldVector;

// Assumes 'table' is an Iceberg Table instance (loaded from a catalog)
TableScan scan = table.newScan();

try (ArrowReader arrowReader = new ArrowReader(scan, /*batchSize*/ 4096, /*reuseContainers*/ false)) {
    CloseableIterable<CombinedScanTask> tasks = scan.planTasks();
    try (CloseableIterator<ColumnarBatch> batchIter = arrowReader.open(tasks)) {
        while (batchIter.hasNext()) {
            ColumnarBatch batch = batchIter.next();
            int numRows = batch.numRows();
            for (int col = 0; col < batch.numCols(); col++) {
                ColumnVector vec = batch.column(col);
                FieldVector arrowVec = vec.getFieldVector();
                // Process Arrow FieldVector directly
            }
        }
    }
}
```

#### b) Projection Pushdown

```java
// Iceberg's scan API natively supports column projection
TableScan scan = table.newScan()
    .select("id", "name", "timestamp");  // Only read these columns

try (ArrowReader arrowReader = new ArrowReader(scan, 4096, false)) {
    // The reader only materializes the projected columns
}
```

#### c) Predicate/Filter Pushdown

```java
import org.apache.iceberg.expressions.Expressions;

// Iceberg expressions support rich predicate pushdown:
// - Partition pruning: skips entire data files
// - Row group statistics: skips row groups within files
// - (If Iceberg is configured) Column index pruning
TableScan scan = table.newScan()
    .filter(Expressions.and(
        Expressions.greaterThanOrEqual("timestamp", "2024-01-01T00:00:00Z"),
        Expressions.equal("region", "EMEA")
    ))
    .select("id", "name", "amount");

try (ArrowReader arrowReader = new ArrowReader(scan, 4096, false)) {
    CloseableIterable<CombinedScanTask> tasks = scan.planTasks();
    try (CloseableIterator<ColumnarBatch> batchIter = arrowReader.open(tasks)) {
        while (batchIter.hasNext()) {
            ColumnarBatch batch = batchIter.next();
            // Only data matching the filter is returned
        }
    }
}
```

#### d) Limit

No native limit in ArrowReader; must be implemented by the caller.

#### e) Row Group Pruning

Handled automatically by Iceberg's scan planning layer. Iceberg maintains manifest-level and file-level statistics, and `TableScan.planFiles()` returns only the `FileScanTask` objects that could match the filter predicate. Within each file, Parquet row group statistics further prune data.

#### f) Bloom Filter Usage

Iceberg added Parquet bloom filter support for row group skipping in [PR #4938](https://github.com/apache/iceberg/pull/4938). When bloom filters are present in the underlying Parquet files, Iceberg can use them during scan planning.

#### g) Reading from S3

```java
import org.apache.iceberg.aws.s3.S3FileIO;
import org.apache.iceberg.StaticTableOperations;
import org.apache.iceberg.BaseTable;

// Configure S3FileIO
S3FileIO fileIO = new S3FileIO(() -> S3Client.builder()
    .region(Region.US_EAST_1)
    .credentialsProvider(DefaultCredentialsProvider.create())
    .build());

// Load table from metadata location
StaticTableOperations ops = new StaticTableOperations(
    "s3://bucket/warehouse/db/table/metadata/v2.metadata.json", fileIO);
Table table = new BaseTable(ops, "my_table");

TableScan scan = table.newScan()
    .filter(Expressions.greaterThan("amount", 1000));

try (ArrowReader arrowReader = new ArrowReader(scan, 4096, false)) {
    // Reads Parquet files from S3 transparently
}
```

---

## 3. Trade-offs Between Approaches

### Comparison Matrix

| Dimension | parquet-hadoop (parquet-mr) | Arrow Dataset API (JNI) | Iceberg ArrowReader |
|---|---|---|---|
| **Memory Model** | On-heap (Java objects: Group, GenericRecord) | Off-heap (native C++ allocations, managed by Arrow allocator) | Off-heap (Arrow FieldVectors allocated via RootAllocator) |
| **Memory Management** | JVM GC | BufferAllocator / NativeMemoryPool -- caller must close | BufferAllocator -- ArrowReader owns and closes batches |
| **Performance** | Row-by-row materialization; no vectorized decoding for the Group model | Vectorized C++ Parquet decoder; >2x faster than row-based ([ARROW-6720](https://github.com/apache/arrow/pull/5522)) | Vectorized Java Parquet decoder (derived from Spark); fast but pure Java |
| **Dependency Weight** | parquet-hadoop-bundle (~30 MB) + hadoop-client-api/runtime (~100 MB) | arrow-dataset JNI + native .so/.dylib (~50 MB) + arrow-vector/memory (~5 MB) | iceberg-core/parquet/arrow (~15 MB) + parquet-hadoop-bundle + hadoop + arrow-vector/memory |
| **Column Projection** | Yes (via projected MessageType) | Yes (via ScanOptions column list) | Yes (via TableScan.select()) |
| **Predicate Pushdown** | Yes (FilterApi/FilterCompat with statistics, dictionary, bloom filter, column index) | Yes (via Substrait expressions to C++ layer) | Yes (via Iceberg Expressions; partition pruning + statistics + bloom filters) |
| **Bloom Filter Support** | Yes (read and apply via ParquetFileReader.readBloomFilter or automatic with RowGroupFilter) | Depends on C++ build flags; not directly configurable from Java | Yes (Iceberg PR #4938 added bloom filter row group skipping) |
| **Column Index / Page Skipping** | Yes (readFilteredRowGroup with RowRanges) | Yes (handled by C++ layer) | Partial (depends on Iceberg version) |
| **Arrow Vector Output** | No -- requires manual conversion | Yes -- native Arrow output | Yes -- native Arrow FieldVectors |
| **S3/Remote Storage** | Yes (via Hadoop S3A filesystem; mature) | Conditional (requires native lib compiled with S3; reliability issues reported) | Yes (via Iceberg S3FileIO; production-grade) |
| **Maturity** | Production-grade, 10+ years | Experimental, API unstable | Production-grade for supported types |
| **Complex Types** | Full support (groups, lists, maps) | Full support (via C++ Parquet reader) | Limited (ListType, MapType, StructType incomplete) |
| **Limit Pushdown** | Manual (stop iterating) | Manual (stop iterating) | Manual (stop iterating) |

### Key Trade-off Analysis

**parquet-hadoop** is the safest choice for maximum Parquet feature coverage. It supports every Parquet feature (bloom filters, column indexes, page-level filtering, all data types, encryption). The cost is row-by-row materialization into Java heap objects, which is slower than vectorized approaches, and requires conversion to produce Arrow vectors. The Hadoop dependency is the major pain point -- it pulls in ~100 MB of JARs even when no Hadoop cluster is used.

**Arrow Dataset API** would be ideal if it were more mature. It offers the best theoretical performance (C++ vectorized decoding) and produces Arrow vectors natively. However, the experimental status, JNI stability issues, platform-specific native library requirements, and incomplete S3 support make it risky for production use today. The Substrait-based filter API adds complexity.

**Iceberg ArrowReader** is the best balance for Iceberg-table use cases. It produces Arrow vectors, supports projection and predicate pushdown through Iceberg's elegant expression API, handles S3 natively, and benefits from Iceberg's partition pruning and manifest-level statistics. The main limitations are the requirement for Iceberg table metadata (not standalone Parquet files) and incomplete complex type support.

---

## 4. How Elasticsearch Uses Parquet Today

Elasticsearch implements two separate Parquet reading paths in its ES|QL datasource plugins.

### 4.1 ParquetFormatReader (esql-datasource-parquet)

**File:** `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetFormatReader.java`

This reader uses **parquet-hadoop** (approach 1A) to read standalone Parquet files. Key architectural details:

- **Storage abstraction:** Uses `ParquetStorageObjectAdapter` to adapt ES's `StorageObject` interface (which supports HTTP, S3, and local files) to Parquet's `InputFile` / `SeekableInputStream` interface. This adapter implements range-based seeks by closing and reopening streams at the target position.

- **Reading model:** Uses the `Group` record model with `GroupRecordConverter`. Records are read row-by-row from `PageReadStore` (row groups), then converted to ES|QL `Page` objects containing `Block` arrays (BooleanBlock, IntBlock, LongBlock, DoubleBlock, BytesRefBlock).

- **Column projection:** Supported at the attribute level -- the `read()` method accepts a list of projected column names, and only those columns are included in the output `Page`. However, the projection is applied AFTER reading the full schema from Parquet, not pushed down to the Parquet column reader level. The full row group is still decoded.

- **Batch reading:** Configurable batch size controls how many rows are read per `Page`.

**Dependencies (from build.gradle):**
```
parquet-hadoop-bundle:1.16.0
hadoop-client-api:3.4.2
hadoop-client-runtime:3.4.2
```

### 4.2 IcebergSourceOperatorFactory (esql-datasource-iceberg)

**File:** `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergSourceOperatorFactory.java`

This factory uses **Iceberg ArrowReader** (approach 1C) for reading Parquet files within Iceberg tables. Key details:

- **Vectorized reading:** Uses `org.apache.iceberg.arrow.vectorized.ArrowReader` with configurable batch size and `reuseContainers=false` for safety.

- **Predicate pushdown:** Filters are pushed down through Iceberg's `TableScan.filter()` method. The `IcebergPushdownFilters` class (file: `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergPushdownFilters.java`) converts ES|QL expressions to Iceberg expressions supporting: `=`, `!=`, `<`, `<=`, `>`, `>=`, `IS NULL`, `IS NOT NULL`, `IN(...)`, `Range`, `AND`, `OR`, `NOT`.

- **Column projection:** Pushed down via `TableScan.select(columnNames)`.

- **Arrow-to-ES|QL bridge:** `ColumnarBatch` objects from ArrowReader are converted to `VectorSchemaRoot` by extracting underlying `FieldVector` instances.

- **S3 integration:** Uses `S3FileIOFactory` for native S3 access.

**Dependencies (from build.gradle):**
```
iceberg-core, iceberg-aws, iceberg-parquet, iceberg-arrow (version from versions.iceberg)
parquet-hadoop-bundle:1.16.0
hadoop-client-api:3.4.2, hadoop-client-runtime:3.4.2
arrow-vector:18.3.0, arrow-memory-core:18.3.0 (compileOnly)
AWS SDK v2 modules for S3
```

### 4.3 What Pushdowns Are Currently Implemented

| Pushdown Feature | ParquetFormatReader | IcebergSourceOperatorFactory |
|---|---|---|
| Column projection | Yes (attribute-level, post-read) | Yes (scan-level, pre-read) |
| Predicate pushdown (statistics) | **No** | Yes (via Iceberg scan planning) |
| Predicate pushdown (bloom filter) | **No** | Depends on Iceberg version/config |
| Row group pruning | **No** | Yes (via Iceberg manifest + row group stats) |
| Column index / page skipping | **No** | Partial (via Iceberg) |
| Limit pushdown | **No** | **No** |
| Partition pruning | N/A (no partitioning) | Yes (via Iceberg hidden partitioning) |

### 4.4 What's Missing

**In ParquetFormatReader:**
1. **No predicate pushdown at all.** The reader reads every row group and every row, filtering only in the ES|QL engine. Adding `FilterApi` / `FilterCompat` integration would allow row group skipping via statistics and bloom filters.
2. **No true column projection pushdown.** While the output Page only contains projected columns, the `GroupRecordConverter` still materializes the full schema. A projected `MessageType` should be passed to `ColumnIOFactory.getColumnIO()` so that only the requested columns' pages are decoded.
3. **No limit pushdown.** The iterator reads all data; early termination should be possible.
4. **Row-by-row materialization.** The `Group` model materializes each record individually. A vectorized approach (either via Arrow Dataset or a custom columnar reader) would improve throughput.
5. **No bloom filter usage.**
6. **No column index / page skipping.**

**In IcebergSourceOperatorFactory:**
1. **Not yet operational.** The `get(DriverContext)` method throws `UnsupportedOperationException` -- the operator factory is scaffolded but not yet integrated with the ES|QL async operator infrastructure.
2. **No limit pushdown.**
3. **Complex type support** depends on Iceberg ArrowReader's limitations.

---

## 5. Recommendations

### For Reading Standalone Parquet Files from S3

**Recommended: Enhanced parquet-hadoop reader with pushdowns.**

The current `ParquetFormatReader` should be enhanced rather than replaced, because:
- parquet-hadoop is already a dependency and well-understood.
- It supports the full Parquet feature set including bloom filters, column indexes, and all data types.
- S3 access works reliably via `ParquetStorageObjectAdapter` (custom `InputFile` implementation) or Hadoop S3A.
- The Hadoop dependency is already paid for.

Specific enhancements to prioritize:
1. **True column projection:** Pass a projected `MessageType` to `ColumnIOFactory.getColumnIO()` so only requested columns are decoded.
2. **Predicate pushdown via FilterApi/FilterCompat:** Add `RowGroupFilter.filterRowGroups()` with statistics-based pruning, and `readFilteredRowGroup()` for column-index page skipping.
3. **Bloom filter integration:** Enable `parquet.filter.bloom.enabled` in `ParquetReadOptions` and use `FilterCompat` with equality predicates.
4. **Vectorized decoding (longer term):** Consider adopting Iceberg's `BaseVectorizedParquetValuesReader` approach to decode Parquet pages directly into ES|QL Blocks, bypassing the `Group` intermediate representation.

The Arrow Dataset API (JNI) is not recommended for this use case due to its experimental status and unreliable S3 support. If/when it matures, it would be the ideal replacement, since it produces Arrow vectors natively with full C++ performance.

### For Reading Parquet Files Within Iceberg Tables

**Recommended: Continue with Iceberg ArrowReader (current approach).**

The `IcebergSourceOperatorFactory` is correctly architected. The priority should be completing the integration with the ES|QL async operator infrastructure (the `get(DriverContext)` TODO). Iceberg's ArrowReader provides:
- Automatic partition pruning and data file skipping from Iceberg's metadata layer.
- Predicate pushdown via `IcebergPushdownFilters` (already implemented and tested).
- Column projection via `TableScan.select()`.
- Arrow vector output that can be efficiently converted to ES|QL Pages.
- Production-grade S3 support via `S3FileIO`.

### For Maximum Pushdown Capability

**Recommended: parquet-hadoop with full FilterApi integration.**

parquet-hadoop offers the most comprehensive pushdown capability of any Java Parquet reader:
- **Statistics-based row group pruning** (min/max per column per row group)
- **Dictionary-based filtering** (skip row groups where the dictionary doesn't contain the filter value)
- **Bloom filter row group skipping** (for high-cardinality equality predicates)
- **Column index page skipping** (skip individual pages within a column chunk based on per-page min/max)
- **Record-level filtering** (apply predicates during record assembly)

This is a strict superset of what the Arrow Dataset API or Iceberg ArrowReader can offer directly, because parquet-hadoop operates at the lowest level and exposes every Parquet optimization primitive. The trade-off is that these must be wired up manually, whereas Iceberg provides them semi-automatically through its scan planning layer.

### Summary Decision Matrix

| Use Case | Recommended Approach | Rationale |
|---|---|---|
| Standalone .parquet from S3 | Enhanced parquet-hadoop | Full pushdown control, already a dependency, reliable S3 |
| Iceberg tables from S3 | Iceberg ArrowReader | Partition pruning, metadata-driven planning, Arrow vectors |
| Maximum performance (future) | Arrow Dataset API (JNI) | C++ vectorized decoding -- but wait for maturity |
| Maximum pushdown | parquet-hadoop + FilterApi | Statistics + dictionary + bloom + column index + page skipping |

---

## Sources

- [Apache Arrow Java Dataset Documentation](https://arrow.apache.org/docs/java/dataset.html)
- [Apache Arrow Java Cookbook - Dataset](https://arrow.apache.org/cookbook/java/dataset.html)
- [Apache Iceberg ArrowReader Javadoc](https://iceberg.apache.org/javadoc/latest/org/apache/iceberg/arrow/vectorized/ArrowReader.html)
- [parquet-java/parquet-hadoop README](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/README.md)
- [parquet-arrow SchemaConverter source](https://github.com/apache/parquet-java/blob/master/parquet-arrow/src/main/java/org/apache/parquet/arrow/schema/SchemaConverter.java)
- [parquet-arrow on Maven Central](https://mvnrepository.com/artifact/org.apache.parquet/parquet-arrow)
- [ARROW-6720: JNI Parquet Adapter PR](https://github.com/apache/arrow/pull/5522)
- [arrow-java#227: Push-down filtering in Java](https://github.com/apache/arrow-java/issues/227)
- [arrow-java#219: Unable to read S3 files using Arrow Dataset](https://github.com/apache/arrow-java/issues/219)
- [arrow#39919: JNI Error when reading parquet file](https://github.com/apache/arrow/issues/39919)
- [Iceberg PR #2286: Add Arrow vectorized reader](https://github.com/apache/iceberg/pull/2286)
- [Iceberg PR #4938: Support reading Parquet row group bloom filter](https://github.com/apache/iceberg/pull/4938)
- [Iceberg #6003: Vectorized Read issues](https://github.com/apache/iceberg/issues/6003)
- [Blake Smith: How to use Parquet Java without Hadoop (2024)](https://blakesmith.me/2024/10/05/how-to-use-parquet-java-without-hadoop.html)
- [PARQUET-1822: Parquet without Hadoop dependencies](https://issues.apache.org/jira/browse/PARQUET-1822)
- [Baeldung: Introduction to Java Parquet](https://www.baeldung.com/java-intro-to-apache-parquet)
- [Parquet Bloom Filter Spec](https://parquet.apache.org/docs/file-format/bloomfilter/)
- [Arrow Parquet Late Materialization Blog Post (2025)](https://arrow.apache.org/blog/2025/12/11/parquet-late-materialization-deep-dive/)
- [DeepWiki: Reading Parquet Files in parquet-java](https://deepwiki.com/apache/parquet-java/4.2-reading-parquet-files)
- [FilterApi source (parquet-java)](https://github.com/apache/parquet-java/blob/master/parquet-column/src/main/java/org/apache/parquet/filter2/predicate/FilterApi.java)
- [RowGroupFilter source (parquet-java)](https://github.com/apache/parquet-java/blob/master/parquet-hadoop/src/main/java/org/apache/parquet/filter2/compat/RowGroupFilter.java)

### Elasticsearch Codebase Files Referenced

- `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetFormatReader.java`
- `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetStorageObjectAdapter.java`
- `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-parquet/build.gradle`
- `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-parquet/README.md`
- `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergSourceOperatorFactory.java`
- `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergPushdownFilters.java`
- `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergTableCatalog.java`
- `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-iceberg/build.gradle`
- `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-iceberg/README.md`
- `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/test/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetFormatReaderTests.java`
