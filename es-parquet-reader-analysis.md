# Elasticsearch Parquet Reader Implementation: Deep Analysis

## 1. Complete Read Pipeline (File Open to Page Output)

### 1.1 Entry Points

The Parquet reader is registered as an ESQL `FormatReader` via the plugin system.

**Plugin registration** (`ParquetDataSourcePlugin.java:38-54`):
```java
public class ParquetDataSourcePlugin extends Plugin implements DataSourcePlugin {
    public Map<String, FormatReaderFactory> formatReaders(Settings settings) {
        return Map.of("parquet", (s, blockFactory) -> new ParquetFormatReader(blockFactory));
    }
}
```

The `ExternalSourceOperatorFactory` (in the core ESQL module) invokes the `FormatReader.read()` method, passing projected column names extracted from ESQL `Attribute` objects:

**Operator creation** (`ExternalSourceOperatorFactory.java:94-111`):
```java
public SourceOperator get(DriverContext driverContext) {
    List<String> projectedColumns = new ArrayList<>(attributes.size());
    for (Attribute attr : attributes) {
        projectedColumns.add(attr.name());
    }
    StorageObject storageObject = storageProvider.newObject(path);
    CloseableIterator<Page> pages = formatReader.read(storageObject, projectedColumns, batchSize);
    return new ExternalSourceOperator(pages, driverContext);
}
```

### 1.2 Schema Discovery Phase

**File:** `ParquetFormatReader.java:67-86`

1. `ParquetStorageObjectAdapter` wraps the `StorageObject` as a Parquet `InputFile`.
2. `ParquetReadOptions` are built with `SKIP_ROW_GROUPS` metadata filter -- this avoids reading row group metadata during schema-only reads (line 77):
   ```java
   ParquetReadOptions options = ParquetReadOptions.builder()
       .withMetadataFilter(ParquetMetadataConverter.SKIP_ROW_GROUPS).build();
   ```
3. `ParquetFileReader.open()` reads the file footer (magic bytes + metadata).
4. `getFileMetaData().getSchema()` extracts the `MessageType` schema.
5. Each top-level field is mapped via `convertParquetTypeToEsql()` to an ESQL `DataType` and wrapped in a `ReferenceAttribute`.

### 1.3 Data Reading Phase

**File:** `ParquetFormatReader.java:88-122`

1. `ParquetStorageObjectAdapter` wraps the `StorageObject` again (no caching of the adapter).
2. `ParquetReadOptions.builder().build()` -- **default options, no filter, no column filter, no bloom filter config**.
3. `ParquetFileReader.open()` reads the **entire file footer including all row group metadata**.
4. Schema is read again (`getFileMetaData().getSchema()`) and converted to attributes.
5. Projected column names are matched against schema attributes by name (lines 106-119). Unmatched projected columns get `DataType.NULL`.
6. A `ParquetPageIterator` is created with the reader, full schema, projected attributes, batch size, and block factory.

### 1.4 Row Group Reading (ParquetPageIterator)

**File:** `ParquetFormatReader.java:173-383`

The iterator maintains state:
- `currentRowGroup`: the `PageReadStore` for the current row group
- `recordReader`: a `RecordReader<Group>` for materializing records
- `rowsRemainingInGroup`: countdown within the current row group

**`hasNext()` (lines 202-223):**
1. If `rowsRemainingInGroup > 0`, returns true immediately.
2. Otherwise, calls `reader.readNextRowGroup()` which reads the next row group's column chunks from the file.
3. Creates a `RecordReader<Group>` using `GroupRecordConverter` over the **full schema** (line 218):
   ```java
   recordReader = columnIO.getRecordReader(currentRowGroup, new GroupRecordConverter(parquetSchema));
   ```

**`next()` (lines 226-253):**
1. Reads up to `batchSize` records via `recordReader.read()`, collecting them into a `List<Group>`.
2. Calls `convertToPage(batch)` to transform the list into an ESQL `Page`.

### 1.5 Page Conversion

**File:** `ParquetFormatReader.java:255-288`

For each projected attribute:
1. `findFieldIndex()` does a linear scan of the Group's schema fields to find the field by name (lines 290-301).
2. `createBlock()` dispatches on `DataType` to create typed blocks (lines 271-287).
3. Each block builder iterates over all `Group` objects in the batch, checking `getFieldRepetitionCount()` for nulls (e.g., line 306) and appending values.

### 1.6 I/O Adapter: ParquetStorageObjectAdapter

**File:** `ParquetStorageObjectAdapter.java:27-215`

Wraps `StorageObject` as Parquet's `InputFile`:
- `getLength()`: delegates to `storageObject.length()` (line 43-45)
- `newStream()`: creates a `StorageObjectSeekableInputStream` (line 48-50)

The `StorageObjectSeekableInputStream` (lines 62-214):
- Maintains a current `InputStream` and tracks `position`.
- **Forward seeks**: attempts `InputStream.skip()`. If skip fails, reopens the stream at the target position (line 97-99).
- **Backward seeks**: closes current stream, opens a new range-read stream at the new position via `storageObject.newStream(newPos, remainingBytes)` (lines 115-126).
- **ByteBuffer reads**: allocates temporary `byte[]`, reads into it, then copies to `ByteBuffer` (lines 191-205). This involves an **unnecessary extra copy** for every ByteBuffer-based read.

---

## 2. Pushdowns: Implemented vs Missing

### 2.1 Column Projection Pushdown

**Status: PARTIALLY IMPLEMENTED (logical only, not physical)**

The reader accepts a `List<String> projectedColumns` parameter (line 89) and filters the attributes list accordingly (lines 104-119). However:

- **The Parquet `RecordReader` is created over the FULL schema** (line 218):
  ```java
  recordReader = columnIO.getRecordReader(currentRowGroup, new GroupRecordConverter(parquetSchema));
  ```
  This means ALL columns in the file are deserialized into `Group` objects, then only the projected columns are extracted during block creation.

- **True column projection pushdown** would require creating a projected `MessageType` schema (using `MessageType.select()` or `Types.buildMessage()` with only needed columns) and passing that to both `ColumnIOFactory.getColumnIO()` and `GroupRecordConverter`. Parquet-mr natively supports this -- the `ParquetFileReader` would then skip reading column chunks that aren't needed.

- **Impact**: On wide tables (e.g., 100+ columns), the current approach reads and deserializes all column data even when the query only needs 2-3 columns. This is the single most impactful missing optimization.

### 2.2 Predicate/Filter Pushdown

**Status: NOT IMPLEMENTED**

- `ParquetFormatReader` does not implement `FilterPushdownSupport` (confirmed by grep).
- `ParquetDataSourcePlugin` does not override `filterPushdownSupport(Settings)` (returns default empty map).
- The `FormatReader.read()` SPI signature does not accept any filter/predicate parameter (line 58 of `FormatReader.java`):
  ```java
  CloseableIterator<Page> read(StorageObject object, List<String> projectedColumns, int batchSize)
  ```
- The `PushFiltersToSource` optimizer rule (line 54) does check for `ExternalSourceExec` and delegates to `FilterPushdownRegistry`, but no Parquet pushdown is registered.

**What parquet-mr supports that is unused:**
- `ParquetFileReader.readFilteredRowGroup()` -- reads a row group with a filter predicate applied
- `FilterCompat` -- Parquet's filter compatibility layer
- `FilterApi` / `FilterPredicate` -- Parquet's filter predicate API for row-group-level and page-level filtering
- `ParquetReadOptions.builder().withRecordFilter(filter)` -- record-level filtering

### 2.3 Row Group Pruning

**Status: NOT IMPLEMENTED**

- `reader.readNextRowGroup()` is called unconditionally (line 212). Every row group is read sequentially.
- Parquet row groups contain min/max statistics in their metadata. `ParquetFileReader.getRowGroups()` returns `BlockMetaData` objects with `ColumnChunkMetaData.getStatistics()` that can be used to skip entire row groups.
- The reader does not examine row group statistics at all.

### 2.4 Bloom Filters

**Status: NOT IMPLEMENTED**

- No references to bloom filters anywhere in the parquet datasource module.
- Parquet 1.16.0 (used per `build.gradle:13`) supports bloom filter reading via `BloomFilterReader` and `ParquetFileReader.readBloomFilter()`.
- Bloom filters enable efficient equality predicate pushdown (e.g., `WHERE id = 'abc'`).

### 2.5 Page-Level Statistics (Column Index)

**Status: NOT IMPLEMENTED**

- No references to `ColumnIndex`, `OffsetIndex`, or `PageIndex` in the parquet datasource.
- Parquet 1.11+ supports page-level statistics that enable skipping individual data pages within a column chunk, providing finer-grained pruning than row-group-level statistics.

### 2.6 Comparison with Iceberg Reader

The Iceberg reader (`IcebergSourceOperatorFactory.java:52-261`) demonstrates a more advanced approach:

1. **Filter pushdown**: Accepts an `Expression filter` parameter (line 58) and applies it via `scan.filter(filter)` (line 141). Iceberg translates this to row group pruning + file pruning.
2. **Column projection**: Uses `scan.select(columnNames)` (line 149) which propagates to the Parquet reader underneath.
3. **Vectorized reading**: Uses Iceberg's `ArrowReader` which reads Parquet data directly into Arrow columnar vectors (line 164), avoiding row-by-row materialization.
4. **Batch size control**: Passes `pageSize` to `ArrowReader` for batch size (line 164).

---

## 3. Type Mapping (Parquet Types to ESQL Types)

**File:** `ParquetFormatReader.java:149-171`

| Parquet Physical Type | Parquet Logical Type | ESQL DataType | Notes |
|---|---|---|---|
| `BOOLEAN` | any | `BOOLEAN` | Direct mapping |
| `INT32` | `DateLogicalTypeAnnotation` | `DATETIME` | Date as days since epoch |
| `INT32` | other/none | `INTEGER` | |
| `INT64` | `TimestampLogicalTypeAnnotation` | `DATETIME` | Timestamp as millis/micros |
| `INT64` | other/none | `LONG` | |
| `FLOAT` | any | `DOUBLE` | Widened to double (line 160) |
| `DOUBLE` | any | `DOUBLE` | Direct mapping |
| `BINARY` | `StringLogicalTypeAnnotation` | `KEYWORD` | UTF-8 string |
| `BINARY` | other/none | `KEYWORD` | All binary defaults to keyword |
| `FIXED_LEN_BYTE_ARRAY` | `StringLogicalTypeAnnotation` | `KEYWORD` | |
| `FIXED_LEN_BYTE_ARRAY` | other/none | `KEYWORD` | |
| Non-primitive (Group) | - | `UNSUPPORTED` | Line 151 |
| `INT96` | - | `UNSUPPORTED` | Falls through to default |
| Any with `DECIMAL` logical | - | Mapped by physical type | Decimal annotation is ignored |

### Missing Type Mappings

| Parquet Type | Gap |
|---|---|
| `INT96` (legacy timestamp) | Not mapped. Many older Parquet files (Hive, Spark < 3.0) use INT96 for timestamps. Returns `UNSUPPORTED`. |
| `DECIMAL` (logical) | Decimal logical annotation is completely ignored. INT32/INT64 decimals get mapped as INTEGER/LONG without scale adjustment. FIXED_LEN_BYTE_ARRAY decimals become KEYWORD. |
| Nested types (MAP, LIST, STRUCT) | All non-primitive types return `UNSUPPORTED` (line 151). No flattening or nested access. |
| `ENUM` logical type | BINARY with ENUM annotation maps to KEYWORD (falls through to default binary handling). Works correctly by accident. |
| `UUID` logical type | FIXED_LEN_BYTE_ARRAY with UUID maps to KEYWORD. The bytes are not formatted as UUID string. |
| `TIME` logical type (INT32/INT64) | Maps to INTEGER/LONG respectively. Not recognized as a time type. |
| `INT32` with `IntLogicalTypeAnnotation` (INT_8, INT_16, UINT_8, etc.) | Always maps to INTEGER regardless of bit width or signedness. |

### Type Conversion Issues

1. **FLOAT widening** (line 160): `FLOAT` maps to `DataType.DOUBLE`. During block creation, `group.getFloat()` is called and widened via `builder.appendDouble(group.getFloat(...))` (line 353). This is correct but introduces floating-point representation changes.

2. **DATE handling** (line 158): INT32 + DateLogicalTypeAnnotation maps to `DATETIME`, but the value is read as a raw integer via `group.getLong()` (line 285 dispatches to `createLongBlock`). Actually this will fail at runtime -- `INT32` stored values will be read via `getInteger()` if someone passes the correct type, but the mapping sends it to `createLongBlock` which calls `group.getLong()`. This would throw a `ClassCastException` at runtime for INT32 DATE columns because `Group.getLong()` on an INT32 field will fail.

3. **Timestamp handling** (line 159): INT64 + TimestampLogicalTypeAnnotation maps to `DATETIME` and reads as long. The reader does not check whether the timestamp is in MILLIS, MICROS, or NANOS, and does not normalize. ESQL typically expects millisecond timestamps.

4. **Binary handling** (lines 364-376): All BINARY/FIXED_LEN_BYTE_ARRAY values are read via `group.getString()` which attempts UTF-8 conversion (line 370). For non-string binary data, this will produce garbage or throw.

---

## 4. Performance-Relevant Observations

### 4.1 Record-at-a-Time Materialization

**Critical Issue** (lines 232-248):

```java
List<Group> batch = new ArrayList<>(batchSize);
for (int i = 0; i < rowsToRead; i++) {
    Group group = recordReader.read();
    batch.add(group);
}
```

The reader uses `GroupRecordConverter` which materializes **every row as a `Group` object** (essentially a row-oriented representation). This is then converted column-by-column into ESQL Blocks.

This is the **worst-case usage pattern** for Parquet:
- Parquet stores data in columnar format for efficient columnar access.
- The `Group` API reconstructs rows from columns (de-columnarizing the data).
- The block builders then re-columnarize the data.
- This is a **columnar -> row -> columnar** round-trip that negates Parquet's columnar benefits.

**Better approach**: Use Parquet's `ColumnReader` API directly to read each column independently and populate ESQL blocks directly without the `Group` intermediate.

### 4.2 No Projection at the Parquet I/O Level

As noted in section 2.1, the `RecordReader` is created with the full schema (line 218). This means:
- All column chunks in each row group are read from storage (I/O cost).
- All columns are decoded and decompressed (CPU cost).
- All columns are materialized in `Group` objects (memory cost).
- Only then are non-projected columns discarded during block creation.

### 4.3 Linear Field Lookup

**File:** `ParquetFormatReader.java:290-301`

```java
private int findFieldIndex(Group group, String fieldName) {
    for (int i = 0; i < fieldCount; i++) {
        if (name.equals(fieldName)) return i;
    }
    return -1;
}
```

This is called for **every column of every batch**. The field name -> index mapping could be cached once at iterator construction time.

### 4.4 ByteBuffer Extra Copy

**File:** `ParquetStorageObjectAdapter.java:191-205`

```java
public int read(java.nio.ByteBuffer buf) throws IOException {
    byte[] temp = new byte[bytesToRead];
    int bytesRead = read(temp, 0, bytesToRead);
    if (bytesRead > 0) {
        buf.put(temp, 0, bytesRead);
    }
    return bytesRead;
}
```

Allocates a temporary byte array for every ByteBuffer read, then copies into the buffer. Parquet-mr uses ByteBuffer reads extensively for column chunk decoding. This extra allocation + copy adds GC pressure and reduces throughput.

If the ByteBuffer has a backing array (`buf.hasArray()`), the read can go directly into `buf.array()` at `buf.arrayOffset() + buf.position()`.

### 4.5 No Async I/O

The `FormatReader.read()` method is synchronous. While `FormatReader.readAsync()` exists (line 66-80), it simply wraps the sync method in an executor. The `ParquetStorageObjectAdapter` uses synchronous `InputStream` reads. For remote storage (S3, HTTP), this blocks the thread during each column chunk read.

### 4.6 Stream Reopening on Backward Seeks

**File:** `ParquetStorageObjectAdapter.java:108-126`

Backward seeks close the current stream and open a new one via `storageObject.newStream(newPos, remainingBytes)`. For remote storage, this means a new HTTP request per backward seek. Parquet reading involves many backward seeks (footer at end of file, then column chunks at various positions). Each seek could trigger a new HTTP range request.

### 4.7 Memory: No Circuit Breaker Integration

The `blockFactory` is obtained from ESQL's infrastructure and uses circuit breakers. However, the intermediate `List<Group>` objects (line 233) and the `Group` objects themselves are allocated outside circuit breaker accounting. For large batch sizes or wide schemas, this can consume significant untracked heap memory.

### 4.8 No Row Group Size Awareness

The reader has no awareness of row group sizes when determining batch sizes. If a row group has 1M rows and `batchSize` is 1024, it will make 1000 `recordReader.read()` calls within that row group. This is fine logically but means the entire row group's column data must be kept in memory (inside `PageReadStore`) while only a slice is being converted.

---

## 5. Specific Improvement Opportunities

### 5.1 Physical Column Projection (High Impact, Moderate Effort)

Create a projected `MessageType` from `projectedColumns` and use it for both the `ColumnIOFactory` and `GroupRecordConverter`:

```java
// Build projected schema
List<Type> projectedFields = new ArrayList<>();
for (String col : projectedColumns) {
    if (parquetSchema.containsField(col)) {
        projectedFields.add(parquetSchema.getType(col));
    }
}
MessageType projectedSchema = new MessageType(parquetSchema.getName(), projectedFields);

// Use projected schema for reading
MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(projectedSchema, parquetSchema);
RecordReader<Group> reader = columnIO.getRecordReader(rowGroup, new GroupRecordConverter(projectedSchema));
```

This would eliminate I/O, decompression, and decoding for non-projected columns.

### 5.2 Direct Column Reading (High Impact, High Effort)

Replace `GroupRecordConverter` + `Group` with direct `ColumnReader` usage. Read each projected column independently and populate ESQL `BlockBuilder` directly:

```java
// For each column, get ColumnReader from PageReadStore
ColumnReadStore columnReadStore = new ColumnReadStoreImpl(
    rowGroup, new DummyRecordConverter(projectedSchema).getRootConverter(),
    projectedSchema, /* createdBy */ null
);
for (ColumnDescriptor col : projectedSchema.getColumns()) {
    ColumnReader reader = columnReadStore.getColumnReader(col);
    // Read values directly into BlockBuilder
}
```

This eliminates the columnar -> row -> columnar round-trip entirely.

### 5.3 Row Group Pruning via Statistics (High Impact, Low Effort)

After reading footer metadata, examine row group statistics before calling `readNextRowGroup()`:

```java
List<BlockMetaData> rowGroups = reader.getRowGroups();
for (BlockMetaData block : rowGroups) {
    ColumnChunkMetaData chunkMeta = block.getColumnChunkMetaData(filterColumn);
    Statistics<?> stats = chunkMeta.getStatistics();
    if (stats.hasNonNullValue() && canSkipRowGroup(stats, filterPredicate)) {
        continue; // Skip this row group
    }
    reader.readRowGroup(block);
}
```

### 5.4 Filter Pushdown via FilterPushdownSupport SPI (High Impact, Moderate Effort)

Implement `FilterPushdownSupport` in `ParquetDataSourcePlugin`:

1. Convert ESQL `Expression` predicates to Parquet `FilterPredicate` objects.
2. Use `ParquetFileReader.readFilteredRowGroup()` or `FilterCompat.get(filter)`.
3. Register in `filterPushdownSupport()` for the "parquet" source type.

The infrastructure already exists in the optimizer (`PushFiltersToSource.planFilterExecForExternalSource()`).

### 5.5 Bloom Filter Support (Medium Impact, Low Effort)

For equality predicates, read bloom filters from row group metadata:

```java
BloomFilter bloomFilter = reader.readBloomFilter(columnChunkMetaData);
if (bloomFilter != null && !bloomFilter.findHash(bloomFilter.hash(value))) {
    // Skip this row group -- value definitely not present
}
```

### 5.6 Fix ByteBuffer Read Performance (Low Impact, Low Effort)

```java
public int read(java.nio.ByteBuffer buf) throws IOException {
    if (buf.hasArray()) {
        int bytesRead = read(buf.array(), buf.arrayOffset() + buf.position(), buf.remaining());
        if (bytesRead > 0) buf.position(buf.position() + bytesRead);
        return bytesRead;
    }
    // fallback to temp array for direct buffers
    ...
}
```

### 5.7 Cache Field Name to Index Mapping (Low Impact, Trivial Effort)

Build a `Map<String, Integer>` once per row group (or once per iterator) instead of linear-scanning for each column of each batch.

### 5.8 Add Missing Type Mappings (Medium Impact, Low Effort)

- **INT96 timestamps**: Convert to millis-since-epoch for DATETIME.
- **DECIMAL**: Read with proper scale adjustment, map to ESQL's numeric types.
- **Nested types**: At minimum, support flattened access to struct fields.
- **Timestamp unit normalization**: Check `TimestampLogicalTypeAnnotation.getUnit()` and normalize to millis.

### 5.9 INT32 DATE Bug Fix (Correctness, Trivial)

The current mapping of INT32 + DateLogicalTypeAnnotation to DATETIME (line 158) dispatches to `createLongBlock` (line 285), which calls `group.getLong()`. But INT32 fields should use `group.getInteger()`. This will fail at runtime. The fix: either map INT32 DATE to a dedicated handler that reads the integer and converts to a long (days-to-millis), or map it to INTEGER and let ESQL handle the conversion.

---

## 6. Dependencies

**File:** `build.gradle:11-49`

| Dependency | Version | Purpose |
|---|---|---|
| `parquet-hadoop-bundle` | 1.16.0 | Parquet format reading (bundled to avoid jar hell) |
| `hadoop-client-api` | 3.4.2 | Hadoop `Configuration` class (required by Parquet internals) |
| `hadoop-client-runtime` | 3.4.2 | Hadoop runtime (required by Parquet internals) |

The Hadoop dependency exists solely because `ParquetReadOptions.Builder()` internally creates a `HadoopParquetConfiguration` which requires the `Configuration` class (build.gradle lines 38-45). This is a parquet-mr limitation -- it was designed with Hadoop as a core dependency.

---

## 7. Summary of Files Analyzed

| File | Absolute Path | Lines | Role |
|---|---|---|---|
| ParquetFormatReader.java | `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetFormatReader.java` | 384 | Main reader: schema discovery, data reading, type conversion |
| ParquetDataSourcePlugin.java | `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetDataSourcePlugin.java` | 54 | Plugin entry point, registers format reader |
| ParquetStorageObjectAdapter.java | `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/main/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetStorageObjectAdapter.java` | 215 | Adapts StorageObject to Parquet InputFile + SeekableInputStream |
| build.gradle | `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-parquet/build.gradle` | 147 | Dependencies: parquet 1.16.0, hadoop 3.4.2 |
| ParquetFormatReaderTests.java | `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/test/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetFormatReaderTests.java` | 518 | Unit tests for schema reading, data reading, projection, batching |
| ParquetStorageObjectAdapterTests.java | `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-parquet/src/test/java/org/elasticsearch/xpack/esql/datasource/parquet/ParquetStorageObjectAdapterTests.java` | 288 | Unit tests for seek, read, ByteBuffer operations |
| FormatReader.java (SPI) | `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FormatReader.java` | 85 | SPI interface: no filter parameter in read() |
| FilterPushdownSupport.java (SPI) | `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/spi/FilterPushdownSupport.java` | 128 | SPI for filter pushdown: YES/NO/RECHECK model |
| IcebergSourceOperatorFactory.java | `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql-datasource-iceberg/src/main/java/org/elasticsearch/xpack/esql/datasource/iceberg/IcebergSourceOperatorFactory.java` | 261 | Iceberg reader: has filter pushdown, column projection, vectorized Arrow reading |
| ExternalSourceOperatorFactory.java | `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/datasources/ExternalSourceOperatorFactory.java` | 298 | Invokes FormatReader.read() with projected columns |
| PushFiltersToSource.java | `/Users/oleglvovitch/github/mine/arrow/elasticsearch/x-pack/plugin/esql/src/main/java/org/elasticsearch/xpack/esql/optimizer/rules/physical/local/PushFiltersToSource.java` | 287 | Optimizer rule: external source filter pushdown infrastructure exists but unused by Parquet |
