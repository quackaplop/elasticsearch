# Elasticsearch Parquet Reader Implementation: Deep Analysis

> **Last verified: 2026-03-17 against elastic/main (fetched fresh from elastic/elasticsearch)**
>
> **Current state on elastic/main:**
> - **Columnar reading via `ColumnReadStoreImpl` + `ColumnReader`** — DONE (#143703, merged 2026-03-06). `GroupRecordConverter` is gone. No more columnar→row→columnar anti-pattern.
> - **Column projection pushdown** — DONE. Both reader-level (`buildProjectedSchema`) and optimizer-level (`PruneColumns` handles `ExternalRelation`, #143903).
> - **Limit pushdown** — DONE (#143515)
> - **Row-group-level split parallelism** — DONE (#144018)
> - **Statistics extraction** — DONE (#143940, `extractStatistics()` reads min/max/null counts from row group metadata)
> - **Type support gaps fixed** — DONE (#144059: INT96, DECIMAL, Float16, LIST, UUID)
> - **Buffer reuse** — DONE (#143700)
> - **Allocation overhead fixes** — DONE (#143791)
> - **Predicate/filter pushdown** — NOT IMPLEMENTED
> - **Bloom filters** — NOT IMPLEMENTED
> - **Row group pruning via predicates** — NOT IMPLEMENTED (statistics are extracted but not used for skipping)
> - **No Arrow in the read path** (Arrow only used for Flight output format)

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

### 1.4 Row Group Reading (ParquetColumnIterator)

**File:** `ParquetFormatReader.java` (class `ParquetColumnIterator`, lines ~418-548)

As of #143703 (merged 2026-03-06), the reader uses **true columnar reading** via `ColumnReadStoreImpl` and `ColumnReader`. `GroupRecordConverter` and the `Group` intermediate are completely gone.

The iterator (`ParquetColumnIterator`) maintains state:
- `columnReaders[]`: one `ColumnReader` per projected column
- `columnInfos[]`: per-column metadata (descriptor, type, def/rep levels)
- `rowsRemainingInGroup`: countdown within the current row group
- `rowBudget`: limit pushdown tracking (#143515)

**`hasNext()` (lines ~477-493):**
1. If `rowBudget` exhausted, returns false.
2. If `rowsRemainingInGroup > 0`, returns true.
3. Otherwise, calls `advanceRowGroup()` which reads next row group and creates `ColumnReader` per column via `ColumnReadStoreImpl`.

**`next()` (lines ~518-548):**
1. For each column, calls `readColumnBlock(columnReaders[col], info, rowsToRead)` which reads directly into typed arrays (`boolean[]`, `int[]`, `long[]`, `double[]`, `BytesRef[]`).
2. Builds ESQL Blocks from the arrays (e.g., `blockFactory.newIntArrayVector(values, rows).asBlock()`).
3. No `Group` objects, no per-row dispatch — pure column-at-a-time decoding.

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

**Status: FULLY IMPLEMENTED on elastic/main**

Both layers are in place:

1. **Reader level:** `buildProjectedSchema()` creates a projected `MessageType`. Only projected column chunks are read from the file.
2. **Optimizer level:** `PruneColumns` handles `ExternalRelation` via `pruneColumnsInExternalRelation()`, so unused columns are pruned before reaching the reader.

### 2.2 Predicate/Filter Pushdown

**Status: NOT IMPLEMENTED**

- `ParquetFormatReader` does not implement `FilterPushdownSupport`.
- No Parquet filter pushdown is registered in `ParquetDataSourcePlugin`.
- Only Iceberg has filter pushdown (via `IcebergPushdownFilters`).

**What parquet-mr supports that is unused:**
- `ParquetFileReader.readFilteredRowGroup()` -- reads a row group with a filter predicate applied
- `FilterCompat` / `FilterApi` / `FilterPredicate` -- row-group-level and page-level filtering
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

> **Updated 2026-03-17:** Type mapping significantly expanded by #144059.

| Parquet Physical Type | Parquet Logical Type | ESQL DataType | Notes |
|---|---|---|---|
| `BOOLEAN` | any | `BOOLEAN` | Direct mapping |
| `INT32` | `DateLogicalTypeAnnotation` | `DATETIME` | Date as days since epoch |
| `INT32` | `DecimalLogicalTypeAnnotation` | `DOUBLE` | Decimal → double |
| `INT32` | other/none | `INTEGER` | |
| `INT64` | `TimestampLogicalTypeAnnotation` | `DATETIME` | |
| `INT64` | `DecimalLogicalTypeAnnotation` | `DOUBLE` | |
| `INT64` | other/none | `LONG` | |
| `INT96` | - | `DATETIME` | **Fixed in #144059** — legacy timestamps now supported |
| `FLOAT` | any | `DOUBLE` | Widened to double |
| `DOUBLE` | any | `DOUBLE` | Direct mapping |
| `BINARY` / `FIXED_LEN_BYTE_ARRAY` | `DecimalLogicalTypeAnnotation` | `DOUBLE` | **Fixed in #144059** |
| `BINARY` / `FIXED_LEN_BYTE_ARRAY` | `Float16LogicalTypeAnnotation` | `DOUBLE` | **Added in #144059** |
| `BINARY` / `FIXED_LEN_BYTE_ARRAY` | other/none | `KEYWORD` | |
| Non-primitive (Group) | `ListLogicalTypeAnnotation` | Element type | **Added in #144059** — LIST of primitives supported |
| Non-primitive (Group) | other | `UNSUPPORTED` | MAP, STRUCT still unsupported |

### Remaining Type Gaps

| Parquet Type | Gap |
|---|---|
| Nested types (MAP, STRUCT) | Returns `UNSUPPORTED`. LIST of primitives is now supported. |
| `TIME` logical type (INT32/INT64) | Maps to INTEGER/LONG, not recognized as time. |
| `ENUM` logical type | Maps to KEYWORD via binary fallback. Works but not explicit. |

### Type Conversion Notes

> Most of the previous type conversion issues (DATE bug, timestamp normalization, binary handling)
> were fixed by #143703 and #144059. The columnar reader uses `ColumnReader` directly with proper
> per-type methods (`cr.getInteger()`, `cr.getLong()`, `cr.getBoolean()`, `cr.getBinary()`),
> and has dedicated handlers for DATE (days→millis), INT96 timestamps, and DECIMAL types.

---

## 4. Performance-Relevant Observations

### 4.1 Record-at-a-Time Materialization

> **FIXED (#143703, merged 2026-03-06).** `GroupRecordConverter` and `Group` objects are gone.
> The reader now uses `ColumnReadStoreImpl` + `ColumnReader` for true column-at-a-time decoding
> directly into typed arrays → ESQL Blocks. No more columnar→row→columnar anti-pattern.

### 4.2 ~~No~~ Projection at the Parquet I/O Level

> **FIXED (2026-03-09, #143903):** Physical projection is now implemented via `buildProjectedSchema()`.
> The `RecordReader` is created with the projected schema, so only requested column chunks
> are read from storage, decoded, and decompressed. Non-projected columns are skipped at I/O level.

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

> **Partially addressed:** With #143703, `Group` objects are gone. The reader now allocates typed arrays (`int[]`, `long[]`, etc.) directly, which are smaller and shorter-lived. However, these arrays are still outside circuit breaker accounting.

### 4.8 No Row Group Size Awareness

The reader has no awareness of row group sizes when determining batch sizes. If a row group has 1M rows and `batchSize` is 1024, it will make 1000 `recordReader.read()` calls within that row group. This is fine logically but means the entire row group's column data must be kept in memory (inside `PageReadStore`) while only a slice is being converted.

---

## 5. Specific Improvement Opportunities

### 5.1 Physical Column Projection — DONE

> **Fully implemented on elastic/main.** Both reader-level `buildProjectedSchema()` and
> optimizer-level `PruneColumns` for `ExternalRelation` are merged.

### 5.2 Direct Column Reading — DONE

> **Merged in #143703 (2026-03-06).** `GroupRecordConverter` replaced with `ColumnReadStoreImpl`
> + `ColumnReader`. True column-at-a-time decoding directly into ESQL Blocks.

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

### 5.7 ~~Cache Field Name to Index Mapping~~ — DONE

> No longer applicable. The columnar reader (#143703) pre-builds `ColumnInfo[]` and
> `ColumnReader[]` arrays indexed by column position. No name lookups during reading.

### 5.8 ~~Add Missing Type Mappings~~ — MOSTLY DONE

> Fixed by #144059: INT96, DECIMAL, Float16, LIST of primitives, UUID all supported.
> Remaining gaps: MAP, STRUCT, TIME logical type.

### 5.9 ~~INT32 DATE Bug Fix~~ — DONE

> Fixed by the columnar reader (#143703). `readDatetimeColumn()` has a dedicated
> `readInt32DateColumn()` path that reads `cr.getInteger()` and converts days→millis.

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

## 7. Key Files (elastic/main, 2026-03-17)

| File | Lines | Role |
|---|---|---|
| `ParquetFormatReader.java` | 1088 | Main reader: schema discovery, columnar reading, type conversion, split discovery |
| `ParquetDataSourcePlugin.java` | ~50 | Plugin entry point, registers format reader |
| `ParquetStorageObjectAdapter.java` | ~215 | Adapts StorageObject to Parquet InputFile + SeekableInputStream |
| `FormatReadContext.java` | — | Consolidated read context (projectedColumns, batchSize, rowLimit) |
| `RangeAwareFormatReader.java` | — | SPI for row-group-level split parallelism |
| `PruneColumns.java` | — | Optimizer: prunes unused columns for ExternalRelation |
| `PushFiltersToSource.java` | — | Optimizer: filter pushdown infrastructure (exists but unused by Parquet) |

All files under `x-pack/plugin/esql-datasource-parquet/` and `x-pack/plugin/esql/`.
