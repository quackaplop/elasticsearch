# Parquet Reader Benchmark Plan

## Goal

Answer: **which Parquet reader gives the best performance for ES|QL external data sources, given realistic pushdown scenarios, measured end-to-end through ESQL Page output?**

---

## 1. Readers Under Test

| # | Reader | Library | Native? | Pushdown Surface |
|---|--------|---------|---------|-----------------|
| R1 | **parquet-mr + pushdowns** | parquet-java 1.16.0 | No | FilterPredicate, schema projection, column index, bloom filter, dictionary filter |
| R2 | **Arrow Dataset JNI** | arrow-dataset 18.3.0 | Yes (C++) | ScanOptions.columns, ScanOptions.substraitFilter |
| R3 | **Iceberg ArrowReader** | iceberg-arrow + parquet-mr | No | Iceberg Expression pushdown, schema projection |
| R4 | **Arrow Dataset JNI + zero-copy Block** | Same as R2 | Yes | Same as R2 |
| R5 | **Iceberg ArrowReader + zero-copy Block** | Same as R3 | No | Same as R3 |

R4/R5 require building a thin Block implementation that wraps Arrow FieldVector memory directly instead of copying element-by-element. This isolates reader performance from conversion overhead.

---

## 2. Pushdowns to Test

| Pushdown | parquet-mr (R1) | Arrow Dataset (R2/R4) | Iceberg (R3/R5) |
|----------|----------------|----------------------|-----------------|
| **Projection** | `requestedSchema` subset | `ScanOptions.columns()` | `scan.select()` |
| **Limit** | Manual: stop iterating after N rows | Manual: stop after N batches/rows | Manual: stop after N rows |
| **Row group stats** | `FilterPredicate` + `useStatsFilter` | Substrait filter expression | Iceberg `Expression` |
| **Page index** | `useColumnIndexFilter` + `readNextFilteredRowGroup()` | Not exposed via JNI | Via parquet-mr underneath |
| **Bloom filter** | `useBloomFilter` | Not exposed via JNI | Via parquet-mr underneath |
| **Dictionary filter** | `useDictionaryFilter` | Not exposed via JNI | Via parquet-mr underneath |

---

## 3. Test Datasets

All datasets use **NYC Taxi (TLC) Yellow Trip Data** — a real-world, widely-used benchmark dataset. No synthetic data generation needed.

### D1: Full monthly file (realistic scan)
- **Source**: NYC TLC Yellow Taxi, single month (e.g. `yellow_tripdata_2024-01.parquet`)
- **Shape**: ~3M rows, 19 columns
- **Types**: TIMESTAMP (tpep_pickup/dropoff_datetime), DOUBLE (fare, tip, distance), INT64 (passenger_count, payment_type, RatecodeID), STRING (store_and_fwd_flag, VendorID)
- **Sorted by**: pickup_datetime (natural ordering in TLC data — enables row group pruning tests)
- **Compression**: Snappy (as published by TLC)
- **Size**: ~50-100MB per monthly file
- **Purpose**: Tests throughput, predicate pushdown on timestamps, row group pruning, limit pushdown

### D3: Small subset (startup overhead)
- **Source**: First 100K rows of a D1 file (extract with PyArrow or DuckDB one-liner)
- **Shape**: 100K rows, same 19 columns
- **Purpose**: Tests reader startup overhead, metadata parsing cost, time-to-first-Page
- **Size**: ~2-3MB

### Storage Locations

| Dataset | AWS S3 | Azure Blob |
|---------|--------|------------|
| **D1** | `s3://nyc-tlc/trip data/yellow_tripdata_2024-01.parquet` | `wasbs://nyctlc@azureopendatastorage.blob.core.windows.net/yellow/puYear=2024/puMonth=1/` |
| **D3** | Upload extracted subset to own bucket | Upload extracted subset to own container |
| **HTTP** (fallback) | `https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2024-01.parquet` | Same HTTP source |

**Note**: Azure data is partitioned by year/month with Hive-style paths. S3 has flat files. For local benchmarking, just download via HTTP — no credentials needed.

### D3 Generation (one-liner)
```bash
# Using DuckDB CLI
duckdb -c "COPY (SELECT * FROM 'yellow_tripdata_2024-01.parquet' LIMIT 100000) TO 'yellow_tripdata_100k.parquet' (FORMAT PARQUET)"
# Or PyArrow
python3 -c "import pyarrow.parquet as pq; t = pq.read_table('yellow_tripdata_2024-01.parquet'); pq.write_table(t.slice(0, 100000), 'yellow_tripdata_100k.parquet')"
```

---

## 4. Benchmark Scenarios

Each scenario = (Reader, Dataset, Pushdown combination)

### S1: Full scan, all columns
- Read entire file, all columns → Page
- Measures raw throughput without pushdown benefit

### S2: Projection pushdown
- D2 (wide table): read 5 columns out of 100
- Measures I/O reduction from column pruning

### S3: Limit pushdown
- D1: read first 1000 rows, all columns
- Measures time-to-first-result and ability to stop early

### S4: Predicate + row group pruning
- D1: `WHERE timestamp > '2024-06-01' AND timestamp < '2024-06-02'` (~10% selectivity)
- Data sorted by timestamp → row groups have tight min/max stats
- Measures row group skipping effectiveness

### S5: High-selectivity filter
- D1: `WHERE trace_id = '<specific_value>'` (~0.001% selectivity)
- Tests bloom filter benefit (R1 and R3 only — Arrow JNI doesn't expose bloom filters)

### S6: Combined pushdowns
- D2: `WHERE col_0 > 1000` + projection to 10 columns + LIMIT 10000
- Realistic query combining all pushdowns

### S7: Conversion overhead isolation
- Same data, compare R2 vs R4 (Arrow JNI copy vs wrap)
- Same data, compare R3 vs R5 (Iceberg copy vs wrap)
- Isolates the Page conversion cost

---

## 5. Metrics

| Metric | Unit | How |
|--------|------|-----|
| **Throughput** | rows/sec, MB/sec | Total rows / wall time |
| **Latency to first Page** | ms | Time from reader open to first Page available |
| **Total time** | ms | Open → last Page → close |
| **Peak memory** | bytes | Track via MemoryMXBean (heap) + BufferAllocator (off-heap) |
| **Pages produced** | count | Total Pages emitted |
| **Row groups read** | count | vs total row groups (measures pruning) |
| **GC pressure** | ms | G1GC pause time during run |

---

## 6. Project Structure

```
benchmarks/parquet-reader/
├── build.gradle                    # Dependencies: parquet-mr, arrow-dataset, iceberg-arrow, JMH
├── src/main/java/
│   └── org/elasticsearch/benchmark/parquet/
│       ├── ParquetBenchmark.java           # JMH benchmark class
│       ├── readers/
│       │   ├── ParquetMrReader.java        # R1: parquet-mr with all pushdowns
│       │   ├── ArrowDatasetReader.java     # R2: Arrow Dataset JNI
│       │   ├── IcebergArrowReader.java     # R3: Iceberg ArrowReader
│       │   └── ReaderInterface.java        # Common interface
│       ├── conversion/
│       │   ├── GroupToPage.java            # parquet-mr Group → Page
│       │   ├── ArrowToPage.java            # ArrowToBlockConverter path (copy)
│       │   └── ArrowWrapPage.java          # Zero-copy Block wrapping Arrow vectors
│       ├── pushdown/
│       │   ├── PushdownConfig.java         # Describes pushdowns for a scenario
│       │   ├── ParquetMrPushdown.java      # Builds FilterPredicate, requestedSchema
│       │   ├── ArrowPushdown.java          # Builds ScanOptions with Substrait
│       │   └── IcebergPushdown.java        # Builds Iceberg Expression
│       └── data/
│           └── DatasetGenerator.java       # Generates test Parquet files
├── src/main/resources/
│   └── scenarios.json                      # Benchmark scenario definitions
└── scripts/
    ├── generate-data.sh                    # Generate test datasets
    ├── run-local.sh                        # Run on local machine
    └── run-aws.sh                          # Run on AWS EC2
```

### Location & Dependency Strategy

**Standalone repo**: `elastic/esql-parquet-benchmark` (private). No binary exchange with ES repo.

Any ES compute classes needed (Page, Block, BlockFactory) are **copied as source** into the benchmark repo. This keeps the two repos completely independent — no mavenLocal, no submodules, no published artifacts.

---

## 7. Build Considerations

### Dependencies needed
```gradle
dependencies {
    // ES compute classes — copied as source into src/main/java/
    // (Page, Block interfaces, BlockFactory, block builders)

    // parquet-mr
    implementation 'org.apache.parquet:parquet-hadoop-bundle:1.16.0'
    implementation 'org.apache.hadoop:hadoop-client-api:3.4.1'

    // Arrow Dataset (JNI)
    implementation 'org.apache.arrow:arrow-dataset:18.3.0'
    implementation 'org.apache.arrow:arrow-memory-unsafe:18.3.0'

    // Iceberg Arrow
    implementation 'org.apache.iceberg:iceberg-arrow:1.8.1'  // check ES version

    // JMH
    implementation 'org.openjdk.jmh:jmh-core:1.37'
    annotationProcessor 'org.openjdk.jmh:jmh-generator-annprocess:1.37'
}
```

### Native library (Arrow JNI)
`arrow-dataset:18.3.0` JAR includes pre-built natives for Linux x64/aarch64 and macOS x64/aarch64. `JniLoader` extracts them at runtime. No extra packaging needed.

---

## 8. Zero-Copy Block Wrapper (R4/R5)

**Already implemented** in PR #142981 (`swallez:esql/arrow-native`, DRAFT).

Sylvain's PR adds Arrow-native Block & Vector implementations backed by `ArrowBuf`:
- `DoubleArrowBufBlock/Vector`, `IntArrowBufBlock/Vector`, `LongArrowBufBlock/Vector`
- `FloatArrowBufBlock/Vector`, `BooleanArrowBufBlock/Vector`, `BytesRefArrowBufBlock/Vector`
- `AbstractArrowBufBlock` / `AbstractArrowBufVector` base classes
- `CircuitBreakerAllocationListener` — bridges Arrow allocator to ES circuit breaker
- Refactored `ArrowToBlockConverter` to produce Arrow-native blocks instead of copying

Initial benchmark from the PR (dense doubles, ns/op — lower is better):
```
sequential  double/arrow      1.614   (fastest)
sequential  double/array      1.691
sequential  double/vector     1.523
random      double/arrow      2.026   (fastest non-vector)
random      double/array      2.272
random      double/vector     4.069
```

**Source for benchmark**: Pull the Arrow-native block code from `swallez:esql/arrow-native` branch.
Key files under `x-pack/plugin/esql/compute/src/main/java/org/elasticsearch/compute/data/arrow/`:
- `AbstractArrowBufBlock.java` (457 lines)
- `AbstractArrowBufVector.java` (108 lines)
- `BooleanArrowBufBlock.java`, `BytesRefArrowBufBlock.java` (hand-written)
- `{Double,Float,Int,Long}ArrowBufBlock.java` (generated from template)
- `{Double,Float,Int,Long}ArrowBufVector.java` (generated from template)
- `CircuitBreakerAllocationListener.java`

PR also adds `libs/arrow` module and updates `ArrowToBlockConverter` to use the new blocks.

**Status**: DRAFT, with open TODOs around BytesRef null handling in MV entries and allocator lifecycle. For benchmark purposes, the primitive-type blocks are ready to use.

---

## 9. Execution Strategy

### Phase 1: Local (your Mac, ARM)
- Generate small datasets (D3 + scaled-down D1/D2)
- Implement all 5 readers + conversion paths
- Run JMH benchmarks
- Validate correctness (all readers produce identical Pages)
- Get initial numbers

### Phase 2: AWS (Linux x86_64)
- EC2 instance: c6i.2xlarge (8 vCPU, 16GB) or similar
- Generate full-size datasets (D1, D2, D3)
- Run same JMH benchmarks
- Compare numbers across architectures
- Test with data on local NVMe AND S3 (if relevant)

### Phase 3: Analysis
- Produce comparison table: reader × scenario × metric
- Identify which reader wins for which use case
- Quantify the copy-vs-wrap overhead (R2 vs R4, R3 vs R5)
- Recommend reader choice for ES external data sources

---

## 10. Apples-to-Apples Guarantees

1. **Same data**: All readers read identical Parquet files
2. **Same output**: All readers produce ESQL Page objects; correctness validated by comparing Page contents
3. **Same JVM**: Same JDK, same JVM flags, same heap size
4. **Same machine**: Same EC2 instance or same laptop
5. **Same warmup**: JMH handles warmup iterations
6. **Same batch size**: All readers configured for same batch size (~32K rows)
7. **Pushdown parity**: When testing a pushdown, all readers that support it have it enabled; readers that don't support it are marked as such in results

---

## 11. Open Questions

1. **Where to get Iceberg version?** Check `versions.iceberg` in ES gradle. Need matching iceberg-arrow JAR.
2. **Substrait expression construction**: Arrow JNI filter pushdown requires Substrait binary. Need `io.substrait:isthmus` dependency for SQL→Substrait conversion.
3. **S3 testing**: Worth testing with data on S3? Adds latency dimension but also complexity. Could be Phase 3.
4. **Thread model**: Run single-threaded first (isolate reader performance), then multi-threaded (realistic).
5. **JDK version**: Match ES's JDK (21+).
