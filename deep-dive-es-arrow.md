# Deep Dive: Apache Arrow Usage in Elasticsearch

**Date:** 2026-03-04
**Scope:** Current Arrow usage in the Elasticsearch codebase (branch: main)

---

## 1. Overview

Arrow is used in three distinct areas, all version **18.3.0**, all in `x-pack`:

1. **ES|QL Arrow output format** (`x-pack/plugin/esql/arrow/`) — serializing query results as Arrow IPC
2. **Arrow Flight data source** (`x-pack/plugin/esql-datasource-grpc/`) — reading from Flight servers
3. **Iceberg data source** (`x-pack/plugin/esql-datasource-iceberg/`) — Arrow vectorized Parquet reading

---

## 2. Dependencies

### 2.1 Arrow Output Module
**Build:** `x-pack/plugin/esql/arrow/build.gradle`
- `arrow-vector:18.3.0`, `arrow-format:18.3.0`, `arrow-memory-core:18.3.0` (implementation)
- `arrow-memory-unsafe:18.3.0` (runtimeOnly)
- `flatbuffers-java:23.5.26`

### 2.2 Flight Connector
**Build:** `x-pack/plugin/esql-datasource-grpc/build.gradle`
- `flight-core:18.3.0`, `arrow-vector:18.3.0`, `arrow-memory-core:18.3.0` (implementation)
- `arrow-memory-unsafe:18.3.0` (runtimeOnly)
- gRPC 1.78.0 (pinned — 1.79.0 breaks Arrow Flight 18.3.0)

### 2.3 Iceberg
**Build:** `x-pack/plugin/esql-datasource-iceberg/build.gradle`
- `iceberg-arrow` (implementation, Arrow transitives excluded)
- `arrow-vector:18.3.0`, `arrow-memory-core:18.3.0` (compileOnly — runtime from esql/arrow)

### 2.4 Dependency Management
- All Arrow transitives excluded (`ExcludeAllTransitivesRule` in ComponentMetadataRulesPlugin.java:192-194)
- Arrow's memory manager shimmed to no-op in production (`AllocationManagerShim`)

---

## 3. Arrow Output Format (Block → Arrow IPC)

### 3.1 Entry Point
`EsqlResponseListener.java:142-148` — when `format=arrow`, creates `ArrowResponse`

### 3.2 ArrowResponse
Implements `ChunkedRestResponseBodyPart`, streams three segments:
1. **Schema** — `MessageSerializer.serialize(schema)`, maps ESQL columns to Arrow Fields
2. **Pages** — each ESQL `Page` → `ArrowRecordBatch` with custom `BufWriter` closures
3. **EOS** — `ArrowStreamWriter.writeEndOfStream()`

**Key design**: Does NOT use Arrow memory. Creates dummy `ArrowBuf` for size tracking, writes directly from ESQL Block data via closures.

### 3.3 Type Mapping (Output)
| ES|QL Type | Arrow Type | Notes |
|-----------|-----------|-------|
| boolean | BIT | Bitpacked |
| integer, counter_integer | INT | |
| long, counter_long | BIGINT | |
| unsigned_long | UINT8 | |
| double, counter_double | FLOAT8 | |
| keyword, text | VARCHAR | |
| date | TIMESTAMPMILLI | Millis since epoch |
| ip | VARBINARY | IPv4-mapped shortened to 4 bytes |
| geo_*, cartesian_* | VARBINARY | WKB format |
| version | VARCHAR | Binary→string transform |
| _source | VARCHAR | xcontent→JSON transform |
| null, unsupported | NULL | |

Each field includes `elastic:type` metadata.

### 3.4 Multivalued Fields
Encoded as Arrow variable-size LIST wrapping the value vector.

---

## 4. Arrow Flight Connector (Arrow → Block)

### 4.1 Architecture
```
FlightConnectorFactory.resolveMetadata()
  → FlightClient.getSchema() → FlightTypeMapping.toAttributes()

FlightConnector.execute()
  → FlightClient.getInfo() → ticket
  → FlightClient.getStream(ticket) → FlightStream
    → FlightResultCursor → Page(blocks)
```

### 4.2 Key Classes
- **GrpcDataSourcePlugin** — handles `flight://` and `grpc://` schemes
- **FlightConnectorFactory** — default port 47470, schema discovery, split provider
- **FlightConnector** — manages FlightClient, uses real RootAllocator
- **FlightResultCursor** — wraps FlightStream, converts VectorSchemaRoot → Page
- **FlightTypeMapping** — Arrow→ESQL type mapping
- **FlightSplitProvider** — multi-endpoint parallel reads via FlightEndpoint→FlightSplit

### 4.3 Type Mapping (Input)
| Arrow Type | ESQL Type |
|-----------|----------|
| Int (≤32 bit) | INTEGER |
| Int (>32 bit) | LONG |
| FloatingPoint | DOUBLE |
| Utf8 | KEYWORD |
| Bool | BOOLEAN |
| Timestamp | DATETIME |

---

## 5. Iceberg Data Source

### 5.1 Vectorized Reading
`IcebergSourceOperatorFactory` uses Iceberg's `ArrowReader` for vectorized Parquet:
- Creates `ArrowReader` with configurable page size
- Opens `CloseableIterator<ColumnarBatch>`
- Adapter extracts `FieldVector` from Iceberg's `ColumnVector` wrappers
- Creates `VectorSchemaRoot` from extracted vectors

### 5.2 Status
`get(DriverContext)` throws `UnsupportedOperationException` — full async integration TODO.

---

## 6. What ES Does NOT Use from Arrow

- `arrow-dataset` (Dataset API) — uses Iceberg reader instead
- `arrow-gandiva` (expression compiler) — has own compute engine
- `arrow-jdbc` (JDBC adapter)
- `arrow-compression` (codecs)
- `arrow-c-data` (C Data Interface)
- `flight-sql` (Flight SQL protocol)
- Arrow compute kernels (not available in pure Java)

---

## 7. FormatReader SPI Design Decision

`FormatReader` deliberately outputs ESQL `Page` NOT Arrow vectors: "to avoid mandating Arrow as a dependency for all format implementations." The Parquet format reader (`ParquetFormatReader`) reads Parquet natively and produces Pages directly — no Arrow involved.

---

## 8. GitHub Activity

### PRs (from `gh pr list --search "arrow"`)
Active development on Arrow output format, Flight connector, and Iceberg integration.

### Known Issues
- gRPC 1.79.0 breaks Arrow Flight 18.3.0 (build.gradle:62 comment)
- Counter type signedness question (ArrowResponse.java:387)
- Potential optimization: direct memory dumps from ESQL blocks (BlockConverter TODOs)
- CBOR/SMILE support for _source (ArrowResponse.java:416)
