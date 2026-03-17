# Arrow in Elasticsearch: Current State

**Purpose:** Concise overview of how Elasticsearch uses Apache Arrow today.

---

## 1. Three Integration Points

Elasticsearch uses Arrow v18.3.0 in three areas, all within `x-pack`:

### 1.1 ES|QL Arrow Output Format
**Module:** `x-pack/plugin/esql/arrow/`
**Purpose:** Serialize ES|QL query results as Arrow IPC streaming format.
**Content type:** `application/vnd.apache.arrow.stream`

The implementation is production-ready and cleverly avoids Arrow's memory manager entirely. `ArrowResponse` creates dummy `ArrowBuf` objects for size tracking only, writing directly from ES|QL Block data via `BufWriter` closures. `AllocationManagerShim` reflectively replaces Arrow's default allocator with one that throws `UnsupportedOperationException`.

**Type coverage:** 20 ES|QL types mapped to Arrow types (boolean→BIT, integer→INT, long→BIGINT, double→FLOAT8, keyword→VARCHAR, date→TIMESTAMPMILLI, ip→VARBINARY, geo types→VARBINARY/WKB, etc.). Multivalued fields use Arrow's variable-size LIST layout. Each field carries `elastic:type` metadata.

**Reverse direction:** `ArrowToBlockConverter` converts Arrow vectors back to ES|QL Blocks, supporting 9 Arrow types (FLOAT4, FLOAT8, BIGINT, INT, BIT, VARCHAR, VARBINARY, TIMESTAMPMICRO, TIMESTAMPMICROTZ).

### 1.2 Arrow Flight Data Source
**Module:** `x-pack/plugin/esql-datasource-grpc/`
**Purpose:** Query external data sources via Arrow Flight protocol.
**Schemes:** `flight://`, `grpc://`

Full Flight client implementation:
- `FlightConnectorFactory` discovers schema via `FlightClient.getSchema()`
- `FlightConnector` executes queries via `getInfo()` + `getStream(ticket)`
- `FlightResultCursor` wraps `FlightStream` → ES|QL `Page` conversion
- `FlightSplitProvider` enables parallel reads across multiple FlightEndpoints
- Uses **real** `RootAllocator` (Flight needs actual Arrow memory for received batches)

**Known constraint:** gRPC pinned at 1.78.0 because 1.79.0 breaks Arrow Flight 18.3.0 (internal API removal: `ReadableBuffer.readBytes`).

### 1.3 Iceberg Data Source
**Module:** `x-pack/plugin/esql-datasource-iceberg/`
**Purpose:** Query Iceberg tables stored in S3 using Arrow vectorized Parquet reading.

Uses `iceberg-arrow`'s `ArrowReader` for high-performance columnar reads. Arrow dependencies are `compileOnly` (provided at runtime by the esql/arrow module). The async operator integration is incomplete — `get(DriverContext)` throws `UnsupportedOperationException`.

---

## 2. What ES Does NOT Use

| Arrow Component | Status | Why Not |
|----------------|--------|---------|
| `arrow-dataset` | Not used | Uses Iceberg reader or ParquetFormatReader instead |
| `arrow-gandiva` | Not used | ES|QL has its own compute engine |
| `flight-sql` | Not used | Uses raw Flight protocol only |
| `arrow-jdbc` | Not used | |
| `arrow-compute` | Not available | Arrow Java has no compute kernel library |
| Arrow filesystem (S3/GCS) | Not used | Uses own StorageProvider SPI |

---

## 3. Key Design Decision

The `FormatReader` SPI deliberately outputs ES|QL `Page` — NOT Arrow vectors. From the Javadoc: *"to avoid mandating Arrow as a dependency for all format implementations."* The `ParquetFormatReader` has zero Arrow imports — it reads Parquet natively via `parquet-hadoop`.

This means Arrow is **isolated** to three specific paths: output serialization, Flight ingestion, and Iceberg reading. The core compute engine and generic format readers are Arrow-free.

---

## 4. Dependencies & Management

All Arrow transitives excluded via `ExcludeAllTransitivesRule`. Jackson versions aligned with ES. Memory manager shimmed in production. Arrow is loaded lazily — only when a query actually targets an Arrow-related backend.
