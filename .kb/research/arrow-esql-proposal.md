# Proposal: Apache Arrow Integration in ES|QL for External Data Sources

**Purpose:** Concrete proposals for how to leverage Apache Arrow in ES|QL's external data source architecture, with risks and pitfalls.

---

## 1. Current State Assessment

### 1.1 What ES|QL Already Has
ES|QL has three Arrow integration points today (all v18.3.0):

1. **Arrow IPC output** — production-ready serialization of query results for Arrow-aware clients
2. **Arrow Flight connector** — reads from external Flight servers with parallel split support
3. **Iceberg/Arrow reader** — vectorized Parquet reading via `iceberg-arrow` (async integration incomplete)

Additionally, the `datasources/` framework provides:
- `StorageProvider` SPI for blob stores (S3, GCS, HTTP)
- `FormatReader` SPI for file formats (Parquet, CSV, NDJSON)
- `TableCatalog` SPI for catalogs (Iceberg)
- `ConnectorFactory` SPI for connectors (Flight/gRPC)
- `FilterPushdownSupport` SPI for predicate pushdown
- `DataSourceModule` with lazy loading to defer heavy dependencies

### 1.2 Key Design Decision Already Made
The `FormatReader` SPI outputs ES|QL `Page` — NOT Arrow vectors. Javadoc states: *"to avoid mandating Arrow as a dependency for all format implementations."* This is deliberate and means Arrow is isolated to specific paths.

### 1.3 The Gap
The current architecture has Arrow at the **edges** (input from Flight/Iceberg, output for clients) but not in the **middle** (format readers produce Pages, not Arrow). This creates conversion boundaries:

```
Current:  Parquet → parquet-hadoop → Page → Compute
          Flight → Arrow VectorSchemaRoot → Page → Compute
          Iceberg → Arrow ArrowReader → Page → Compute (incomplete)
```

---

## 2. Proposal: Three Integration Levels

We propose three levels of Arrow integration, each building on the previous, with increasing benefit but also increasing risk.

### Level 1: Arrow as Universal External Source Exchange Format (Recommended First Step)

**What:** Standardize on Arrow `VectorSchemaRoot` as the internal exchange format between external sources and the ES|QL compute engine. All external format readers convert to Arrow first, then a single `ArrowToBlockConverter` converts to ES|QL Pages.

```
Proposed:  Parquet → Arrow VectorSchemaRoot → Page → Compute
           CSV    → Arrow VectorSchemaRoot → Page → Compute
           JSON   → Arrow VectorSchemaRoot → Page → Compute
           Flight → Arrow VectorSchemaRoot → Page → Compute  (already works)
           Iceberg→ Arrow VectorSchemaRoot → Page → Compute  (already works)
```

**Why:**
- **Single conversion path**: One `ArrowToBlockConverter` instead of per-format converters
- **Leverage Arrow readers**: Arrow's CSV/JSON/Parquet readers are mature and optimized
- **Ecosystem compatibility**: Any Arrow-producing library becomes a potential source
- **Already proven**: Flight and Iceberg paths already use this pattern

**Implementation:**
1. Extend `ArrowToBlockConverter` to cover all ES|QL types (currently 9 Arrow types → need ~15)
2. Create `ArrowFormatReader` wrapper that adapts Arrow file readers to produce `VectorSchemaRoot`
3. Optionally keep current `ParquetFormatReader` as a fast path (avoids Arrow intermediary for Parquet → Page)

**Effort:** Medium — `ArrowToBlockConverter` needs expansion, Arrow file readers need wrapping
**Risk:** Low — additive change, existing paths unaffected

### Level 2: Arrow-Native Pushdown Framework

**What:** Leverage Arrow's ecosystem for pushdown optimizations, particularly for Parquet reading from blob stores.

**Key insight from DataFusion:** Effective pushdowns operate at three layers:
1. **Optimizer rules** (generic) — ES|QL already has these
2. **Source-level metadata** (format-specific) — row group pruning, bloom filters
3. **Decode-time filtering** (format-specific) — late materialization

**Implementation for Parquet (highest impact):**

**2a. Row Group Pruning:**
- When planning a Parquet scan, read Parquet metadata (footer + row group stats)
- Translate ES|QL filter expressions to Parquet predicate evaluators
- Skip row groups where statistics prove no rows can match
- **Benefit:** Can eliminate 90%+ of I/O for selective queries on partitioned/sorted data

**2b. Projection Pushdown:**
- Already partially supported via `FormatReader.read(object, projectedColumns, batchSize)`
- Ensure column projection reaches the Parquet column chunk level (only decompress needed columns)

**2c. Bloom Filter Support:**
- For equality predicates (`WHERE id = 'abc'`), check Parquet bloom filters before reading row groups
- Parquet stores optional bloom filters per column per row group

**2d. Page-Level Pruning (stretch goal):**
- Use Parquet page index (column index + offset index) for finer-grained skipping
- Higher complexity, incremental benefit over row group pruning

**Effort:** High — requires Parquet metadata reading, predicate translation, integration with ES|QL optimizer
**Risk:** Medium — predicate translation is complex, edge cases in type mapping

### Level 3: Arrow Flight as Federation Protocol

**What:** Use Arrow Flight SQL as the standard protocol for federating queries to external systems. ES|QL becomes both a Flight SQL client (querying external databases) and potentially a Flight SQL server (serving ES|QL results).

**As Flight SQL Client:**
- Query external databases (PostgreSQL, Snowflake, DuckDB) that expose Flight SQL endpoints
- Push predicates, projections, and limits to the remote system via SQL
- Receive results as Arrow batches — zero conversion needed with Level 1 in place

**As Flight SQL Server (stretch goal):**
- Expose ES|QL as a Flight SQL endpoint
- Enable external systems (DataFusion, Spark, Polars) to query Elasticsearch via Arrow Flight
- Much more efficient than current REST JSON API for analytical workloads

**Implementation:**
1. Add `flight-sql` dependency (currently not used)
2. Implement Flight SQL client in `FlightConnectorFactory` — translate ES|QL predicates to SQL WHERE clauses
3. For server: implement `FlightSqlProducer` backed by ES|QL query execution

**Effort:** High — Flight SQL client is moderate; server is significant
**Risk:** Medium — Flight SQL spec is stable but Java implementation maturity varies

---

## 3. Recommended Implementation Sequence

```
Phase 1 (Near-term):  Level 1 — Arrow as exchange format
                      + Row group pruning for Parquet (Level 2a)
                      + Projection pushdown to column level (Level 2b)

Phase 2 (Mid-term):   Bloom filter support (Level 2c)
                      + Flight SQL client (Level 3 partial)
                      + Complete Iceberg async integration

Phase 3 (Long-term):  Page-level pruning (Level 2d)
                      + Flight SQL server (Level 3 complete)
                      + Dynamic filter pushdown for external joins
```

---

## 4. Risks, Pitfalls & Mitigations

### 4.1 Memory Management Complexity (HIGH RISK)

**Risk:** Arrow's memory model (off-heap, reference-counted `ArrowBuf`, `BufferAllocator` hierarchy) conflicts with ES|QL's memory model (circuit breaker + `BlockFactory`).

**Evidence:**
- ES already shims Arrow's allocator to no-op for output (`AllocationManagerShim`)
- Flight connector uses real `RootAllocator` — creating a parallel memory tracking path
- Iceberg reader also uses `RootAllocator(Long.MAX_VALUE)` — no memory limit

**Pitfall:** Two memory accounting systems means neither has a complete picture. Under memory pressure, Arrow allocations can bypass circuit breakers and cause OOM.

**Mitigation:**
- Implement a custom `BufferAllocator` backed by ES's circuit breaker
- Convert Arrow buffers to ES|QL Blocks eagerly (don't hold Arrow memory across operator boundaries)
- Set hard limits on Arrow `RootAllocator` matching available circuit breaker headroom
- Add memory accounting bridge: Arrow allocation listeners → circuit breaker reservation

### 4.2 Dependency Weight (MEDIUM RISK)

**Risk:** Arrow pulls in significant transitive dependencies (gRPC, Netty, Protobuf, Guava, Jackson, FlatBuffers). Version conflicts with ES's own dependencies.

**Evidence:**
- gRPC already pinned at 1.78.0 because 1.79.0 breaks Arrow Flight
- Arrow transitives must be excluded via `ExcludeAllTransitivesRule`
- Netty version alignment required
- Adding `flight-sql` would pull in more gRPC/Protobuf dependencies

**Mitigation:**
- Continue excluding transitives and managing versions explicitly
- Consider classloader isolation (separate classloader for Arrow-heavy plugins)
- Pin Arrow version and upgrade deliberately, testing gRPC/Netty compatibility
- Monitor Arrow Java release notes for breaking changes

### 4.3 Arrow Java Maturity Gap (MEDIUM RISK)

**Risk:** Arrow Java is less mature than C++/Python/Rust implementations. Key gaps:
- No native compute kernel library (all compute must be application-level)
- Dataset API is JNI to C++ and marked "early development"
- No pure-Java filesystem abstraction for S3/GCS
- Smaller community than C++/Rust

**Pitfall:** Investing heavily in Arrow Java patterns that the community later deprecates or changes.

**Mitigation:**
- Use Arrow Java for what it's good at: type system, IPC, Flight protocol
- Don't depend on Arrow's Dataset API or JNI-based features
- Keep ES|QL's own compute engine — don't try to use Arrow compute
- Use ES's own StorageProvider SPI for blob store access
- Follow Arrow Java releases closely; the standalone repo (since v18.2.0) suggests increasing independence

### 4.4 Type System Mismatch (MEDIUM RISK)

**Risk:** Arrow's type system doesn't perfectly match ES|QL's. Known gaps:
- ES|QL `ip` type has no Arrow equivalent (mapped to VARBINARY with custom logic)
- ES|QL `version` type requires binary-to-string transformation
- ES|QL `geo_point`/`geo_shape` use WKB encoding in VARBINARY
- ES|QL `unsigned_long` maps to Arrow UINT8 but Java doesn't have unsigned types natively
- Arrow FLOAT4 must be upcast to DOUBLE (ES|QL has no float type)
- Timestamp precision: ES|QL uses millis; Arrow sources may send micros/nanos

**Pitfall:** Silent data corruption from incorrect type conversion, especially for edge cases (NaN, Infinity, max values, timezone handling).

**Mitigation:**
- `elastic:type` metadata on Arrow fields (already implemented) preserves original type info
- Comprehensive round-trip tests for all type combinations (ArrowResponseTests already covers 20 types)
- Explicit error on unsupported Arrow types rather than silent fallback
- Document the type mapping contract

### 4.5 Performance Regression Risk (LOW-MEDIUM RISK)

**Risk:** Adding Arrow as an intermediary format adds a conversion step. For formats that can produce ES|QL Pages directly (like the current `ParquetFormatReader`), going through Arrow first could be slower.

**Evidence from DataFusion:** Their Parquet filter pushdown (late materialization) was implemented but NOT enabled by default due to performance regressions on some queries.

**Mitigation:**
- Keep direct Parquet → Page path as fast path
- Arrow intermediary only for sources that naturally produce Arrow (Flight, Iceberg, Arrow IPC files)
- Benchmark before/after for all format readers
- Make Arrow intermediary opt-in per format, not mandatory

### 4.6 gRPC/Flight Version Coupling (LOW RISK)

**Risk:** Arrow Flight depends on specific gRPC versions. gRPC 1.79.0 already broke Arrow Flight 18.3.0 (`ReadableBuffer.readBytes` removal). Future gRPC or Netty updates may cause similar breaks.

**Mitigation:**
- Pin gRPC version explicitly (already done)
- Test gRPC upgrades against Arrow Flight before adopting
- Monitor Arrow Flight release notes for gRPC compatibility changes
- Consider gRPC shading if version conflicts become severe

### 4.7 Security Surface (LOW RISK)

**Risk:** Arrow Flight opens a gRPC channel to external servers. `arrow-memory-unsafe` uses `sun.misc.Unsafe` and requires `--add-opens`.

**Mitigation:**
- Flight connections should go through ES's security infrastructure (TLS, authentication)
- Validate all incoming Arrow data (schema, buffer sizes, validity bitmaps)
- `--add-opens` already required and configured in build.gradle
- Consider sandboxing Arrow operations in a security-constrained context

---

## 5. What NOT to Do

1. **Don't replace ES|QL's compute engine with Arrow compute** — Arrow Java has no compute kernels. ES|QL's engine (Blocks, Pages, Operators, Drivers) is mature and well-optimized for its use case.

2. **Don't use Arrow's Dataset API (JNI)** — it requires native libraries, is marked "early development," and adds JNI complexity. Use ES's own StorageProvider + FormatReader SPIs.

3. **Don't make Arrow mandatory for all format readers** — the current design (FormatReader → Page) is correct for formats that can produce Pages directly. Arrow should be an option, not a requirement.

4. **Don't try to use Arrow's filesystem abstraction** — ES has its own StorageProvider SPI with S3/GCS support. Arrow's Java filesystem abstractions require JNI to C++.

5. **Don't adopt Arrow's memory management wholesale** — ES's circuit breaker model is production-proven. Bridge Arrow memory into circuit breakers, don't replace them.

6. **Don't assume Arrow Java and Arrow C++/Rust have feature parity** — they don't. Java is significantly behind on compute, filesystem, and dataset functionality.

---

## 6. Architecture Decision: Where Do Pushdowns Live?

Based on DataFusion's proven model and ES|QL's existing architecture:

### 6.1 Logical Optimizer (Generic, Format-Agnostic)
- Filter pushdown (move predicates toward source) — **already exists**
- Projection pushdown (eliminate unused columns) — **already exists**
- Limit pushdown — **already exists**
- These rules work identically for Lucene sources and external sources

### 6.2 Physical Optimizer (Source-Specific)
- **For Lucene**: `PushFiltersToSource`, `PushTopNToSource`, etc. — **already exists**
- **For External Sources**: New rules needed:
  - `PushFiltersToExternalSource` — translate ES|QL predicates to format-specific filters (Parquet row group pruning, Iceberg partition pruning)
  - `PushProjectionToExternalSource` — ensure column projection reaches the reader
  - `PushLimitToExternalSource` — pass limit to source for early termination

### 6.3 Source/Format Level (Deepest)
- Parquet: row group stats, bloom filters, page index
- Iceberg: partition pruning, manifest filtering
- Flight: pass predicates to remote server (if supported)
- CSV/JSON: projection only (no predicate pushdown at source level)

### 6.4 The `FilterPushdownSupport` SPI
ES|QL already has this interface. It needs:
- Integration with the physical optimizer (currently it exists but isn't fully wired)
- Per-format predicate translation (ES|QL expression → Parquet filter, Iceberg expression, SQL WHERE)
- Capability declaration: "I can handle equality, comparison, IN, IS NULL on these columns"

---

## 7. Summary

| Recommendation | Priority | Effort | Risk | Benefit |
|---------------|----------|--------|------|---------|
| Arrow as exchange format (Level 1) | High | Medium | Low | Single conversion path, ecosystem compat |
| Parquet row group pruning (Level 2a) | High | High | Medium | 90%+ I/O reduction for selective queries |
| Projection to column level (Level 2b) | High | Low | Low | Significant I/O savings on wide tables |
| Bloom filter support (Level 2c) | Medium | Medium | Low | Better equality predicate pruning |
| Flight SQL client (Level 3 partial) | Medium | Medium | Medium | Federated queries to external DBs |
| Memory management bridge | High | Medium | High | Safety — prevents OOM from Arrow allocations |
| Page-level pruning (Level 2d) | Low | High | Medium | Incremental benefit over row group pruning |
| Flight SQL server (Level 3 complete) | Low | High | Medium | Enables external systems to query ES via Arrow |
| Dynamic filter pushdown | Low | High | Medium | Join optimization for external sources |

The highest-impact, lowest-risk starting point is **Level 1 + row group pruning + memory management bridge**. This gives ES|QL a unified external source pipeline with meaningful performance optimization, without architectural risk.

---

## References

- Deep dive backing files: `/tmp/deep-dive-arrow-codebase.md`, `/tmp/deep-dive-arrow-public.md`, `/tmp/deep-dive-datafusion.md`, `/tmp/deep-dive-es-arrow.md`
- Reconciliation: `/tmp/reconciliation.md` — all 15 claims verified TRUE
- DataFusion pushdown blog: https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/
- DataFusion filter pushdown: https://datafusion.apache.org/blog/2025/03/21/parquet-pushdown/
- Arrow overview: https://arrow.apache.org/overview/
- Arrow Flight benchmarking: https://arxiv.org/abs/2204.03032
