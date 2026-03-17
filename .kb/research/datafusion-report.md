# Apache DataFusion: Architecture, Pushdowns & Capabilities

**Purpose:** Understanding DataFusion as an Arrow-native query engine and its pushdown model, for informing ES|QL integration decisions.

---

## 1. What DataFusion Is

Apache DataFusion is a **fast, extensible query engine written in Rust** that uses Apache Arrow as its native in-memory format. It provides both SQL and DataFrame APIs and is designed to be embedded in other systems — not used as a standalone database.

Key stats:
- **40+ production systems** use DataFusion (InfluxDB 3.0, Comet/Spark, GlareDB, GreptimeDB, Delta Lake, Ballista, Arroyo, and more)
- **Fastest single-node Parquet engine** on ClickBench as of Nov 2024 [[1]](#ref-1)
- **30%+ performance improvement** between v34 and v43 in one year
- Published at **SIGMOD 2024** as a peer-reviewed paper [[2]](#ref-2)

---

## 2. Architecture

### 2.1 Pipeline
```
SQL / DataFrame API
        │
   SQL Parser (sqlparser-rs)
        │
   Logical Planner → LogicalPlan
        │
   Logical Optimizer (rule-based, max 16 passes)
        │
   Physical Planner → ExecutionPlan
        │
   Physical Optimizer (rule-based)
        │
   Execution Engine (streaming, vectorized, multi-threaded)
        │
   Arrow RecordBatches
```

### 2.2 Comparison with ES|QL Pipeline
```
ES|QL:      Parse → Analyze → PreOptimize → LogicalOptimize → Map → PhysicalOptimize → Execute
DataFusion: Parse → Plan → LogicalOptimize → PhysicalPlan → PhysicalOptimize → Execute
```

The pipelines are remarkably similar. Both are rule-based, multi-pass, with logical and physical optimization stages. Key differences:
- ES|QL has an Analysis phase (type resolution, function binding) that DataFusion handles during planning
- ES|QL has PreOptimizer for async operations (field-caps, enrichment) — DataFusion has no equivalent
- ES|QL's physical optimizer is heavily Lucene-specific; DataFusion's is format-agnostic

---

## 3. Pushdown Capabilities (Deep Analysis)

### 3.1 Projection Pushdown
**Layer:** Logical optimizer
**What:** Eliminates unneeded columns as early as possible
**Universal:** Works for all TableProvider implementations

For Parquet: only reads column chunks needed — dramatic I/O savings for wide tables.
For CSV/JSON: can select columns during parsing.

**ES|QL equivalent:** `InsertFieldExtraction` in physical optimizer — lazy field materialization.

### 3.2 Filter/Predicate Pushdown
**Layer:** Logical optimizer + Physical execution

DataFusion implements a multi-stage approach for Parquet:

**Stage 1 — Row Group Pruning:**
Uses min/max statistics per row group per column. `WHERE a > 10` skips row groups where `max(a) <= 10`. Also uses **Bloom filters** when available (equality predicates like `WHERE id = 42`).

**Stage 2 — Page-Level Pruning:**
Uses optional page-level statistics for finer granularity within row groups.

**Stage 3 — Filter Pushdown / Late Materialization:**
Evaluates filters **during Parquet decoding** at the row level:
1. First process only filter columns, building a boolean mask
2. Then selectively decode only matching rows from other columns

Optimized single-pass pipeline interleaves both phases, caching at most 2 pages per column (~2MB). Results: **15% total time reduction** on ClickBench, up to **2.24x speedup** on selective queries [[3]](#ref-3).

**Important:** Filter pushdown is implemented but **NOT enabled by default** due to some performance regressions. Configuration: `datafusion.execution.parquet.pushdown_filters` [[4]](#ref-4).

**For CSV/JSON:** No metadata-based pruning possible. Filter applied after reading (in-engine).

**ES|QL equivalent:** `PushFiltersToSource` pushes predicates to Lucene queries — analogous but Lucene-specific. For external sources, `FilterPushdownSupport` SPI exists.

### 3.3 Limit Pushdown
**Layer:** Logical optimizer
Pushes max row counts downward, enabling specialized implementations (TopK instead of Sort+Limit).

**ES|QL equivalent:** `PushLimitToSource` in physical optimizer.

### 3.4 Dynamic Filter Pushdown (2024-2025)
**Layer:** Physical execution
Runtime filters from hash joins pushed to source reads. If one side of a join has few matching values, those values filter the other side's scan at the source. "Dramatically improving performance" for star-schema workloads.

**ES|QL equivalent:** Not implemented.

### 3.5 Which Layer Handles What

| Optimization | DataFusion Layer | ES|QL Equivalent |
|-------------|------------------|------------------|
| Column pruning | Logical optimizer | Physical: InsertFieldExtraction |
| Filter to logical | Logical optimizer | Logical: PushDownAndCombineFilters |
| Filter to Lucene | N/A | Physical: PushFiltersToSource |
| Filter to Parquet | Physical: RowFilter | FormatReader-level (future) |
| Row group pruning | ParquetExec metadata | N/A (Lucene handles this) |
| Limit pushdown | Logical optimizer | Physical: PushLimitToSource |
| TopN fusion | Physical optimizer | Logical: CombineLimitTopN |
| Dynamic filters | Physical execution | Not implemented |
| Aggregate pushdown | Statistics only | Physical: PushStatsToSource |

### 3.6 Key Insight for ES|QL Integration
DataFusion proves that effective pushdowns for Parquet/CSV/JSON operate at **three layers**:
1. **Optimizer rules** (generic) — move predicates/projections down in the plan
2. **Source-level metadata** (format-specific) — row group pruning, bloom filters
3. **Decode-time filtering** (format-specific) — late materialization during Parquet decoding

For ES|QL external sources, layers 1 and 2 are most relevant. Layer 3 provides incremental benefit but adds significant complexity.

---

## 4. Optimization Architecture

### 4.1 Logical Optimizer Rules
"Always optimizations" present in virtually all query engines:
1. Filter Pushdown — move filters early
2. Projection Pushdown — eliminate unneeded columns
3. Limit Pushdown — push max row counts down
4. Expression Simplification & Constant Folding
5. OUTER JOIN → INNER JOIN Elimination
6. Common Subexpression Elimination
7. Subquery → JOIN Rewriting (SEMI JOIN, ANTI JOIN)

### 4.2 Physical Optimizer Rules
1. Algorithm Selection — TopK, sorted vs hash grouping
2. Sort and Distribution Optimization
3. Statistics-Based Optimization — answer queries from stats without data access
4. Join Order Optimization (basic syntactic, not cost-based)

### 4.3 Cost-Based vs Rule-Based
DataFusion deliberately avoids sophisticated CBO: "any one particular set of heuristics and cost model is unlikely to work well for the wide variety of DataFusion users." Instead provides extensible framework for custom optimizers [[5]](#ref-5).

**Comparison with ES|QL:** Both are rule-based. ES|QL has ~66 logical rules (61% generic) and ~15 physical rules (90% Lucene-specific). DataFusion has ~20+ logical and ~10 physical rules, nearly all generic.

---

## 5. Execution Model

### 5.1 Streaming Vectorized Execution
- **Pull-based**: Downstream operators pull `RecordBatch` from upstream
- **Streaming**: Data flows without full materialization
- **Vectorized**: Column-at-a-time operations on Arrow arrays
- **Multi-threaded**: Partitioned execution across threads
- **Async I/O**: Non-blocking reads from object stores

### 5.2 ExecutionPlan Trait
```rust
trait ExecutionPlan {
    fn execute(&self, partition: usize, context: Arc<TaskContext>) 
        -> Result<SendableRecordBatchStream>;
    fn output_partitioning(&self) -> Partitioning;
    fn output_ordering(&self) -> Option<&[PhysicalSortExpr]>;
}
```

### 5.3 Comparison with ES|QL Compute Engine
| Aspect | DataFusion | ES|QL Compute |
|--------|-----------|---------------|
| Data unit | RecordBatch (Arrow) | Page (Block[]) |
| Execution | Pull-based streaming | Push-based cooperative (Driver) |
| Threading | Partitioned multi-thread | Driver per pipeline, thread pool |
| Memory | Arrow allocators | Circuit breaker + BlockFactory |
| Yield | Async/await | Cooperative yield (maxTime/maxIterations) |
| Distributed | Via Ballista (separate) | Built-in Exchange infrastructure |

---

## 6. Data Source Extensibility

### 6.1 TableProvider Trait
```rust
trait TableProvider {
    fn schema(&self) -> SchemaRef;
    fn scan(&self, projection: Option<&Vec<usize>>, filters: &[Expr], 
            limit: Option<usize>) -> Result<Arc<dyn ExecutionPlan>>;
    fn supports_filters_pushdown(&self, filters: &[&Expr]) 
        -> Result<Vec<TableProviderFilterPushDown>>;
}
```

Custom sources implement `TableProvider` and declare which filters they can handle. The optimizer respects these declarations.

**Comparison with ES|QL:** `ExternalSourceFactory` + `FilterPushdownSupport` serves a similar role but is less tightly integrated with the optimizer.

### 6.2 ObjectStore Trait
Cloud storage abstraction supporting range reads, listing, put/get:
- `object_store` crate handles S3, GCS, Azure, local, in-memory
- Range reads critical for Parquet column chunk access
- Retry logic, credential management built in

---

## 7. DataFusion vs DuckDB vs Velox

| Aspect | DataFusion | DuckDB | Velox |
|--------|-----------|--------|-------|
| Language | Rust | C++ | C++ |
| Primary use | Embeddable engine / building block | Embedded analytical database | Execution engine toolkit |
| Target | Engine builders | End users / analysts | Engine builders (Presto/Spark) |
| Data format | Arrow RecordBatch | Custom + Parquet | Arrow-compatible vectors |
| Extensibility | Very high (10+ APIs) | Moderate | High (toolkit) |
| SQL completeness | Good (growing) | Excellent | No SQL (execution only) |
| License | Apache 2.0 | MIT | Apache 2.0 |
| JVM interop | Community JNI (not production) | JDBC | None |

**Key takeaway for ES|QL:** DataFusion is the closest architectural analog — both are embeddable query engines with rule-based optimizers. But DataFusion is Rust-only with no production JVM bindings, so it can't be directly embedded. Its architecture and pushdown model are excellent **design references** for ES|QL's external data source support.

---

## 8. Relevance for ES|QL

### 8.1 What ES|QL Can Learn
1. **Pushdown architecture**: Three-layer model (optimizer rules → source metadata → decode-time) is well-proven
2. **TableProvider pattern**: Source declares pushdown capabilities, optimizer respects — similar to ES|QL's `FilterPushdownSupport` but more tightly integrated
3. **Parquet optimizations**: Row group pruning + bloom filters + page pruning provide substantial speedups before any row-level processing
4. **Dynamic filter pushdown**: Significant optimization opportunity for joins with external data

### 8.2 What DataFusion Gets from Arrow That ES|QL Could Too
1. **RecordBatch as universal data unit**: All operators, all sources, one format
2. **Zero-copy source access**: mmap, Flight, IPC all produce ready-to-use Arrow data
3. **Ecosystem compatibility**: Any Arrow-producing system is instantly a data source

---

## References

<a id="ref-1"></a>**[1]** "Apache DataFusion is now the fastest single node engine for querying Apache Parquet files" — https://datafusion.apache.org/blog/2024/11/18/datafusion-fastest-single-node-parquet-clickbench/

<a id="ref-2"></a>**[2]** SIGMOD 2024: "Apache Arrow DataFusion: A Fast, Embeddable, Modular Analytic Query Engine" — https://dl.acm.org/doi/10.1145/3626246.3653368

<a id="ref-3"></a>**[3]** "Efficient Filter Pushdown in Parquet" — https://datafusion.apache.org/blog/2025/03/21/parquet-pushdown/

<a id="ref-4"></a>**[4]** "Parquet Pruning in DataFusion: Read Only What Matters" — https://datafusion.apache.org/blog/2025/03/20/parquet-pruning/

<a id="ref-5"></a>**[5]** "Optimizing SQL in DataFusion, Part 2: Optimizers" — https://datafusion.apache.org/blog/2025/06/15/optimizing-sql-dataframes-part-two/
