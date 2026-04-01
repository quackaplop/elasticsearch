# Arrow Research Index

All deliverables and backing research for the Apache Arrow / ES|QL deep dive.

## Deliverables

| # | Document | Pages | Description |
|---|----------|-------|-------------|
| 1 | [arrow-general.md](arrow-general.md) | 6 | Apache Arrow ecosystem overview: format, IPC, Flight, format readers, blob stores, optimizations |
| 2 | [datafusion-report.md](datafusion-report.md) | 6 | DataFusion architecture, pushdown capabilities, optimizer comparison with ES|QL |
| 3 | [es-arrow-usage.md](es-arrow-usage.md) | 2 | Current Arrow usage in Elasticsearch (output format, Flight connector, Iceberg) |
| 4 | [arrow-esql-proposal.md](arrow-esql-proposal.md) | 6 | Proposal for Arrow integration in ES|QL external data sources, with risks and pitfalls |

## Additional Reports

| # | Document | Description |
|---|----------|-------------|
| 5 | [arrow-allocators.md](arrow-allocators.md) | Arrow memory allocator system in Java — architecture, customization mechanisms, ES integration |
| 6 | [arrow-parquet-reading.md](arrow-parquet-reading.md) | Reading Parquet with Arrow Java — 5 approaches, trade-offs, pushdown examples with code |

## Parquet Reader Deep Dive

| # | Document | Description |
|---|----------|-------------|
| 7 | [arrow-cpp-parquet-reader.md](arrow-cpp-parquet-reader.md) | Arrow C++ Parquet reader — 8-stage pipeline, class hierarchy, pushdowns, SIMD, competitive comparison |
| 8 | [arrow-java-parquet-reader.md](arrow-java-parquet-reader.md) | Arrow Java JNI adapter — Dataset module, C Data Interface, pushdown support, memory model |
| 9 | [parquet-mr-analysis.md](parquet-mr-analysis.md) | parquet-mr/parquet-java — architecture, 4-level filter stack, ES usage analysis |
| 10 | [es-parquet-reader-analysis.md](es-parquet-reader-analysis.md) | ES ParquetFormatReader — code analysis, columnar-row-columnar anti-pattern, improvement opportunities |
| 11 | [arrow-parquet-critique.md](arrow-parquet-critique.md) | **Synthesis critique** — strengths, weaknesses, competitive analysis (Arrow vs DuckDB vs parquet-mr vs arrow-rs) |
| 12 | [arrow-java-jni-stability.md](arrow-java-jni-stability.md) | **JNI stability assessment** — code analysis, GitHub issues catalog, adoption evidence, honest risk assessment |

## Benchmark

| # | Document | Description |
|---|----------|-------------|
| 13 | [benchmark-infrastructure.md](benchmark-infrastructure.md) | Benchmark infra, dataset matrix, early results (arrow-zerocopy 8-10x faster than parquet-mr) |

## Backing Deep Dive Files

Raw research findings with full citations — keep for reference.

| File | Source |
|------|--------|
| [deep-dive-arrow-codebase.md](deep-dive-arrow-codebase.md) | Arrow GitHub repos, Java Maven artifacts, component tree |
| [deep-dive-arrow-public.md](deep-dive-arrow-public.md) | Public articles, docs, presentations on Arrow |
| [deep-dive-datafusion.md](deep-dive-datafusion.md) | DataFusion architecture, pushdowns, execution model |
| [deep-dive-es-arrow.md](deep-dive-es-arrow.md) | Elasticsearch codebase Arrow usage |
| [reconciliation.md](reconciliation.md) | 15 claims verified against codebase — all TRUE |
