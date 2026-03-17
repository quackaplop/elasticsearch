# Parquet Reader Benchmark Infrastructure & Early Results

## Overview

End-to-end benchmark comparing 5 Parquet reader implementations across 19 datasets,
9 query scenarios, and 8 S3A transport profiles at 3 network distance tiers.

## Benchmark Matrix

- **5 Readers**: parquet-mr, parquet-mr-columnar, arrow-copy, arrow-zerocopy, iceberg-arrow
- **9 Scenarios**: full-scan, projection-5col, limit-1000, limit-100k, filter-selective (~5%), filter-wide (~70%), filter+projection, filter+limit-1000, projection+limit-1000
- **8 S3A Transports**: baseline, baseline+crt, vectored, vectored+crt, s3a2, s3a2+crt, s3a2-tuned, s3a2-tuned+crt
- **19 Datasets**: 7 single-file + 3 multi-file (each in arrow-writer and parquet-mr-writer variants), plus monthly-partitions
- **3 Distance Tiers**: same-region (us-east-1→us-east-1), cross-close (eu-west-1→us-east-1), cross-far (ap-southeast-1→us-east-1)
- **Total data points per tier**: 8 × 19 × 9 × 5 = 6,840

## Infrastructure

### EC2 Instances (m5.2xlarge, 8 vCPU, 32GB RAM)
| Tier | Instance | IP | Bucket |
|------|----------|-----|--------|
| same-region | i-040a9d360fefc52ad | 98.86.221.49 | esql-parquet-bench-same-region |
| cross-close | i-0e7c30e56ec27dce0 | 108.129.214.80 | esql-parquet-bench-cross-close |
| cross-far | i-0e7a3206f0003712f | 13.250.36.39 | esql-parquet-bench-cross-far |

- SSH key: `/tmp/esql-bench-key`
- IAM role with scoped S3 read policy
- All buckets in us-east-1 with identical data (~5.13 GB each)
- JDK 21 (Amazon Corretto)

### Resilience
- systemd user service (`bench.service`) with `Restart=on-failure`, `RestartSec=60`
- `loginctl enable-linger` for persistence without SSH session
- Resume-safe script: skips transports with completed CSV output
- Lock file prevents concurrent runs
- Partial S3 upload after each transport completes

### Dataset Generation
- **Arrow-writer datasets**: generated with PyArrow (`scripts/generate-datasets.py`), Data Page v2, `createdBy: parquet-cpp-arrow`
- **parquet-mr-writer datasets**: generated with Java streaming writer (`DatasetGenerator.java`), `PARQUET_1_0` format
- Source data: NYC yellow taxi 2024 (6 months, ~41M rows)
- Schema: 19 columns (timestamps, numerics, strings, geo)

### Datasets

| Dataset Pattern | Rows | Size | Purpose |
|----------------|------|------|---------|
| micro-batch-1k-28kb | 1K | 28KB | Tiny batch (startup overhead) |
| pre-compaction-100k-2mb | 100K | 2MB | Pre-compaction segment |
| single-partition-1m-17mb | 1M | 17MB | Typical single partition |
| over-partitioned-1m-100rg | 1M | 18MB | 100 row groups (over-partitioned) |
| legacy-snappy-1m-17mb | 1M | 17-21MB | Snappy compression |
| streaming-backlog-50x3mb | 3M | 50-54MB | 50 small files (streaming backlog) |
| wide-fact-table-1m-100col | 1M | 269-298MB | 100 columns (wide table) |
| monthly-partitions | 19.5M | 319MB | 6-month monthly partitions (arrow only) |
| spark-output-3m-30rg-10f | 30M | 467-499MB | Spark-style output |
| production-15m-256mb-4f | 56-60M | 903MB-1GB | Production-scale |

Each dataset exists in both `-arrow` and `-parquetmr` variants (except monthly-partitions).

## Early Results (Smoke Test — same-region, baseline transport, 0 warmup)

### Full-Scan Throughput (rows/sec) on production-15m dataset

| Reader | parquetmr-writer | arrow-writer |
|--------|-----------------|--------------|
| parquet-mr | 305K | 313K |
| parquet-mr-columnar | 748K | 817K |
| arrow-copy | 1.49M | 1.95M |
| arrow-zerocopy | **2.46M** | **2.85M** |
| iceberg-arrow | 923K | 899K |

### Key Observations
1. **arrow-zerocopy is 8-10x faster than parquet-mr** on full scans
2. **arrow-copy is 5-6x faster** than parquet-mr
3. **parquet-mr-columnar is 2.5x faster** than row-based parquet-mr
4. **Arrow-written files read faster** with Arrow readers (~15-20% for arrow-copy/zerocopy)
5. **iceberg-arrow is 3x parquet-mr** but slower than direct Arrow readers
6. **Wide tables amplify the gap**: arrow-zerocopy 360K rows/s vs parquet-mr 32K = **11x** on 100-col table
7. **Memory**: arrow-zerocopy uses dramatically less heap (252MB vs 2.5GB for parquet-mr on streaming datasets)
8. **CPU utilization**: arrow-zerocopy 19-27% vs parquet-mr 80-90% — I/O bound vs CPU bound

### Latency to First Page (ms) — production-15m

| Reader | parquetmr-writer | arrow-writer |
|--------|-----------------|--------------|
| parquet-mr | 7,835 | 3,147 |
| parquet-mr-columnar | 8,572 | 3,756 |
| arrow-copy | 5,434 | 2,546 |
| arrow-zerocopy | 4,807 | 2,629 |
| iceberg-arrow | 3,703 | 2,052 |

## Code Locations

| Component | Path |
|-----------|------|
| BenchmarkRunner | `esql-parquet-benchmark/src/main/java/.../BenchmarkRunner.java` |
| S3AProfile | `esql-parquet-benchmark/src/main/java/.../S3AProfile.java` |
| DatasetGenerator | `esql-parquet-benchmark/src/main/java/.../DatasetGenerator.java` |
| PyArrow generator | `esql-parquet-benchmark/scripts/generate-datasets.py` |
| Run script | `esql-parquet-benchmark/scripts/run-benchmarks.sh` |
| Build config | `esql-parquet-benchmark/build.gradle` |

## Status (as of 2026-03-11)

Full benchmark running across all 3 tiers under systemd.
- same-region: 3/8 transports done, ~24h remaining
- cross-close: 1/8 done, ~50h remaining
- cross-far: 0/8 done, ~80h remaining

Results auto-upload to `s3://<bucket>/results/<tier>/` after each transport.
