# Claim Reconciliation: Deep Dive Files vs Actual Codebase

**Date:** 2026-03-04

## Summary: All 15 claims VERIFIED TRUE

| # | Claim | Verdict | Evidence |
|---|-------|---------|----------|
| 1 | Arrow version 18.3.0 | TRUE | esql/arrow/build.gradle:16-18, esql-datasource-grpc/build.gradle:31-47, esql-datasource-iceberg/build.gradle:14 |
| 2 | AllocationManagerShim disables allocator | TRUE | AllocationManagerShim.java:33,61-63 — throws UnsupportedOperationException |
| 3 | ArrowResponse uses dummy ArrowBuf | TRUE | ArrowResponse.java:267-269, BlockConverter.java:417-419 — dummyArrowBuf() for size tracking only |
| 4 | FormatReader SPI outputs Page not Arrow | TRUE | FormatReader.java:58 returns CloseableIterator<Page>, Javadoc:26-27 |
| 5 | ParquetFormatReader does NOT use Arrow | TRUE | ParquetFormatReader.java:10-22 — zero org.apache.arrow imports, Javadoc:45-56 |
| 6 | Iceberg get() throws UnsupportedOperationException | TRUE | IcebergSourceOperatorFactory.java:98-107 |
| 7 | FlightConnector uses RootAllocator | TRUE | FlightConnector.java:55 — `new RootAllocator()` |
| 8 | gRPC pinned at 1.78.0 | TRUE | esql-datasource-grpc/build.gradle:62-67 |
| 9 | Flight supports flight:// and grpc:// | TRUE | GrpcDataSourcePlugin.java:27-29 — `Set.of("flight", "grpc")` |
| 10 | Arrow Java has no compute kernels | TRUE | Zero matches for arrow.algorithm or arrow.compute imports |
| 11 | arrow-dataset not used | TRUE | Zero matches in build.gradle files |
| 12 | flight-sql not used | TRUE | Zero matches in build.gradle files |
| 13 | Iceberg uses iceberg-arrow | TRUE | esql-datasource-iceberg/build.gradle:81, IcebergSourceOperatorFactory.java:18-20 |
| 14 | ArrowToBlockConverter supports 9 types | TRUE | ArrowToBlockConverter.java:71-82 — exactly FLOAT4,FLOAT8,BIGINT,INT,BIT,VARCHAR,VARBINARY,TIMESTAMPMICRO,TIMESTAMPMICROTZ |
| 15 | elastic:type metadata on fields | TRUE | BlockConverter.java:35-36, ArrowResponseTests.java:467 |
