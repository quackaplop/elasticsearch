# Parquet Reader Production Stability Assessment

**Date:** 2026-03-25
**Validated against:** `elastic/main` (commit d0aea540405)
**Reader:** `ParquetFormatReader.java` (1,171 lines)
**Tests:** `ParquetFormatReaderTests.java` (1,349 lines, 33 test methods)

## Status by Finding

### FIXED on elastic/main

| Finding | Evidence |
|---------|----------|
| Block leak on exception | Lines 611-624: try/catch with `Releasables.closeExpectNoException(blocks)` |
| GroupRecordConverter anti-pattern | Replaced by `ColumnReadStoreImpl` + `ColumnReader` (lines 585-596). `NoOpGroupConverter` is a stub. |
| INT96 timestamps | Line 436 → DataType.DATETIME. Decode at lines 824-843. Tested. |
| DECIMAL (all backing types) | Lines 423-440 → DataType.DOUBLE. Decode at lines 729-748. 4 dedicated tests. |
| Float16 | Lines 442-443 → DataType.DOUBLE. Decode at lines 751-767. Tested. |
| LIST types | Tested for LIST\<INT\>, LIST\<STRING\>, LIST with nulls. |
| Missing column → NULL block | Line 249/616: missing projected columns produce constant null blocks. |

### STILL PRESENT on elastic/main

| Finding | Severity | Evidence | Tracked |
|---------|----------|----------|---------|
| **Circuit breaker gaps** | P0 | No pre-registration before `readNextRowGroup()` (line 579). parquet-mr internal buffers (up to 128MB/RG) invisible to CB. Java arrays for column data allocated without CB tracking. | #282 (DS-M2) |
| **Error messages** | P0 | Lines 574, 623: bare `RuntimeException`. No `ElasticsearchException` anywhere. Users see raw parquet-mr stack traces. | Not tracked |
| **No malformed file handling** | P1 | No pre-validation, no friendly error for corrupt/truncated files. No test. | Not tracked |
| **Schema type mismatch** | P1 | No detection or test for type mismatches (e.g., file has INT32, query expects STRING). | Not tracked |
| **No circuit breaker test** | P1 | Line 62: all tests use `NoopCircuitBreaker`. No real/mock CB test. | Part of #282 |
| **Unsigned integer overflow** | P2 | No UINT annotation handling. Large unsigned values wrap to negative. | #316 (DS-M3) |
| **MAP/STRUCT/nested types** | P2 | Returns UNSUPPORTED → null blocks. No crash, but silent data loss. | #320 (DS-M3) |

### Test Coverage (33 methods)

**Covered:** basic read, all primitive types, DECIMAL (4 variants), INT96, Float16, UUID, LIST (3 tests), nullability (2 tests), projection, limit, batching, row-group splits, range reads.

**Not covered:** empty files (0 rows), malformed files, wide schemas (100+ cols), schema type mismatch, circuit breaker integration, MAP/nested structs.

## Production Readiness Verdict

The reader is **functionally complete** — all pushdowns, type support, and the columnar architecture are shipped. The remaining gaps are in **hardening**: circuit breaker integration (#282), error messages, and edge case testing. The block leak (the only data-loss-risk bug) is already fixed.

For a March TP (tech preview): shippable with documented limitations. Circuit breaker gaps (#282) are the main OOM risk. Error messages are cosmetic but affect user experience.

For GA: #282 (circuit breaker) must be resolved. Error messages should be improved. Test coverage for malformed files and type mismatches should be added.
