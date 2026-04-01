# ClickBench Local Comparison

- **Date**: 2026-04-01 19:56
- **Dataset**: 1m (Snappy-compressed Parquet)
- **ES**: 16GB heap, HTTPS, built from elastic/main (with Snappy fix #145393)
- **ClickHouse**: v26.4.1.501, HTTP
- **Iterations**: 3 per query, median reported
- **Host**: Olegs-MacBook-Pro, Apple M4 Pro, 48GB

## Results

| # | Query | CH (ms) | ES (ms) | Ratio | Winner | CH rows | ES rows | Match |
|---|-------|---------|---------|-------|--------|---------|---------|-------|
| Q01 | STATS count = COUNT(*) | 18 | 1649 | CH 91.6x | CH | 1 | 1 | yes |
| Q02 | WHERE AdvEngineID != 0 | STATS count = C | 15 | 264 | CH 17.6x | CH | 1 | 1 | yes |
| Q03 | STATS s = SUM(AdvEngineID), c = COUNT(*) | 17 | 303 | CH 17.8x | CH | 1 | 1 | yes |
| Q04 | STATS u = COUNT_DISTINCT(UserID) | 21 | 296 | CH 14.1x | CH | 1 | 1 | yes |
| Q05 | STATS u = COUNT_DISTINCT(SearchPhrase) | 21 | 297 | CH 14.1x | CH | 1 | 1 | yes |
| Q06 | STATS mn = MIN(EventDate), mx = MAX(Even | 16 | 286 | CH 17.9x | CH | 1 | 1 | yes |
| Q07 | WHERE AdvEngineID != 0 | STATS c = COUNT | 16 | 305 | CH 19.1x | CH | 5 | 5 | yes |
| Q08 | STATS u = COUNT_DISTINCT(UserID) BY Regi | 22 | 306 | CH 13.9x | CH | 10 | 10 | yes |
| Q09 | STATS s = SUM(AdvEngineID), c = COUNT(*) | 24 | 295 | CH 12.3x | CH | 10 | 10 | yes |
| Q10 | WHERE MobilePhoneModel != "" | STATS u = | 19 | 290 | CH 15.3x | CH | 10 | 10 | yes |
| Q11 | WHERE MobilePhoneModel != "" | STATS u = | 18 | 370 | CH 20.6x | CH | 10 | 10 | yes |
| Q12 | WHERE SearchPhrase != "" | STATS c = COU | 20 | 315 | CH 15.8x | CH | 10 | 10 | yes |
| Q13 | STATS c = COUNT(*) BY UserID | SORT c DE | 23 | 305 | CH 13.3x | CH | 10 | 10 | yes |
| Q14 | WHERE UserID == 435090932899640449 | KEE | 18 | 302 | CH 16.8x | CH | 0 | 0 | yes |
| Q15 | WHERE URL LIKE "*google*" | STATS count  | 36 | 409 | CH 11.4x | CH | 1 | 1 | yes |
| Q16 | STATS s0=SUM(ResolutionWidth), s1=SUM(Re | 18 | 322 | CH 17.9x | CH | 1 | 1 | yes |
| Q17 | WHERE CounterID == 62 AND EventDate >= D | 59 | 382 | CH 6.5x | CH | 10 | 10 | yes |

## Summary

- **ClickHouse wins**: 17
- **ES wins**: 0
- **Ties**: 0
- **Failures**: 0
- **Row mismatches**: 0
