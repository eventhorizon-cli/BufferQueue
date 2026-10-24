# Common-path Optimization Measurements

[简体中文](common-path-optimizations.zh-CN.md)

The complete normalized data is in [common-path-optimizations.csv](common-path-optimizations.csv). Every row keeps the
scenario, before/candidate or before/after variant, run tag, BenchmarkDotNet settings, configuration, mean, error,
standard deviation, and allocation.

## Environment and Method

- Apple M2 Max, 12 cores (verified with `sysctl`), macOS 27.0.1, Arm64.
- .NET 10.0.0 and BenchmarkDotNet 0.15.8. Sections A and B use baseline `e615da8`, the same core as `091ac24`.
  Section C uses `791c399`, which includes the notification optimization.
- Each run used one launch. The initial auto-commit comparison used 4 warmup and 10 measured iterations; its
  confirmation used 8 warmup and 30 measured iterations. The notification, sparse, and minimum-progress comparisons used
  6 warmup and 20 measured iterations.
- Measurements ran sequentially on the same host. These short drain/path benchmarks are useful for directional
  comparison, not a complete application throughput claim. Reported `Error` and `StdDev` are retained in the CSV.

## A: Auto-commit Reader Reuse

`MemoryAutoCommitOptimizationBenchmark` prefills 8,192 `int` values, then drains one partition with one registered
group. It covers `BatchSize` 1, 10, and 100, both `bounded` and `unbounded`, and both `AutoCommit` and `ManualCommit`.
The manual rows are a control path; the helper is shared, so they are not an untouched-code control.

The candidate reused the reader lookup in the pull path. The initial run was mixed, so it was repeated with longer
warmup and measurement counts. The confirmation remained mixed: several unbounded and larger-batch auto rows improved,
but bounded `BatchSize = 1` regressed from 1,346.09 us to 1,404.17 us (+4.31%). Allocation stayed unchanged. The
reader-reuse candidate was therefore rejected and no production performance change from this experiment was retained.

The four 12-row runs (initial before/candidate and confirmation before/candidate) are all included in the CSV.

## B: Notification Handshake

`MemoryNotificationOptimizationBenchmark` calls `NotifyNewDataAvailable` on a registered fake partition. Each producer sends 8,192 notifications; the measured operation includes one `Task.Run` per producer. “No waiters”
means the consumer is registered and active, but has no pending wait operation; it does not mean that the consumer is
absent. The candidate uses a version-and-flag handshake so a producer notification cannot be lost while a consumer
transitions into waiting. The concurrent completion and data-race invariants are covered by the focused concurrency
tests.

The isolated notification path improved from 148.70 us to 89.31 us with one producer (-39.94%), and from 1,594.30 us to
565.59 us with four producers (-64.52%). Allocations were unchanged at 224 B and 704 B respectively.

The existing `MemorySparseBatchBenchmark` checks the realistic sequential path: produce one item, consume one item, and
repeat for 8,192 items. It covers Auto/Manual Commit, `bounded`/`unbounded`, and `BatchSize` 1, 10, 100, and 1000 even
though each pull has only one available item. After the handshake change, Auto Commit improved by 4.27% to 10.08% and
Manual Commit by 5.21% to 10.60% across these configurations. Allocation was unchanged at 196.6 KB in every row.
These 2 notification rows plus 16 sparse rows for each variant are included in the CSV.

## C: Minimum-progress cache candidates (not retained)

The group cases use the same 6-warmup/20-iteration job, 8,192 prefetched values, one partition, and `bounded` capacity.
Each pull reads one item and is followed by an explicit commit (Manual Commit). `FastReaderWithIdleGroups` drains one
group while the others remain idle; `LockstepReaders` reads and commits one item per group in each round.
Single-group controls cover Auto Commit and Manual Commit, both capacity modes, and BatchSize 1/10/100.
The baseline is the `791c399` line. The three candidates are a record-key cache, a raw index-equality cache, and a
tied-count cache. The CSV contains 52 rows: baseline 16, record-key candidate 16, index candidate 4, and tied-count
candidate 16. The 12 single-group rows per applicable run are retained as raw controls; the implementation is nearly unchanged on that
path, so their cross-run movement is not presented as a stable gain.

| Case | Baseline `791c399` | Record-key candidate | Index candidate | Tied-count candidate |
|---|---:|---:|---:|---:|
| FastReader, groups=2 | 359.2 us / 6.07 / 192 KB | 608.5 us / 19.08 / 192 KB | 387.8 us / 12.12 / 192 KB | 396.7 us / 12.26 / 192 KB |
| Lockstep, groups=2 | 697.7 us / 22.30 / 384 KB | 1,446.2 us / 30.17 / 384 KB | 1,020.9 us / 17.46 / 384 KB | 854.4 us / 35.37 / 384 KB |
| FastReader, groups=16 | 569.1 us / 14.28 / 192 KB | 682.2 us / 14.28 / 192 KB | 478.9 us / 7.52 / 192 KB | 397.6 us / 15.51 / 192 KB |
| Lockstep, groups=16 | 6,844.5 us / 1,555.66 / 3072 KB | 10,250.4 us / 4,393.83 / 3072 KB | 8,058.1 us / 3,473.54 / 3072 KB | 7,893.4 us / 282.41 / 3072 KB |

Each cell is `Mean / Error / Allocated`; the full `StdDev` and all raw rows are in the CSV. Record-key regressed all
four group cases. Index and tied-count improved only the idle-reader case with 16 groups; both still regressed the
two-group cases and the 16-group lockstep case. The 16-group lockstep distribution is bimodal and has a large error
margin, but the two-group regressions are clear. All three minimum-progress cache candidates are rejected and none is
merged.

## Reproduce

Run the focused notification comparison with:

```bash
dotnet run -c Release --project tests/BufferQueue.Benchmarks/BufferQueue.Benchmarks.csproj -- \
  --filter '*MemoryNotificationOptimizationBenchmark*' --warmupCount 6 --iterationCount 20
```

Run the sequential sparse-batch path with:

```bash
dotnet run -c Release --project tests/BufferQueue.Benchmarks/BufferQueue.Benchmarks.csproj -- \
  --filter '*MemorySparseBatchBenchmark*' --warmupCount 6 --iterationCount 20
```

The rejected reader-reuse implementation is not retained. Its recorded runs remain in the CSV;
`MemoryAutoCommitOptimizationBenchmark` can still measure the current Auto/Manual Commit paths.
Compare the retained notification change against its parent checkout, using the same benchmark source and settings.

Run the current group-progress workload with:

```bash
dotnet run -c Release --project tests/BufferQueue.Benchmarks/BufferQueue.Benchmarks.csproj -- \
  --filter '*MemoryGroupProgressOptimizationBenchmark*' --warmupCount 6 --iterationCount 20
```

The rejected cache implementations are not included in the repository.
