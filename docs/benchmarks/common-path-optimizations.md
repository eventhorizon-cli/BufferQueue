# Common-path Optimization Measurements

[简体中文](common-path-optimizations.zh-CN.md)

The complete normalized data is in [common-path-optimizations.csv](common-path-optimizations.csv). Every row keeps the
scenario, before/candidate or before/after variant, run tag, BenchmarkDotNet settings, configuration, mean, error,
standard deviation, and allocation.

## Environment and Method

- Apple M2 Max, 12 cores (verified with `sysctl`), macOS 27.0.1, Arm64.
- .NET 10.0.0 and BenchmarkDotNet 0.15.8. The baseline core/runtime reference is `e615da8`, the same core as `091ac24` for
  this benchmark line.
- Each run used one launch. The initial auto-commit comparison used 4 warmup and 10 measured iterations; its
  confirmation used 8 warmup and 30 measured iterations. The notification and sparse comparisons used 6 warmup and
  20 measured iterations.
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
