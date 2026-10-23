# Memory Queue Benchmarks

[Simplified Chinese](memory-queues.zh-CN.md)

This report is the detailed record behind the charts in the root README. The checked-in summary file is
[memory-queues-results.csv](memory-queues-results.csv). It contains the six producer rows and the 85 rows from the
three consume benchmark classes. The CSV retains the measured mean, error, standard deviation, allocation, and every
configuration used for the charts; the tables below keep the comparison readable by showing mean time and allocation.

## Primary Consume Comparison

The primary comparison uses `ChannelReader<T>.ReadAllAsync` and `IBufferPullConsumer<T>.ConsumeAsync`
with `AutoCommit = true`. Their signatures expose the same asynchronous stream shape:

```csharp
// ChannelReader<T>
IAsyncEnumerable<T> ReadAllAsync(CancellationToken cancellationToken = default);
// IBufferPullConsumer<T>
IAsyncEnumerable<IEnumerable<T>> ConsumeAsync(CancellationToken cancellationToken = default);
```

Both are consumed with `await foreach`. Channel yields one item per iteration; BufferQueue yields one batch.
`ReadAllAsync` continues until the completed channel is drained, rather than collecting all items into one batch.
This compares the cost of consuming the same messages through asynchronous streams, including BufferQueue's batching
and Auto Commit behavior; it does not imply identical delivery semantics. `ChannelReader<T>.ReadAsync` returns
`ValueTask<T>` for a single read and would be a separate asynchronous comparison. `ChannelReader<T>.TryRead` is a
synchronous reference, not the primary asynchronous baseline.

Channel has no equivalent `BatchSize`; its baseline is shown once for each consumer count and is independent of the
BufferQueue setting. BufferQueue uses the five configured values `1, 10, 50, 100, 1000`.

Each run drains 8,192 prefetched `int` values. The BufferQueue topic uses 12 partitions and either 1 or 12
consumers. The benchmark completes the Channel writer after prefill so `ReadAllAsync` has a finite workload. Every
consumer runs in its own `Task.Run`. All Channel readers share one channel; BufferQueue consumers read their assigned
partitions. Channel readers count locally and the result is aggregated after all readers finish.
BufferQueue counts each assigned consumer's batches and validates both its assigned count and the aggregate total.
There is no business work per item. The measured time includes the task scheduling used to run the readers.

### unbounded

Channel reference (fixed `BatchSize = 1` representative):

| Consumers | Channel TryRead (us) | Channel ReadAllAsync (us) | TryRead allocated | ReadAllAsync allocated |
|---:|---:|---:|---:|---:|
| 1 | 139.000 | 513.130 | 592 B | 952 B |
| 12 | 3,446.620 | 3,381.500 | 2,656 B | 4,952 B |

BufferQueue `ConsumeAsync` with `AutoCommit = true`:

| BatchSize | Consumers | Mean (us) | Allocated |
|---:|---:|---:|---:|
| 1 | 1 | 1,037.650 | 197,960 B |
| 1 | 12 | 1,953.220 | 203,536 B |
| 10 | 1 | 201.040 | 54,728 B |
| 10 | 12 | 289.340 | 60,304 B |
| 50 | 1 | 56.280 | 39,368 B |
| 50 | 12 | 67.700 | 44,944 B |
| 100 | 1 | 39.070 | 37,352 B |
| 100 | 12 | 43.780 | 42,928 B |
| 1000 | 1 | 20.910 | 50,024 B |
| 1000 | 12 | 31.770 | 55,536 B |

### bounded

`bounded` uses a BufferQueue capacity of 8,192 and a Channel capacity of 8,192. The queues are prefetched before the
timed method, so this is a drain benchmark and does not measure producer waiting for capacity.

Channel reference (fixed `BatchSize = 1` representative):

| Consumers | Channel TryRead (us) | Channel ReadAllAsync (us) | TryRead allocated | ReadAllAsync allocated |
|---:|---:|---:|---:|---:|
| 1 | 131.060 | 480.810 | 592 B | 952 B |
| 12 | 1,036.560 | 2,760.870 | 2,656 B | 4,952 B |

BufferQueue `ConsumeAsync` with `AutoCommit = true`:

| BatchSize | Consumers | Mean (us) | Allocated |
|---:|---:|---:|---:|
| 1 | 1 | 1,296.740 | 197,960 B |
| 1 | 12 | 1,811.020 | 203,536 B |
| 10 | 1 | 248.920 | 54,728 B |
| 10 | 12 | 329.410 | 60,304 B |
| 50 | 1 | 63.740 | 39,368 B |
| 50 | 12 | 75.650 | 44,944 B |
| 100 | 1 | 42.920 | 37,352 B |
| 100 | 12 | 48.370 | 42,928 B |
| 1000 | 1 | 33.180 | 50,024 B |
| 1000 | 12 | 33.840 | 55,600 B |

### Single-reader Channel comparison

This class keeps one consumer and compares the default Channel implementation with Channel's `SingleReader` option.
The BufferQueue row still uses 12 partitions and one consumer.

Channel reference (fixed `BatchSize = 1` representative):

| Default TryRead (us) | Default ReadAllAsync (us) | SingleReader TryRead (us) | SingleReader ReadAllAsync (us) |
|---:|---:|---:|---:|
| 143.240 | 509.780 | 77.910 | 393.280 |

BufferQueue `ConsumeAsync` with `AutoCommit = true`:

| BatchSize | Mean (us) | Allocated |
|---:|---:|---:|
| 1 | 1,078.010 | 197,264 B |
| 10 | 207.230 | 54,032 B |
| 50 | 50.720 | 38,672 B |
| 100 | 36.190 | 36,656 B |
| 1000 | 17.060 | 49,328 B |

The Channel methods do not consume `BatchSize`-sized batches; the parameter changes only the BufferQueue method.
`SingleReader = true` is a caller promise that at most one reader consumes the Channel; it is not the 12-consumer
configuration.

For one consumer at `BatchSize = 10`, the asynchronous unbounded BufferQueue row is 201.040 us versus the fixed
Channel `ReadAllAsync` reference at 513.130 us; the bounded values are 248.920 us versus 480.810 us. This is one
measured configuration, not a claim that BufferQueue is faster at every batch size or consumer count. Comparing it
with `TryRead` would compare an asynchronous BufferQueue path with the synchronous reference.

Chart reading: the horizontal axis uses equally spaced BatchSize configurations; spacing does not represent numeric
distance. The time axis is logarithmic. Points and lines are means, while error bars or bands use BenchmarkDotNet's
reported `Error`. Channel baselines are fixed at representative `BatchSize = 1` and do not depend on BufferQueue's
`BatchSize`.

## Producer Comparison

The producer benchmark prefills 8,192 `int` values using 12 concurrent producer tasks (`Environment.ProcessorCount`). The batch method sends a
`ReadOnlyMemory<int>` batch; the single-item method sends one item at a time. These are separate API shapes, so the
batch row is not a claim that a single-item producer has the same work.

| Mode | Method | Mean (us) | Allocated |
|---|---|---:|---:|
| unbounded | Channel_Produce_Concurrent | 291.690 | 134.53 KB |
| unbounded | MemoryBufferQueue_Produce_Concurrent | 338.260 | 54.96 KB |
| unbounded | MemoryBufferQueue_Produce_ReadOnlyMemoryBatch_Concurrent | 38.330 | 54.90 KB |
| bounded | Channel_Produce_Concurrent | 305.510 | 67.33 KB |
| bounded | MemoryBufferQueue_Produce_Concurrent | 339.820 | 55.13 KB |
| bounded | MemoryBufferQueue_Produce_ReadOnlyMemoryBatch_Concurrent | 35.880 | 55.08 KB |

Source: [ChannelVsMemoryBufferQueueProduceBenchmark.cs](../../tests/BufferQueue.Benchmarks/ChannelVsMemoryBufferQueueProduceBenchmark.cs).

### Partition-Key Routing Benchmark

This retained routing measurement predates the current consumer comparison; it was not rerun with the current
consume benchmark workload. The following [`MemoryBufferPartitionerBenchmark`](../../tests/BufferQueue.Benchmarks/MemoryBufferPartitionerBenchmark.cs)
measures the complete Memory-mode producer path with `8` partitions and `8,192` messages repeated `832` times,
`6` warmup iterations, and `15` measured iterations. Results are per produced item on macOS 27.0.1,
ARM64, and .NET 10.0.0; no path allocated managed memory. String keys contain `7` UTF-16 code units;
complete-key hashing takes more time as keys get longer.

| Routing | Mean | Relative to round robin |
| --- | ---: | ---: |
| Round robin | `13.40 ns` | `1.00x` |
| `int` key | `12.03 ns` | `0.90x` |
| `string` key (XXH3-64, complete UTF-16 key) | `12.94 ns` | `0.97x` |
| Custom message, numeric `CustomerId` key | `11.19 ns` | `0.83x` |

Run this producer benchmark with:

```bash
dotnet run -c Release --project tests/BufferQueue.Benchmarks/BufferQueue.Benchmarks.csproj -- --filter '*MemoryBufferPartitionerBenchmark*'
```

The separate [`StringPartitionRoutingBenchmark`](../../tests/BufferQueue.Benchmarks/StringPartitionRoutingBenchmark.cs)
measures only string routing for prebuilt keys of `4`, `9`, `64`, `120`, `121`, `256`, and `1024` UTF-16 code units across `8` partitions.
The `120`/`121` cases straddle XXH3's long-input threshold of `240` bytes.
It uses one launch, `6` warmup iterations, and `10` measured iterations with a target iteration time of `100 ms`.
Its results are per routing call and exclude queue writes; the table above comes from the producer benchmark.
Run the key-length benchmark with:

```bash
dotnet run -c Release --project tests/BufferQueue.Benchmarks/BufferQueue.Benchmarks.csproj -- --filter '*StringPartitionRoutingBenchmark*'
```

## Read-path Optimization Measurements

These focused comparisons are kept separate from the README chart data. Both use the same baseline that already
included the Frozen reader lookup experiment, minimum-reader scan changes, the published segment-range check, and the
producer lock-handoff fix. The second comparison does not include the first comparison's code change.

### A: single-item reader path

The `single-read-before` and `single-read-after` reports compare the exact configured `BatchSize = 1` reader
specialization. The table is restricted to `PartitionNumber = 1 or 12`, `GroupCount = 1 or 2`, and both `bounded`
values. Allocation is unchanged in every row. One consumer drains the queue; when `GroupCount = 2`, the second group stays
idle to exercise retention and lookup. This is not a two-active-consumer throughput measurement.

| Method | Partitions | bounded | Groups | Before (us) | After (us) | Change | Allocated |
|---|---:|---|---:|---:|---:|---:|---:|
| AutoCommit | 1 | false | 1 | 1,150.40 | 1,038.29 | -9.7% | 192.2 KB |
| ManualCommit | 1 | false | 1 | 1,211.73 | 1,125.88 | -7.1% | 192.2 KB |
| AutoCommit | 1 | false | 2 | 1,164.23 | 1,080.87 | -7.2% | 192.2 KB |
| ManualCommit | 1 | false | 2 | 1,150.82 | 1,093.86 | -4.9% | 192.2 KB |
| AutoCommit | 1 | true | 1 | 1,386.09 | 1,314.30 | -5.2% | 192.2 KB |
| ManualCommit | 1 | true | 1 | 1,416.45 | 1,362.05 | -3.8% | 192.2 KB |
| AutoCommit | 1 | true | 2 | 1,386.86 | 1,338.85 | -3.5% | 192.2 KB |
| ManualCommit | 1 | true | 2 | 1,395.87 | 1,318.03 | -5.6% | 192.2 KB |
| AutoCommit | 12 | false | 1 | 1,141.83 | 1,056.00 | -7.5% | 192.2 KB |
| ManualCommit | 12 | false | 1 | 1,161.65 | 1,075.52 | -7.4% | 192.2 KB |
| AutoCommit | 12 | false | 2 | 1,143.07 | 1,085.14 | -5.1% | 192.2 KB |
| ManualCommit | 12 | false | 2 | 1,176.54 | 1,117.84 | -5.0% | 192.2 KB |
| AutoCommit | 12 | true | 1 | 1,399.46 | 1,324.05 | -5.4% | 192.2 KB |
| ManualCommit | 12 | true | 1 | 1,461.01 | 1,351.73 | -7.5% | 192.2 KB |
| AutoCommit | 12 | true | 2 | 1,363.69 | 1,352.77 | -0.8% | 192.2 KB |
| ManualCommit | 12 | true | 2 | 1,389.13 | 1,372.10 | -1.2% | 192.2 KB |

The before/after rows are retained in [read-path-optimizations.csv](read-path-optimizations.csv).

### B: sparse batches with one item available per pull

`MemorySparseBatchBenchmark` produces and consumes sequentially, one item at a time, and verifies that every pull
contains exactly one item. `BatchSize` is 10, 100, or 1000, but the available count is intentionally one. This
isolates the lazy allocation path from concurrent throughput. The after version allocates the batch array only after a
second item is found.

| Method | BatchSize | bounded | Before (ms) | After (ms) | Change | Before allocated | After allocated |
|---|---:|---|---:|---:|---:|---:|---:|
| AutoCommit | 10 | false | 2.104 | 1.998 | -5.0% | 772.6 KB | 196.6 KB |
| ManualCommit | 10 | false | 1.583 | 1.518 | -4.1% | 772.6 KB | 196.6 KB |
| AutoCommit | 10 | true | 2.846 | 2.702 | -5.1% | 772.6 KB | 196.6 KB |
| ManualCommit | 10 | true | 2.385 | 2.275 | -4.6% | 772.6 KB | 196.6 KB |
| AutoCommit | 100 | false | 2.162 | 1.997 | -7.6% | 3,652.6 KB | 196.6 KB |
| ManualCommit | 100 | false | 1.669 | 1.927 | +15.5% | 3,652.6 KB | 196.6 KB |
| AutoCommit | 100 | true | 2.888 | 2.805 | -2.9% | 3,652.6 KB | 196.6 KB |
| ManualCommit | 100 | true | 2.443 | 2.310 | -5.4% | 3,652.6 KB | 196.6 KB |
| AutoCommit | 1000 | false | 3.055 | 2.035 | -33.4% | 32,452.6 KB | 196.6 KB |
| ManualCommit | 1000 | false | 2.689 | 1.526 | -43.3% | 32,452.6 KB | 196.6 KB |
| AutoCommit | 1000 | true | 3.786 | 2.727 | -28.0% | 32,452.6 KB | 196.6 KB |
| ManualCommit | 1000 | true | 3.594 | 2.244 | -37.6% | 32,452.6 KB | 196.6 KB |

The `1000` allocation reduction is `32,452.6 KB` to `196.6 KB`. These are binary KB values reported by
BenchmarkDotNet, where 1 KB is 1,024 bytes. The full-batch case was not a clear large gain; the strong result here is
specifically the sparse one-item-per-pull case.

The before/after rows are retained in [read-path-optimizations.csv](read-path-optimizations.csv).

The combined check is retained in [read-path-optimizations.csv](read-path-optimizations.csv). It keeps A and B
independent while checking their combined read-path behavior.

## Reproduction

Benchmark source files:

- [Unbounded consume](../../tests/BufferQueue.Benchmarks/UnboundedChannelVsMemoryBufferQueueConsumeBenchmark.cs)
- [Bounded consume](../../tests/BufferQueue.Benchmarks/BoundedChannelVsMemoryBufferQueueConsumeBenchmark.cs)
- [Single-reader consume](../../tests/BufferQueue.Benchmarks/SingleReaderChannelVsMemoryBufferQueueConsumeBenchmark.cs)
- [Memory commit modes](../../tests/BufferQueue.Benchmarks/MemoryCommitModeBenchmark.cs)
- [Sparse available-item path](../../tests/BufferQueue.Benchmarks/MemorySparseBatchBenchmark.cs)

The main consume and producer result set can be reproduced with separate quoted filters:

```shell
dotnet run --project tests/BufferQueue.Benchmarks -c Release -- --filter '*UnboundedChannelVsMemoryBufferQueueConsumeBenchmark*' --warmupCount 6 --iterationCount 15 --launchCount 1
dotnet run --project tests/BufferQueue.Benchmarks -c Release -- --filter '*BoundedChannelVsMemoryBufferQueueConsumeBenchmark*' --warmupCount 6 --iterationCount 15 --launchCount 1
dotnet run --project tests/BufferQueue.Benchmarks -c Release -- --filter '*SingleReaderChannelVsMemoryBufferQueueConsumeBenchmark*' --warmupCount 6 --iterationCount 15 --launchCount 1
dotnet run --project tests/BufferQueue.Benchmarks -c Release -- --filter '*ChannelVsMemoryBufferQueueProduceBenchmark*' --warmupCount 6 --iterationCount 15 --launchCount 1
```

Focused comparisons used:

```shell
dotnet run --project tests/BufferQueue.Benchmarks -c Release -- --filter '*MemoryCommitModeBenchmark*'
dotnet run --project tests/BufferQueue.Benchmarks -c Release -- --filter '*MemorySparseBatchBenchmark*'
```

The consume job used one launch, six warmups, 15 measured iterations, and invocation count one. The focused jobs used
one launch, six warmups, 20 measured iterations, and invocation count one. The producer benchmark used its fixed job
configuration. The environment was .NET SDK 10.0.100, .NET 10.0.0 Arm64, BenchmarkDotNet 0.15.8, macOS 27.0.1.
The source checkout was based on `e0a06318a7a09c194578a072432a68d1adf42311` with the local benchmark and read-path
changes applied.

BenchmarkDotNet reported short-iteration and environment-priority warnings on this machine. The numbers are useful
for trends and regression checks, but they are not an end-to-end production throughput guarantee. The raw CSV keeps
the error and standard-deviation columns for readers who need to judge variance.
