# Memory Queue Benchmarks

[Simplified Chinese](memory-queues.zh-CN.md)

This report is the detailed record behind the charts in the root README. The checked-in summary file is
[memory-queues-results.csv](memory-queues-results.csv). It contains the eight producer rows and the 105 rows from the
three consume benchmark classes. The CSV retains the measured mean, error, standard deviation, allocation, and every
configuration used for the charts; the tables below keep the comparison readable by showing mean time and allocation.

## Primary Consume Comparison

The primary comparison uses `ChannelReader<T>.ReadAllAsync` and `IBufferPullConsumer<T>.ConsumeAsync`
with BufferQueue `AutoCommit = true`. Their signatures expose the same asynchronous stream shape:

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

The Manual Commit rows were measured separately on the same `e615da8` production baseline after adding the benchmark
method. They use one launch, six warmup iterations, and fifteen measured iterations on the stated .NET 10 / Apple M2
Max environment; each async batch is followed by `CommitAsync`. The existing Auto Commit and Channel rows remain the
checked-in baseline workload.

### unbounded

Channel reference (fixed `BatchSize = 1` representative):

| Consumers | Channel TryRead (us) | Channel ReadAllAsync (us) | TryRead allocated (B) | ReadAllAsync allocated (B) |
|---:|---:|---:|---:|---:|
| 1 | 139.000 | 513.130 | 592 B | 952 B |
| 12 | 3,446.620 | 3,381.500 | 2,656 B | 4,952 B |

BufferQueue `ConsumeAsync` with `AutoCommit = true`:

| BatchSize | Consumers | Mean (us) | Allocated (KB) |
|---:|---:|---:|---:|
| 1 | 1 | 1,037.650 | 193.32 KB |
| 1 | 12 | 1,953.220 | 198.77 KB |
| 10 | 1 | 201.040 | 53.45 KB |
| 10 | 12 | 289.340 | 58.89 KB |
| 50 | 1 | 56.280 | 38.45 KB |
| 50 | 12 | 67.700 | 43.89 KB |
| 100 | 1 | 39.070 | 36.48 KB |
| 100 | 12 | 43.780 | 41.92 KB |
| 1000 | 1 | 20.910 | 48.85 KB |
| 1000 | 12 | 31.770 | 54.23 KB |

BufferQueue `ConsumeAsync` with `AutoCommit = false` and `CommitAsync` after each batch:

| BatchSize | Consumers | Mean (us) | Allocated (KB) |
|---:|---:|---:|---:|
| 1 | 1 | 1,108.690 | 193.34 KB |
| 1 | 12 | 2,247.950 | 198.78 KB |
| 10 | 1 | 244.890 | 53.46 KB |
| 10 | 12 | 260.880 | 58.91 KB |
| 50 | 1 | 58.970 | 38.46 KB |
| 50 | 12 | 66.070 | 43.91 KB |
| 100 | 1 | 43.190 | 36.49 KB |
| 100 | 12 | 44.490 | 41.94 KB |
| 1000 | 1 | 26.830 | 48.87 KB |
| 1000 | 12 | 33.010 | 54.31 KB |

### bounded

`bounded` uses a BufferQueue capacity of 8,192 and a Channel capacity of 8,192. The queues are prefetched before the
timed method, so this is a drain benchmark and does not measure producer waiting for capacity.

Channel reference (fixed `BatchSize = 1` representative):

| Consumers | Channel TryRead (us) | Channel ReadAllAsync (us) | TryRead allocated (B) | ReadAllAsync allocated (B) |
|---:|---:|---:|---:|---:|
| 1 | 131.060 | 480.810 | 592 B | 952 B |
| 12 | 1,036.560 | 2,760.870 | 2,656 B | 4,952 B |

BufferQueue `ConsumeAsync` with `AutoCommit = true`:

| BatchSize | Consumers | Mean (us) | Allocated (KB) |
|---:|---:|---:|---:|
| 1 | 1 | 1,296.740 | 193.32 KB |
| 1 | 12 | 1,811.020 | 198.77 KB |
| 10 | 1 | 248.920 | 53.45 KB |
| 10 | 12 | 329.410 | 58.89 KB |
| 50 | 1 | 63.740 | 38.45 KB |
| 50 | 12 | 75.650 | 43.89 KB |
| 100 | 1 | 42.920 | 36.48 KB |
| 100 | 12 | 48.370 | 41.92 KB |
| 1000 | 1 | 33.180 | 48.85 KB |
| 1000 | 12 | 33.840 | 54.30 KB |

BufferQueue `ConsumeAsync` with `AutoCommit = false` and `CommitAsync` after each batch:

| BatchSize | Consumers | Mean (us) | Allocated (KB) |
|---:|---:|---:|---:|
| 1 | 1 | 1,393.820 | 193.34 KB |
| 1 | 12 | 2,118.930 | 198.78 KB |
| 10 | 1 | 281.260 | 53.46 KB |
| 10 | 12 | 332.750 | 58.91 KB |
| 50 | 1 | 72.730 | 38.46 KB |
| 50 | 12 | 84.620 | 43.91 KB |
| 100 | 1 | 49.200 | 36.49 KB |
| 100 | 12 | 53.830 | 41.94 KB |
| 1000 | 1 | 29.230 | 48.87 KB |
| 1000 | 12 | 38.020 | 54.31 KB |

### Single-reader Channel comparison

This class keeps one consumer and compares the default Channel implementation with Channel's `SingleReader` option.
The BufferQueue row still uses 12 partitions and one consumer.

Channel reference (fixed `BatchSize = 1` representative):

| Default TryRead (us) | Default ReadAllAsync (us) | SingleReader TryRead (us) | SingleReader ReadAllAsync (us) |
|---:|---:|---:|---:|
| 143.240 | 509.780 | 77.910 | 393.280 |

BufferQueue `ConsumeAsync` with `AutoCommit = true`:

| BatchSize | Mean (us) | Allocated (KB) |
|---:|---:|---:|
| 1 | 1,078.010 | 192.64 KB |
| 10 | 207.230 | 52.77 KB |
| 50 | 50.720 | 37.77 KB |
| 100 | 36.190 | 35.80 KB |
| 1000 | 17.060 | 48.17 KB |

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

The consumer charts show both async BufferQueue modes, with `ReadAllAsync` as the primary Channel comparison and
`TryRead` as a gray synchronous reference. Manual Commit uses the same `ConsumeAsync` workload with
`AutoCommit = false` and calls `CommitAsync` after each batch; Channel has no built-in acknowledgement, checkpoint, or
replay equivalent.

## Producer Comparison

The producer benchmark writes 8,192 `int` values using 12 concurrent `Task.Run` producer tasks. Queue creation and
task scheduling are inside the timed method. The primary Channel comparison uses asynchronous `WriteAsync`; synchronous
`TryWrite` is retained as a reference. BufferQueue uses asynchronous `ProduceAsync` for both item and
`ReadOnlyMemory<int>` batch calls. Bounded capacity is sufficient for the complete workload, so this measurement does
not include producer backpressure waiting.

| Mode | Method | Mean (us) | Allocated (KB) |
|---|---|---:|---:|
| unbounded | `Channel.WriteAsync(item)` | 324.900 | 135.47 KB |
| unbounded | `Channel.TryWrite(item)` (sync reference) | 281.590 | 134.53 KB |
| unbounded | `BufferQueue.ProduceAsync(item)` | 321.910 | 54.96 KB |
| unbounded | `BufferQueue.ProduceAsync(ReadOnlyMemory<int>)` | 36.420 | 54.90 KB |
| bounded | `Channel.WriteAsync(item)` | 393.650 | 68.27 KB |
| bounded | `Channel.TryWrite(item)` (sync reference) | 293.020 | 67.33 KB |
| bounded | `BufferQueue.ProduceAsync(item)` | 350.020 | 55.13 KB |
| bounded | `BufferQueue.ProduceAsync(ReadOnlyMemory<int>)` | 36.020 | 55.08 KB |

![Unbounded producer APIs](memory-producer-unbounded.svg)

![Bounded producer APIs](memory-producer-bounded.svg)

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

## Memory Allocation

These charts normalize BenchmarkDotNet's allocation display to `KiB/op` (`1 KiB = 1,024 B`). Consumption runs drain
8,192 prefetched items from 12 partitions with 1 or 12 consumers; `BatchSize` values are categorical, and the y-axis
is logarithmic. Queue and consumer setup is excluded from the measured allocation. The values are managed memory per
8,192-item benchmark operation, not peak resident memory or per-item allocation. Both Channel references use the rows recorded alongside `BatchSize = 1`;
BatchSize only affects BufferQueue, and the Auto/Manual curves are nearly overlapping.

![unbounded, 1 consumer](memory-unbounded-1-consumer-allocations.svg)

![unbounded, 12 consumers](memory-unbounded-12-consumers-allocations.svg)

![bounded, 1 consumer](memory-bounded-1-consumer-allocations.svg)

![bounded, 12 consumers](memory-bounded-12-consumers-allocations.svg)


Producer allocation includes queue/channel creation and producer task scheduling inside the timed operation. It uses
12 tasks and 8,192 items; `Channel WriteAsync` is the async baseline, `Channel TryWrite` is a synchronous reference,
and the two BufferQueue bars are the item and batch producer APIs. These are not peak resident-memory or per-item
allocation measurements.

![unbounded producer](memory-producer-unbounded-allocations.svg)

![bounded producer](memory-producer-bounded-allocations.svg)

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
