# Memory Queue Benchmarks

[Simplified Chinese](memory-queues.zh-CN.md)

This report is the historical 12-partition archive linked from the root README, not the current main chart dataset.
The README's current measured charts come from the single-partition report. The checked-in summary file is
[memory-queues-results.csv](memory-queues-results.csv). It contains the eight producer rows and the 105 rows from the
three consume benchmark classes. The CSV retains the measured mean, error, standard deviation, allocation, and every
configuration used for the charts; the tables below keep the comparison readable by showing mean time and allocation.

Unless noted otherwise, the measurements in this report use Apple M2 Max (12 cores), macOS 27.0.1 / Arm64,
.NET 10.0.0, the runtime-provided `System.Threading.Channels` assembly 10.0.0.0 (file version 10.0.25.52411),
and BenchmarkDotNet 0.15.8 in Release mode. The benchmark project has no separate `System.Threading.Channels`
package reference, so these Channel rows use the framework assembly shipped with the tested runtime.

## 12-Partition Charts

This compact index collects the historical 12-partition charts retained by this report. The current README leads with
the [single-partition produce and consume report](single-partition.md); these charts are the original multi-partition
reference and must not be read as a same-run comparison with that report.

![Historical unbounded producer time](memory-producer-unbounded.svg)

![Historical bounded producer time](memory-producer-bounded.svg)

![Historical unbounded producer allocations](memory-producer-unbounded-allocations.svg)

![Historical bounded producer allocations](memory-producer-bounded-allocations.svg)

![Historical unbounded consume time, 1 consumer](memory-unbounded-1-consumer.svg)

![Historical unbounded consume time, 12 consumers](memory-unbounded-12-consumers.svg)

![Historical bounded consume time, 1 consumer](memory-bounded-1-consumer.svg)

![Historical bounded consume time, 12 consumers](memory-bounded-12-consumers.svg)

![Historical unbounded consume allocations, 1 consumer](memory-unbounded-1-consumer-allocations.svg)

![Historical unbounded consume allocations, 12 consumers](memory-unbounded-12-consumers-allocations.svg)

![Historical bounded consume allocations, 1 consumer](memory-bounded-1-consumer-allocations.svg)

![Historical bounded consume allocations, 12 consumers](memory-bounded-12-consumers-allocations.svg)

## Producer Comparison

The producer benchmark writes 8,192 `int` values using 12 concurrent `Task.Run` producer tasks. Queue creation and
task scheduling are inside the timed method. The primary Channel comparison uses asynchronous `WriteAsync`; synchronous
`TryWrite` is retained as a reference. BufferQueue uses asynchronous `ProduceAsync` for both item and
`ReadOnlyMemory<int>` batch calls. Bounded capacity is sufficient for the complete workload, so this measurement does
not include producer backpressure waiting.

| Mode | Method | Mean (µs) | Allocated (KiB) |
|---|---|---:|---:|
| unbounded | `Channel.WriteAsync(item)` | 337.960 | 135.47 |
| unbounded | `Channel.TryWrite(item)` | 287.757 | 134.53 |
| unbounded | `BufferQueue.ProduceAsync(item)` | 331.357 | 54.96 |
| unbounded | `BufferQueue.ProduceAsync(ReadOnlyMemory<int>)` | 37.201 | 54.90 |
| bounded | `Channel.WriteAsync(item)` | 391.972 | 68.27 |
| bounded | `Channel.TryWrite(item)` | 302.205 | 67.33 |
| bounded | `BufferQueue.ProduceAsync(item)` | 343.133 | 55.13 |
| bounded | `BufferQueue.ProduceAsync(ReadOnlyMemory<int>)` | 37.588 | 55.08 |

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

## Primary Consume Comparison

The current README leads with the new [single-partition produce and consume measurements](single-partition.md).
This report is the historical 12-partition archive, including the 1-consumer reference; partition counts must not be
inferred from consumer counts. The compact [12-partition chart index](#12-partition-charts) keeps those historical
visuals together.

This section measures prefilled queue draining through asynchronous BufferQueue consumption and Channel references.
The primary BufferQueue rows use `AutoCommit = true`; Manual Commit and the synchronous `TryRead` reference are
reported separately below.

Channel has no equivalent `BatchSize`; its baseline is shown once for each consumer count and is independent of the
BufferQueue setting. BufferQueue uses the five configured values `1, 10, 50, 100, 1000`.

Each run drains 8,192 prefetched `int` values. The BufferQueue topic uses 12 partitions and either 1 or 12
consumers. The benchmark completes the Channel writer after prefill so `ReadAllAsync` has a finite workload. Every
consumer runs in its own `Task.Run`. All Channel readers share one channel; BufferQueue consumers read their assigned
partitions. Channel readers count locally and the result is aggregated after all readers finish.
BufferQueue counts each assigned consumer's batches and validates both its assigned count and the aggregate total.
There is no business work per item. The measured time includes the task scheduling used to run the readers.

All producer, Auto Commit, Manual Commit, and Channel rows were rerun on checkout `e63d152`, which combines the
singleton-enumerator and partial-array optimizations. The [run record](memory-queues-run.json) identifies the exact
source and job configuration. Manual Commit calls `CommitAsync` after every batch. BufferQueue uses each batch's
`Count()` rather than enumerating its items; neither business processing nor application-side item enumeration is
included. The [foreach measurements](batch-enumeration.md) and [sparse-batch measurements](partial-batch-allocation.md)
measure those separate workloads.

### Unbounded

Channel references use the fixed `BatchSize=1` rows; BatchSize does not control Channel:

| Consumers | TryRead (µs) | ReadAllAsync (µs) | TryRead allocated (B) | ReadAllAsync allocated (B) |
|---:|---:|---:|---:|---:|
| 1 | 157.512 | 552.609 | 592 | 952 |
| 12 | 3,656.109 | 3,411.669 | 2,656 | 4,880 |

BufferQueue `ConsumeAsync`, `AutoCommit=true`:

| BatchSize | Consumers | Mean (µs) | Allocated (KiB) |
|---:|---:|---:|---:|
| 1 | 1 | 1,099.643 | 193.34 |
| 1 | 12 | 1,875.452 | 198.78 |
| 10 | 1 | 206.557 | 52.77 |
| 10 | 12 | 271.154 | 58.22 |
| 50 | 1 | 60.115 | 37.30 |
| 50 | 12 | 61.096 | 42.75 |
| 100 | 1 | 42.929 | 35.34 |
| 100 | 12 | 50.456 | 40.72 |
| 1000 | 1 | 26.591 | 33.65 |
| 1000 | 12 | 34.381 | 39.03 |

BufferQueue `ConsumeAsync`, `AutoCommit=false` with `CommitAsync` after each batch:

| BatchSize | Consumers | Mean (µs) | Allocated (KiB) |
|---:|---:|---:|---:|
| 1 | 1 | 1,120.218 | 193.34 |
| 1 | 12 | 2,120.025 | 198.78 |
| 10 | 1 | 237.301 | 52.77 |
| 10 | 12 | 284.029 | 58.22 |
| 50 | 1 | 63.995 | 37.30 |
| 50 | 12 | 66.911 | 42.75 |
| 100 | 1 | 48.254 | 35.34 |
| 100 | 12 | 51.163 | 40.72 |
| 1000 | 1 | 27.331 | 33.65 |
| 1000 | 12 | 35.608 | 39.09 |

### Bounded

BufferQueue and Channel capacities are both 8,192. Queues are prefilled; producer backpressure is not measured.

Channel references use the fixed `BatchSize=1` rows; BatchSize does not control Channel:

| Consumers | TryRead (µs) | ReadAllAsync (µs) | TryRead allocated (B) | ReadAllAsync allocated (B) |
|---:|---:|---:|---:|---:|
| 1 | 139.754 | 528.048 | 592 | 952 |
| 12 | 1,033.320 | 2,669.361 | 2,656 | 4,952 |

BufferQueue `ConsumeAsync`, `AutoCommit=true`:

| BatchSize | Consumers | Mean (µs) | Allocated (KiB) |
|---:|---:|---:|---:|
| 1 | 1 | 1,353.710 | 193.34 |
| 1 | 12 | 1,847.269 | 198.78 |
| 10 | 1 | 248.727 | 52.77 |
| 10 | 12 | 325.134 | 58.22 |
| 50 | 1 | 71.242 | 37.30 |
| 50 | 12 | 77.631 | 42.75 |
| 100 | 1 | 49.041 | 35.34 |
| 100 | 12 | 46.269 | 40.72 |
| 1000 | 1 | 27.448 | 33.65 |
| 1000 | 12 | 37.191 | 39.03 |

BufferQueue `ConsumeAsync`, `AutoCommit=false` with `CommitAsync` after each batch:

| BatchSize | Consumers | Mean (µs) | Allocated (KiB) |
|---:|---:|---:|---:|
| 1 | 1 | 1,390.612 | 193.34 |
| 1 | 12 | 2,284.660 | 198.78 |
| 10 | 1 | 261.925 | 52.77 |
| 10 | 12 | 322.325 | 58.22 |
| 50 | 1 | 73.574 | 37.30 |
| 50 | 12 | 77.823 | 42.75 |
| 100 | 1 | 52.502 | 35.34 |
| 100 | 12 | 50.216 | 40.78 |
| 1000 | 1 | 29.387 | 33.65 |
| 1000 | 12 | 38.730 | 39.09 |

### Single-reader Channel comparison

This class keeps one consumer and compares the default Channel implementation with Channel's `SingleReader` option.
The BufferQueue row still uses 12 partitions and one consumer.

Channel reference (fixed `BatchSize = 1` representative):

| Default TryRead (µs) | Default ReadAllAsync (µs) | SingleReader TryRead (µs) | SingleReader ReadAllAsync (µs) |
|---:|---:|---:|---:|
| 156.689 | 546.949 | 87.412 | 416.228 |

BufferQueue `ConsumeAsync` with `AutoCommit = true`:

| BatchSize | Mean (µs) | Allocated (KiB) |
|---:|---:|---:|
| 1 | 1,078.955 | 192.64 |
| 10 | 205.803 | 52.08 |
| 50 | 56.551 | 36.61 |
| 100 | 42.820 | 34.64 |
| 1000 | 22.325 | 32.95 |

The Channel methods do not consume `BatchSize`-sized batches; the parameter changes only the BufferQueue method.
`SingleReader = true` is a caller promise that at most one reader consumes the Channel; it is not the 12-consumer
configuration.

For one consumer at `BatchSize = 10`, the asynchronous unbounded BufferQueue row is 206.557 µs versus the fixed
Channel `ReadAllAsync` reference at 552.609 µs; the bounded values are 248.727 µs versus 528.048 µs. This is one
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

`ChannelReader<T>.ReadAllAsync` is the primary asynchronous comparison because both methods expose streams consumed
with `await foreach`:

```csharp
// ChannelReader<T>
IAsyncEnumerable<T> ReadAllAsync(CancellationToken cancellationToken = default);
// IBufferPullConsumer<T>
IAsyncEnumerable<IReadOnlyList<T>> ConsumeAsync(CancellationToken cancellationToken = default);
```

Channel yields one item per iteration; BufferQueue yields one batch. The current batch is an already materialized
`IReadOnlyList<T>` with `Count` and indexed access; it still supports `foreach` and LINQ. `ReadAllAsync` continues until the channel
completes and is drained; it does not collect all items into one batch. `ChannelReader<T>.ReadAsync` is a separate single-item `ValueTask<T>` comparison,
and `TryRead` is a synchronous reference rather than the primary asynchronous baseline.

The refreshed measurements in this report were run before the public batch signature changed from `IEnumerable<T>`
to `IReadOnlyList<T>`; the operation and snapshot data are otherwise unchanged, and the tables were not rerun for this
API-only adjustment.

## Memory Allocation

Producer allocation includes queue/channel creation and producer task scheduling inside the timed operation. It uses
12 tasks and 8,192 items; `Channel WriteAsync` is the async baseline, `Channel TryWrite` is a synchronous reference,
and the two BufferQueue bars are the item and batch producer APIs. These are not peak resident-memory or per-item
allocation measurements.

Consumer charts normalize BenchmarkDotNet's allocation display to `KiB/op` (`1 KiB = 1,024 B`). Consumption runs drain
8,192 prefetched items from 12 partitions with 1 or 12 consumers; `BatchSize` values are categorical, and the y-axis
is logarithmic. Queue and consumer setup is excluded from the measured allocation. The values are managed memory per
8,192-item benchmark operation, not peak resident memory or per-item allocation. BatchSize controls both BufferQueue
and Channel array batching. Auto/Manual allocations are nearly equal, so one combined curve shows the Auto Commit measurements.

BufferQueue allocates independent batch snapshots that survive later pulls, commits, segment recycling, and asynchronous
application processing. Elements are copied by value for value types and by reference for reference types, without deep
cloning. The original Channel references read individual items without creating batch containers.

The added `Channel_ReadAllAsync_Batched` curve also returns independently retained batches: `ReadAllAsync` fills a new
`int[BatchSize]`, and the batch iterator yields a `ReadOnlyMemory<int>` view of the filled prefix. Arrays are never pooled
or reused; a partial final batch does not allocate another trimmed copy. This comparison includes the cost of creating
batch storage on both sides, while preserving their different shared-queue/partition behavior. Auto / Manual Commit
allocation remains similar; Channel has no commit equivalent.

The original series retain their recorded values after a successful 113-case rerun; the new 20 array-batching cases are
recorded separately in [Channel array-batching measurements](channel-batch-aggregation.md). That report includes all elapsed
times, allocations, raw data, and the partial-batch and concurrency differences. The README's consumption allocation
charts compare independently retained batches; its elapsed-time charts retain the native per-item Channel baseline.

### Per-item Channel Reference

These original charts compare BufferQueue batch snapshots with Channel per-item reads. They remain useful for assessing
native item handoff, but Channel creates no batch container here, so they have limited relevance to the allocation cost
of returning retained batches. For that workload, use the [array-batching comparison](channel-batch-aggregation.md).
The Channel references below use the rows recorded alongside `BatchSize = 1` and stay horizontal; BatchSize affects
only BufferQueue in these charts.

![unbounded, 1 consumer](memory-unbounded-1-consumer-per-item-allocations.svg)

![unbounded, 12 consumers](memory-unbounded-12-consumers-per-item-allocations.svg)

![bounded, 1 consumer](memory-bounded-1-consumer-per-item-allocations.svg)

![bounded, 12 consumers](memory-bounded-12-consumers-per-item-allocations.svg)

## Read-path Optimization Measurements

These earlier focused comparisons are retained separately from the refreshed README data; they were not rerun here.
Both use the same historical baseline that already
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

Benchmark sources:

- [Producer](../../tests/BufferQueue.Benchmarks/ChannelVsMemoryBufferQueueProduceBenchmark.cs)
- [Unbounded consume](../../tests/BufferQueue.Benchmarks/UnboundedChannelVsMemoryBufferQueueConsumeBenchmark.cs)
- [Bounded consume](../../tests/BufferQueue.Benchmarks/BoundedChannelVsMemoryBufferQueueConsumeBenchmark.cs)
- [Single-reader consume](../../tests/BufferQueue.Benchmarks/SingleReaderChannelVsMemoryBufferQueueConsumeBenchmark.cs)

Run the producer and consumer classes separately and sequentially:

```sh
dotnet run --project tests/BufferQueue.Benchmarks -c Release -- --filter 'BufferQueue.Benchmarks.ChannelVsMemoryBufferQueueProduceBenchmark.*' --exporters json
dotnet run --project tests/BufferQueue.Benchmarks -c Release -- --filter 'BufferQueue.Benchmarks.UnboundedChannelVsMemoryBufferQueueConsumeBenchmark.*' --warmupCount 6 --iterationCount 20 --launchCount 1 --exporters json
dotnet run --project tests/BufferQueue.Benchmarks -c Release -- --filter 'BufferQueue.Benchmarks.BoundedChannelVsMemoryBufferQueueConsumeBenchmark.*' --warmupCount 6 --iterationCount 20 --launchCount 1 --exporters json
dotnet run --project tests/BufferQueue.Benchmarks -c Release -- --filter 'BufferQueue.Benchmarks.SingleReaderChannelVsMemoryBufferQueueConsumeBenchmark.*' --warmupCount 6 --iterationCount 20 --launchCount 1 --exporters json
```

The producer's fixed job uses one launch, six warmups and fifteen measured iterations. The three consumer jobs
use one launch, six warmups and twenty measured iterations; `IterationSetup` requires invocation count and unroll
factor one. The [run record](memory-queues-run.json) retains source fingerprints, exact configuration and result counts.
All 113 rows were refreshed on combined checkout `e63d152b388a99069602a76131b9589bfd8b472c`.

BenchmarkDotNet reported short-iteration and environment-priority warnings. The CSV retains error and standard
deviation; `Error` is the half-width of the reported 99.9% confidence interval. Consumer task scheduling, especially
with twelve readers, adds variance. These measurements do not establish production throughput or backpressure latency.
The historical routing and read-path experiments above retain their original measurements and configurations.
