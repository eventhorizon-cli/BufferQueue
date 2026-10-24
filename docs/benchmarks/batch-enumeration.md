# Singleton batch enumeration allocation

[English](batch-enumeration.md) | [简体中文](batch-enumeration.zh-CN.md)

This investigation addresses [#59](https://github.com/eventhorizon-cli/BufferQueue/issues/59).
It is separate from the historical Count-only end-to-end results and charts. It measures an actual
`foreach` checksum and compares the existing compiler-generated singleton iterator with a private
explicit enumerator. It changes neither public traversal APIs nor singleton snapshot allocation.

## Method

Baseline: `50f867f44a92b70ea2ff59fade12e87f9494b4bd`. Candidate: the same source plus the explicit
singleton enumerator. Both use the identical checked-in
[`BatchEnumerationProbe`](../../tests/BufferQueue.Benchmarks/BatchEnumerationProbe.cs).
Release, SDK 10.0.100, .NET 10.0.0, macOS 27.0.1 Arm64, Apple M2 Max. Runs were sequential, with no concurrent builds or benchmarks. CPU timings are elapsed drain time,
not hardware CPU counters.

Each case has six warmup drains and twenty recorded samples. Each sample accumulates individual
drains until at least 10 ms of measured time has elapsed. Prefill, queue/channel creation, payload
creation, and consumer registration are outside both counters. The measured window includes async
enumerator creation/disposal, pulls, ordinary per-item `foreach`, checksum, and manual commits when
configured. All operations must complete synchronously; otherwise the probe fails. Allocation uses
`GC.GetAllocatedBytesForCurrentThread`; time uses `Stopwatch`. Every enumerated drain validates both
item count and checksum. Count-only controls validate count without traversing items.

Memory uses twelve partitions and 8,192 items, with batch sizes 1, 10, and 1,000. Additional sentinels
cover a reference payload, Manual Commit, bounded capacity, twelve consumers, and twelve items with
configured batch size 1,000 (one available item per partition). Other settings retain unbounded,
Auto Commit, and one consumer. This selected matrix is not a full Cartesian coverage claim.

Channel uses one prefilled completed channel, `ReadAllAsync`, the same payload/checksum, and the same
number of consumer loops. Twelve loops drain sequential quotas from that one channel; Memory loops
also drain sequentially, owning their assigned partitions. These are synchronous allocation/drain
probes, **not concurrent throughput measurements**. Channel is a supplementary control; batch size
and commit settings do not alter its behavior.

The first pass used default runtime settings, baseline then candidate. A diagnostic confirmation
reversed the order with `DOTNET_TieredCompilation=0` for both. Disabling tiering is a different runtime
policy, not an equivalent replacement for default-policy results. The default pass has substantial
warmup/JIT-phase variability, including changes in unchanged Count-only controls. No CPU speedup is
established by these measurements.

## Default runtime results

Times are mean ± sample standard deviation, in microseconds per complete drain (20 samples).
Allocation per drain was constant across recorded samples in each case. All rows use ordinary `foreach`.

| Case | Baseline µs | Candidate µs | Baseline B | Candidate B |
| --- | ---: | ---: | ---: | ---: |
| int, batch 1 | 1138.75 ± 117.13 | 1128.61 ± 130.76 | 458880 | 393344 |
| int, batch 10 | 105.85 ± 3.37 | 108.74 ± 3.35 | 80096 | 80096 |
| int, batch 1000 | 40.66 ± 0.48 | 40.85 ± 1.75 | 49280 | 49280 |
| reference, batch 1 | 913.58 ± 149.05 | 880.77 ± 130.54 | 524416 | 458880 |
| int, batch 1, Manual | 479.36 ± 19.13 | 483.54 ± 12.22 | 458880 | 393344 |
| int, batch 1, bounded | 626.99 ± 32.40 | 608.70 ± 31.87 | 458880 | 393344 |
| int, batch 1, 12 consumers | 437.07 ± 12.37 | 430.11 ± 6.83 | 460288 | 394752 |
| int, configured 1000, 12 actual items | 0.738 ± 0.005 | 0.730 ± 0.003 | 800 | 704 |

For 8,192 singleton batches, both payload types save 65,536 B: **8 B per singleton enumeration**.
The measured enumerator sizes change from 32 to 24 B for `int`, and 40 to 32 B for the reference
payload. The independent singleton snapshot still costs 24 B in these cases. This is a 14.3% reduction
in total `int` foreach drain allocation and a 12.5% reduction for the reference payload, not
allocation-free enumeration. The sparse sentinel saves 96 B for twelve singleton enumerations.
Batch 10/1,000 do not return singleton batches in this prefilled workload and remain unchanged.

Count-only controls remain exactly 196736 B (singleton), 53504 B (batch 10), 48800 B (batch 1000),
198144 B (12 consumers), and 416 B (sparse). Reference, Manual, and bounded singleton controls also
remain 196736 B. No snapshot allocation regression was measured. Count-only default timing changes
include −14.3% and +20.6% in unchanged paths, so timing differences cannot be attributed solely to
this iterator. Channel allocations also remain unchanged: 128 B for one value-type consumer,
136 B for one reference-type consumer, and 1536 B for twelve value-type consumer loops.

## Diagnostic reverse-order confirmation

| Case | Baseline µs | Candidate µs |
| --- | ---: | ---: |
| int, batch 1 | 687.82 ± 24.30 | 668.39 ± 32.75 |
| reference, batch 1 | 682.51 ± 11.12 | 687.90 ± 10.56 |
| int, batch 1, Manual | 702.38 ± 10.47 | 697.18 ± 16.44 |
| int, batch 1, bounded | 750.82 ± 14.19 | 733.29 ± 12.94 |
| int, batch 1, 12 consumers | 694.47 ± 12.86 | 678.56 ± 17.83 |

Allocations exactly reproduce the default pass. The reference timing difference is +0.8%, within
observed sample variation; unchanged Channel controls also drift. No material reference-payload
regression is established. The supported conclusion is the allocation saving, without a CPU-win claim.

## Reproduce and inspect

```sh
dotnet build tests/BufferQueue.Benchmarks/BufferQueue.Benchmarks.csproj -c Release
dotnet tests/BufferQueue.Benchmarks/bin/Release/net10.0/BufferQueue.Benchmarks.dll --batch-enumeration-probe > enumeration.csv
# Diagnostic runtime policy, applied identically to baseline and candidate:
DOTNET_TieredCompilation=0 dotnet tests/BufferQueue.Benchmarks/bin/Release/net10.0/BufferQueue.Benchmarks.dll --batch-enumeration-probe > enumeration-diagnostic.csv
```

Build the same probe on the baseline and candidate separately; do not compare different harnesses.
The baseline production assembly was preserved before applying the candidate change. The
[raw samples](batch-enumeration-raw.csv) retain all four passes, including the noisy default pass;
the [summary](batch-enumeration-summary.csv) includes every Memory/Channel and Count-only control,
mean, sample standard deviation, minimum, maximum, and allocation. Raw `drains` records the sample
aggregation count; times and allocations are normalized per complete drain.
