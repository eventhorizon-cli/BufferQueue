# Partial Batch Allocation

[English](partial-batch-allocation.md) | [简体中文](partial-batch-allocation.zh-CN.md)

The [measurements](partial-batch-allocation.csv) contain 88 rows with complete scenario fields, source hashes,
means, 99.9% confidence-interval margins, standard deviations, and allocated bytes. This experiment isolates
partial-array sizing from the separate enumeration work in [#59](https://github.com/eventhorizon-cli/BufferQueue/issues/59).

## Change and scope

Baseline: main `50f867f44a92b70ea2ff59fade12e87f9494b4bd`, with the identical benchmark source in both checkouts.
The candidate changes only the Memory reader's allocation boundary. When its first array allocation reaches a
published tail without a visible successor, it allocates for the copied prefix and returns that prefix. Appends
published after the observation remain available to the next pull. Full single-segment batches do not perform an
additional successor read. The existing single-item representation is unchanged.

The optimization does not pre-scan the linked segment chain or add producer counters. If a successor is already
visible at the first allocation, the existing requested-size array remains; not every cross-segment partial batch
receives an exact-size array. Returned storage remains independently owned, without pooling or exposure of recyclable slots.

## Workloads

Environment: Apple M2 Max (12 cores), macOS 27.0.1 / Arm64, .NET 10.0.0, BenchmarkDotNet 0.15.8, Release.
The full matrix uses one launch, six warmups, twenty measured iterations, invocation count one and unroll factor one.
A four-case confirmation runs candidate first, then baseline, with ten warmups and forty iterations. Runs are sequential,
with no concurrent builds or benchmarks. These short operations have minimum-iteration-time warnings; timing conclusions
must retain their uncertainty. The CSV records retained measurement counts after BenchmarkDotNet outlier handling.

`MemoryPartialBatchAllocationBenchmark` validates actual counts and a checksum by enumerating every item.
Each operation processes 8,192 `int` values with segment size 1,024:

- Sparse cases use one partition and one consumer. Each producer burst makes 1, 2 or 10 items available and is
  immediately followed by a pull. The operation includes production and consumption; it is not concurrent producer
  throughput. Ten-item bursts end with a final two-item batch. Bounded capacity is 64 and does not saturate.
- Prefilled controls exclude setup and production. They use one partition/one consumer or twelve partitions/twelve
  consumers, including consumer task scheduling. Bounded capacity is 8,192.
- Auto Commit advances after each pull; Manual Commit calls `CommitAsync` after every enumerated batch.

These are isolated before/after measurements, not replacements for the README's Channel comparison.

## Final candidate results

At configured BatchSize 1,000, two available items reduce allocation from **16,781,928 to 266,856 B/op (98.41%)**.
Ten available items reduce it from **3,363,432 to 107,400 B/op (96.81%)**. Each allocation result is identical across
Auto/Manual Commit and bounded/unbounded in this matrix.

| Available items | Mode | Commit | Baseline µs | Candidate µs | Change |
|---:|---|---|---:|---:|---:|
| 2 | unbounded | Auto | 1,528.98 | 844.11 | -44.79% |
| 2 | unbounded | Manual | 1,436.04 | 841.87 | -41.38% |
| 2 | bounded | Auto | 2,172.42 | 1,460.26 | -32.78% |
| 2 | bounded | Manual | 2,071.38 | 1,450.59 | -29.97% |
| 10 | unbounded | Auto | 458.32 | 369.61 | -19.35% |
| 10 | unbounded | Manual | 445.71 | 368.66 | -17.29% |
| 10 | bounded | Auto | 834.27 | 742.52 | -11.00% |
| 10 | bounded | Manual | 823.19 | 741.45 | -9.93% |

With two available items, unbounded Auto Commit at configured BatchSize 10/50/100 reduces allocation by
52.50%/78.07%/86.90%; mean time changes by -3.13%/-7.50%/-8.46%. A large configured maximum no longer forces a large
array for the usual unlinked sparse tail.

The prefilled BatchSize 1 control has unchanged allocation. Its initial mean moves +1.77%, but reversed confirmation
moves -0.08%. Single-available-item allocation is also unchanged: unbounded Auto/Manual initial means move +2.01%/+0.24%,
and confirmation moves +0.29%/+0.15%. The confirmation error intervals overlap; these small differences are not evidence
of a speedup or a material regression. The strong two-item sentinel repeats its gain: 1,440.33 to 828.53 µs (-42.48%).

Prefilled one-consumer Auto Commit sizes 10/100/1000 show initial mean changes of -8.45%/-8.80%/-6.31%; allocation falls
only for final partial tails. Twelve-consumer size-100 controls allocate 45,552 to 44,272 B/op. Their 21–30% lower initial
means are sensitive to task scheduling and have not been separately confirmed.

| Reversed confirmation | Baseline mean ± error, µs | Candidate mean ± error, µs |
|---|---:|---:|
| full-batch1-unbounded-auto-1c | 1,084.35 ± 11.57 | 1,083.51 ± 15.73 |
| sparse1-batch1000-unbounded-auto-1c | 1,561.62 ± 29.73 | 1,566.19 ± 23.72 |
| sparse1-batch1000-unbounded-manual-1c | 1,563.40 ± 25.25 | 1,565.72 ± 19.64 |
| sparse2-batch1000-unbounded-auto-1c | 1,440.33 ± 34.56 | 828.53 ± 17.45 |

## Earlier candidate and correctness

The CSV retains `candidate-v1` and `reverse-confirmation-v1` rows. V1 put the zero-batch guard at the start of the batch
reader. The unbounded Manual single-available-item control moved +1.89% initially and +1.75% on reversal, with overlapping
intervals. The final candidate moves that check into the first-array-allocation block, keeping it off the singleton
staging path. A fresh complete candidate matrix and paired reverse confirmation produce the final results above; the
V1 measurements are not silently discarded or presented as the final implementation.

Focused tests cover sparse allocation independent of configured maximum, retained reference snapshots across
append/replay/commit/recycling, exact bounded capacity release, and segment sizes two/three across `ulong` wrap.
The mixed-producer/two-group concurrency matrix also runs BatchSize 1,000 with bounded/unbounded and round-robin/keyed
routing. Commit and wake-up synchronization are unchanged. Final format/restore/Release-build/full-solution checks pass
on .NET 8 and .NET 10: 292 core and 133 MMF tests per framework, 850 executions in total.

## Reproduce

```sh
dotnet run -c Release --project tests/BufferQueue.Benchmarks/BufferQueue.Benchmarks.csproj -- \
  --filter '*MemoryPartialBatchAllocationBenchmark*' --exporters json
```

Use the same harness in a baseline checkout and the candidate checkout. JSON `FullName` preserves complete scenario
names; displayed `Parameters` can be truncated. Production-file and harness SHA-256 fingerprints in the CSV identify
which source was used. The final pair is `baseline/initial` versus `candidate/final`; the final reversed pair uses
`reverse-confirmation` for both variants.
