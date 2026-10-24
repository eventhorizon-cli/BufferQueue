# Memory Queue 基准测试

[English](memory-queues.md)

这份报告记录根 README 图表背后的完整基准测试。汇总文件是
[memory-queues-results.csv](memory-queues-results.csv)，包含 8 行 producer 结果和 3 个消费 benchmark 类的 105
行结果。CSV 保留 mean、error、standard deviation、allocation 以及每个配置；下面的表格为便于阅读主要展示
mean 和 allocation。

除特别说明外，本报告使用 Apple M2 Max（12 核）、macOS 27.0.1 / Arm64、.NET 10.0.0 和
BenchmarkDotNet 0.15.8 Release 配置。

## Producer 对比

Producer benchmark 使用 12 个并发 `Task.Run` producer task 写入 8,192 个 `int`。Queue 创建和 task scheduling
都在计时方法内。Channel 的主比较使用异步 `WriteAsync`，同步 `TryWrite` 作为参考；BufferQueue 的 item 和
`ReadOnlyMemory<int>` batch 都使用异步 `ProduceAsync`。bounded capacity 足够容纳完整 workload，因此这里不测
producer 等待背压的路径。

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

![unbounded Producer API](memory-producer-unbounded.zh-CN.svg)

![bounded Producer API](memory-producer-bounded.zh-CN.svg)

源码：[ChannelVsMemoryBufferQueueProduceBenchmark.cs](../../tests/BufferQueue.Benchmarks/ChannelVsMemoryBufferQueueProduceBenchmark.cs)。

### PartitionKey 路由性能

以下是保留的历史路由测量，早于当前 consumer 对比，未使用当前消费 benchmark workload 重跑。下面的
[`MemoryBufferPartitionerBenchmark`](../../tests/BufferQueue.Benchmarks/MemoryBufferPartitionerBenchmark.cs)
测量完整的 Memory 模式 producer 路径，使用 `8` 个 partition、`8,192` 条消息重复 `832` 次、`6` 次预热和
`15` 次测量迭代。结果为单条写入耗时，运行环境为 macOS 27.0.1、ARM64 和 .NET 10.0.0；
所有路径均无托管内存分配。字符串 key 包含 `7` 个 UTF-16 码元；完整 key 哈希的耗时随 key 长度增加。

| 路由方式 | Mean | 相对轮询 |
| --- | ---: | ---: |
| 轮询 | `13.40 ns` | `1.00x` |
| `int` key | `12.03 ns` | `0.90x` |
| `string` key（XXH3-64，完整 UTF-16 key） | `12.94 ns` | `0.97x` |
| 自定义消息，取数值 `CustomerId` key | `11.19 ns` | `0.83x` |

通过以下命令运行该 producer 基准：

```bash
dotnet run -c Release --project tests/BufferQueue.Benchmarks/BufferQueue.Benchmarks.csproj -- --filter '*MemoryBufferPartitionerBenchmark*'
```

独立的 [`StringPartitionRoutingBenchmark`](../../tests/BufferQueue.Benchmarks/StringPartitionRoutingBenchmark.cs)
仅测量字符串路由，使用预先构造的 `4`、`9`、`64`、`120`、`121`、`256`、`1024` 个 UTF-16 码元的 key 和 `8` 个 partition。
`120`/`121` 两组覆盖 XXH3 的 `240` 字节长输入阈值两侧。
它使用一次启动、`6` 次预热和 `10` 次测量迭代，目标迭代时间为 `100 ms`。
其结果为单次路由耗时，不包含队列写入；上表数据来自 producer 基准。
通过以下命令运行 key 长度基准：

```bash
dotnet run -c Release --project tests/BufferQueue.Benchmarks/BufferQueue.Benchmarks.csproj -- --filter '*StringPartitionRoutingBenchmark*'
```

## 主要消费对比

本节测量预填充队列的消费路径，包含异步 BufferQueue 消费和 Channel 参考。主要 BufferQueue 行使用
`AutoCommit = true`；Manual Commit 和同步 `TryRead` 参考在下面单独展示。

Channel 没有对应的 `BatchSize`；它的 baseline 按 consumer 数量只展示一次，与 BufferQueue 的配置无关。BufferQueue
使用 `1、10、50、100、1000` 五个配置。

每次运行消费预填充的 8,192 个 `int`。BufferQueue 使用 12 个 partition，consumer 数量为 1 或 12。预填充后
complete Channel writer，确保 `ReadAllAsync` 有明确结束点。每个 consumer 在独立的 `Task.Run` 中运行。Channel reader 共享同一个 Channel，BufferQueue consumer
读取各自分配的 partition。Channel
reader 本地计数，所有 reader 完成后再汇总。BufferQueue 按分配给每个 consumer 的 batch 计数，并校验每个分配计数
和总数。每条消息没有业务处理；计时包含运行 reader 的 task scheduling。

Manual Commit 行是在相同的 `e615da8` 生产基线之上单独测量的，新增 benchmark 方法后使用一次 launch、6 次 warmup、
15 次测量迭代，并在说明的 .NET 10 / Apple M2 Max 环境中运行；每个 async batch 后调用 `CommitAsync`。已有的
Auto Commit 和 Channel 行仍作为汇总基线。

### unbounded

Channel reference（固定的 `BatchSize = 1` 代表值）：

| Consumers | Channel TryRead (us) | Channel ReadAllAsync (us) | TryRead allocation (B) | ReadAllAsync allocation (B) |
|---:|---:|---:|---:|---:|
| 1 | 139.000 | 513.130 | 592 B | 952 B |
| 12 | 3,446.620 | 3,381.500 | 2,656 B | 4,952 B |

BufferQueue `ConsumeAsync`（`AutoCommit = true`）：

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

BufferQueue `ConsumeAsync`（`AutoCommit = false`，每个 batch 后调用 `CommitAsync`）：

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

`bounded` 使用容量 8,192 的 BufferQueue 和容量 8,192 的 Channel。队列在计时方法开始前已预填充，因此这里
是 drain benchmark，不测 producer 等待容量的路径。

Channel reference（固定的 `BatchSize = 1` 代表值）：

| Consumers | Channel TryRead (us) | Channel ReadAllAsync (us) | TryRead allocation (B) | ReadAllAsync allocation (B) |
|---:|---:|---:|---:|---:|
| 1 | 131.060 | 480.810 | 592 B | 952 B |
| 12 | 1,036.560 | 2,760.870 | 2,656 B | 4,952 B |

BufferQueue `ConsumeAsync`（`AutoCommit = true`）：

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

BufferQueue `ConsumeAsync`（`AutoCommit = false`，每个 batch 后调用 `CommitAsync`）：

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

### Single-reader Channel 对比

这个 benchmark 固定一个 consumer，对比默认 Channel 实现和 Channel 的 `SingleReader` 选项。BufferQueue 仍使用
12 个 partition 和一个 consumer。

Channel reference（固定的 `BatchSize = 1` 代表值）：

| Default TryRead (us) | Default ReadAllAsync (us) | SingleReader TryRead (us) | SingleReader ReadAllAsync (us) |
|---:|---:|---:|---:|
| 143.240 | 509.780 | 77.910 | 393.280 |

BufferQueue `ConsumeAsync`（`AutoCommit = true`）：

| BatchSize | Mean (us) | Allocated (KB) |
|---:|---:|---:|
| 1 | 1,078.010 | 192.64 KB |
| 10 | 207.230 | 52.77 KB |
| 50 | 50.720 | 37.77 KB |
| 100 | 36.190 | 35.80 KB |
| 1000 | 17.060 | 48.17 KB |

Channel 方法不会消费 `BatchSize` 大小的 batch；该参数只改变 BufferQueue 方法。`SingleReader = true` 是调用方
保证最多只有一个 reader 消费 Channel，不是 12-consumer 配置。

以一个 consumer、`BatchSize = 10` 为例，异步 unbounded BufferQueue 为 201.040 us，固定 Channel `ReadAllAsync`
参考值为 513.130 us；bounded 对应为 248.920 us 和 480.810 us。这是一个实测配置，不能据此声称 BufferQueue
在所有 batch size 或 consumer 数量下都更快。与 `TryRead` 对比时，应按 async 与 sync 接口的对比解读。

图表阅读说明：横轴将各档 BatchSize 配置等距排列，间距不代表数值差；耗时纵轴使用对数刻度。
点和线表示 mean，误差线或区间使用 BenchmarkDotNet 报告的 `Error`。
Channel baseline 固定为代表性的 `BatchSize = 1`，与 BufferQueue 的 `BatchSize` 无关。

消费图表同时展示两种 async BufferQueue 模式，以 `ReadAllAsync` 作为 Channel 主要对比，灰色 `TryRead` 线作为同步参考。Manual Commit 使用 `AutoCommit = false`，每个 batch 后调用
`CommitAsync`；Channel 没有内建的等价确认、checkpoint 或重放机制。

主要异步对比选择 `ChannelReader<T>.ReadAllAsync`，因为两边的方法都返回可以通过 `await foreach` 消费的异步流：

```csharp
// ChannelReader<T>
IAsyncEnumerable<T> ReadAllAsync(CancellationToken cancellationToken = default);
// IBufferPullConsumer<T>
IAsyncEnumerable<IEnumerable<T>> ConsumeAsync(CancellationToken cancellationToken = default);
```

Channel 每轮返回一条消息，BufferQueue 每轮返回一个 batch。`ReadAllAsync` 会持续读取直到已完成的 Channel 被读空，
并不是把全部数据收集成一个 batch。`ChannelReader<T>.ReadAsync` 返回单次读取的 `ValueTask<T>`，属于另一项单条异步对比；
`TryRead` 是同步参考，不是主要异步基线。

## 内存分配

Producer allocation 的计时包含 queue/channel creation 和 producer task scheduling。测试使用 12 个 task 写入 8,192 条消息；
`Channel WriteAsync` 是 async baseline，`Channel TryWrite` 是同步参考，BufferQueue 的两个 bar 分别表示逐条和 batch producer API。
这些数值不是峰值驻留内存，也不是单条消息 allocation。

![unbounded producer](memory-producer-unbounded-allocations.zh-CN.svg)

![bounded producer](memory-producer-bounded-allocations.zh-CN.svg)

Consumer 图将 BenchmarkDotNet 的 allocation 统一转换为 `KiB/op`（`1 KiB = 1,024 B`）。消费测试从 12 个 partition
中消费预填充的 8,192 条消息，分别使用 1 个和 12 个 consumer；`BatchSize` 横轴按分类配置等距显示，纵轴使用对数刻度。
队列和 consumer 的 setup 不计入测量 allocation。数值表示每次消费 8,192 条消息的 benchmark operation 分配的托管内存，
不是峰值驻留内存，也不是单条消息 allocation。两条 Channel 参考线均取自与 `BatchSize = 1` 一起记录的结果，BatchSize 只影响 BufferQueue；Auto/Manual 曲线基本重合。

![unbounded，1 个 consumer](memory-unbounded-1-consumer-allocations.zh-CN.svg)

![unbounded，12 个 consumer](memory-unbounded-12-consumers-allocations.zh-CN.svg)

![bounded，1 个 consumer](memory-bounded-1-consumer-allocations.zh-CN.svg)

![bounded，12 个 consumer](memory-bounded-12-consumers-allocations.zh-CN.svg)

## Read-path 优化测量

下面的 focused comparison 与 README 图表数据分开。两组测试使用同一个 baseline，该 baseline 已包含 Frozen reader
lookup、minimum-reader scan、published segment range check 以及 producer lock-handoff fix。第二组不包含第一组的代码变更。

### A：single-item reader path

`single-read-before` 和 `single-read-after` 对比确切配置为 `BatchSize = 1` 的 reader specialization。表格限定为
`PartitionNumber = 1 或 12`、`GroupCount = 1 或 2`，并覆盖两个 `bounded` 值。所有行的 allocation 不变。只有一个 consumer 消费数据；`GroupCount = 2` 时，第二个 group 保持空闲，
用于覆盖 retention 和查找路径，并非两个活跃 consumer 的吞吐测试。

| Method | Partitions | bounded | Groups | Before (us) | After (us) | Change | Allocation |
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

before/after 行保存在 [read-path-optimizations.csv](read-path-optimizations.csv) 中。

### B：每次只有一个可用 item 的 sparse batch

`MemorySparseBatchBenchmark` 按顺序生产和消费，每次只生产一个 item，并验证每次 pull 恰好包含一个 item。
`BatchSize` 为 10、100 或 1000，但 available count 始终为 1。这个测试把 lazy allocation path 与并发吞吐隔离开。
after 版本只有在找到第二个 item 后才分配 batch array。

| Method | BatchSize | bounded | Before (ms) | After (ms) | Change | Before allocation | After allocation |
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

`BatchSize = 1000` 时 allocation 从 `32,452.6 KB` 降到 `196.6 KB`。这些是 BenchmarkDotNet 报告的二进制 KB，
1 KB 等于 1,024 bytes。完整 batch 场景没有显示出明确的大幅收益；这里的强收益特指每次只有一个 item 的 sparse 场景。

before/after 行保存在 [read-path-optimizations.csv](read-path-optimizations.csv) 中。

合并检查保存在 [read-path-optimizations.csv](read-path-optimizations.csv) 中。它保持 A 和 B 独立，同时检查两项
read-path 优化合并后的行为。

## 复现方式

Benchmark 源码：

- [Unbounded consume](../../tests/BufferQueue.Benchmarks/UnboundedChannelVsMemoryBufferQueueConsumeBenchmark.cs)
- [Bounded consume](../../tests/BufferQueue.Benchmarks/BoundedChannelVsMemoryBufferQueueConsumeBenchmark.cs)
- [Single-reader consume](../../tests/BufferQueue.Benchmarks/SingleReaderChannelVsMemoryBufferQueueConsumeBenchmark.cs)
- [Memory commit modes](../../tests/BufferQueue.Benchmarks/MemoryCommitModeBenchmark.cs)
- [Sparse available-item path](../../tests/BufferQueue.Benchmarks/MemorySparseBatchBenchmark.cs)

主消费和 producer 结果可以使用以下分开的 quoted filter 复现：

```shell
dotnet run --project tests/BufferQueue.Benchmarks -c Release -- --filter '*UnboundedChannelVsMemoryBufferQueueConsumeBenchmark*' --warmupCount 6 --iterationCount 15 --launchCount 1
dotnet run --project tests/BufferQueue.Benchmarks -c Release -- --filter '*BoundedChannelVsMemoryBufferQueueConsumeBenchmark*' --warmupCount 6 --iterationCount 15 --launchCount 1
dotnet run --project tests/BufferQueue.Benchmarks -c Release -- --filter '*SingleReaderChannelVsMemoryBufferQueueConsumeBenchmark*' --warmupCount 6 --iterationCount 15 --launchCount 1
dotnet run --project tests/BufferQueue.Benchmarks -c Release -- --filter '*ChannelVsMemoryBufferQueueProduceBenchmark*' --warmupCount 6 --iterationCount 15 --launchCount 1
```

Focused comparison 使用：

```shell
dotnet run --project tests/BufferQueue.Benchmarks -c Release -- --filter '*MemoryCommitModeBenchmark*'
dotnet run --project tests/BufferQueue.Benchmarks -c Release -- --filter '*MemorySparseBatchBenchmark*'
```

主消费 job 使用 1 次 launch、6 次 warmup、15 次 measured iteration、invocation count 1。Focused job 使用 1 次
launch、6 次 warmup、20 次 measured iteration、invocation count 1。Producer benchmark 使用自身的 fixed job 配置。
环境为 .NET SDK 10.0.100、.NET 10.0.0 Arm64、BenchmarkDotNet 0.15.8、macOS 27.0.1。源码 checkout 基于
`e0a06318a7a09c194578a072432a68d1adf42311`，并应用本地 benchmark 和 read-path 改动。

BenchmarkDotNet 在该机器上报告了 short-iteration 和 environment-priority warnings。这些数字适合观察趋势和回归，
不能当作生产端到端吞吐保证。原始 CSV 保留 error 和 standard-deviation 列，便于进一步判断方差。
