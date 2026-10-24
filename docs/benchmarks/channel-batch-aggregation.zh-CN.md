# Channel 批量聚合

[English](channel-batch-aggregation.md)

本报告说明与[内存队列 benchmark](memory-queues.zh-CN.md)配套的 Channel allocation 对照 workload。普通 Channel
`ReadAllAsync` 行仍然是原始的单条读取 baseline。本 workload 表示应用读取这些单条数据后自行创建并填充独立数组，
再按 batch 处理；它不改变 Channel API，也不替换已有 baseline。

完整测量行已保存到
[channel-batch-aggregation-results.csv](channel-batch-aggregation-results.csv)，运行配置和源码指纹保存到
[channel-batch-aggregation-run.json](channel-batch-aggregation-run.json)。

## 为什么增加这个对照

`ChannelReader<T>.ReadAllAsync` 每轮返回一条数据，而 BufferQueue `ConsumeAsync` 返回已生成的批次。把原始
Channel stream 与返回 batch 的 API 直接比较，可以说明 API 层面的差异，但不会包含应用在需要 batch 形式的
Channel 数据时付出的分配成本。本对照补充这个 allocation 场景，但不声称两种实现完全相同。

Channel helper 通过 `ReadAllAsync` 每条读取一次。每个 reader 在某个 batch 收到第一条数据时，延迟分配一个新的
固定大小 `int[BatchSize]`，随后按顺序填充，并以 `ReadOnlyMemory<int>` 返回。完整 batch 暴露整个数组；最后的
未满 batch 只暴露已填充的前缀。数组不池化、不复用，也不通过 trim-copy 生成第二个数组。计时中的 consumer
只对每个返回 batch 累加 `ReadOnlyMemory<int>.Length`，并校验所有 reader 的总数为 8,192；不增加逐条业务处理，
也不再做一次应用侧逐条遍历。

这模拟了保留 batch 所需要的独立所有权：每个返回值都由独立数组支持。但它仍然只是聚合对照。Channel 的多个
reader 共享一个 reader 拓扑，而 BufferQueue consumer 读取各自分配的 partition，因此最后未满 batch 的数量和分布
可能不同。Channel 没有确认或重放契约，因此不适用 `Auto Commit` 和 `Manual Commit`。原始的独立 Channel
baseline 仍是主要的单条 stream 对比。本报告不包含 `SingleReader` 的 batch 测量。

## Workload

两边使用已有的预填充消费 workload：BufferQueue 使用 12 个 partition、8,192 条 `int`，consumer 数量为 1 或 12。
Channel 对照使用一个已完成的预填充 Channel，多个 reader 共享它。矩阵覆盖容量为 8,192 的 `bounded` 和 `unbounded`，
`BatchSize` 为 `1`、`10`、`50`、`100`、`1000`。

helper 返回满批，或在已完成的 Channel 被读空时交付最后一个不满批次。它不实现 BufferQueue 在实时稀疏流量下的
机会式部分批次读取。因此这些结果适用于预填充负载，不代表等待凑批的延迟或通用的 Channel 组批策略。
额外耗时属于应用侧适配层，不能归因于 Channel 原生读取 API 变慢。

Queue/channel 创建和预填充不在计时内。计时方法包含每个 reader 的一个 `Task.Run`、异步 iterator，以及该对照产生的
新数组。每个 reader 累加自己返回的 memory 长度，最后校验总数为 8,192，避免遗漏尾 batch 后仍得到看似合理的 allocation
结果。

benchmark 使用 Apple M2 Max（12 核）、macOS 27.0.1 / Arm64、.NET 10.0.0、runtime 自带的
`System.Threading.Channels` 10.0.0.0，以及 BenchmarkDotNet 0.15.8 Release 配置。运行使用一次 launch、六次 warmup、
二十次测量迭代，invocation count 为 1，unroll factor 为 1。这些短操作可能触发 BenchmarkDotNet 的
minimum-iteration-time 和 high-priority 警告；原始 CSV 和运行记录保留测量配置与不确定性，不把这些警告解释成性能结论。

## 结果

测量矩阵包含上面列出的 bounded/unbounded、consumer 数量和 batch size 组合。CSV 保留全部 20 组的 mean、error、
standard deviation 和 allocated bytes；下面列出全部 20 行，展示 mean +/- 99.9% 置信区间半宽、标准差和 allocation。

### Bounded

| Consumers | BatchSize | Mean (us) | Error (us) | StdDev (us) | Allocated (KiB) |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 1,197.836 | 12.061 | 11.846 | 257.15 |
| 1 | 10 | 900.465 | 29.514 | 33.989 | 52.40 |
| 1 | 50 | 793.739 | 18.644 | 20.722 | 37.02 |
| 1 | 100 | 779.517 | 15.272 | 17.588 | 35.10 |
| 1 | 1000 | 810.886 | 23.922 | 27.548 | 36.52 |
| 12 | 1 | 2,525.362 | 152.856 | 169.899 | 263.38 |
| 12 | 10 | 2,777.501 | 162.398 | 187.017 | 58.88 |
| 12 | 50 | 2,807.492 | 240.245 | 276.666 | 44.34 |
| 12 | 100 | 2,699.128 | 131.536 | 146.202 | 43.81 |
| 12 | 1000 | 2,696.101 | 220.518 | 253.949 | 54.53 |

### Unbounded

| Consumers | BatchSize | Mean (us) | Error (us) | StdDev (us) | Allocated (KiB) |
|---:|---:|---:|---:|---:|---:|
| 1 | 1 | 1,227.838 | 27.349 | 30.398 | 257.15 |
| 1 | 10 | 929.907 | 23.680 | 26.321 | 52.40 |
| 1 | 50 | 837.535 | 24.350 | 28.041 | 37.02 |
| 1 | 100 | 812.735 | 18.946 | 21.059 | 35.10 |
| 1 | 1000 | 847.898 | 25.181 | 27.989 | 36.52 |
| 12 | 1 | 3,220.943 | 335.153 | 385.963 | 263.38 |
| 12 | 10 | 3,555.212 | 193.549 | 222.891 | 58.81 |
| 12 | 50 | 3,542.770 | 206.239 | 237.504 | 44.34 |
| 12 | 100 | 3,607.383 | 202.344 | 233.020 | 44.64 |
| 12 | 1000 | 3,654.890 | 204.607 | 235.626 | 50.53 |

对于一个 consumer，BatchSize 从 `10` 到 `100` 时，数组聚合的 Channel allocation 已经接近现有 BufferQueue
`Auto Commit` allocation；原始 Channel 单条读取 baseline 不是这个比较对象。每次 operation 消费 8,192 条数据：

| BatchSize | Channel 数组聚合 (KiB) | BufferQueue Auto Commit (KiB) |
|---:|---:|---:|
| 1 | 257.15 | 193.34 |
| 10 | 52.40 | 52.77 |
| 50 | 37.02 | 37.30 |
| 100 | 35.10 | 35.34 |
| 1000 | 36.52 | 33.65 |

因此，中间 batch size 的表面 allocation 差距大部分消失。但两种 workload 仍不相同：Channel helper 为每个非空 batch
创建新的 `int[BatchSize]`，而 BufferQueue 的快照表示和尾 batch 行为不同。`BatchSize=1` 时，数组对照需要为每条数据
分配一个数组；`BatchSize=1000` 时，最后的部分数组和 reader 拓扑细节会影响结果。十二个 consumer 时，共享 Channel 的调度
和不同的部分 batch 分布会影响耗时与分配。表中的 Error 和 StdDev 仅描述耗时，不是内存分配的误差范围。

README 消费 allocation 图对比本对照与 BufferQueue 返回批次的成本；[Channel 逐条读取图](memory-queues.zh-CN.md#channel-逐条读取参考)保留在详情中。
耗时主图继续使用现有的原始 Channel baseline。这里记录数组
对照的耗时只是为了完整性，不替换主要的异步耗时对比，也不是普遍吞吐结论。
图例使用 `逐条读取` 表示原始 Channel baseline，使用 `数组聚合` 表示本对照。

新增方法是
[`Channel_ReadAllAsync_Batched`](../../tests/BufferQueue.Benchmarks/UnboundedChannelVsMemoryBufferQueueConsumeBenchmark.cs)，
以及 bounded 类中的对应方法
[`BoundedChannelVsMemoryBufferQueueConsumeBenchmark`](../../tests/BufferQueue.Benchmarks/BoundedChannelVsMemoryBufferQueueConsumeBenchmark.cs)。
共享的聚合 helper 是
[`ChannelBatchConsumption`](../../tests/BufferQueue.Benchmarks/ChannelBatchConsumption.cs)。

## 批次内存分配图

![Unbounded, 1 个 consumer](memory-unbounded-1-consumer-allocations.zh-CN.svg)

![Unbounded, 12 个 consumer](memory-unbounded-12-consumers-allocations.zh-CN.svg)

![Bounded, 1 个 consumer](memory-bounded-1-consumer-allocations.zh-CN.svg)

![Bounded, 12 个 consumer](memory-bounded-12-consumers-allocations.zh-CN.svg)

## 复现

```sh
dotnet run -c Release --project tests/BufferQueue.Benchmarks -- --filter '*ChannelVsMemoryBufferQueueConsumeBenchmark.Channel_ReadAllAsync_Batched' --warmupCount 6 --iterationCount 20 --launchCount 1 --exporters json
```

代码使用 `Environment.ProcessorCount` 设置多 consumer 数量和 BufferQueue partition 数量。记录环境有 12 个逻辑处理器，
换机器重跑可能改变拓扑。README 原有系列保留原记录值；本次单独测量 20 组之前，已成功重跑原有全部 113 组。
