# 通用路径优化测量

[English](common-path-optimizations.md)

完整的规范化数据在 [common-path-optimizations.csv](common-path-optimizations.csv) 中。每一行都保留场景、before/candidate 或
before/after 版本、run 标签、BenchmarkDotNet 配置、测试参数、mean、error、standard deviation 和 allocation。

## 环境和方法

- Apple M2 Max，12 核（通过 `sysctl` 验证），macOS 27.0.1，Arm64。
- .NET 10.0.0 和 BenchmarkDotNet 0.15.8。A、B 两节的 baseline 为 `e615da8`，与 `091ac24` 使用相同的 core；
  C 节使用包含 notification 优化的 `791c399`。
- 每次运行一次 launch。auto commit 初始对比使用 4 次 warmup 和 10 次测量迭代；confirmation 使用 8 次 warmup 和
  30 次测量迭代。notification、sparse 和 minimum-progress 对比使用 6 次 warmup 和 20 次测量迭代。
- 所有测量在同一台机器上串行执行。这些较短的 drain/path benchmark 适合方向性比较，不代表完整应用吞吐量。
  CSV 保留了报告的 `Error` 和 `StdDev`。

## A：auto commit reader 复用

`MemoryAutoCommitOptimizationBenchmark` 预填充 8,192 个 `int`，然后从一个 partition、一个已注册 group 中消费。
覆盖 `BatchSize` 1、10、100，`bounded` 和 `unbounded`，以及 `AutoCommit` 和 `ManualCommit`。Manual 行是 control path；
两者共享 helper，因此它不是完全未修改的 control。

候选方案在 pull path 中复用 reader lookup。初始运行结果混合，因此增加 warmup 和测量次数进行了 confirmation。confirmation
仍然混合：部分 unbounded 和较大 batch 的 auto 行有所改善，但 bounded、`BatchSize = 1` 从 1,346.09 us 退化到
1,404.17 us（+4.31%）。allocation 没有变化。因此 reader 复用候选被拒绝，这次实验没有保留生产性能改动。

四组 12 行结果（初始 before/candidate 和 confirmation before/candidate）全部在 CSV 中。

## B：notification handshake

`MemoryNotificationOptimizationBenchmark` 在一个已注册的 fake partition 上调用 `NotifyNewDataAvailable`。每个 producer 发送 8,192 次通知，计时包含每个 producer 的一次 `Task.Run`。
“no waiters” 表示 consumer 已注册且处于 active 状态，但当前没有 pending wait；不是指 consumer 不存在。候选方案使用 version-and-flag
handshake，使 consumer 切换到 waiting 状态时不会丢失 producer notification。并发完成和 data-race 不变量由 focused concurrency
tests 覆盖。

单独测量 notification path 时，1 个 producer 从 148.70 us 降至 89.31 us（-39.94%），4 个 producer 从 1,594.30 us 降至
565.59 us（-64.52%）。allocation 分别保持 224 B 和 704 B 不变。

已有的 `MemorySparseBatchBenchmark` 测量实际的顺序路径：生产一条、消费一条，重复 8,192 次。它覆盖 Auto/Manual Commit、
`bounded`/`unbounded` 以及 `BatchSize` 1、10、100、1000，虽然每次 pull 只有一条可用数据。handshake 改动后，Auto Commit 在这些
配置中改善 4.27% 到 10.08%，Manual Commit 改善 5.21% 到 10.60%。每一行的 allocation 都保持 196.6 KB 不变。
每个版本的 2 行 notification 和 16 行 sparse 结果都在 CSV 中。

## C：minimum-progress cache 候选（均未保留）

group 场景使用相同的 6 次 warmup/20 次测量迭代、8,192 条预填充数据、一个 partition 和 `bounded` capacity。
每次读取一条后显式提交（Manual Commit）。`FastReaderWithIdleGroups` 只消费一个 group，其他 group 保持空闲；
`LockstepReaders` 每轮依次为每个 group 读取并提交一条。single-group 对照覆盖 Auto Commit 和 Manual Commit、
两种 capacity 模式及 BatchSize 1/10/100。baseline
是 `791c399` 这条代码线。三个候选分别是 record-key cache、raw index equality cache 和 tied-count cache。CSV 共保留
52 行：baseline 16 行、record-key candidate 16 行、index candidate 4 行、tied-count candidate 16 行。各适用 run 的 12 行
single-group 结果只作为 raw control 保留；该路径的实现几乎没有变化，因此不把跨 run 波动描述成稳定收益。

| Case | Baseline `791c399` | Record-key candidate | Index candidate | Tied-count candidate |
|---|---:|---:|---:|---:|
| FastReader，groups=2 | 359.2 us / 6.07 / 192 KB | 608.5 us / 19.08 / 192 KB | 387.8 us / 12.12 / 192 KB | 396.7 us / 12.26 / 192 KB |
| Lockstep，groups=2 | 697.7 us / 22.30 / 384 KB | 1,446.2 us / 30.17 / 384 KB | 1,020.9 us / 17.46 / 384 KB | 854.4 us / 35.37 / 384 KB |
| FastReader，groups=16 | 569.1 us / 14.28 / 192 KB | 682.2 us / 14.28 / 192 KB | 478.9 us / 7.52 / 192 KB | 397.6 us / 15.51 / 192 KB |
| Lockstep，groups=16 | 6,844.5 us / 1,555.66 / 3072 KB | 10,250.4 us / 4,393.83 / 3072 KB | 8,058.1 us / 3,473.54 / 3072 KB | 7,893.4 us / 282.41 / 3072 KB |

每个单元格依次为 `Mean / Error / Allocated`；完整的 `StdDev` 和所有 raw 行都在 CSV 中。record-key 在四个 group case
中全部退化。index 和 tied-count 只改善了 16 个 group 的 idle-reader case；两个 group 的两个 case 以及 16 个 group
的 lockstep case 仍然退化。16-group lockstep 的分布是双峰且误差较大，但两个 group 的退化已经很明确。三个
minimum-progress cache 候选全部拒绝，不合入代码。

## 复现

运行 notification 对比：

```bash
dotnet run -c Release --project tests/BufferQueue.Benchmarks/BufferQueue.Benchmarks.csproj -- \
  --filter '*MemoryNotificationOptimizationBenchmark*' --warmupCount 6 --iterationCount 20
```

运行顺序 sparse-batch path：

```bash
dotnet run -c Release --project tests/BufferQueue.Benchmarks/BufferQueue.Benchmarks.csproj -- \
  --filter '*MemorySparseBatchBenchmark*' --warmupCount 6 --iterationCount 20
```

reader 复用的候选实现未保留，CSV 保存其已测结果；`MemoryAutoCommitOptimizationBenchmark` 仍可测量当前的
Auto/Manual Commit 路径。复现已保留的通知优化时，应使用相同 benchmark 源码与参数，对比该提交及其父提交。

运行当前 group-progress 工作负载：

```bash
dotnet run -c Release --project tests/BufferQueue.Benchmarks/BufferQueue.Benchmarks.csproj -- \
  --filter '*MemoryGroupProgressOptimizationBenchmark*' --warmupCount 6 --iterationCount 20
```

被拒绝的 cache 实现未包含在仓库中。
