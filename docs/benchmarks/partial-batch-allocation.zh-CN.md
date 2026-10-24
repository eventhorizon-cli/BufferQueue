# 小批次数组分配

[English](partial-batch-allocation.md) | [简体中文](partial-batch-allocation.zh-CN.md)

[测量数据](partial-batch-allocation.csv) 包含 88 行，保留完整配置、源码哈希、均值、99.9% 置信区间半宽、标准差和分配量。
本实验单独测量数组大小调整，与 [#59](https://github.com/eventhorizon-cli/BufferQueue/issues/59) 的枚举器优化分开。

## 改动范围

Baseline 为 main `50f867f44a92b70ea2ff59fade12e87f9494b4bd`，两个 checkout 使用相同 benchmark 源码。
Candidate 只改变 Memory reader 的数组分配边界：首次分配数组时，如果已到达发布范围末尾且没有可见后继 segment，
就按已复制前缀的长度分配并返回。观察之后新发布的数据留给下一次 pull。完整的单 segment 批次不增加后继读取，
现有单条数据的表示方式不变。

不预先遍历 segment 链，也不增加 producer 计数器。首次分配时已有可见后继的情况仍按请求大小分配，因此并非所有
跨 segment 的部分批次都会获得精确大小的数组。返回的数据仍是独立副本，不使用池化，也不暴露可回收的 slot。

## 测量方式

环境：Apple M2 Max（12 核），macOS 27.0.1 / Arm64，.NET 10.0.0，BenchmarkDotNet 0.15.8，Release。
完整矩阵使用一次 launch、六次 warmup、二十次测量，invocation count 和 unroll factor 均为一。
四个控制配置反向复测：先 candidate 后 baseline，使用十次 warmup、四十次测量。各轮顺序执行，没有并行构建或 benchmark。
这些短操作存在 minimum-iteration-time 警告，解释耗时必须保留不确定性；CSV 记录 BenchmarkDotNet 排除离群值后保留的测量数。

`MemoryPartialBatchAllocationBenchmark` 实际枚举每条消息并验证条数与 checksum。每次 operation 处理 8,192 条 `int`，
segment 大小为 1,024：

- 稀疏场景使用一个 partition、一个 consumer。每次写入 1、2 或 10 条后立即读取，计时包括写入和消费，
  不代表并发 producer 吞吐。每次写入 10 条时最后一批只有 2 条。bounded 容量为 64，不触发满载等待。
- 预填充控制组不计 setup 和写入，使用一个 partition/一个 consumer，或十二个 partition/十二个 consumer，
  包含 consumer 任务调度。bounded 容量为 8,192。
- Auto Commit 在 pull 后推进；Manual Commit 在每批枚举完成后调用 `CommitAsync`。

这些是独立前后对照，不替换 README 中与 Channel 的比较。

## 最终结果

配置 BatchSize=1000、实际可用 2 条时，每个 operation 的分配从 **16,781,928 降到 266,856 B（减少 98.41%）**。
实际可用 10 条时，从 **3,363,432 降到 107,400 B（减少 96.81%）**。这两组分配结果在 Auto/Manual Commit、bounded/unbounded
四种组合下完全相同。

| 可用条数 | 模式 | 提交 | Baseline µs | Candidate µs | 变化 |
|---:|---|---|---:|---:|---:|
| 2 | unbounded | Auto | 1,528.98 | 844.11 | -44.79% |
| 2 | unbounded | Manual | 1,436.04 | 841.87 | -41.38% |
| 2 | bounded | Auto | 2,172.42 | 1,460.26 | -32.78% |
| 2 | bounded | Manual | 2,071.38 | 1,450.59 | -29.97% |
| 10 | unbounded | Auto | 458.32 | 369.61 | -19.35% |
| 10 | unbounded | Manual | 445.71 | 368.66 | -17.29% |
| 10 | bounded | Auto | 834.27 | 742.52 | -11.00% |
| 10 | bounded | Manual | 823.19 | 741.45 | -9.93% |

实际可用 2 条时，unbounded Auto Commit 在 BatchSize 10/50/100 下的分配分别减少 52.50%/78.07%/86.90%，
平均耗时分别变化 -3.13%/-7.50%/-8.46%。常见的无后继稀疏尾部不再因配置上限较大而分配大数组。

预填充 BatchSize=1 控制组分配不变，初轮平均耗时变化 +1.77%，反向复测为 -0.08%。实际只有 1 条的控制组分配也不变：
unbounded Auto/Manual 初轮耗时变化 +2.01%/+0.24%，复测为 +0.29%/+0.15%。复测误差区间重叠，这些小差异不能证明提速或
明显回退。两条数据的重点配置则重复了收益：从 1,440.33 降到 828.53 µs，减少 42.48%。

预填充单消费者 Auto Commit 的 BatchSize 10/100/1000 初轮平均耗时变化为 -8.45%/-8.80%/-6.31%，分配减少只来自最后的
未满批次。十二消费者 BatchSize=100 从 45,552 降到 44,272 B/op；初轮耗时下降 21%–30%，受任务调度影响，尚未单独复测确认。

| 反向复测配置 | Baseline 均值 ± error，µs | Candidate 均值 ± error，µs |
|---|---:|---:|
| full-batch1-unbounded-auto-1c | 1,084.35 ± 11.57 | 1,083.51 ± 15.73 |
| sparse1-batch1000-unbounded-auto-1c | 1,561.62 ± 29.73 | 1,566.19 ± 23.72 |
| sparse1-batch1000-unbounded-manual-1c | 1,563.40 ± 25.25 | 1,565.72 ± 19.64 |
| sparse2-batch1000-unbounded-auto-1c | 1,440.33 ± 34.56 | 828.53 ± 17.45 |

## 早期候选与正确性

CSV 保留 `candidate-v1` 和 `reverse-confirmation-v1` 数据。V1 在 batch reader 入口检查零 batch，unbounded Manual
实际单条场景的平均耗时初轮增加 1.89%，反向增加 1.75%，误差区间重叠。最终版本把检查移入首次分配数组的分支，
让暂存单条数据的路径不执行该检查。最终结果来自重新执行的完整 candidate 矩阵和反向配对复测，没有丢弃 V1 数据，
也没有把早期测量当成最终实现结果。

针对性测试覆盖：稀疏分配不随配置上限增加、引用类型快照在 append/replay/commit/回收后的稳定性、精确释放 bounded 容量，
以及 segment 大小 2／3 时跨 `ulong` 回绕。混合 producer、两个 group 的并发矩阵也增加 BatchSize=1000，覆盖
bounded/unbounded、round-robin/keyed 路由。另有确定性测试覆盖等待登记前释放容量的交接，以及跨 segment 部分批次的集合边界。提交与唤醒的同步机制不变。
最终格式、restore、Release 构建和完整测试在 .NET 8／10 均通过：每个框架 294 个 core、133 个 MMF，共 854 次测试。

## 复现

```sh
dotnet run -c Release --project tests/BufferQueue.Benchmarks/BufferQueue.Benchmarks.csproj -- \
  --filter '*MemoryPartialBatchAllocationBenchmark*' --exporters json
```

在 baseline 和 candidate checkout 中使用同一个 harness。JSON 的 `FullName` 保留完整配置，展示用的 `Parameters` 可能被截断。
CSV 中 production 文件和 harness 的 SHA-256 标识各轮源码。最终完整比较为 `baseline/initial` 与 `candidate/final`；
最终反向比较为两者的 `reverse-confirmation`。
