# Singleton batch 枚举分配

[English](batch-enumeration.md) | [简体中文](batch-enumeration.zh-CN.md)

本次调查对应 [#59](https://github.com/eventhorizon-cli/BufferQueue/issues/59)，与历史 Count-only 端到端结果和图表分开。
探针执行实际的 `foreach` checksum，比较原有编译器生成的 singleton iterator 与私有显式 enumerator。
不改变公开遍历 API，也不减少 singleton snapshot 本身的分配。

## 方法

Baseline：`50f867f44a92b70ea2ff59fade12e87f9494b4bd`。Candidate：相同源码加显式 singleton enumerator。
两者使用完全相同的已入库
[`BatchEnumerationProbe`](../../tests/BufferQueue.Benchmarks/BatchEnumerationProbe.cs)。
环境为 Release、SDK 10.0.100、.NET 10.0.0、macOS 27.0.1 Arm64、Apple M2 Max。
各轮顺序执行，没有并发构建或 benchmark。CPU 时间实际是 drain 的 elapsed time，不是硬件 CPU counter。

每个配置先做六次 warmup drain，再记录二十个 sample。每个 sample 累计多个独立 drain，直到测量时间至少达到
10 ms。预填充、queue/channel 创建、payload 创建、consumer 注册都不计入时间和分配。测量窗口包括 async enumerator
创建和 Dispose、pull、普通逐项 `foreach`、checksum，以及配置为 Manual Commit 时的提交。
所有操作必须同步完成，否则探针失败。分配使用 `GC.GetAllocatedBytesForCurrentThread`，时间使用 `Stopwatch`。
每个实际枚举的 drain 都校验条数和 checksum；Count-only control 只校验条数，不逐项遍历。

Memory 使用十二个 partition、8,192 条数据，主配置的 batch size 为 1、10、1,000。额外 sentinel 分别覆盖引用类型
payload、Manual Commit、bounded 容量、十二个 consumer，以及配置 batch size 1,000 但只有十二条数据的情况
（每个 partition 只有一条）。其余配置保持 unbounded、Auto Commit、一个 consumer。这是选定矩阵，不代表完整的笛卡尔积覆盖。

Channel 使用一个已预填充并完成写入的 channel，通过 `ReadAllAsync` 读取相同 payload、计算相同 checksum，
consumer loop 数量也相同。十二个 loop 从同一个 channel 顺序读取各自的 quota；Memory loop 也顺序读取各自分配的
partition。这是同步分配/drain 探针，**不是并发吞吐测试**。Channel 是补充 control；batch size 和 commit 配置不影响其行为。

首轮使用默认 runtime 设置，先 baseline 后 candidate。随后反转顺序，对两者都设置 `DOTNET_TieredCompilation=0`
做诊断性确认。禁用 tiering 是不同的 runtime policy，不能替代默认设置下的结果。默认轮次存在较大的 warmup/JIT
阶段波动，未修改的 Count-only control 耗时也发生变化。本次测量不能证明 CPU 加速。

## 默认 runtime 结果

时间为每个完整 drain 的均值 ± sample 标准差，单位 µs（20 个 sample）。每个配置按 drain 归一化的分配在所有已记录 sample 中均保持恒定。
下表都使用普通 `foreach`。

| 配置 | Baseline µs | Candidate µs | Baseline B | Candidate B |
| --- | ---: | ---: | ---: | ---: |
| int，batch 1 | 1138.75 ± 117.13 | 1128.61 ± 130.76 | 458880 | 393344 |
| int，batch 10 | 105.85 ± 3.37 | 108.74 ± 3.35 | 80096 | 80096 |
| int，batch 1000 | 40.66 ± 0.48 | 40.85 ± 1.75 | 49280 | 49280 |
| reference，batch 1 | 913.58 ± 149.05 | 880.77 ± 130.54 | 524416 | 458880 |
| int，batch 1，Manual | 479.36 ± 19.13 | 483.54 ± 12.22 | 458880 | 393344 |
| int，batch 1，bounded | 626.99 ± 32.40 | 608.70 ± 31.87 | 458880 | 393344 |
| int，batch 1，12 consumer | 437.07 ± 12.37 | 430.11 ± 6.83 | 460288 | 394752 |
| int，配置 1000，实际 12 条 | 0.738 ± 0.005 | 0.730 ± 0.003 | 800 | 704 |

8,192 个 singleton batch 在两种 payload 下都节省 65,536 B，即**每次 singleton 枚举减少 8 B**。
测得的 `int` enumerator 从 32 B 降到 24 B，reference payload 从 40 B 降到 32 B。
这些配置中的独立 singleton snapshot 仍为 24 B。完整 `int` foreach drain 的分配下降 14.3%，reference payload
下降 12.5%；枚举并非 allocation-free。稀疏 sentinel 的十二次 singleton 枚举节省 96 B。
在本次预填充 workload 中，batch 10/1,000 不返回 singleton batch，因此分配不变。

Count-only control 分配完全不变：singleton 为 196736 B，batch 10 为 53504 B，batch 1000 为 48800 B，
十二个 consumer 为 198144 B，稀疏配置为 416 B。reference、Manual 和 bounded 的 singleton control 同样保持
196736 B，没有测得 snapshot 分配回退。未修改的 Count-only 路径默认耗时变化包括 −14.3% 和 +20.6%，
因此不能把耗时变化完全归因于 iterator。Channel 分配也保持不变：一个值类型 consumer 为 128 B，
一个引用类型 consumer 为 136 B，十二个值类型 consumer loop 为 1536 B。

## 反向顺序的诊断性确认

| 配置 | Baseline µs | Candidate µs |
| --- | ---: | ---: |
| int，batch 1 | 687.82 ± 24.30 | 668.39 ± 32.75 |
| reference，batch 1 | 682.51 ± 11.12 | 687.90 ± 10.56 |
| int，batch 1，Manual | 702.38 ± 10.47 | 697.18 ± 16.44 |
| int，batch 1，bounded | 750.82 ± 14.19 | 733.29 ± 12.94 |
| int，batch 1，12 consumer | 694.47 ± 12.86 | 678.56 ± 17.83 |

分配与默认轮次完全一致。reference 耗时变化为 +0.8%，在 sample 波动范围内；未修改的 Channel control 同样有漂移。
没有证据证明引用类型发生实质性能回退。可支持的结论是分配减少，不宣称 CPU 提速。

## 复现和原始数据

```sh
dotnet build tests/BufferQueue.Benchmarks/BufferQueue.Benchmarks.csproj -c Release
dotnet tests/BufferQueue.Benchmarks/bin/Release/net10.0/BufferQueue.Benchmarks.dll --batch-enumeration-probe > enumeration.csv
# Diagnostic runtime policy, applied identically to baseline and candidate:
DOTNET_TieredCompilation=0 dotnet tests/BufferQueue.Benchmarks/bin/Release/net10.0/BufferQueue.Benchmarks.dll --batch-enumeration-probe > enumeration-diagnostic.csv
```

在 baseline 和 candidate 上分别构建相同探针，不比较不同 harness。应用 candidate 修改前已单独保留 baseline
production assembly。[原始 sample](batch-enumeration-raw.csv) 保留四轮数据，包括波动较大的默认轮次；
[汇总](batch-enumeration-summary.csv) 包含全部 Memory/Channel 和 Count-only control 的均值、sample 标准差、
最小值、最大值和分配。原始 `drains` 记录 sample 聚合次数；时间和分配均按每个完整 drain 归一化。
