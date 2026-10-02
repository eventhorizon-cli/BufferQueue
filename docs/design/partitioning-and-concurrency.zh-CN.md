# Partition 与并发

[English](partitioning-and-concurrency.md) | [简体中文](partitioning-and-concurrency.zh-CN.md)

[设计文档首页](../README.zh-CN.md)

## Partition 与 Consumer Group

每个 topic 可以包含一个或多个 partition。Producer 默认使用 round-robin 分发。Memory 和
MemoryMappedFile topic 都可以通过带 key-selector delegate 的 `UsePartitionKey`，
把相同 key 路由到同一个 partition。Selector 必须保持确定性，并能安全地被并发调用。

Consumer 按 consumer group 创建。一个 group 可以包含多个 consumer，partition 会在组内均分：

- Consumer 数量必须大于零。
- Consumer 数量不能超过 partition 数量。
- 每个 group 有独立的读取进度。
- 每个 group 都会消费 topic 的全部消息，但组内通过 partition 分配实现负载均衡。

例如，五个 partition 和两个 consumer：

~~~text
consumer-0: partition-0, partition-1, partition-2
consumer-1: partition-3, partition-4
~~~

不同 group 相互独立。两个 group 消费同一个 topic 时各自维护进度。Group 创建后 consumer 数量固定；
同一个 queue 实例中重复创建相同 group name 会被拒绝。

Memory consumer group 可以与生产并发创建，已有 group 的分配保持不变。生产继续进行时创建的 group 从各
partition 当前仍可读的最早位置开始；不存在 topic 全局原子的注册时刻，也不保证该 group 能读取全部历史记录。

顺序只在单个 partition 内得到保证，不保证跨 partition 的全局顺序。同一 group 中不同 consumer 获得
不同 partition 分配，因此不会有两个组成员竞争消费同一个 partition。

## PartitionKey 路由

Round-robin 路由将追加操作分散到不同 partition。启用 PartitionKey 路由后：

- 数值 selector 的结果必须是有限整数，并使用 `(key - 1)` 对
  `PartitionNumber` 的归一化数学取模映射；零和负数也可以作为 key。
- 字符串 selector 对完整 key 使用种子为零的 XXH3-64，不使用 `string.GetHashCode()`。
  规范输入是 UTF-16 码元序列按小端序列化的字节，包括代理码元及内嵌空字符；不执行 Unicode
  规范化或大小写转换，也不添加 BOM、终止符，且不会替换未配对代理码元。无符号 64 位哈希对
  `PartitionNumber` 取模得到 partition。空字符串合法，并使用非零的 XXH3 空输入哈希；null 被拒绝。
  单 partition 在参数校验后始终选择零。

规范的 UTF-16LE 输入和固定种子使映射在受支持的小端 .NET 环境重启后保持稳定。实现通过
`MemoryMarshal` span 将字符串数据直接交给 `System.IO.Hashing`，不分配编码缓冲区。MMF topic
数据不计划在字节序不同的平台之间迁移。输入超过 240 字节（超过 120 个 UTF-16 码元）且硬件
支持时，XXH3 使用 SIMD；更短输入使用专用标量路径，标量 fallback 产生相同哈希。直接依赖为
`System.IO.Hashing` 10.0.9，提供 net8.0 和 net10.0 资产，且没有额外的生产传递依赖。实现采用
成熟的 `System.IO.Hashing` XXH3，避免在本仓库复制大型哈希实现。参见 [runtime 源码](https://github.com/dotnet/runtime/tree/v10.0.9/src/libraries/System.IO.Hashing)
和 [xxHash 参考实现](https://github.com/Cyan4973/xxHash)。

这将直接替换原来的四码元映射，不提供旧模式。升级会改变字符串 partition 分配；已有
MMF 日志仍可读取，但同 key 的新消息可能进入其他 partition，因此不保证跨升级的按 key 顺序。

该映射与 Kafka 不兼容。Kafka 的 Java key 分区器对序列化后的 key 字节使用 Murmur2，先与 `0x7fffffff`
按位与，再对 `partitionCount` 取模；这是独立的映射规则，并非本 UTF-16LE XXH3-64 规则的 SIMD 替代方案。

相同 key 因此能保持 partition 内顺序，不同 key 仍可能映射到同一个 partition。

对于持久化的 MemoryMappedFile topic，重启前后不能改变 selector、PartitionKey 路由行为或
`PartitionNumber`。调小已有 MMF topic 的 partition count 时，如果已有 partition directory
超过当前配置，启动也会失败。恢复规则参见
[MemoryMappedFile 存储](memory-mapped-file.zh-CN.md)。

## 批量路由

`ProduceAsync` 和 `TryProduceAsync` 的批量重载仍按单条数据进行路由。一个批次不会因为整体提交，
就被固定分配到某一个 partition：

- Round-robin 路由在处理批次中的每条数据时都会轮转一次。
- PartitionKey 路由会为每条数据调用 selector；相同 key 的数据会在选定 partition 内保持输入顺序。

同一批次中被路由到不同 partition 的数据不具备全局顺序保证。Queue 仍然只保证每个 partition 内的顺序。

有界 Memory topic 处于 `Wait` 模式时，超过配置容量的输入批次会被切成连续的容量大小切片。每个切片仍按
单条数据路由，因此 round-robin 会跨切片继续轮转；相同 PartitionKey 在其选定 partition 内仍保持输入顺序。

## 单进程并发

Queue 的设计目标是在一个进程内并发生产和消费：

- Producer 默认通过 round-robin counter 选择 partition，或通过配置的 PartitionKey selector 选择。
  Selector 实现必须能安全地被并发调用。
- Memory 模式中，默认 round-robin 的 partition 选择和批量范围预留使用短时的 topic 级协调锁。每个已接纳
  的批量或批量切片都会先一次性取得完整容量，再保留选择范围，并在每个 partition 的 append lock 内追加
  对应切片。存在活跃批量 append 时，单条 round-robin 写入也会取得目标 partition 的锁；Consumer 的状态
  变更使用同一个协调锁。PartitionKey 路由先选择 partition，再获取该 partition 的 append lock。同一
  partition 的 append 仍然串行。
- Memory partition 只会在写入 item 后发布可读 segment cursor，因此 consumer 不会读到未写入 slot，
  读取时也不需要获取 append lock。
- Consumer group 的创建受 queue-level lock 保护。
- Memory consumer 的注册和注销由现有的 partition append lock 串行化。注册时先从当前 `HashSet` 构建通知集合，
  初始化 reader 后，再通过 volatile 写入发布集合。已发布的集合不会再修改。Producer 通过 volatile 读取集合并
  在锁外遍历，因此生产路径不会复制集合，也不会为每条数据新增通知锁。在途通知可能仍使用旧集合，并通知刚刚
  注销的 consumer；注销不会等待这类通知完成。新 consumer 会先读取已有数据再进入等待，因此没有出现在旧通知
  集合中本身不会导致丢失唤醒。
- Consumer 等待和唤醒状态受 `ReaderWriterLockSlim` 保护。
- MemoryMappedFile 的 producer 和 consumer checkpoint 使用 replace-or-move 语义，读取方不会看到
  部分写入的 offset 文件。

MemoryMappedFile 不提供针对同一 topic directory 的跨进程写入协调。除非加入外部协调，否则应把它视为
一个 active queue 实例使用的本地持久化机制。

## 相关文档

[Consumer 模型与投递](consumer-model.zh-CN.md)介绍 batch pull、等待、提交和投递。
[Memory 存储](memory.zh-CN.md)和 [MemoryMappedFile 存储](memory-mapped-file.zh-CN.md)说明
选定 partition 后的存储行为。
