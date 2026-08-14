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

顺序只在单个 partition 内得到保证，不保证跨 partition 的全局顺序。同一 group 中不同 consumer 获得
不同 partition 分配，因此不会有两个组成员竞争消费同一个 partition。

## PartitionKey 路由

Round-robin 路由将追加操作分散到不同 partition。启用 PartitionKey 路由后：

- 数值 selector 的结果必须是有限整数，并使用 `(key - 1)` 对
  `PartitionNumber` 的归一化数学取模映射；零和负数也可以作为 key。
- 字符串 selector 只使用前四个 UTF-16 字符计算 partition index，不使用
  `string.GetHashCode()`。

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

## 单进程并发

Queue 的设计目标是在一个进程内并发生产和消费：

- Producer 默认通过 round-robin counter 选择 partition，或通过配置的 PartitionKey selector 选择。
  Selector 实现必须能安全地被并发调用。
- Memory 模式中，默认 round-robin 路由使用一个 append lock 串行执行 partition 选择和 append。单条数据在
  容量可用时，也会在这把锁内完成容量接纳；批量预留以及从 `Wait` 恢复的写入，则会先一次性取得所需容量，
  再获取 append lock。PartitionKey 路由先选择 partition，再获取该 partition 的 append lock，因此不同
  partition 的 append 可以并行进行；同一 partition 的 append 仍然串行。
- Memory partition 只会在写入 item 后发布可读 segment cursor，因此 consumer 不会读到未写入 slot，
  读取时也不需要获取 append lock。
- Consumer group 的创建受 queue-level lock 保护。
- Consumer 等待和唤醒状态受 `ReaderWriterLockSlim` 保护。
- MemoryMappedFile 的 producer 和 consumer checkpoint 使用 replace-or-move 语义，读取方不会看到
  部分写入的 offset 文件。

MemoryMappedFile 不提供针对同一 topic directory 的跨进程写入协调。除非加入外部协调，否则应把它视为
一个 active queue 实例使用的本地持久化机制。

## 相关文档

[Consumer 模型与投递](consumer-model.zh-CN.md)介绍 batch pull、等待、提交和投递。
[Memory 存储](memory.zh-CN.md)和 [MemoryMappedFile 存储](memory-mapped-file.zh-CN.md)说明
选定 partition 后的存储行为。
