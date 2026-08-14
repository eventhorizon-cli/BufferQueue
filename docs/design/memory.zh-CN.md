# Memory 存储

[English](memory.md) | [简体中文](memory.zh-CN.md)

[设计文档首页](../README.zh-CN.md)

## 存储结构

Memory 模式用于进程内缓冲，并针对批量消费做了优化。
`MemoryBufferPartition<T>` 使用
`MemoryBufferSegment<T>` 链表保存数据。每个 segment 持有一个固定长度的 item array。

~~~text
head segment -> segment -> ... -> tail segment
~~~

每条记录的 offset 由 `MemoryBufferPartitionOffset` 表示。这里的 offset 是逻辑 item
位置，不是字节位置。

Memory 数据和 consumer offset 只在当前进程生命周期内存在。进程重启后不会恢复它们。

## 写入路径

`MemoryBufferProducer<T>` 默认按 round-robin 选择 partition，启用 PartitionKey 路由后
则使用 key selector。数值和字符串的精确路由规则参见
[Partition 与并发](partitioning-and-concurrency.zh-CN.md)。

批量写入沿用单条数据的路由规则。Round-robin 会在写入批次中的每条数据时轮转一次，而不会将整个批次
固定写入某一个 partition。按 key 路由时，会为每条数据调用 selector；相同 key 的数据在其所在 partition 内
保持输入顺序。

Partition 会向 tail segment 追加数据。Tail 已满时，它会创建新 segment，或者复用一个已被所有 consumer
group 完全消费的旧 segment。

默认 round-robin 路由时，Memory producer 和 partition 共享一个 append lock，用于串行执行 partition
选择、有界容量计数和 append。PartitionKey 路由会在获取该 partition 的 append lock 前先选择 partition，
因此并发 producer 可以并行向不同 partition 写入；同一个 partition 内的 append 仍然串行。

选中的 partition 写入 item 后，使用 release write 发布新的可读 cursor。Consumer 读取已发布区间时不获取
append lock，因此不会读到尚未写入的 slot。Enqueue 成功后，partition 会通知所有已注册 consumer。

## 读取与提交

每个 consumer group 在每个 partition 上都有一个 reader。Reader 保存：

- 当前 segment；
- 当前 read position；
- 上次读取数量。

`TryPull` 最多返回 `BatchSize` 条数据。只有调用 `Commit` 后，reader 的
已提交位置才会推进。因此在 queue 实例仍存活期间，未提交 batch 可能再次交付。

## Segment 复用

Memory 模式可以复用旧 segment。只有当所有 consumer group 都已经消费过某个 segment 的末尾后，该 segment
才可以复用。这可以避免慢速 group 尚未读取的数据被覆盖。

## 有界容量

Memory 模式支持通过 `MemoryBufferQueueOptions.BoundedCapacity` 配置可选的有界容量。

配置容量且 queue 已满时：

- `ProduceAsync` 抛出 `BufferQueueFullException`。
- `TryProduceAsync` 返回 `false`。

对于批量写入，容量检查会在写入前一次性预留整批数据所需的容量。剩余容量不足时，`ProduceAsync` 抛出异常，
`TryProduceAsync` 返回 `false`；两者都不会写入部分数据。两种批量输入形式均遵循这一规则，其中非数组的
`IEnumerable<T>` 会先物化为数组。

MemoryMappedFile 当前不支持有界容量。持久化存储行为参见
[MemoryMappedFile 存储](memory-mapped-file.zh-CN.md)。
