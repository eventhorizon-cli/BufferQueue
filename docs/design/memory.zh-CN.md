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

默认 round-robin 路由时，Memory producer 和 partition 共享一个 append lock，用于串行执行 partition 选择和
append；单条数据能够立即写入时，也会在这把锁内预留容量。批量写入以及从 `Wait` 恢复的写入会先预留容量，
再获取 append lock。PartitionKey 路由会先选择 partition，再获取对应的 append lock，因此并发 producer
可以同时写入不同 partition；同一 partition 内仍然串行 append。

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

Memory 模式可以通过 `MemoryBufferQueueOptions.BoundedCapacity` 限制整个 topic 的容量。该上限由 topic 下的
所有 partition 共享。

容量不可用时，`MemoryBufferQueueOptions.FullMode` 控制 `ProduceAsync` 的行为，默认值为
`BufferQueueFullMode.Wait`：

- `Wait` 异步等待容量，并可通过方法的 `CancellationToken` 取消等待。
- `Fail` 立即抛出 `BufferQueueFullException`。

`TryProduceAsync` 永远不会等待有界容量。当单条数据或整个批次无法接纳时，它会立即返回
`false`。

批量写入会在 append 前一次性预留整批数据所需的容量。剩余容量不足时，`TryProduceAsync` 返回 `false`，
且不会写入部分数据；`ProduceAsync` 在 `Wait` 模式下等待整批所需的容量，调用方应传入取消令牌，以便
终止背压等待。在 `Fail` 模式下，`ProduceAsync` 会抛出异常。如果批次大小超过配置的总容量，`Wait` 模式
会抛出 `ArgumentOutOfRangeException`，避免请求永远无法满足。两种批量输入形式都遵循这一规则；非数组的
`IEnumerable<T>` 会先物化为数组。

每个 partition 只有在所有已知 consumer group 的最小 committed position 向前推进后，才会把对应容量归还给
topic 共享的容量门。释放范围可以只覆盖 segment 的一部分；尚未提交到同一位置的慢速 group 会继续占用这部分
容量。Topic 尚未创建 consumer group 时，不会释放容量。

如果新的 consumer group 在容量已经释放后才注册，它会从当前逻辑上的最早位置开始消费。即使旧数据所在的物理
segment 还没有被复用，新 group 也无法再读取该位置之前的数据。

MemoryMappedFile 当前不支持有界容量。持久化存储行为参见
[MemoryMappedFile 存储](memory-mapped-file.zh-CN.md)。
