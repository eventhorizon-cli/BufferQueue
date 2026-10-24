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

内部的 `MemoryBufferPartitionOffset` 只包含一个 `ulong Index`。offset 运算使用 unchecked 语义，因此从最大值继续递增
会回到零，减法也在同一个 64 位环形域内进行。位置比较使用两个环形位置的有符号差值；只要参与比较的
live window 严格小于 `2^63` 个位置，比较结果就是明确的。live window 包括仍保留的 segment 数据和所有
reader position。Memory queue 不可能保留 `2^63` 条数据，因此跨越回绕点时，正常读取、提交、未提交批次重放、
bounded 容量释放和 segment 复用都会继续工作。

这里限制的是同时保留的 item 位置跨度，不是历史累计流量或字节数。即使每条 item 只占 1 字节，保留 `2^63` 条
也需要 8 EiB（约 9.22 EB）内存，还没有计算 segment 的额外开销，因此这个范围足以覆盖实际的内存驻留窗口。

原来的 `Generation` 字段用于记录 `Index` 已经回绕了多少次。Memory 存储只需要比较同一个 partition 当前仍保留的
窗口内的位置，不会拿这些位置与已丢弃历史中的任意位置比较。慢速 group 会让未读 segment 继续驻留内存，因此它的
落后距离也包含在同一个受内存限制的窗口内。回绕比较已经能满足这个需求，即使 `Index` 发生回绕，也不需要
`Generation`。移除该字段后，每个 offset 从 16 字节缩小到 8 字节，同时省去 generation 比较和进位、借位处理。
这个决定依赖的是有效窗口的跨度约束，而不是假设 64 位计数器永远不会回绕。

这是 Memory 内部的 offset 表示，不会改变公共 API，也不会改变 MemoryMappedFile 的记录和 offset 格式。

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

默认 round-robin 路由在 bounded 和 unbounded 模式下都使用一个短时的 topic 级协调锁，串行执行 partition 选择和
批量选择范围的预留。整批容量接纳成功后，批量会保留一段连续的选择范围，按步长访问每个 partition 对应的数据切片，
并在该 partition 自己的 append lock 内追加，因此无需为每个 partition 物化一个 `List<T>`。并发的单条和批量 append
如果指向同一个 partition，仍然会串行执行。没有活跃批量时，单条写入只使用协调锁；否则，在获取和持有目标
partition lock 时也会继续持有协调锁，因此活跃批量结束后，它不会与只使用协调锁的路径竞争。由于批量 append 的完成发生在协调锁之外，
活跃批量计数的增加和减少使用原子操作。Consumer 的状态变更也使用同一个协调锁，避免与未同步的 append 重叠。
PartitionKey 路由会先选择 partition，再获取对应的 append lock。同一 partition 内的 append 仍然串行。

选中的 partition 写入 item 后，使用 release write 发布新的可读 cursor。Consumer 读取已发布区间时不获取
append lock，因此不会读到尚未写入的 slot。Enqueue 成功后，partition 会通知所有已注册 consumer。

## 读取与提交

每个 consumer group 在每个 partition 上都有一个 reader。Reader 保存：

- 当前 segment；
- 当前 read position；
- 上次读取数量。

Consumer group 的成员变化并不频繁。私有的 `ReaderRegistry` 会在 group 注册或注销时，在现有的 append 同步机制下
重新构建并原子发布不可变的 `FrozenDictionary` 查找快照。Reader 使用已发布的快照查找 group，不需要额外的查找锁。
这个冻结快照只负责冻结 group 成员关系；Reader 的 committed position 仍由现有同步机制保护。

`TryPull` 最多返回 `BatchSize` 条数据。只有调用 `Commit` 后，reader 的
已提交位置才会推进。因此在 queue 实例仍存活期间，未提交 batch 可能再次交付。

`BatchSize` 是上限。只要还有已发布的数据，pull 就会跨越已发布的 segment 继续读取。Reader 会先暂存第一条数据，
只有发现第二条可用数据后才分配 batch array。如果当前只有一条数据可用，无论配置的 `BatchSize` 是多少，
都会返回独立的 singleton snapshot。`BatchSize = 1` 在 private reader 内使用专用路径，但仍复用相同的无锁 published-slot
检查；commit 和容量释放的同步机制保持不变。

首次分配数组时，如果已到达发布范围的末尾且没有可见的下一个 segment，reader 会按已观察到的 item 数量
分配数组，并在该边界结束本次 pull。随后 producer 新发布的数据留给下一次 pull。这避免了稀疏尾部按配置
上限分配数组，也不需要额外扫描可用范围或增加 producer 计数器。如果跨 segment 批次已经有可见后继，
仍保留按请求大小分配的路径。返回的 batch 持有独立副本，在后续 append、commit 和 segment 回收后仍然有效。

每个 singleton snapshot 都为每次枚举创建独立的 enumerator。枚举可以交错、嵌套，或由不同线程独立执行；
commit 和 segment 复用后 snapshot 仍保持有效。Reset 不受支持。Dispose 只停止当前枚举，不改变 snapshot 或其他
enumerator。私有 singleton enumerator 只保存捕获的 item 和遍历状态，不持有 snapshot 对象。

## Segment 复用

Memory 模式可以复用旧 segment。只有当所有 consumer group 都已经消费过某个 segment 的末尾后，该 segment
才可以复用。这可以避免慢速 group 尚未读取的数据被覆盖。

## bounded 容量

Memory 模式可以通过 `MemoryBufferQueueOptions.BoundedCapacity` 限制整个 topic 的容量。该上限由 topic 下的
所有 partition 共享。

容量不可用时，`MemoryBufferQueueOptions.FullMode` 控制两种 Producer 方法的行为，默认值为
`BufferQueueFullMode.Wait`：

- `Wait` 下，`ProduceAsync` 和 `TryProduceAsync` 都会异步等待容量。输入完整接纳后，
  `TryProduceAsync` 返回 `true`；两种调用都可通过 `CancellationToken` 取消。
- `Fail` 下，`ProduceAsync` 立即抛出 `BufferQueueFullException`，`TryProduceAsync` 返回 `false`。

批次大小不超过配置容量时，会在 append 前一次性预留整批所需容量。`Fail` 模式下，剩余容量不足会整批
拒绝，且不会写入部分数据；`Wait` 模式下，两种方法都会等待整批容量，再作为一次接纳写入。

`Wait` 模式下，超过配置容量的批次会按输入顺序切成不超过 `BoundedCapacity` 条的连续切片。每个切片都先
等待并一次性预留完整容量，再追加，然后处理下一个切片。`ProduceAsync` 和 `TryProduceAsync` 都遵循此规则；
队列写满时，`TryProduceAsync` 不会返回 `false`。取消只能发生在两个切片之间，因此已经完成的切片会保留，
但已接纳的单个切片不会只写入一部分。`Fail` 模式下，超过容量的批次会整批拒绝。两种批量输入形式都遵循
这些规则；非数组的 `IEnumerable<T>` 会先物化为数组。

每个 partition 只有在所有已知 consumer group 的最小 committed position 向前推进后，才会把对应容量归还给
topic 共享的容量门。释放范围可以只覆盖 segment 的一部分；尚未提交到同一位置的慢速 group 会继续占用这部分
容量。Topic 尚未创建 consumer group 时，不会释放容量。

如果新的 consumer group 在容量已经释放后才注册，它会从当前逻辑上的最早位置开始消费。即使旧数据所在的物理
segment 还没有被复用，新 group 也无法再读取该位置之前的数据。

MemoryMappedFile 当前不支持 bounded 容量。持久化存储行为参见
[MemoryMappedFile 存储](memory-mapped-file.zh-CN.md)。
