# MemoryMappedFile 存储

[English](memory-mapped-file.md) | [简体中文](memory-mapped-file.zh-CN.md)

[设计文档首页](../README.zh-CN.md)

## 范围与配置

MemoryMappedFile 模式把生产的数据持久化到内存映射分段文件，同时持久化 producer offset 和已提交的
consumer offset。它是面向一个 active queue 实例的本地持久化缓冲机制，提供简单恢复；新记录何时到达
显式持久化边界由配置的 flush 策略决定。

`MemoryMappedFileBufferQueueOptions<T>.SegmentSizeInBytes` 配置 segment size，默认值为
`256L * 1024 * 1024`（256 MiB）。

`MaxRetainedConsumedSegments` 控制每个 partition 删除已完整消费 segment 的策略：

- 默认值 `null` 表示不删除。
- `0` 表示不保留任何可回收的已消费 segment。
- 正数表示保留最新的对应数量的可回收 segment。

MMF 不支持有界容量，也不协调多个进程的 writer。它保证单个 partition 内顺序，而不保证跨 partition
全局顺序。持久化 topic 在重启前后必须保持兼容的 PartitionKey 路由行为和
`PartitionNumber`；参见 [Partition 与并发](partitioning-and-concurrency.zh-CN.md)。

## 目录结构

每个 topic partition 存储在：

~~~text
{DataDirectory}/{TopicName}/partition-{PartitionId:D5}/
~~~

数据 segment 按 segment index 命名：

~~~text
00000000000000000000.log
00000000000000000001.log
...
~~~

Consumer offset 位于：

~~~text
{DataDirectory}/{TopicName}/partition-{PartitionId:D5}/offsets/
~~~

每个 consumer group 在 `offsets` 下有一个可读目录。目录名为
`{escaped-group-name}`。如果 group name 可以作为一个合法的文件夹名，就直接使用；
只有不能作为单个路径组件的字符，例如 `/`，才会被百分号编码。百分号本身也会被编码，
避免与已转义名称冲突。

~~~text
{DataDirectory}/{TopicName}/partition-{PartitionId:D5}/offsets/{escaped-group-name}/consumer.offset
~~~

例如，topic 为 `orders`、partition 为 `0`、group 为
`billing-worker-1` 时：

~~~text
bufferqueue/orders/partition-00000/offsets/billing-worker-1/consumer.offset
~~~

如果 group 为 `orders/worker 1`，斜杠会被编码，空格保持可见：

~~~text
bufferqueue/orders/partition-00000/offsets/orders%2Fworker 1/consumer.offset
~~~

Partition producer offset 和最早保留的 segment boundary 位于：

~~~text
{DataDirectory}/{TopicName}/partition-{PartitionId:D5}/producer.offset
{DataDirectory}/{TopicName}/partition-{PartitionId:D5}/earliest.offset
~~~

例如：

~~~text
bufferqueue/orders/partition-00000/producer.offset
~~~

`earliest.offset`、`producer.offset` 和 consumer offset 文件都包含一个
8 字节 little-endian integer。

## 记录格式

每条记录格式为：

~~~text
4 bytes  payload length, little-endian int32
N bytes  payload
1 byte   record end marker
~~~

Record end marker 用于在读取和恢复时检测未完整写入或损坏的记录。

如果当前 segment 的剩余空间不能容纳下一条记录，且至少还剩四个字节，partition 会写入 segment-end marker，
然后从下一个 segment 继续。剩余空间少于四个字节时，未使用尾部被视为 segment padding。Segment-end marker
使用长度为 `-1` 的 `int32` 表示。

## 序列化与 Schema 兼容性

`MemoryMappedFileBufferQueueOptions<T>` 提供一个可插拔属性：

- `Serializer: IMemoryMappedFileSerializer<T>`

`IMemoryMappedFileSerializer<T>` 同时定义两个操作：
`Serialize(T)` 返回 `byte[]`，
`Deserialize(ReadOnlyMemory<byte>)` 返回 `T`。

现有实现包括：

- 默认使用的 internal `System.Text.Json` 实现。
- 基于 MessagePack for C# 的 `MessagePackMemoryMappedFileSerializer<T>`。
  无参构造函数使用 `MessagePackSerializerOptions.Standard`，另一个构造函数接收显式
  options。自定义 resolver 和 formatter 必须能安全地被并发调用。
- `UnmanagedMemoryMappedFileSerializer<T>`，要求
  `T : unmanaged`，直接复制值的 native 内存表示。反序列化要求 payload 长度与
  `Unsafe.SizeOf<T>()` 严格相等。

使用标准 MessagePack options 时，自定义类型应使用 `[MessagePackObject]` 和稳定的数值
`[Key]`。应用 project 应直接引用 MessagePack，因为 MMF package 对 MessagePack 的传递
runtime 依赖不会提供 analyzer 和 source generator。可以通过自定义 options 启用 contractless 序列化，
但它会把成员名写入持久化格式，不是推荐的 MMF schema。Resolver、key、压缩和安全配置都是持久化格式的
一部分；已删除字段的数值 key 不能复用。

Unmanaged serializer 省去格式编解码，但不是零拷贝，因为当前 serializer 契约和 partition 仍会创建
payload byte array。Native endianness、padding、field order、packing、runtime 和 process architecture
都是 wire format 的一部分。`[StructLayout]` 是可选的，但建议显式固定 sequential 或 explicit
layout 和 packing。不要持久化 pointer-sized 或 process-specific 字段。

Serializer 及其 wire schema 是 topic 持久化格式的一部分，在 queue 重启和应用升级时必须与已有记录兼容。
同一个配置 serializer 实例由 topic 的所有 partition 共享，并且可能被并发调用。实现必须线程安全，
两个操作都不能返回 `null`。

## Flush 边界与写入

`MemoryMappedFileBufferQueueOptions<T>` 提供两种 flush 策略：

- `MemoryMappedFileFlushStrategy.Immediate` 是默认策略，每条记录后都显式 flush。
- `MemoryMappedFileFlushStrategy.Batch` 在一个 partition 追加
  `FlushBatchSize` 条记录后显式 flush。`FlushBatchSize` 默认值为
  `100`。

无论使用何种策略，segment rollover 和 consumer commit 都是无条件 flush 边界。因此 Batch 模式可能在达到
`FlushBatchSize` 前就 flush。如果未满一批的尾部记录后续没有继续生产，也没有 rollover 或
consumer commit，则不保证已被显式 flush。

`MemoryMappedFileBufferProducer<T>` 使用共享 partitioner，默认选择 round-robin 路由，
也可以使用配置的 key selector。数值和字符串路由都是确定性的，字符串路由不使用
`string.GetHashCode()`。

批量写入在 MMF 中仍按记录逐条处理。批次中的每条数据都会经过正常的 partition 选择、序列化、记录写入和
flush 策略处理。一个批次既不会合并成一条 MMF 记录，也不会作为单个 partition 的分组或一次 flush 的单位；
Round-robin 会对每条数据轮转一次，基于 key 的顺序仍然只在 partition 内保证。

选定 partition 后，producer 会序列化 item、计算 record size、找到 active segment、写入 record、推进
进程内 write offset、应用配置的 flush 策略，然后通知 consumer。

每次达到 flush 边界时，partition 都会先 flush memory-mapped accessor，然后才把对应 offset 写到
`producer.offset`。Segment rollover 会在写入下一个 segment 前 flush 已完成的 segment。
Consumer commit 也会 flush 待处理日志数据并推进 `producer.offset`，再持久化其
consumer offset。

如果序列化后的 item 大于 segment size，生产会以 `InvalidOperationException` 失败。

## 恢复

Partition 启动时读取 `earliest.offset`；文件不存在时使用 `0`，然后尝试读取
`producer.offset`。Producer checkpoint 缺失时，启动从最早保留 offset 开始扫描。如果 checkpoint
有效，且指向不早于最早保留 offset 的实际 record boundary，启动从该位置向后扫描以找到最后一个有效的
write offset。

扫描遇到下列情况时停止：

- 空 length；
- 非正 length，且不是 segment-end marker；
- 跨越 segment boundary 的 record；
- 缺失的 record end marker。

这样既能让正常启动更快，也能容忍预期内的崩溃窗口：

- 数据已经 flush，但 `producer.offset` 尚未更新。
- 操作系统在显式 flush 前已持久化 pending batch 中的完整 record。
- 尾部数据只写入了一部分。

这些情况下扫描都会找到最后一个有效 record boundary。由于 `producer.offset` 只会在对应日志
成功 flush 后推进，恢复可以将其作为安全 checkpoint，并向后扫描其他完整 record。在 Batch 模式下，异常
终止后没有显式 flush 的部分尾 batch 记录可能不存在。

明显不一致的 checkpoint 状态属于损坏，会快速失败。`earliest.offset` 必须对齐 segment
boundary，且不能大于 producer offset。保留区间内的 segment file 必须连续。无效 offset、缺失的保留
segment、segment file 尺寸不正确、不在 record boundary 上的 checkpoint、已有 consumer group directory
但缺少 `consumer.offset`，以及配置的 partition count 小于已有 partition directory 数量，
都会抛出异常，而不是创建替代文件或重置进度。

## Consumer Checkpoint

MMF 按 partition 和 consumer group 持久化已提交 offset。调用 `Commit` 时，partition 会先
强制 flush 待处理日志数据并推进 `producer.offset`，再将已提交 offset 以 8 字节
little-endian integer 写到该 group 的 `consumer.offset` checkpoint。这个顺序避免持久化的
consumer checkpoint 超过已经成功 flush 的日志数据。

Checkpoint 写入先写临时文件，再 replace 或 move，因此读取方不会观察到部分写入文件。

Consumer group 被分配 partition 时，如果尚无 checkpoint，每个 partition 会在最早保留 offset 创建初始
checkpoint。因此 group 在第一次 pull 前就会参与 retention。创建 reader 时：

- 已有且有效的 checkpoint 从其存储 offset 开始读取。
- 新 checkpoint 从最早保留 offset 开始读取。
- 长度无效的 checkpoint 或负数 offset 抛出 `InvalidDataException`。
- 早于最早保留 offset、超过当前 write offset 或不在 record boundary 上的 offset 会抛出异常，而不是
  静默重置进度。

初始 offset 和后续已提交 offset 都会持久化。如果 batch 已读取但未提交，checkpoint 不会推进，下一个
queue 实例会再次读取该 batch。

## Segment 保留策略

只有当所有已知 consumer group 都已经提交越过某个 segment 的末尾后，该 segment 才可回收。Partition 会
计算所有已持久化 group checkpoint 的最小 committed offset，包括当前进程内未激活的 group。它会在不跳过
record 的前提下跨 segment-end marker 和 padding 规范化 offset。没有已知 group 时不会删除任何 segment。

Retention 值为 `N` 时，partition 保留最新的 `N` 个可回收 segment，以及第一个
尚未被所有 group 完全消费的 segment 及其之后的所有 segment。慢速、未提交、离线和废弃 group checkpoint
都会阻止回收。

移除废弃 group 是显式管理操作：停止 queue，并在每个 partition 中删除完整的
`offsets/{escaped-group-name}/` 目录，而不只是删除其中的
`consumer.offset` 文件。

删除在 consumer 成功提交后执行，也会在启动时重试未完成清理。持久化顺序为：

1. Flush 日志数据并推进 `producer.offset`。
2. 持久化 consumer offset。
3. 原子推进 `earliest.offset` 到新的 segment boundary。
4. Dispose 旧 segment 的 mapped view 并删除其文件。

如果进程在第三步后停止，旧文件可能仍然在逻辑保留区间之外，并会在后续启动或提交时删除。如果某个 segment
在 `earliest.offset` 已推进后仍无法删除，consumer commit 仍然已经持久化，并会抛出说明可以
重试清理的 `IOException`。

## Producer Checkpoint

到达 flush 边界时，partition flush memory-mapped accessor，并把最新成功 flush 的 producer offset 以
8 字节 little-endian integer 写入 `producer.offset`。Immediate 为每条 record 建立该边界。
Batch 在累计 `FlushBatchSize` 条 record 后建立该边界；segment rollover 和 consumer commit
无论待处理数量都会建立该边界。

Producer-offset 写入使用与 consumer checkpoint 相同的临时文件加 replace-or-move 模式。Producer checkpoint
是启动优化和恢复提示，不是唯一事实来源。Batch 模式仍有部分 batch 等待时，它可能落后于进程内 write offset。

启动时，partition 从持久化 producer offset 向后校验 record。如果 checkpoint 落后于数据文件中的完整记录，
扫描会追上。如果 checkpoint 文件不存在，从最早保留 offset 开始扫描。如果 checkpoint 存在但与日志不一致，
恢复会快速失败。
