# BufferQueue 设计文档

## 目标

BufferQueue 是一个面向 .NET 的、按 Topic 划分的强类型缓冲队列库。它提供统一的队列模型，并允许底层存储实现可插拔。

当前代码包含两种存储模式：

- Memory 模式：数据保存在当前进程内的分段内存结构中。
- MemoryMappedFile 模式：数据序列化后写入内存映射分段文件，并持久化 writer offset 和已提交的 consumer offset。

两种模式共享相同的 producer、pull consumer、consumer group、partition 分配、批量消费和等待唤醒语义。存储差异被隔离在内部 partition 抽象之后。

两种实现在 project 和 package 边界上相互独立：

- `BufferQueue` 包含共享队列抽象、Memory 存储和 push consumer 集成。
- `BufferQueue.MemoryMappedFile` 包含可选的 MemoryMappedFile 存储实现，并依赖 `BufferQueue`。

核心 `BufferQueue` project 不引用 `BufferQueue.MemoryMappedFile`。MMF project 通过 friend assembly 访问复用共享的 internal 队列抽象。这个拆分不会改变公共 namespace 或 `.UseMemoryMappedFile(...)` 注册调用。

## 公共模型

公共入口是 `IBufferQueue`。应用通过它按 topic name 获取 producer 或创建 consumer。

```csharp
var producer = bufferQueue.GetProducer<Foo>("topic-foo");
var consumer = bufferQueue.CreatePullConsumer<Foo>(new BufferPullConsumerOptions
{
    TopicName = "topic-foo",
    GroupName = "group-a",
    AutoCommit = false,
    BatchSize = 100
});
```

公共 API 保持较小的表面积：

- `IBufferProducer<T>`：向 topic 生产强类型数据。
- `IBufferPullConsumer<T>`：从 topic 批量消费数据。
- `IBufferConsumerCommitter`：用于手动提交消费进度。
- `BufferPullConsumerOptions`：配置 topic、group、auto commit 和 batch size。
- `BufferOptionsBuilder`：把存储实现注册到依赖注入容器中。

内部每个已注册 topic 都对应一个 `IBufferQueue<T>`。非泛型 `BufferQueue` 根据 topic name 从 DI 容器中解析对应的 typed queue。

## 总体架构

```text
Application
    |
    v
IBufferQueue
    |
    v
BufferQueue
    |
    v
按 topic name 注册的 IBufferQueue<T>
    |
    v
BufferQueue<TItem>
    |
    +-- IBufferProducer<TItem>
    +-- BufferPullConsumer<TItem>
    +-- IBufferPartition<TItem>[]
            |
            +-- MemoryBufferPartition<TItem>
            +-- MemoryMappedFileBufferPartition<TItem>
```

`BufferQueue<TItem>` 是单个 typed topic 的共享抽象队列父类，承载上层通用队列行为：

- 校验 consumer 参数；
- 阻止同一个 queue 实例中重复创建相同 consumer group；
- 创建 `BufferPullConsumer<TItem>`；
- 把 partitions 均分给同一个 group 内的多个 consumers；
- 暴露 topic producer。

具体队列实现只负责创建自己的 partition 和 producer：

- `MemoryBufferQueue<T>` 创建 `MemoryBufferPartition<T>[]` 和 `MemoryBufferProducer<T>`。
- `MemoryMappedFileBufferQueue<T>` 创建 `MemoryMappedFileBufferPartition<T>[]` 和 `MemoryMappedFileBufferProducer<T>`。

## 内部 Partition 抽象

存储实现通过 `IBufferPartition<TItem>` 接入上层逻辑。

```csharp
internal interface IBufferPartition<TItem>
{
    int PartitionId { get; }

    void RegisterConsumer(IBufferPartitionConsumer<TItem> consumer);

    void Enqueue(TItem item);

    bool TryPull(string groupName, int batchSize, out IEnumerable<TItem>? items);

    void Commit(string groupName);
}
```

队列和 consumer 逻辑只依赖这个抽象。因此，上层行为只实现一套，而 partition 可以使用完全不同的存储方式。

`IBufferPartitionConsumer<TItem>` 是 partition 用来通知 consumer 的最小接口。当有新数据写入时，partition 通过该接口唤醒等待中的 consumer。

## Partition 和 Consumer Group

每个 topic 可以包含一个或多个 partitions。Producer 使用 round-robin 方式把数据分发到 partitions。

Consumer 按 consumer group 创建。一个 group 可以包含多个 consumers。Partitions 会在同一个 group 内的 consumers 之间均分：

- consumer 数量必须大于 0；
- consumer 数量不能超过 partition 数量；
- 每个 group 有独立的读取进度；
- 每个 group 都会消费 topic 的完整数据，但同一个 group 内通过 partition 分配实现负载均衡。

例如 5 个 partitions 和 2 个 consumers：

```text
consumer-0: partition-0, partition-1, partition-2
consumer-1: partition-3, partition-4
```

不同 consumer group 之间相互独立。两个 group 消费同一个 topic 时，各自维护自己的消费进度。

## Pull Consumer 设计

`BufferPullConsumer<TItem>` 是唯一的通用 pull consumer 实现，不依赖具体存储。

它负责：

- 保存已分配的 partitions；
- 按 round-robin 选择 partition；
- 从 partition 拉取批量数据；
- 当前 partition 无数据时尝试其他已分配 partitions；
- 所有 partitions 都无数据时异步等待；
- 手动提交已消费批次；
- 在 `AutoCommit` 开启时自动提交。

消费接口返回批量数据的异步流：

```csharp
await foreach (var batch in consumer.ConsumeAsync(cancellationToken))
{
    foreach (var item in batch)
    {
        // process item
    }

    await consumer.CommitAsync();
}
```

当 `AutoCommit` 为 false 时，consumer 的读取位置不会在 `CommitAsync` 之前推进。这在当前进程内提供 at-least-once 语义。对 MemoryMappedFile 模式来说，已经到达 flush 边界的记录还能跨进程重启保留该语义；`CommitAsync` 本身会强制建立这个边界。

## Consumer 唤醒设计

Consumer 在无数据时不应该自旋。通用 consumer 使用 `PendingDataValueTaskSource<T>` 等待新数据。

流程如下：

1. Consumer 尝试从选中的 partition 拉取数据。
2. 如果没有数据，再尝试所有其他已分配 partitions。
3. 如果仍然没有数据，重置 pending-data value task source 并进入等待。
4. Producer 向某个 partition 追加数据。
5. Partition 通过 `IBufferPartitionConsumer<TItem>` 通知已注册 consumers。
6. Consumer 增加 pending-data version，并完成 pending value task。
7. Consumer 被唤醒后，从触发通知的 partition 尝试拉取数据。

pending-data version 用来避免 lost wake-up：如果数据在最后一次拉取尝试和进入等待状态之间到达，consumer 可以检测到版本变化并重新尝试消费。

## Memory 模式

Memory 模式面向进程内缓冲和批量消费优化。

### 存储结构

`MemoryBufferPartition<T>` 使用 `MemoryBufferSegment<T>` 链表保存数据。每个 segment 持有一个固定长度的对象数组。

```text
head segment -> segment -> ... -> tail segment
```

每条记录的 offset 由 `MemoryBufferPartitionOffset` 表示。这里的 offset 是逻辑 item 位置，不是字节位置。

### 写入

`MemoryBufferProducer<T>` 按 round-robin 选择 partition，并通过选中的 partition append 数据。

Partition 会尝试写入当前 tail segment。如果 tail segment 已满，就创建新 segment，或者复用一个已经被所有 consumer groups 消费完成的旧 segment。

Memory queue 的 producer 和 partitions 共享一个 append lock，用于串行执行 round-robin 路由、bounded
capacity 计数和 append。选中的 partition 写入 item 后，使用 release write 发布新的可读 cursor。
Consumer 读取已发布区间时不获取 append lock。

写入成功后，partition 通知所有已注册 consumers。

### 读取和提交

每个 consumer group 在每个 partition 上都有一个 reader。Reader 保存：

- 当前 segment；
- 当前 read position；
- 上次读取数量。

`TryPull` 最多读取 `BatchSize` 条数据。只有调用 `Commit` 后，reader 的已提交读取位置才会推进。

### Segment 复用

Memory 模式可以复用旧 segment。只有当所有 consumer groups 都已经消费过某个 segment 的结尾后，该 segment 才能被复用。这可以避免慢 consumer group 还没读到的数据被覆盖。

### 容量控制

Memory 模式支持通过 `MemoryBufferQueueOptions.BoundedCapacity` 配置有界容量。

配置有界容量后：

- 队列满时，`ProduceAsync` 抛出 `MemoryBufferQueueFullException`；
- 队列满时，`TryProduceAsync` 返回 `false`。

## MemoryMappedFile 模式

MemoryMappedFile 模式把生产的数据持久化到内存映射分段文件，同时持久化 writer offset 和已提交的 consumer offsets，适合本地持久化缓冲和简单恢复。新追加记录何时到达显式持久化边界由配置的 flush 策略决定。

### 目录结构

每个 topic 和 partition 的数据位于：

```text
{DataDirectory}/{TopicName}/partition-{PartitionId:D5}/
```

数据 segment 文件按 segment index 命名：

```text
00000000000000000000.log
00000000000000000001.log
...
```

Consumer offset 位于：

```text
{DataDirectory}/{TopicName}/partition-{PartitionId:D5}/offsets/
```

每个 consumer group 在 `offsets` 下对应一个可读目录。目录名格式是 `{escaped-group-name}`。如果 group name 本身可以作为合法文件夹名，就直接使用原始名称；只有 `/` 这类不能作为单个路径组件的字符才会被百分号编码。百分号 `%` 本身也会被编码，避免和已转义名称发生冲突。

```text
{DataDirectory}/{TopicName}/partition-{PartitionId:D5}/offsets/{escaped-group-name}/offset
```

例如 topic 为 `orders`、partition 为 `0`、group 为 `billing-worker-1` 时，路径是：

```text
bufferqueue/orders/partition-00000/offsets/billing-worker-1/offset
```

如果 group name 是 `orders/worker 1`，斜杠会被编码，空格保持可见：

```text
bufferqueue/orders/partition-00000/offsets/orders%2Fworker 1/offset
```

Partition writer offset 存储在：

```text
{DataDirectory}/{TopicName}/partition-{PartitionId:D5}/writer.offset
```

例如：

```text
bufferqueue/orders/partition-00000/writer.offset
```

### 记录格式

每条数据记录格式如下：

```text
4 bytes  payload length，little-endian int32
N bytes  payload
1 byte   record end marker
```

record end marker 用于在读取和恢复时检测未完整写入或损坏的记录。

如果当前 segment 剩余空间不足以写入下一条记录，并且至少还剩 4 字节，partition 会写入 segment-end marker，然后从下一个 segment 继续写；如果剩余空间不足 4 字节，则把未使用的尾部直接视为 segment padding。segment-end marker 使用 int32 length 值 `-1` 表示。

### 序列化

`MemoryMappedFileBufferQueueOptions<T>` 提供一个可插拔序列化属性：

- `Serializer: IMemoryMappedFileSerializer<T>`

`IMemoryMappedFileSerializer<T>` 在一个契约中同时定义两个操作：`Serialize(T)` 返回 `byte[]`，`Deserialize(ReadOnlyMemory<byte>)` 返回 `T`。

当前提供以下实现：

- 默认使用的 internal `System.Text.Json` 实现；
- 基于 MessagePack for C# 的 `MessagePackMemoryMappedFileSerializer<T>`。无参构造函数使用 `MessagePackSerializerOptions.Standard`，另一个构造函数可以接收显式 MessagePack options。自定义 resolver 和 formatter 必须能够安全并发使用；
- `UnmanagedMemoryMappedFileSerializer<T>`，要求 `T : unmanaged`，直接复制值的 native 内存表示。反序列化时要求 payload 长度与 `Unsafe.SizeOf<T>()` 严格相等。

使用标准 MessagePack options 时，自定义类型应使用 `[MessagePackObject]` 和稳定的数字 `[Key]`。应用项目应直接引用 MessagePack，因为 `BufferQueue.MemoryMappedFile` 包对 MessagePack 的传递 runtime 依赖不会提供它的 analyzer 和 source generator。可以通过自定义 options 启用 contractless 序列化，但它会把成员名写入持久化格式，不是推荐的 MMF schema。Resolver、key、压缩和安全配置都是持久化格式的一部分；已删除字段的数字 key 不能复用。

Unmanaged serializer 可以消除格式编解码，但不是零拷贝实现，因为当前 serializer 契约和 partition 仍会创建 payload byte array。Native endianness、padding、字段顺序、packing、runtime 和进程架构都属于 wire format。`[StructLayout]` 是可选的，但建议显式固定 sequential 或 explicit layout 及 packing。不应持久化 pointer-sized 或进程相关字段。

Serializer 及其 wire schema 是 topic 持久化格式的一部分，在 queue 重启和应用升级时必须与已有记录保持兼容。

配置的 serializer 实例由 topic 的所有 partition 共享，并且可能被并发调用。实现必须是线程安全的，两个操作都不能返回 `null`。

### Flush 策略

`MemoryMappedFileBufferQueueOptions<T>` 提供两种 flush 策略：

- `MemoryMappedFileFlushStrategy.Immediate` 是默认策略，每写入一条记录都显式 flush；
- `MemoryMappedFileFlushStrategy.Batch` 在一个 partition 累积追加 `FlushBatchSize` 条记录后显式 flush，`FlushBatchSize` 默认为 `100`。

在两种策略下，segment rollover 和 consumer commit 都是无条件的 flush 边界。因此，Batch 模式也可能在达到 `FlushBatchSize` 之前 flush。如果后续没有继续生产，也没有 segment rollover 或 consumer commit，未满一批的尾部记录不保证已经被显式 flush。

### 写入

`MemoryMappedFileBufferProducer<T>` 按 round-robin 选择 partition。Partition 序列化 item，计算记录大小，找到当前 segment，写入记录，推进进程内 write offset，应用配置的 flush 策略，并通知 consumers。

每次到达 flush 边界时，partition 都会先 flush memory-mapped accessor，只有 flush 成功后才把对应 offset 写入 `writer.offset`。Segment rollover 会在写入下一个 segment 前 flush 已完成的 segment。Consumer commit 也会 flush 待处理的日志数据并推进 `writer.offset`，然后才持久化 consumer offset。

如果序列化后的 item 大于 segment size，生产会失败并抛出 `InvalidOperationException`。

### 恢复

MemoryMappedFile partition 启动时，会先尝试读取 `writer.offset`。如果文件不存在，则从 offset `0` 开始扫描。如果存储的 writer offset 有效，并且指向真实的 record boundary，就从该位置继续向后扫描，找到最后一个有效 write offset。

扫描遇到以下情况时停止：

- 空 length；
- 非 positive length，且不是 segment-end marker；
- 记录跨越 segment 边界；
- record end marker 缺失。

这样既能让正常启动更快，也能容忍预期内的崩溃窗口：

- 数据已经 flush，但 `writer.offset` 还没更新；
- 操作系统在显式 flush 之前已持久化 pending batch 中的完整记录；
- 尾部数据只写入了一部分。

这些情况下，启动扫描都会找到最后一个有效记录边界。由于 `writer.offset` 只会在对应日志成功 flush 后推进，恢复可以把它作为安全 checkpoint，并继续向后扫描其他完整记录。在 `Batch` 模式下，异常终止后未显式 flush 的尾部记录可能不存在。明显不一致的 checkpoint 状态仍会被视为损坏并快速失败。如果 `writer.offset` 无效、超过实际日志尾部，或者没有对齐到 record boundary，启动会抛出异常，而不是静默 fallback。

### Offset 持久化

MemoryMappedFile 模式按 partition 和 consumer group 持久化已提交 offset。

调用 `Commit` 时，partition 会先强制 flush 待处理的日志数据并推进 `writer.offset`，然后把已提交 offset 以 8 字节 little-endian integer 写入该 group 的 `offset` checkpoint 文件。这个顺序可以避免持久化的 consumer offset 超过已经成功 flush 的日志数据。Checkpoint 写入过程先写临时文件，再 replace 或 move，避免读取到部分写入的 offset 文件。

Reader 创建时：

- 如果 offset 文件存在且包含有效 offset，从该 offset 开始读取；
- 如果 offset 文件不存在，从 `0` 开始读取；
- 如果 offset 文件长度错误或包含负数 offset，抛出 `InvalidDataException`；
- 如果存储的 offset 超过当前 write offset，则抛出异常，而不是静默重置消费进度。

只有已提交 offset 会被持久化。如果 consumer 读取了一个 batch 但没有提交，下一个 queue 实例会再次读取该 batch。

### Writer Offset 持久化

到达 flush 边界时，partition 会先 flush memory-mapped file accessor，再把最新成功 flush 的 writer offset 写入 `writer.offset`，格式是 8 字节 little-endian integer。`Immediate` 会为每条记录建立该边界；`Batch` 会在累积 `FlushBatchSize` 条记录时建立该边界，而 segment rollover 或 consumer commit 会忽略待处理数量并建立该边界。

Writer offset 写入使用和 consumer offset 相同的临时文件加 replace 或 move 模式，避免有效 offset 文件被部分写入文件替换。

Writer offset 是启动优化和恢复提示，不是唯一事实来源。在 `Batch` 模式下，如果仍有未满一批的记录等待 flush，它可能落后于当前进程内 write offset。启动时 partition 仍会从存储的 writer offset 向后校验记录。如果持久化的 writer offset 落后于数据文件中的完整记录，扫描会追上；如果 writer offset 文件不存在，partition 会从头扫描。如果 writer offset 文件存在但和日志不一致，恢复会快速失败。

## Push Consumer 模式

Push consumer 模式构建在 pull consumer 之上。Host service 扫描带 attribute 的 push consumers，创建对应 pull consumers，并把 batch 交给 push consumer 实现处理。

Auto-commit push consumer 在成功处理后自动提交。Manual-commit push consumer 会收到 `IBufferConsumerCommitter`，由业务代码决定何时提交。

## 依赖注入

库会注册一个公共 `IBufferQueue` 服务。每个 topic 会作为 keyed `IBufferQueue<T>` 服务注册。

Memory 模式：

```csharp
services.AddBufferQueue(builder =>
{
    builder.UseMemory(memory =>
    {
        memory.AddTopic<Foo>(options =>
        {
            options.TopicName = "topic-foo";
            options.PartitionNumber = 4;
            options.SegmentSize = 1024;
        });
    });
});
```

MemoryMappedFile 模式：

应用必须引用 `BufferQueue.MemoryMappedFile` project 或 package。它会传递依赖核心 `BufferQueue` 包，公共 namespace 和注册 API 保持不变。

MemoryMappedFile topic queue 由依赖注入容器创建并持有。Dispose service provider 时会关闭所有 partition view 和 memory-mapped-file handle。Dispose 只负责释放资源，不是显式 flush 边界，也不会为待处理 batch 推进 `writer.offset`。

```csharp
services.AddBufferQueue(builder =>
{
    builder.UseMemoryMappedFile(memoryMappedFile =>
    {
        memoryMappedFile.AddTopic<Foo>(options =>
        {
            options.TopicName = "topic-foo";
            options.PartitionNumber = 4;
            options.SegmentSize = 64L * 1024 * 1024;
            options.DataDirectory = "/var/lib/bufferqueue";
            options.FlushStrategy = MemoryMappedFileFlushStrategy.Batch;
            options.FlushBatchSize = 100;
        });
    });
});
```

不同 topic 可以使用不同存储模式，只要 topic name 不重复即可。

## 并发模型

当前实现面向单进程内的并发生产和消费。

关键并发点：

- Producer 使用 round-robin 计数器选择 partition；Memory producer 在串行 append 区间内推进该计数器。
- Memory queue 的 producer 和 partitions 共享一个 lock，串行执行 round-robin 路由、bounded capacity 计数和 append。
- Memory partition 只在对应 item 写入完成后发布 segment cursor，因此 consumer 不会读到尚未写入的 slot，也不需要获取 append lock。
- Consumer group 创建由 queue 级别 lock 保护。
- Consumer 等待和唤醒状态由 `ReaderWriterLockSlim` 保护。
- MemoryMappedFile writer offset 和 consumer offset 写入使用 replace/move 语义，避免部分写入的 offset 文件。

当前设计不提供多个进程同时写入同一个 memory-mapped-file topic directory 的协调能力。除非增加外部协调机制，否则 MemoryMappedFile 模式应视为一个 active queue 实例使用的本地持久化机制。

## 投递语义

手动提交模式提供 at-least-once 行为：

- 如果 batch 已读取但未提交，该 batch 可能再次被投递；
- Auto-commit 会在成功拉取后立即推进进度；
- Manual commit 会在业务代码调用 `CommitAsync` 后推进进度。

Memory 模式的 offset 保存在进程内存中。MemoryMappedFile 模式会把 writer offset 和已提交 consumer offsets 持久化到磁盘。Consumer commit 会先强制待处理日志数据到达 flush 边界。在 `Batch` 模式下，异常终止后未提交且未满一批的尾部记录不保证仍然存在。

## 扩展点

主要扩展点是 `IBufferPartition<TItem>`。新增一种存储实现通常需要：

- 实现一个 `IBufferPartition<TItem>` partition 类型；
- 实现一个 producer，负责选择 partition 并调用 `Enqueue`；
- 实现一个继承 `BufferQueue<TItem>` 的 queue 类型，把 partitions 和 producer 传给父类构造函数；
- 提供 options 和 DI builder 扩展。

存储实现不应该重复实现通用 queue 和 consumer 行为。

## 已知限制

- MemoryMappedFile 模式当前持久化 consumer offsets，但不会自动回收旧数据 segment files。
- MemoryMappedFile 模式当前没有 bounded capacity。
- MemoryMappedFile 模式不协调多个进程同时写同一个 topic directory。
- Memory 模式不持久化数据或 offsets。
- 同一个 queue 实例中 consumer group 名称必须唯一；重复创建相同 group 会被拒绝。

## 测试策略

测试项目与生产项目边界保持一致：`BufferQueue.Tests` 覆盖核心与 Memory 实现，`BufferQueue.MemoryMappedFile.Tests` 覆盖可选的 MMF 程序集。

测试覆盖：

- Memory queue 生产和消费；
- 手动提交和自动提交行为；
- consumer 等待和唤醒行为；
- 多 partition 和多 consumer 的 partition 分配；
- Memory segment 行为和复用；
- DI 注册；
- MemoryMappedFile 生产和消费；
- MemoryMappedFile offset 持久化和未提交重放。

这些测试保证两种存储模式对外提供相同的队列语义，同时把各自的存储行为保留在 partition 抽象之后。
