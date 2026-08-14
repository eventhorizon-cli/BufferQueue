# 架构与注册

[English](architecture.md) | [简体中文](architecture.zh-CN.md)

[设计文档首页](../README.zh-CN.md)

## 目标

BufferQueue 是一个面向 .NET 的、按 topic 划分的强类型缓冲队列库，提供统一的队列模型和可插拔的
底层存储实现：

- Memory 把数据保存在进程内的分段内存结构中。
- MemoryMappedFile 把序列化记录写入内存映射分段文件，并持久化 producer offset 和已提交的
  consumer offset。

包同时多目标支持 .NET 8 和 .NET 10。benchmark 有意只运行在 .NET 10。

两种模式共享 producer、pull consumer、consumer group、partition 分配、批量消费和等待唤醒语义。
存储差异被隔离在内部 partition 抽象之后。

两种实现也在 project 和 package 边界上分离：

- `BufferQueue` 包含共享队列抽象、Memory 存储和 push consumer 集成。
- `BufferQueue.MemoryMappedFile` 包含可选的 MemoryMappedFile 存储实现，并依赖
  `BufferQueue`。

核心 project 不引用 MMF project。MMF project 通过 friend assembly 访问并复用共享的 internal 队列抽象。
该拆分不会改变公共 namespace 或 `.UseMemoryMappedFile(...)` 注册调用。

## 公共模型

根据 topic 的确定时机选择 producer 的访问方式：

- 在声明依赖时 topic 已固定：使用 `[FromKeyedServices("topic-name")]` 注入
  `IBufferProducer<T>`。
- topic 需要在运行时确定：注入 `IBufferQueue`，再调用
  `GetProducer<T>(topicName)`。

~~~csharp
public sealed class FooPublisher(
    [FromKeyedServices("topic-foo")] IBufferProducer<Foo> producer)
{
    public ValueTask PublishAsync(Foo item) => producer.ProduceAsync(item);
}

var producer = bufferQueue.GetProducer<Foo>("topic-foo");
var consumer = bufferQueue.CreatePullConsumer<Foo>(new BufferPullConsumerOptions
{
    TopicName = "topic-foo",
    GroupName = "group-a",
    AutoCommit = false,
    BatchSize = 100
});
~~~

公共 API 有意保持较小的表面积：

- `IBufferProducer<T>`：向 topic 生产强类型数据。
- `IBufferPullConsumer<T>`：从 topic 批量消费数据。
- `IBufferConsumerCommitter`：提交手动消费的批次。
- `BufferPullConsumerOptions`：配置 topic、group、auto commit 和 batch size。
- `BufferOptionsBuilder`：把存储实现注册到依赖注入容器。

内部每个已注册 topic 对应一个 `IBufferQueue<T>`。相应的 keyed
`IBufferProducer<T>` 注册会转发到该 queue 持有的 producer。非泛型
`BufferQueue` 根据 topic name 从 DI 容器中解析对应的 typed queue。

### 批量写入

`IBufferProducer<T>` 只定义两个不抛异常的核心写入方法：

- `ValueTask<bool> TryProduceAsync(T item)`
- `ValueTask<bool> TryProduceAsync(ReadOnlyMemory<T> items)`

`BufferProducerExtensions` 在此基础上提供常用的便利 API：

- `ValueTask ProduceAsync(T item)`
- `ValueTask ProduceAsync(ReadOnlyMemory<T> items)`
- `ValueTask ProduceAsync(IEnumerable<T> items)`
- `ValueTask<bool> TryProduceAsync(IEnumerable<T> items)`

~~~csharp
ReadOnlyMemory<Foo> bufferedItems = pendingItems.AsMemory();

await producer.ProduceAsync(bufferedItems);
var accepted = await producer.TryProduceAsync(bufferedItems);

IEnumerable<Foo> itemsFromAnEnumerable = GetPendingItems();
await producer.ProduceAsync(itemsFromAnEnumerable);
~~~

数据已经连续存储，或可以直接表示为内存区间时，应优先使用 `ReadOnlyMemory<T>`。这是核心批量写入形式，
可以避免非数组 `IEnumerable<T>` 在提交前所需的物化。`IEnumerable<T>` 重载用于方便调用；当输入不是数组时，
会先物化为数组，再提交整个批次。非 `Try` 扩展会在核心方法返回 `false` 时抛出 `BufferQueueFullException`。

批量写入是 Producer 的核心能力。不同存储模式下的容量检查、路由和持久化行为，参见对应的设计文档。

## 共享 Queue 边界

~~~text
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
~~~

`BufferQueue<TItem>` 是单个 typed topic 的共享抽象队列父类。它校验 consumer 参数，
阻止同一个 queue 实例中重复创建相同 consumer group，创建
`BufferPullConsumer<TItem>`，在组内分配 partition，并暴露 topic producer。

具体队列只创建自己的存储特定 partition 和 producer：

- `MemoryBufferQueue<T>` 创建
  `MemoryBufferPartition<T>[]` 和 `MemoryBufferProducer<T>`。
- `MemoryMappedFileBufferQueue<T>` 创建
  `MemoryMappedFileBufferPartition<T>[]` 和
  `MemoryMappedFileBufferProducer<T>`。

两个 producer 都使用共享的 `IPartitioner<TItem>`。选定实现会作为 keyed topic
service 注册。共享 partitioner 实现 round-robin 和 PartitionKey 路由，两种存储模式都通过 topic
配置开放这些策略。

存储实现通过 `IBufferPartition<TItem>` 接入通用 queue 和 consumer 逻辑：

~~~csharp
internal interface IBufferPartition<TItem>
{
    int PartitionId { get; }

    void RegisterConsumer(IBufferPartitionConsumer<TItem> consumer);

    void Enqueue(TItem item);

    bool TryPull(string groupName, int batchSize, out IEnumerable<TItem>? items);

    void Commit(string groupName);
}
~~~

上层逻辑只依赖该抽象，因此 partition 可以使用不同的存储策略。
`IBufferPartitionConsumer<TItem>` 是 partition 在数据可用后唤醒 consumer 所使用的最小
通知契约。

## 依赖注入

库注册一个公共 `IBufferQueue` 服务。每个 topic 都以 topic name 为 key 注册
`IBufferQueue<T>` 和 `IBufferProducer<T>` 服务。

Memory 注册：

~~~csharp
services.AddBufferQueue(builder =>
{
    builder.UseMemory(memory =>
    {
        memory.AddTopic<Foo>(options =>
        {
            options.TopicName = "topic-foo";
            options.PartitionNumber = 4;
            options.SegmentSize = 1024;
            options.UsePartitionKey(foo => foo.Id);
        });
    });
});
~~~

MemoryMappedFile 注册要求引用 `BufferQueue.MemoryMappedFile` project 或 package。它会传递依赖
核心 package，公共 namespace 和注册 API 保持不变。

~~~csharp
services.AddBufferQueue(builder =>
{
    builder.UseMemoryMappedFile(memoryMappedFile =>
    {
        memoryMappedFile.AddTopic<Foo>(options =>
        {
            options.TopicName = "topic-foo";
            options.PartitionNumber = 4;
            options.SegmentSizeInBytes = 64L * 1024 * 1024;
            options.MaxRetainedConsumedSegments = 2;
            options.DataDirectory = "/var/lib/bufferqueue";
            options.FlushStrategy = MemoryMappedFileFlushStrategy.Batch;
            options.FlushBatchSize = 100;
            options.UsePartitionKey(foo => foo.Id);
        });
    });
});
~~~

MMF topic queue 由 DI 容器创建并持有。Dispose service provider 时会关闭所有 partition view 和
memory-mapped-file handle。Dispose 只负责释放资源，不是显式 flush 边界，也不会为待处理 batch 推进
`producer.offset`。

不同 topic 可以使用不同存储模式，只要 topic name 不重复即可。

## 扩展点

主要扩展点是 `IBufferPartition<TItem>`。新增一种存储实现通常需要：

- 实现一个 `IBufferPartition<TItem>` partition 类型；
- 实现一个负责选择 partition 并调用 `Enqueue` 的 producer；
- 实现一个继承 `BufferQueue<TItem>` 的 queue 类型，把 partition 和 producer 传给父类构造函数；
- 提供 options 和 DI builder 扩展。

存储实现不应重复实现通用 queue 和 consumer 行为。

## 测试策略

测试项目与生产 project 边界保持一致：

- `BufferQueue.Tests` 覆盖核心与 Memory 实现。
- `BufferQueue.MemoryMappedFile.Tests` 覆盖可选的 MMF assembly。

测试覆盖 Memory 生产和消费、手动和自动提交、consumer 等待和唤醒、partition 分配、segment 复用、DI 注册、
MMF 生产和消费、offset 持久化、恢复和未提交重放。这些测试保证两种存储模式共享对外的 queue 语义，同时把
各自的存储行为保留在 partition 抽象之后。
