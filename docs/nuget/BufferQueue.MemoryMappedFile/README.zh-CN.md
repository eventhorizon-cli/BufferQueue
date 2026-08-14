# BufferQueue.MemoryMappedFile

[English](README.md) | 简体中文

BufferQueue.MemoryMappedFile 是 BufferQueue 的可选本地持久化存储实现。它将序列化后的记录写入按
partition 划分的内存映射分段文件，并持久化 producer checkpoint 和已提交的 consumer offset。

该包支持 .NET 8 和 .NET 10。它面向单个活动 queue 实例设计，不负责协调多个进程同时向同一个 topic
目录写入数据。

## 安装

```shell
dotnet add package BufferQueue.MemoryMappedFile
```

该包依赖核心 `BufferQueue` 包。

## 注册 topic

```csharp
using BufferQueue;
using BufferQueue.MemoryMappedFile;

builder.Services.AddBufferQueue(queue =>
{
    queue
        .UseMemoryMappedFile(storage =>
        {
            storage.AddTopic<OrderEvent>(options =>
            {
                options.TopicName = "order-events";
                options.DataDirectory = "/var/lib/bufferqueue";
                options.PartitionNumber = 4;
                options.UsePartitionKey(orderEvent => orderEvent.Id);
                options.SegmentSizeInBytes = 64L * 1024 * 1024;
                options.MaxRetainedConsumedSegments = 2;
            });
        })
        .AddPushCustomers(typeof(Program).Assembly);
});

public sealed record OrderEvent(long Id, decimal Total);
```

默认序列化器为 System.Text.Json。其他默认值如下：

| 选项 | 默认值 | 说明 |
| --- | ---: | --- |
| `PartitionNumber` | `1` | topic 的 partition 数量 |
| `SegmentSizeInBytes` | `256 MiB` | 每个映射 segment 文件的大小 |
| `FlushStrategy` | `Immediate` | 每追加一条记录就显式 flush |
| `FlushBatchSize` | `100` | 一个 partition 累积多少条记录后执行一次 Batch flush |
| `MaxRetainedConsumedSegments` | `null` | 关闭自动删除 |
| `DataDirectory` | `Path.Combine(AppContext.BaseDirectory, "bufferqueue")` | topic 的存储根目录 |

`DataDirectory` 必须稳定且可写。磁盘路径不包含 CLR 类型名，因此同一目录下不同消息类型的 `TopicName`
必须保持唯一。每个 `(消息类型, topic 名称)` 组合只能注册到一种存储模式。

写入、消费、pull consumer、push consumer 和提交操作与 Memory 存储使用相同的 `IBufferProducer<T>`、
`IBufferQueue` 及相关 API。

Producer 默认按 round-robin 方式选择 partition。`UsePartitionKey` 必须传入 selector 委托。数值 selector
支持内置的 `INumber<TNumber>` 类型，但其返回值必须是有限整数。分区索引按 `(key - 1)` 对
`PartitionNumber` 的归一化数学取模计算，因此 `0` 和负数也可作为 key。字符串 selector 只取前四个 UTF-16
字符来选择 partition。相同 key 会路由到同一个 partition；不同 key 也可能落在同一个 partition。Selector
应当是确定性的，并且能安全地被并发调用。

只要历史记录还需要保持按 key 的顺序，selector 与 `PartitionNumber` 就不能修改。数值和字符串的路由规则在
进程重启后仍保持确定性；字符串路由不使用 `string.GetHashCode()`。

批量写入同样按单条数据路由。round-robin 会为批次中的每条数据轮转一次，而不是把整个批次固定到一个
partition。按 key 路由时，会为批次中的每条数据调用 selector；相同 key 的数据在选定 partition 内保持输入
顺序。顺序保证只在 partition 内成立，不提供跨 partition 的批次全局顺序。

## 写入数据

topic 在依赖注入时已确定，可以通过 keyed producer 注入：

```csharp
using BufferQueue;
using Microsoft.Extensions.DependencyInjection;

public sealed class OrderEventWriter(
    [FromKeyedServices("order-events")] IBufferProducer<OrderEvent> producer)
{
    public ValueTask WriteAsync(OrderEvent orderEvent, CancellationToken cancellationToken = default) =>
        producer.ProduceAsync(orderEvent, cancellationToken);
}
```

需要在运行时选择 topic 时，注入 `IBufferQueue` 并调用 `GetProducer<T>(topicName)`。

`IBufferProducer<T>` 直接提供四个核心方法，每个方法都接收可选的 `CancellationToken`：

~~~csharp
ValueTask<bool> TryProduceAsync(T item, CancellationToken cancellationToken = default);
ValueTask<bool> TryProduceAsync(ReadOnlyMemory<T> items, CancellationToken cancellationToken = default);
ValueTask ProduceAsync(T item, CancellationToken cancellationToken = default);
ValueTask ProduceAsync(ReadOnlyMemory<T> items, CancellationToken cancellationToken = default);
~~~

`BufferProducerExtensions` 只提供接收 `IEnumerable<T>` 的便捷重载，这些重载同样接收 `CancellationToken`：

~~~csharp
CancellationToken cancellationToken = default;
ReadOnlyMemory<OrderEvent> bufferedEvents = pendingEvents.AsMemory();

await producer.ProduceAsync(bufferedEvents, cancellationToken);
var accepted = await producer.TryProduceAsync(bufferedEvents, cancellationToken);

IEnumerable<OrderEvent> eventsFromAnEnumerable = GetPendingEvents();
await producer.ProduceAsync(eventsFromAnEnumerable, cancellationToken);
~~~

数据已经连续存储，或可以直接表示为内存区间时，应优先使用 `ReadOnlyMemory<T>`。这是核心批量写入形式，
能避免非数组 `IEnumerable<T>` 在提交前产生的物化。`IEnumerable<T>` 重载用于方便调用；当输入不是数组时，
会先物化为数组，再提交整个批次。单条和 `ReadOnlyMemory<T>` 的 `ProduceAsync` 都是核心接口方法，不是扩展方法。

MemoryMappedFile topic 没有有界容量。它与 Memory 存储使用相同的、支持取消的 `IBufferProducer<T>` API，
但 `TryProduceAsync` 和 `ProduceAsync` 都不会等待容量，也没有 `FullMode` 配置。批次中的每条数据仍会根据配置的
flush 策略单独路由并持久化为一条记录。

MMF 会将批次中的每条数据视为一条普通记录：选择 partition、序列化、写入记录，然后应用配置的 flush
策略。一个批次不会合并为一条记录，也不会作为单个 partition 的分组或一次 flush 的单位。

## Pull consumer

注册一个 hosted worker，并为 consumer group 取一个明确的名称。下面的示例只有在整个批次处理成功后才提交：

```csharp
using BufferQueue;
using Microsoft.Extensions.Hosting;

builder.Services.AddHostedService<OrderProjectionWorker>();

public sealed class OrderProjectionWorker(IBufferQueue queue) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumer = queue.CreatePullConsumer<OrderEvent>(
            new BufferPullConsumerOptions
            {
                TopicName = "order-events",
                GroupName = "order-projection",
                BatchSize = 100,
                AutoCommit = false
            });

        await foreach (var batch in consumer.ConsumeAsync(stoppingToken))
        {
            foreach (var orderEvent in batch)
            {
                await UpdateProjectionAsync(orderEvent, stoppingToken);
            }

            await consumer.CommitAsync();
        }
    }

    private static Task UpdateProjectionAsync(
        OrderEvent orderEvent,
        CancellationToken cancellationToken)
    {
        // Replace with application processing.
        return Task.CompletedTask;
    }
}
```

使用 `CreatePullConsumers<T>(options, consumerNumber)` 可以将一个 group 的 partition 分配给多个 consumer。
consumer 数量不能超过该 topic 的 partition 数量。

## Push consumer

注册示例中的 `AddPushCustomers` 会扫描指定程序集，并将找到的 Push Consumer 作为 hosted service 启动。

自动提交的 Push Consumer 适用于应用代码尚未处理完批次时，消费进度也可以推进的场景：

```csharp
using BufferQueue.PushConsumer;
using Microsoft.Extensions.DependencyInjection;

[BufferPushCustomer(
    topicName: "order-events",
    groupName: "order-indexing",
    batchSize: 100,
    serviceLifetime: ServiceLifetime.Singleton,
    concurrency: 4)]
public sealed class OrderIndexConsumer : IBufferAutoCommitPushConsumer<OrderEvent>
{
    public async Task ConsumeAsync(
        IEnumerable<OrderEvent> batch,
        CancellationToken cancellationToken)
    {
        foreach (var orderEvent in batch)
        {
            await IndexAsync(orderEvent, cancellationToken);
        }
    }

    private static Task IndexAsync(
        OrderEvent orderEvent,
        CancellationToken cancellationToken)
    {
        // Replace with application processing.
        return Task.CompletedTask;
    }
}
```

如果必须等业务处理完成后才能推进已持久化的 consumer offset，请使用手动提交的 Push Consumer：

```csharp
using BufferQueue.PushConsumer;
using Microsoft.Extensions.DependencyInjection;

[BufferPushCustomer(
    topicName: "order-events",
    groupName: "billing",
    batchSize: 100,
    serviceLifetime: ServiceLifetime.Scoped,
    concurrency: 4)]
public sealed class BillingConsumer : IBufferManualCommitPushConsumer<OrderEvent>
{
    public async Task ConsumeAsync(
        IEnumerable<OrderEvent> batch,
        IBufferConsumerCommitter committer,
        CancellationToken cancellationToken)
    {
        foreach (var orderEvent in batch)
        {
            await BillAsync(orderEvent, cancellationToken);
        }

        await committer.CommitAsync();
    }

    private static Task BillAsync(
        OrderEvent orderEvent,
        CancellationToken cancellationToken)
    {
        // Replace with application processing.
        return Task.CompletedTask;
    }
}
```

`concurrency` 表示该 group 内创建的 consumer 数量，不能超过 topic 的 partition 数量。每个 `GroupName`
都维护独立的持久化 offset。手动提交会先将待处理日志数据推进到 flush 边界，再持久化该 group 的进度。

Singleton Push Consumer 会在多个批次和并发消费循环之间复用，因此实现必须线程安全。Scoped 和 Transient
Push Consumer 会为每个批次创建新的异步 DI scope，并在处理方法结束或抛出异常后释放。

## MessagePack

应用项目应直接引用 MessagePack，使其 analyzer 和 source generator 在应用项目中运行：

```shell
dotnet add package MessagePack
```

定义稳定的数值 key，并选择内置序列化器。下面的完整示例注册了一个使用 MessagePack、Batch flush 和自动
segment 清理的 topic：

```csharp
using BufferQueue;
using BufferQueue.MemoryMappedFile;
using MessagePack;

builder.Services.AddBufferQueue(queue =>
{
    queue.UseMemoryMappedFile(storage =>
    {
        storage.AddTopic<InventoryChanged>(options =>
        {
            options.TopicName = "inventory-events";
            options.DataDirectory = "/var/lib/bufferqueue";
            options.PartitionNumber = 4;
            options.SegmentSizeInBytes = 64L * 1024 * 1024;
            options.FlushStrategy = MemoryMappedFileFlushStrategy.Batch;
            options.FlushBatchSize = 100;
            options.MaxRetainedConsumedSegments = 2;
            options.Serializer =
                new MessagePackMemoryMappedFileSerializer<InventoryChanged>();
        });
    });
});

[MessagePackObject]
public sealed class InventoryChanged
{
    [Key(0)]
    public long ProductId { get; set; }

    [Key(1)]
    public int QuantityDelta { get; set; }
}
```

数值 key、resolver、压缩、安全选项和自定义 formatter 都是持久化 schema 的一部分。已删除的 key 不能复用。
配置的 resolver 和 formatter 必须线程安全。

## Unmanaged 结构

对于布局固定的 unmanaged 值，内置的 unmanaged 序列化器会复制其本机内存表示。下面的完整示例注册了一个
Quote topic：

```csharp
using System.Runtime.InteropServices;
using BufferQueue;
using BufferQueue.MemoryMappedFile;

builder.Services.AddBufferQueue(queue =>
{
    queue.UseMemoryMappedFile(storage =>
    {
        storage.AddTopic<Quote>(options =>
        {
            options.TopicName = "quotes";
            options.DataDirectory = "/var/lib/bufferqueue";
            options.PartitionNumber = 4;
            options.Serializer =
                UnmanagedMemoryMappedFileSerializer<Quote>.Instance;
        });
    });
});

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct Quote
{
    public long Sequence;
    public long Timestamp;
    public double Price;
    public int Quantity;
}
```

字段顺序、packing、本机字节序、运行时和进程架构都是该格式的一部分。避免使用指针大小相关的字段，也不要在
旧记录仍存在时修改布局。队列仍会创建 payload `byte[]`，因此它不是零拷贝读取。

同样支持自定义 `IMemoryMappedFileSerializer<T>`。序列化器实例由多个 partition 共享，必须线程安全。

## Flush 与提交边界

当减少显式 flush 次数比保留不完整尾部记录更重要时，可以使用 Batch flush：

```csharp
options.FlushStrategy = MemoryMappedFileFlushStrategy.Batch;
options.FlushBatchSize = 100;
```

- `Immediate` 会在每追加一条记录后显式 flush。
- `Batch` 会在一个 partition 追加 `FlushBatchSize` 条记录后 flush。
- 切换到下一个 segment 和提交 consumer 始终会将待处理日志数据推进到 flush 边界。
- 异常终止时，尚未达到 Batch 阈值的尾部记录不保证保留。
- 释放 service provider 会关闭映射资源，但不会建立新的待处理 flush 边界。

Consumer 提交会先 flush 待处理日志数据并推进 `producer.offset`，再持久化该 group 的 `consumer.offset`。
因此，手动提交提供 at-least-once 投递，未提交的批次可能再次交付。

## 恢复与 segment 清理

启动时，BufferQueue 将 `producer.offset` 视为安全 checkpoint，并从该位置向后扫描完整记录。如果
`producer.offset` 不存在，则从 `earliest.offset` 开始扫描。格式错误、超出日志范围、未对齐到记录边界的
checkpoint，已有 consumer group 目录中缺少 `consumer.offset`，以及保留范围内缺失的 segment 都会快速失败，
不会静默重置进度。

只有所有已知 consumer group 都提交越过某个 segment 后，该 segment 才能删除。因此，缓慢、未提交、离线或
已废弃的 group 都会阻止清理。`MaxRetainedConsumedSegments` 不是磁盘占用的硬上限。删除废弃 group 时，必须先
停止 queue，并从每个 partition 中删除其完整 group 目录。

## 重要限制

- MemoryMappedFile 不提供多进程 writer 协调。
- MemoryMappedFile 目前不支持有界容量。
- 已有 topic 不要调小 `PartitionNumber`。
- 不要修改持久化 topic 的 key selector 或按 key 路由行为。
- 序列化后的单条记录必须能放入一个 segment。
- 多个 partition 只保证 partition 内顺序，不保证全局 FIFO 顺序。
- 持久化序列化 schema 必须与已有记录兼容。

## 链接

- [BufferQueue 核心包](https://www.nuget.org/packages/BufferQueue/)
- [仓库与文档](https://github.com/eventhorizon-cli/BufferQueue)
- [中文仓库文档](https://github.com/eventhorizon-cli/BufferQueue/blob/main/README.zh-CN.md)
- [MemoryMappedFile 设计文档](https://github.com/eventhorizon-cli/BufferQueue/blob/main/docs/design/memory-mapped-file.zh-CN.md)
- [问题反馈](https://github.com/eventhorizon-cli/BufferQueue/issues)
