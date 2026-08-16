# BufferQueue

[English](README.md) | 简体中文

BufferQueue 是一个按 topic 划分的强类型队列，支持并发生产和按 partition 批量消费。核心包包含分段
Memory 存储、consumer group、pull/push consumer，以及自动提交和手动提交两种消费方式。

BufferQueue 支持 .NET 8 和 .NET 10。

## 安装

```shell
dotnet add package BufferQueue
```

如需本地持久化存储和进程重启后的恢复能力，请安装可选包
[`BufferQueue.MemoryMappedFile`](https://www.nuget.org/packages/BufferQueue.MemoryMappedFile/)。

## 注册 Memory topic

```csharp
using BufferQueue;
using BufferQueue.Memory;

builder.Services.AddBufferQueue(queue =>
{
    queue
        .UseMemory(memory =>
        {
            memory.AddTopic<Order>(topic =>
            {
                topic.TopicName = "orders";
                topic.PartitionNumber = 4;
                topic.UsePartitionKey(order => order.Id);

                // 可选。Memory topic 默认不限制容量。
                topic.BoundedCapacity = 100_000;
                topic.FullMode = BufferQueueFullMode.Wait;
            });
        })
        .AddPushCustomers(typeof(Program).Assembly);
});

public sealed record Order(long Id, decimal Total);
```

每个 `(消息类型, topic 名称)` 组合对应一个强类型队列。一个 topic 可以包含多个 partition，Producer
默认按 round-robin 方式选择写入分区。

`UsePartitionKey` 必须传入 selector 委托。数值 selector 支持内置的 `INumber<TNumber>` 类型，但其返回值
必须是有限整数。分区索引按 `(key - 1)` 对 `PartitionNumber` 的归一化数学取模计算，因此 `0` 和负数也可作为
key。字符串 selector 只取前四个 UTF-16 字符来选择 partition。相同 key 会路由到同一个 partition，并保持
该 partition 内的写入顺序；不同 key 也可能落在同一个 partition。省略 `UsePartitionKey` 则继续使用默认的
round-robin 路由。Selector 必须是确定性的，并且能安全地被并发调用。在 Memory 模式下，并发 Producer 可以
同时向不同 key 所在的 partition 写入；写入同一 partition 时仍会串行执行。

批量写入同样按单条数据路由。round-robin 会为批次中的每条数据轮转一次，不会把整个批次固定写入一个
partition。按 key 路由时，会为批次中的每条数据调用 selector；相同 key 的数据在选定 partition 内保持
输入顺序。顺序保证只在 partition 内成立，不提供跨 partition 的批次全局顺序。

在 Memory 存储中，默认 round-robin 批量会先保留完整的选择范围，再向每个选中的 partition 追加对应的数据
切片。这样不改变逐条路由规则，同时让各切片可以在自己的 partition 锁内追加。

## 写入数据

topic 在依赖注入时已确定，可以通过 keyed `IBufferProducer<T>` 注入：

```csharp
using BufferQueue;
using Microsoft.Extensions.DependencyInjection;

public sealed class OrderWriter(
    [FromKeyedServices("orders")] IBufferProducer<Order> producer)
{
    public ValueTask WriteAsync(Order order, CancellationToken cancellationToken = default) =>
        producer.ProduceAsync(order, cancellationToken);
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
ReadOnlyMemory<Order> bufferedOrders = pendingOrders.AsMemory();

await producer.ProduceAsync(bufferedOrders, cancellationToken);
var accepted = await producer.TryProduceAsync(bufferedOrders, cancellationToken);

IEnumerable<Order> ordersFromAnEnumerable = GetPendingOrders();
await producer.ProduceAsync(ordersFromAnEnumerable, cancellationToken);
~~~

数据已经连续存储，或可以直接表示为内存区间时，应优先使用 `ReadOnlyMemory<T>`。这是核心批量写入形式，
能避免非数组 `IEnumerable<T>` 在提交前产生的物化。`IEnumerable<T>` 重载用于方便调用；当输入不是数组时，
会先物化为数组，再提交整个批次。单条和 `ReadOnlyMemory<T>` 的 `ProduceAsync` 都是核心接口方法，不是扩展方法。

对于配置了有界容量的 Memory topic，`FullMode` 默认为 `Wait`：`ProduceAsync` 和 `TryProduceAsync` 都会异步
等待容量可用；输入完整接纳后，`TryProduceAsync` 返回 `true`。生产代码应传入 `CancellationToken`，以便取消
背压等待。需要立即拒绝写入时，可将 `FullMode` 设为 `Fail`：`ProduceAsync` 会抛出
`BufferQueueFullException`，`TryProduceAsync` 返回 `false`。大小不超过配置容量的批次会整批接纳；超过容量的
`Wait` 批次会按输入顺序切成容量大小的连续切片，每个切片整批接纳，取消时此前完成的切片可能已经可见。`Fail`
模式会整批拒绝超过容量的批次。容量上限由 topic 下的所有 partition 共享。每个 partition 会在所有已知
consumer group 的最小 committed position 向前推进时归还容量；即使只推进到 segment 中间，也会归还相应部分，
因此较慢的 group 仍可能在其他 group 提交后继续占用容量。较晚创建的 consumer group 会从当前逻辑上的最早位置
开始消费，无法读取容量已经释放的数据。

## 批量消费

下面的示例使用手动提交：只有整个批次处理成功后，消费进度才会推进。

```csharp
using BufferQueue;
using Microsoft.Extensions.Hosting;

public sealed class OrderWorker(IBufferQueue queue) : BackgroundService
{
    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        var consumer = queue.CreatePullConsumer<Order>(
            new BufferPullConsumerOptions
            {
                TopicName = "orders",
                GroupName = "order-fulfillment",
                BatchSize = 100,
                AutoCommit = false
            });

        await foreach (var batch in consumer.ConsumeAsync(stoppingToken))
        {
            foreach (var order in batch)
            {
                await ProcessAsync(order, stoppingToken);
            }

            await consumer.CommitAsync();
        }
    }

    private static Task ProcessAsync(Order order, CancellationToken cancellationToken)
    {
        // Replace with application processing.
        return Task.CompletedTask;
    }
}
```

使用 `CreatePullConsumers<T>(options, consumerNumber)` 可以将一个 consumer group 的 partition 分配给多个
consumer。consumer 数量不能超过该 topic 的 partition 数量。

## Push consumer

注册示例中的 `AddPushCustomers` 会扫描指定程序集，找到带有 `BufferPushCustomerAttribute` 的类型，并以 hosted
service 的形式启动其消费循环。

自动提交的 Push Consumer 不需要自行处理提交操作：

```csharp
using BufferQueue.PushConsumer;
using Microsoft.Extensions.DependencyInjection;

[BufferPushCustomer(
    topicName: "orders",
    groupName: "order-indexing",
    batchSize: 100,
    serviceLifetime: ServiceLifetime.Singleton,
    concurrency: 4)]
public sealed class OrderIndexConsumer : IBufferAutoCommitPushConsumer<Order>
{
    public async Task ConsumeAsync(
        IEnumerable<Order> batch,
        CancellationToken cancellationToken)
    {
        foreach (var order in batch)
        {
            await IndexAsync(order, cancellationToken);
        }
    }

    private static Task IndexAsync(Order order, CancellationToken cancellationToken)
    {
        // Replace with application processing.
        return Task.CompletedTask;
    }
}
```

如果批次处理失败后仍需重试，应使用手动提交的 Push Consumer：

```csharp
using BufferQueue.PushConsumer;
using Microsoft.Extensions.DependencyInjection;

[BufferPushCustomer(
    topicName: "orders",
    groupName: "billing",
    batchSize: 100,
    serviceLifetime: ServiceLifetime.Scoped,
    concurrency: 4)]
public sealed class BillingConsumer : IBufferManualCommitPushConsumer<Order>
{
    public async Task ConsumeAsync(
        IEnumerable<Order> batch,
        IBufferConsumerCommitter committer,
        CancellationToken cancellationToken)
    {
        foreach (var order in batch)
        {
            await BillAsync(order, cancellationToken);
        }

        await committer.CommitAsync();
    }

    private static Task BillAsync(Order order, CancellationToken cancellationToken)
    {
        // Replace with application processing.
        return Task.CompletedTask;
    }
}
```

`concurrency` 表示该 group 内创建的 consumer 数量，不能超过 topic 的 partition 数量。

Singleton Push Consumer 会在多个批次和并发消费循环之间复用，因此实现必须线程安全。Scoped 和 Transient
Push Consumer 会为每个批次创建新的异步 DI scope，并在处理方法结束或抛出异常后释放。

## 语义

- Memory topic 及其 consumer offset 仅在当前进程生命周期内存在。
- 每个 consumer group 独立维护进度，并消费该 topic 的全部消息。
- 同一 group 中的 consumer 会分配不同 partition。
- 顺序只在单个 partition 内得到保证，不保证跨 partition 的全局顺序。
- `BatchSize` 是上限，实际返回的批次可能更小。
- 手动提交提供 at-least-once 投递；未提交的批次可能再次交付。
- 自动提交会在成功拉取数据后、应用代码处理前推进进度。
- consumer group 创建后，consumer 数量固定且不能超过 partition 数量。

## 链接

- [仓库与文档](https://github.com/eventhorizon-cli/BufferQueue)
- [中文仓库文档](https://github.com/eventhorizon-cli/BufferQueue/blob/main/README.zh-CN.md)
- [设计文档](https://github.com/eventhorizon-cli/BufferQueue/blob/main/docs/README.zh-CN.md)
- [ASP.NET Core 示例](https://github.com/eventhorizon-cli/BufferQueue/tree/main/samples/WebAPI)
- [问题反馈](https://github.com/eventhorizon-cli/BufferQueue/issues)
