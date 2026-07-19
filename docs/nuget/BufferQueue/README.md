# BufferQueue

BufferQueue is a typed, topic-based in-process queue for concurrent producers
and partitioned batch consumers. The core package includes segmented Memory
storage, consumer groups, pull and push consumers, and auto or manual commit.

BufferQueue supports .NET 8 and later.

## Install

```shell
dotnet add package BufferQueue
```

For local durable storage and restart recovery, install the optional
[`BufferQueue.MemoryMappedFile`](https://www.nuget.org/packages/BufferQueue.MemoryMappedFile/)
package.

## Register a Memory topic

```csharp
using BufferQueue;

builder.Services.AddBufferQueue(queue =>
{
    queue
        .UseMemory(memory =>
        {
            memory.AddTopic<Order>(topic =>
            {
                topic.TopicName = "orders";
                topic.PartitionNumber = 4;

                // Optional. Memory topics are unbounded by default.
                topic.BoundedCapacity = 100_000;
            });
        })
        .AddPushCustomers(typeof(Program).Assembly);
});

public sealed record Order(long Id, decimal Total);
```

Each `(message type, topic name)` pair identifies one typed queue. A topic can
have multiple partitions, and producer calls are distributed across them in
round-robin order.

## Produce

A fixed topic can be injected as a keyed `IBufferProducer<T>`:

```csharp
using BufferQueue;
using Microsoft.Extensions.DependencyInjection;

public sealed class OrderWriter(
    [FromKeyedServices("orders")] IBufferProducer<Order> producer)
{
    public ValueTask WriteAsync(Order order) =>
        producer.ProduceAsync(order);
}
```

For a topic selected at runtime, inject `IBufferQueue` and call
`GetProducer<T>(topicName)`.

On a bounded Memory topic, `ProduceAsync` throws
`MemoryBufferQueueFullException` when the queue is full. Use `TryProduceAsync`
when a `false` result is preferable to an exception.

## Consume in batches

This example uses manual commit, so progress advances only after the batch has
been processed successfully:

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

Use `CreatePullConsumers<T>(options, consumerNumber)` to distribute a consumer
group's partitions across multiple consumers. The consumer count cannot exceed
the topic's partition count.

## Push consumers

`AddPushCustomers` in the registration example scans the specified assembly for
classes marked with `BufferPushCustomerAttribute` and starts their consumption
loops as hosted services.

An auto-commit Push Consumer receives batches without managing the commit
operation itself:

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

Auto commit advances queue progress before application processing. Use a manual
commit Push Consumer when a failed batch must remain eligible for replay:

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

The `concurrency` value creates that many consumers in the group and cannot
exceed the topic's partition count.

A Singleton Push Consumer is reused across batches and concurrent consumer
loops, so it must be thread-safe. Scoped and Transient Push Consumers are
resolved in a new asynchronous DI scope for every batch and are disposed after
the handler completes or throws.

## Semantics

- Memory topics and their consumer offsets exist only for the lifetime of the process.
- Each consumer group has independent progress and receives the topic's messages.
- Consumers in the same group divide partitions between them.
- Ordering is preserved within a partition, not globally across partitions.
- `BatchSize` is an upper bound; a returned batch may contain fewer items.
- Manual commit provides at-least-once delivery; an uncommitted batch may be delivered again.
- Auto commit advances progress after a successful pull, before application processing.
- Consumer count is fixed when a group is created and cannot exceed the partition count.

## Links

- [Repository and documentation](https://github.com/eventhorizon-cli/BufferQueue)
- [Chinese documentation](https://github.com/eventhorizon-cli/BufferQueue/blob/main/README.zh-CN.md)
- [Design](https://github.com/eventhorizon-cli/BufferQueue/blob/main/docs/design.md)
- [ASP.NET Core sample](https://github.com/eventhorizon-cli/BufferQueue/tree/main/samples/WebAPI)
- [Issues](https://github.com/eventhorizon-cli/BufferQueue/issues)
