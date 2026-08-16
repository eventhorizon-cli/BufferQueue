# BufferQueue

English | [简体中文](README.zh-CN.md)

BufferQueue is a typed, topic-based in-process queue for concurrent producers
and partitioned batch consumers. The core package includes segmented Memory
storage, consumer groups, pull and push consumers, and auto or manual commit.

BufferQueue targets .NET 8 and .NET 10.

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

                // Optional. Memory topics are unbounded by default.
                topic.BoundedCapacity = 100_000;
                topic.FullMode = BufferQueueFullMode.Wait;
            });
        })
        .AddPushCustomers(typeof(Program).Assembly);
});

public sealed record Order(long Id, decimal Total);
```

Each `(message type, topic name)` pair identifies one typed queue. A topic can
have multiple partitions. Producer calls use round-robin routing by default.

`UsePartitionKey` requires a selector delegate. Numeric selectors support the
built-in `INumber<TNumber>` types when their result is a finite integer; they
route with the normalized mathematical modulo of `(key - 1)` and
`PartitionNumber`, so zero and negative keys are accepted. String selectors use
only the first four UTF-16 characters to choose a partition. Equal keys are
routed to the same partition and retain their per-partition order; different
keys can share a partition. Omit the call to retain round-robin routing. The selector must be deterministic
and safe for concurrent calls. In Memory mode, concurrent producers can append to different key-selected
partitions in parallel; appends to the same partition remain serialized.

Batch production applies the same routing to every item. A round-robin batch is
not assigned to one partition: selection advances once per item. A key-routed
batch runs its selector for every item and preserves equal-key input order in
the selected partition. Ordering remains per partition rather than global
across the batch.

In Memory storage, a default round-robin batch reserves its complete selection
range before appending the input slices for each selected partition. This keeps
per-item routing unchanged while allowing those slices to append under their
own partition locks.

## Produce

A fixed topic can be injected as a keyed `IBufferProducer<T>`:

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

For a topic selected at runtime, inject `IBufferQueue` and call
`GetProducer<T>(topicName)`.

`IBufferProducer<T>` directly exposes four core methods, each with an optional
`CancellationToken`:

~~~csharp
ValueTask<bool> TryProduceAsync(T item, CancellationToken cancellationToken = default);
ValueTask<bool> TryProduceAsync(ReadOnlyMemory<T> items, CancellationToken cancellationToken = default);
ValueTask ProduceAsync(T item, CancellationToken cancellationToken = default);
ValueTask ProduceAsync(ReadOnlyMemory<T> items, CancellationToken cancellationToken = default);
~~~

`BufferProducerExtensions` provides only the `IEnumerable<T>` convenience
overloads, which also accept a cancellation token:

~~~csharp
CancellationToken cancellationToken = default;
ReadOnlyMemory<Order> bufferedOrders = pendingOrders.AsMemory();

await producer.ProduceAsync(bufferedOrders, cancellationToken);
var accepted = await producer.TryProduceAsync(bufferedOrders, cancellationToken);

IEnumerable<Order> ordersFromAnEnumerable = GetPendingOrders();
await producer.ProduceAsync(ordersFromAnEnumerable, cancellationToken);
~~~

Use `ReadOnlyMemory<T>` when the source is already contiguous or can be exposed
as memory. It is the allocation-conscious core form because it avoids the input
materialization required by a non-array `IEnumerable<T>`. The `IEnumerable<T>` form is
convenient but materializes a non-array input before batch submission. The single-item and
`ReadOnlyMemory<T>` `ProduceAsync` methods are core interface methods, not extensions.

On a bounded Memory topic, `FullMode` defaults to `Wait`: both `ProduceAsync` and
`TryProduceAsync` asynchronously wait until capacity is available, and `TryProduceAsync`
returns `true` after the complete input is accepted. Supply a cancellation token so that the
backpressure wait can be canceled. Set `FullMode` to `Fail` when immediate rejection is required;
`ProduceAsync` then throws `BufferQueueFullException`, while `TryProduceAsync` returns `false`.
A batch no larger than the configured capacity is admitted as a whole. A larger `Wait` batch is
split into consecutive capacity-sized slices; each slice is admitted as a whole, and cancellation
can leave prior completed slices visible. `Fail` rejects an oversized batch as a whole. The capacity
limit is shared by every partition. Each partition returns capacity as the minimum committed position
across all known consumer groups advances, including within a segment, so a slow group can hold
capacity after another group commits. A group created later starts at the current logical earliest
position and cannot read records whose capacity has already been released.

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
- [Design notes](https://github.com/eventhorizon-cli/BufferQueue/blob/main/docs/README.md)
- [ASP.NET Core sample](https://github.com/eventhorizon-cli/BufferQueue/tree/main/samples/WebAPI)
- [Issues](https://github.com/eventhorizon-cli/BufferQueue/issues)
