# BufferQueue.MemoryMappedFile

BufferQueue.MemoryMappedFile is the optional local durable storage provider for
BufferQueue. It stores serialized records in per-partition memory-mapped segment
files and persists producer checkpoints and committed consumer offsets.

It supports .NET 8 and later. It is designed for one active queue instance and
does not coordinate multiple processes writing to the same topic directory.

## Install

```shell
dotnet add package BufferQueue.MemoryMappedFile
```

The package depends on the core `BufferQueue` package.

## Register a topic

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
                options.SegmentSizeInBytes = 64L * 1024 * 1024;
                options.MaxRetainedConsumedSegments = 2;
            });
        })
        .AddPushCustomers(typeof(Program).Assembly);
});

public sealed record OrderEvent(long Id, decimal Total);
```

System.Text.Json is used by default. The other defaults are:

| Option | Default | Meaning |
| --- | ---: | --- |
| `PartitionNumber` | `1` | Partitions in the topic |
| `SegmentSizeInBytes` | `256 MiB` | Size of each mapped segment file |
| `FlushStrategy` | `Immediate` | Explicitly flush every appended record |
| `FlushBatchSize` | `100` | Records per partition before a Batch flush |
| `MaxRetainedConsumedSegments` | `null` | Disable automatic deletion |
| `DataDirectory` | `Path.Combine(AppContext.BaseDirectory, "bufferqueue")` | Topic storage root |

Use a stable, writable `DataDirectory`. The on-disk path does not contain the
CLR type name, so keep `TopicName` unique across message types within the same
directory. Register each `(message type, topic name)` pair in one storage mode
only.

Production and consumption use the same `IBufferProducer<T>`, `IBufferQueue`,
pull consumer, push consumer, and commit APIs as Memory storage.

## Produce

Inject a keyed producer when the topic is fixed at the dependency boundary:

```csharp
using BufferQueue;
using Microsoft.Extensions.DependencyInjection;

public sealed class OrderEventWriter(
    [FromKeyedServices("order-events")] IBufferProducer<OrderEvent> producer)
{
    public ValueTask WriteAsync(OrderEvent orderEvent) =>
        producer.ProduceAsync(orderEvent);
}
```

For a topic selected at runtime, inject `IBufferQueue` and call
`GetProducer<T>(topicName)`.

## Pull consumers

Register a hosted worker and create a purpose-named consumer group. This example
commits only after the batch has been processed successfully:

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

Use `CreatePullConsumers<T>(options, consumerNumber)` to divide the group's
partitions between multiple consumers. The consumer count cannot exceed the
topic's partition count.

## Push consumers

`AddPushCustomers` in the registration example scans the specified assembly and
starts the discovered Push Consumers as hosted services.

An auto-commit Push Consumer is suitable when progress may advance before the
application finishes processing the batch:

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

Use a manual-commit Push Consumer when processing must finish before the
persisted consumer offset advances:

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

The `concurrency` value creates that many consumers in the group and cannot
exceed the topic's partition count. Each GroupName has an independent persisted
offset. A manual commit first forces pending log data to a flush boundary and
then persists that group's progress.

A Singleton Push Consumer is reused across batches and concurrent consumer
loops, so it must be thread-safe. Scoped and Transient Push Consumers are
resolved in a new asynchronous DI scope for every batch and are disposed after
the handler completes or throws.

## MessagePack

Reference MessagePack directly from the application so its analyzer and source
generator run in the application project:

```shell
dotnet add package MessagePack
```

Define stable numeric keys and select the built-in serializer. This standalone
example registers a MessagePack-backed topic with Batch flushing and automatic
segment cleanup:

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

Numeric keys, resolvers, compression, security options, and custom formatters
are part of the persisted schema. Do not reuse removed keys. Configured
resolvers and formatters must be thread-safe.

## Unmanaged structs

For fixed-layout unmanaged values, the built-in unmanaged serializer copies the
native in-memory representation. This complete example registers a Quote topic:

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

Field order, packing, native endianness, runtime, and process architecture are
part of this format. Avoid pointer-sized fields and do not change the layout
while old records remain. The queue still materializes a payload `byte[]`; this
is not a zero-copy reader.

Custom `IMemoryMappedFileSerializer<T>` implementations are also supported.
Serializer instances are shared across partitions and must be thread-safe.

## Flush and commit boundaries

Use Batch flushing when fewer explicit flushes are worth the partial-tail risk:

```csharp
options.FlushStrategy = MemoryMappedFileFlushStrategy.Batch;
options.FlushBatchSize = 100;
```

- `Immediate` explicitly flushes every appended record.
- `Batch` flushes after `FlushBatchSize` records have been appended to a partition.
- Moving to the next segment and committing a consumer always force pending log data to a flush boundary.
- A partial Batch tail is not guaranteed to survive abnormal termination.
- Disposing the service provider closes mapped resources but does not create a pending flush boundary.

A consumer commit flushes pending log data and advances `producer.offset` before
persisting the group's `consumer.offset`. Manual commit therefore provides
at-least-once delivery and may replay an uncommitted batch.

## Recovery and segment cleanup

On startup, BufferQueue treats `producer.offset` as a safe checkpoint and scans
forward over complete records. If `producer.offset` is missing, scanning starts
at `earliest.offset`. Malformed, ahead-of-log, or non-record-aligned checkpoints,
an existing consumer group directory without `consumer.offset`, and missing
segments inside the retained range fail fast instead of silently resetting
progress.

A segment can be deleted only after every known consumer group has committed
past it. Slow, uncommitted, offline, and obsolete groups therefore block
cleanup. `MaxRetainedConsumedSegments` is not a hard disk-usage limit. Remove an
obsolete group only while the queue is stopped, deleting its complete group
directory from every partition.

## Important limits

- MemoryMappedFile does not provide multi-process writer coordination.
- MemoryMappedFile does not currently support bounded capacity.
- Do not reduce `PartitionNumber` for an existing topic.
- A serialized record must fit within one segment.
- Multiple partitions preserve partition order, not global FIFO order.
- Persisted serializer schemas must remain compatible with existing records.

## Links

- [Core BufferQueue package](https://www.nuget.org/packages/BufferQueue/)
- [Repository and documentation](https://github.com/eventhorizon-cli/BufferQueue)
- [Chinese documentation](https://github.com/eventhorizon-cli/BufferQueue/blob/main/README.zh-CN.md)
- [MemoryMappedFile design](https://github.com/eventhorizon-cli/BufferQueue/blob/main/docs/design.md#memorymappedfile-mode)
- [Issues](https://github.com/eventhorizon-cli/BufferQueue/issues)
