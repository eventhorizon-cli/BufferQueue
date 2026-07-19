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
    queue.UseMemoryMappedFile(storage =>
    {
        storage.AddTopic<OrderEvent>(options =>
        {
            options.TopicName = "order-events";
            options.DataDirectory = "/var/lib/bufferqueue";
            options.PartitionNumber = 4;
            options.SegmentSizeInBytes = 64L * 1024 * 1024;
            options.MaxRetainedConsumedSegments = 2;
        });
    });
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

## MessagePack

Reference MessagePack directly from the application so its analyzer and source
generator run in the application project:

```shell
dotnet add package MessagePack
```

Define stable numeric keys and select the built-in serializer:

```csharp
using BufferQueue.MemoryMappedFile;
using MessagePack;

[MessagePackObject]
public sealed class OrderEvent
{
    [Key(0)]
    public long Id { get; set; }

    [Key(1)]
    public decimal Total { get; set; }
}

// Inside AddTopic<OrderEvent>(options => ...):
options.Serializer = new MessagePackMemoryMappedFileSerializer<OrderEvent>();
```

Numeric keys, resolvers, compression, security options, and custom formatters
are part of the persisted schema. Do not reuse removed keys. Configured
resolvers and formatters must be thread-safe.

## Unmanaged structs

For fixed-layout unmanaged values, the built-in unmanaged serializer copies the
native in-memory representation:

```csharp
using System.Runtime.InteropServices;
using BufferQueue.MemoryMappedFile;

[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct Quote
{
    public long Sequence;
    public long Timestamp;
    public double Price;
    public int Quantity;
}

// Inside AddTopic<Quote>(options => ...):
options.Serializer = UnmanagedMemoryMappedFileSerializer<Quote>.Instance;
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
