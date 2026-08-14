# Architecture and Registrations

[English](architecture.md) | [Simplified Chinese](architecture.zh-CN.md)

[Design index](../README.md)

## Purpose

BufferQueue is a typed, topic-based buffering library for .NET. It provides a common queue model
with pluggable storage implementations:

- Memory stores items in in-process segmented memory.
- MemoryMappedFile stores serialized records in memory-mapped segment files and persists producer
  and committed consumer offsets.

The packages multi-target .NET 8 and .NET 10. Benchmarks intentionally target .NET 10 only.

Both modes share producer, pull-consumer, consumer-group, partition-assignment, batching, and
wake-up semantics. Storage-specific behavior is isolated behind the internal partition abstraction.

The implementation is also separated at the project and package boundary:

- `BufferQueue` contains shared queue abstractions, Memory storage, and push-consumer
  integration.
- `BufferQueue.MemoryMappedFile` contains the optional MemoryMappedFile storage
  implementation and depends on `BufferQueue`.

The core project does not reference the MMF project. The MMF project reuses shared internal queue
abstractions through friend-assembly access. This separation does not change the public namespaces
or the `.UseMemoryMappedFile(...)` registration call.

## Public Model

Choose producer access based on when the topic is known:

- For a dependency with a fixed topic, inject `IBufferProducer<T>` with
  `[FromKeyedServices("topic-name")]`.
- For a topic selected at runtime, inject `IBufferQueue` and call
  `GetProducer<T>(topicName)`.

~~~csharp
public sealed class FooPublisher(
    [FromKeyedServices("topic-foo")] IBufferProducer<Foo> producer)
{
    public ValueTask PublishAsync(Foo item, CancellationToken cancellationToken = default) =>
        producer.ProduceAsync(item, cancellationToken);
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

The public API deliberately remains small:

- `IBufferProducer<T>` produces typed items to a topic.
- `IBufferPullConsumer<T>` consumes batches from a topic.
- `IBufferConsumerCommitter` commits manually consumed batches.
- `BufferPullConsumerOptions` configures the topic, group, auto-commit, and batch size.
- `BufferOptionsBuilder` wires storage implementations into dependency injection.

Each registered topic is represented internally as `IBufferQueue<T>`. Its keyed
`IBufferProducer<T>` registration forwards to the producer owned by that queue. The
non-generic `BufferQueue` resolves the typed topic queue from the DI container by topic
name.

### Batch Production

`IBufferProducer<T>` owns the four core write methods. Each accepts an optional
`CancellationToken`:

- `ValueTask<bool> TryProduceAsync(T item, CancellationToken cancellationToken = default)`
- `ValueTask<bool> TryProduceAsync(ReadOnlyMemory<T> items, CancellationToken cancellationToken = default)`
- `ValueTask ProduceAsync(T item, CancellationToken cancellationToken = default)`
- `ValueTask ProduceAsync(ReadOnlyMemory<T> items, CancellationToken cancellationToken = default)`

`BufferProducerExtensions` keeps only the `IEnumerable<T>` convenience overloads:

- `ValueTask ProduceAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)`
- `ValueTask<bool> TryProduceAsync(IEnumerable<T> items, CancellationToken cancellationToken = default)`

~~~csharp
CancellationToken cancellationToken = default;
ReadOnlyMemory<Foo> bufferedItems = pendingItems.AsMemory();

await producer.ProduceAsync(bufferedItems, cancellationToken);
var accepted = await producer.TryProduceAsync(bufferedItems, cancellationToken);

IEnumerable<Foo> itemsFromAnEnumerable = GetPendingItems();
await producer.ProduceAsync(itemsFromAnEnumerable, cancellationToken);
~~~

Use `ReadOnlyMemory<T>` when the source is already contiguous or can be supplied as memory. It
is the allocation-conscious core attempt because it avoids the input materialization required by a
non-array `IEnumerable<T>` form. The `IEnumerable<T>` overload is a convenience API and materializes
a non-array input before dispatching the batch. The core `ProduceAsync` methods are not extensions;
they define the normal write behavior for each storage mode.

Cancellation is checked before a write starts and while a Memory `Wait` write is waiting for
capacity. Once a batch has been admitted and append begins, the token is not polled between items;
cancellation therefore cannot stop the batch halfway through.

Batch production is a core producer capability. Storage-specific batch admission, routing, and
persistence behavior is described in the related design notes.

## Shared Queue Boundary

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
IBufferQueue<T> keyed by topic name
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

`BufferQueue<TItem>` is the shared abstract queue base for one typed topic. It
validates consumer options, prevents duplicate consumer groups in one queue instance, creates
`BufferPullConsumer<TItem>` instances, distributes partitions across consumers in a
group, and exposes the topic producer.

Concrete queues create only storage-specific partitions and producers:

- `MemoryBufferQueue<T>` creates
  `MemoryBufferPartition<T>[]` and `MemoryBufferProducer<T>`.
- `MemoryMappedFileBufferQueue<T>` creates
  `MemoryMappedFileBufferPartition<T>[]` and
  `MemoryMappedFileBufferProducer<T>`.

Both producers use the shared `IPartitioner<TItem>`. The selected implementation is
registered as a keyed topic service. Shared partitioners implement round-robin and partition-key
routing, which both storage modes expose through topic configuration.

Storage implementations connect to the common queue and consumer logic through
`IBufferPartition<TItem>`:

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

The upper layers only depend on this abstraction, allowing each partition implementation to use a
different storage strategy. `IBufferPartitionConsumer<TItem>` is the minimal
notification contract partitions use to wake consumers after data becomes available.

## Dependency Injection

The library registers one public `IBufferQueue` service. Each topic is registered under
its topic name as keyed `IBufferQueue<T>` and `IBufferProducer<T>`
services.

Memory registration:

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

MemoryMappedFile registration requires the `BufferQueue.MemoryMappedFile` project or
package. Its core-package dependency is transitive, and its public namespace and registration API
remain unchanged.

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

MMF topic queues are created and owned by the DI container. Disposing the service provider closes
all partition views and memory-mapped-file handles. Disposal releases resources; it is not an
explicit flush boundary and does not advance `producer.offset` for a pending batch.

Different topics may use different storage modes as long as their topic names are distinct.

## Extension Points

The primary extension point is `IBufferPartition<TItem>`. A new storage
implementation normally provides:

- a partition type implementing `IBufferPartition<TItem>`;
- a producer that selects a partition and calls `Enqueue`;
- a queue type inheriting `BufferQueue<TItem>` and passing partitions plus producer
  to the base constructor;
- options and DI-builder extensions.

Storage implementations should not duplicate the common queue and consumer behavior.

## Testing Strategy

Tests follow the production project boundary:

- `BufferQueue.Tests` covers the core and Memory implementation.
- `BufferQueue.MemoryMappedFile.Tests` covers the optional MMF assembly.

The suite covers Memory production and consumption, manual and auto commit, consumer wait and
wake-up, partition assignment, segment recycling, DI registration, MMF production and consumption,
offset persistence, recovery, and uncommitted replay. This keeps visible queue semantics shared
while storage behavior remains behind the partition abstraction.
