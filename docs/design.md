# BufferQueue Design

## Purpose

BufferQueue is a typed, topic-based buffering library for .NET. It provides a common queue model with pluggable storage implementations. The current codebase contains two storage modes:

- Memory mode: stores items in in-process segmented memory.
- MemoryMappedFile mode: stores serialized records in memory-mapped segment files and persists writer and committed consumer offsets.

Both modes share the same producer, pull-consumer, consumer-group, partition-assignment, batching, and wake-up semantics. The storage-specific behavior is isolated behind the internal partition abstraction.

The implementations are separated at the project and package boundary:

- `BufferQueue` contains the shared queue abstractions, Memory storage, and push-consumer integration.
- `BufferQueue.MemoryMappedFile` contains the optional MemoryMappedFile storage implementation and depends on `BufferQueue`.

The core `BufferQueue` project does not reference `BufferQueue.MemoryMappedFile`. The MMF project reuses the shared internal queue abstractions through friend-assembly access. This split does not change the public namespaces or the `.UseMemoryMappedFile(...)` registration call.

## Public Model

The public entry point is `IBufferQueue`. Applications use it to obtain producers and create consumers by topic name.

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

The public API is intentionally small:

- `IBufferProducer<T>` produces typed items to a topic.
- `IBufferPullConsumer<T>` consumes batches from a topic.
- `IBufferConsumerCommitter` commits manually consumed batches.
- `BufferPullConsumerOptions` configures topic, group, auto-commit, and batch size.
- `BufferOptionsBuilder` wires storage implementations into dependency injection.

Internally, each registered topic is represented as `IBufferQueue<T>`. The non-generic `BufferQueue` resolves the typed topic queue from the DI container by topic name.

## High-Level Architecture

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
```

`BufferQueue<TItem>` is the shared abstract queue base for a single typed topic. It owns the common upper-level queue behavior:

- validates consumer options;
- prevents duplicate consumer groups in one queue instance;
- creates `BufferPullConsumer<TItem>` instances;
- distributes partitions across consumers in the same group;
- exposes the topic producer.

Concrete queue implementations only create their storage-specific partitions and producers:

- `MemoryBufferQueue<T>` creates `MemoryBufferPartition<T>[]` and `MemoryBufferProducer<T>`.
- `MemoryMappedFileBufferQueue<T>` creates `MemoryMappedFileBufferPartition<T>[]` and `MemoryMappedFileBufferProducer<T>`.

## Internal Partition Abstraction

Storage implementations are connected through `IBufferPartition<TItem>`.

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

The queue and consumer logic only depend on this abstraction. This keeps common behavior in one implementation while allowing partitions to use completely different storage strategies.

`IBufferPartitionConsumer<TItem>` is the minimal notification contract used by partitions to wake consumers when new data is available.

## Partitioning and Consumer Groups

Each topic has one or more partitions. Producers distribute items across partitions using round-robin selection.

Consumers are created per consumer group. A group may contain multiple consumers. Partitions are assigned evenly across the consumers in that group:

- consumer count must be greater than zero;
- consumer count cannot exceed partition count;
- each group has an independent read position;
- each group receives all messages for the topic, but partitions are load-balanced within that group.

Example with 5 partitions and 2 consumers:

```text
consumer-0: partition-0, partition-1, partition-2
consumer-1: partition-3, partition-4
```

Different groups are independent. If two groups consume the same topic, each group maintains its own progress.

## Pull Consumer Design

`BufferPullConsumer<TItem>` is the single common pull-consumer implementation. It is not storage-specific.

It is responsible for:

- keeping the assigned partition list;
- selecting partitions in round-robin order;
- pulling batches from partitions;
- trying other assigned partitions when the selected partition has no data;
- waiting asynchronously when no assigned partition has data;
- committing manually consumed batches;
- auto-committing when `AutoCommit` is enabled.

Consumption returns an async stream of batches:

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

When `AutoCommit` is false, the consumer read position does not advance until `CommitAsync` is called. This provides at-least-once semantics inside the current process. In memory-mapped-file mode, it also applies across process restarts for records that have reached a flush boundary; `CommitAsync` itself forces that boundary.

## Consumer Wake-Up Design

Consumers should not spin when no data is available. The common consumer uses `PendingDataValueTaskSource<T>` to wait for new data.

The flow is:

1. Consumer attempts to pull from the selected partition.
2. If no data is found, it attempts all other assigned partitions.
3. If no assigned partition has data, it resets a pending-data value task source.
4. A producer appends data to a partition.
5. The partition notifies registered consumers through `IBufferPartitionConsumer<TItem>`.
6. The consumer increments its pending-data version and completes the pending value task.
7. The consumer resumes and attempts to pull from the partition that produced the notification.

The pending-data version prevents a lost wake-up when data arrives between the last pull attempt and the transition into the waiting state.

## Memory Mode

Memory mode is optimized for in-process buffering and batch consumption.

### Storage Layout

`MemoryBufferPartition<T>` stores data in a linked list of `MemoryBufferSegment<T>` instances. Each segment owns a fixed-size item array.

```text
head segment -> segment -> ... -> tail segment
```

Each record offset is represented by `MemoryBufferPartitionOffset`. Offsets are logical item positions, not byte positions.

### Append

`MemoryBufferProducer<T>` selects a partition in round-robin order and calls `MemoryBufferPartition<T>.Enqueue`.

The partition attempts to append to the current tail segment. If the tail segment is full, a new segment is created or an old fully consumed segment is recycled.

After enqueue succeeds, the partition notifies all registered consumers.

### Read and Commit

Each consumer group has a partition reader. The reader keeps:

- current segment;
- current read position;
- last read count.

`TryPull` reads up to `BatchSize` items. The reader's committed read position is moved only by `Commit`.

### Segment Recycling

Memory mode can recycle old segments. A segment can be recycled only after all consumer groups have consumed past the segment end. This prevents a slow group from losing data that it has not consumed yet.

### Capacity

Memory mode supports optional bounded capacity through `MemoryBufferQueueOptions.BoundedCapacity`.

When bounded capacity is configured:

- `ProduceAsync` throws `MemoryBufferQueueFullException` if the queue is full;
- `TryProduceAsync` returns `false` if the queue is full.

## MemoryMappedFile Mode

MemoryMappedFile mode persists produced data in memory-mapped segment files. It also persists the writer offset and committed consumer offsets. It is designed for local durable buffering with simple recovery. The configured flush strategy determines when newly appended records reach an explicit durability boundary.

### Directory Layout

For each topic and partition, data is stored under:

```text
{DataDirectory}/{TopicName}/partition-{PartitionId:D5}/
```

Data segment files are named by segment index:

```text
00000000000000000000.log
00000000000000000001.log
...
```

Consumer offsets are stored under:

```text
{DataDirectory}/{TopicName}/partition-{PartitionId:D5}/offsets/
```

Each consumer group has one readable directory under `offsets`. The directory name is `{escaped-group-name}`. The group name is used directly when it is a valid folder name. Only characters that are not safe in a single path component, such as `/`, are percent-encoded. The percent sign itself is also encoded to avoid collisions with escaped names.

```text
{DataDirectory}/{TopicName}/partition-{PartitionId:D5}/offsets/{escaped-group-name}/offset
```

For example, topic `orders`, partition `0`, and group `billing-worker-1` use:

```text
bufferqueue/orders/partition-00000/offsets/billing-worker-1/offset
```

If the group name is `orders/worker 1`, the slash is encoded and the space remains visible:

```text
bufferqueue/orders/partition-00000/offsets/orders%2Fworker 1/offset
```

The partition writer offset is stored in:

```text
{DataDirectory}/{TopicName}/partition-{PartitionId:D5}/writer.offset
```

For example:

```text
bufferqueue/orders/partition-00000/writer.offset
```

### Record Format

Each data record is stored as:

```text
4 bytes  payload length, little-endian int32
N bytes  payload
1 byte   record end marker
```

The record end marker is used to detect incomplete or corrupted records during reads and recovery.

If the remaining space in the current segment cannot hold the next record, the partition writes a segment-end marker when at least four bytes remain, then continues in the next segment. With fewer than four bytes remaining, the unused tail itself is treated as segment padding. The segment-end marker is represented by an int32 length value of `-1`.

### Serialization

`MemoryMappedFileBufferQueueOptions<T>` exposes one pluggable serialization property:

- `Serializer: IMemoryMappedFileSerializer<T>`

`IMemoryMappedFileSerializer<T>` keeps both operations in one contract: `Serialize(T)` returns `byte[]`, and `Deserialize(ReadOnlyMemory<byte>)` returns `T`.

The available implementations are:

- an internal `System.Text.Json` implementation used by default;
- `MessagePackMemoryMappedFileSerializer<T>`, backed by MessagePack for C#. Its parameterless constructor uses `MessagePackSerializerOptions.Standard`, and another constructor accepts explicit MessagePack options. Custom resolvers and formatters must be safe for concurrent use;
- `UnmanagedMemoryMappedFileSerializer<T>`, which requires `T : unmanaged` and copies the value's native in-memory representation. Deserialization requires the payload length to exactly match `Unsafe.SizeOf<T>()`.

With standard MessagePack options, custom types should use `[MessagePackObject]` and stable numeric `[Key]` values. The application project should reference MessagePack directly because the `BufferQueue.MemoryMappedFile` package's transitive runtime dependency on MessagePack does not provide its analyzer and source generator. Contractless serialization can be enabled through custom options, but it puts member names in the persisted format and is not the preferred MMF schema. Resolver, key, compression, and security choices are part of the persisted format; removed numeric keys must not be reused.

The unmanaged serializer removes format encoding and decoding but is not zero-copy because the serializer contract and partition currently materialize payload byte arrays. Its native endianness, padding, field order, packing, runtime, and process architecture are part of the wire format. `[StructLayout]` is optional, but explicitly fixing a sequential or explicit layout and packing is recommended. Pointer-sized and process-specific fields should not be persisted.

The serializer and its wire schema are part of the persisted topic format. They must remain compatible with existing records across queue restarts and application upgrades.

The configured serializer instance is shared by all topic partitions and can be invoked concurrently. Implementations must be thread-safe, and neither operation may return `null`.

### Flush Strategies

`MemoryMappedFileBufferQueueOptions<T>` exposes two flush strategies:

- `MemoryMappedFileFlushStrategy.Immediate` is the default and explicitly flushes after every record;
- `MemoryMappedFileFlushStrategy.Batch` explicitly flushes after `FlushBatchSize` records have been appended to a partition. `FlushBatchSize` defaults to `100`.

A segment rollover and a consumer commit are unconditional flush boundaries in both strategies. Batch mode therefore may flush before `FlushBatchSize` is reached. If a partial tail batch receives no subsequent production and there is no segment rollover or consumer commit, it is not guaranteed to have been explicitly flushed.

### Append

`MemoryMappedFileBufferProducer<T>` selects a partition in round-robin order. The partition serializes the item, calculates the record size, finds the active segment, writes the record, advances the in-process write offset, applies the configured flush strategy, and notifies consumers.

At every flush boundary, the partition flushes the memory-mapped accessor first and writes the corresponding offset to `writer.offset` only after that flush succeeds. A segment rollover flushes the completed segment before writing to the next segment. A consumer commit also flushes pending log data and advances `writer.offset` before its consumer offset is persisted.

If a serialized item is larger than the segment size, production fails with `InvalidOperationException`.

### Recovery

When a memory-mapped-file partition starts, it first attempts to read `writer.offset`. If the file is missing, startup scans from offset `0`. If the stored writer offset is valid and points to a real record boundary, startup scans forward from that position to find the last valid write offset.

The scan stops when it finds:

- an empty length;
- a non-positive length other than the segment-end marker;
- a record that would cross the segment boundary;
- a missing record end marker.

This keeps normal startup fast while still tolerating expected crash windows:

- data was flushed, but `writer.offset` was not updated yet;
- the operating system persisted complete records from a pending batch before its explicit flush;
- trailing data was only partially written.

In these cases, the startup scan finds the last valid record boundary. Because `writer.offset` is advanced only after the corresponding log flush succeeds, recovery can use it as a safe checkpoint and scan forward for additional complete records. In `Batch` mode, records in a partial tail batch that was not explicitly flushed may be absent after an abnormal termination. Clearly inconsistent checkpoint state is still treated as corruption and fails fast. If `writer.offset` is invalid, ahead of the actual log, or not aligned to a record boundary, startup throws an exception instead of silently falling back.

### Offset Persistence

MemoryMappedFile mode persists committed offsets per partition and consumer group.

On `Commit`, the partition first forces pending log data to be flushed and advances `writer.offset`. It then writes the committed offset as an 8-byte little-endian integer to the group's `offset` checkpoint file. This ordering prevents a persisted consumer offset from advancing beyond successfully flushed log data. The checkpoint write uses a temporary file followed by replace or move, so readers do not observe a partially written offset file.

On reader creation:

- if the offset file exists and contains a valid offset, reading starts from that offset;
- if the offset file is missing, reading starts from `0`;
- if the offset file has an invalid length or contains a negative offset, reading fails with `InvalidDataException`;
- if the stored offset is beyond the current write offset, the reader throws an exception instead of silently resetting progress.

Only committed offsets are persisted. If a consumer reads a batch but does not commit, the next queue instance reads that batch again.

### Writer Offset Persistence

At a flush boundary, the partition flushes the memory-mapped file accessor and then writes the latest successfully flushed writer offset to `writer.offset` as an 8-byte little-endian integer. `Immediate` creates this boundary for every record. `Batch` creates it when `FlushBatchSize` records have accumulated, and segment rollover or consumer commit creates it regardless of the pending count.

The writer offset write uses the same temporary-file plus replace or move pattern as consumer offsets. This prevents a valid offset file from being replaced by a partially written file.

The writer offset is an optimization and recovery hint, not the only source of truth. In `Batch` mode it may lag the current in-process write offset while a partial batch is pending. On startup, the partition still validates records from the stored writer offset forward. If the persisted writer offset is behind complete records in the data files, the scan catches up. If the writer offset file is missing, the partition scans from the beginning. If the writer offset file is present but inconsistent with the log, recovery fails fast.

## Push Consumer Mode

Push consumer mode is built on top of pull consumers. The host service discovers push consumers by attribute, creates the corresponding pull consumers, and invokes the push consumer implementation with batches.

Auto-commit push consumers commit automatically after successful processing. Manual-commit push consumers receive an `IBufferConsumerCommitter` and decide when to commit.

## Dependency Injection

The library registers a single public `IBufferQueue` service. Each topic is registered as a keyed `IBufferQueue<T>` service.

Memory mode:

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

MemoryMappedFile mode:

The application must reference the `BufferQueue.MemoryMappedFile` project or package. Its dependency on the core `BufferQueue` package is transitive, and the public namespace and registration API remain unchanged.

MemoryMappedFile topic queues are created and owned by the dependency injection container. Disposing the service provider closes every partition view and memory-mapped-file handle. Disposal releases resources but is not an explicit flush boundary and does not advance `writer.offset` for a pending batch.

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

Different topics may use different storage modes as long as they are registered under distinct topic names.

## Concurrency Model

The implementation is designed for concurrent production and consumption within one process.

Important concurrency points:

- Producers choose partitions with atomic round-robin counters.
- Partition append paths use locks where the storage format requires serialized writes.
- Consumer group creation is guarded by a queue-level lock.
- Consumer wait and wake-up state is protected by `ReaderWriterLockSlim`.
- MemoryMappedFile writer and consumer offset writes use replace/move semantics to avoid partial offset files.

The current design does not provide cross-process coordination for multiple writers to the same memory-mapped-file topic directory. MemoryMappedFile mode should be treated as a local persistence mechanism for one active queue instance unless external coordination is added.

## Delivery Semantics

The queue provides at-least-once behavior with manual commit:

- A batch can be delivered again if it was read but not committed.
- Auto-commit advances progress immediately after a successful pull.
- Manual commit advances progress after user code calls `CommitAsync`.

Memory mode keeps offsets in process memory. MemoryMappedFile mode persists the writer offset and committed consumer offsets to disk. A consumer commit first forces pending log data to its flush boundary. In `Batch` mode, an uncommitted partial tail batch is not guaranteed to survive an abnormal termination.

## Extension Points

The main extension point is `IBufferPartition<TItem>`. A new storage implementation should provide:

- a partition type implementing `IBufferPartition<TItem>`;
- a producer that selects partitions and calls `Enqueue`;
- a queue type inheriting `BufferQueue<TItem>` and passing partitions plus producer to the base constructor;
- options and DI builder extensions.

The common queue and consumer behavior should not be duplicated in storage implementations.

## Known Limitations

- MemoryMappedFile mode currently persists consumer offsets but does not reclaim old data segment files.
- MemoryMappedFile mode does not implement bounded capacity.
- MemoryMappedFile mode does not coordinate multiple processes writing to the same topic directory.
- Memory mode does not persist data or offsets.
- Consumer groups are unique per queue instance; creating the same group twice in the same queue instance is rejected.

## Testing Strategy

Tests follow the production project boundary: `BufferQueue.Tests` covers the core and Memory implementation, while `BufferQueue.MemoryMappedFile.Tests` covers the optional MMF assembly.

The test suite covers:

- memory queue production and consumption;
- manual and automatic commit behavior;
- consumer wait and wake-up behavior;
- multi-partition and multi-consumer partition assignment;
- memory segment behavior and recycling;
- DI registration;
- memory-mapped-file production and consumption;
- memory-mapped-file offset persistence and uncommitted replay.

These tests ensure both storage modes share the same visible queue semantics while retaining storage-specific behavior behind the partition abstraction.
