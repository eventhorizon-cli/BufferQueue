# Memory Storage

[English](memory.md) | [Simplified Chinese](memory.zh-CN.md)

[Design index](../README.md)

## Storage Layout

Memory mode is optimized for in-process buffering and batch consumption.
`MemoryBufferPartition<T>` stores data in a linked list of
`MemoryBufferSegment<T>` instances. Each segment owns a fixed-size item array.

~~~text
head segment -> segment -> ... -> tail segment
~~~

Each record offset is represented by `MemoryBufferPartitionOffset`. These are logical
item positions, not byte positions.

Memory data and consumer offsets exist only for the lifetime of the process. Restarting the process
does not recover them.

## Append Path

`MemoryBufferProducer<T>` selects a partition in round-robin order unless
partition-key routing is enabled. The precise numeric and string routing rules are described in
[Partitioning and concurrency](partitioning-and-concurrency.md).

Batch production uses the same per-item routing. A round-robin batch advances selection for every
item rather than assigning the batch to one partition. A key-routed batch runs the selector for
every item and retains equal-key input order within that partition.

The partition appends to its tail segment. When the tail is full, it creates a new segment or
recycles an old segment that every consumer group has fully consumed.

For default round-robin routing, the Memory producer and its partitions share one append lock. The
lock serializes partition selection, bounded-capacity accounting, and append. Partition-key routing
selects a partition before taking that partition's append lock, so concurrent producers can append
to different partitions in parallel. Appends within one partition remain serialized.

After storing the item, the selected partition publishes its new readable cursor with a release
write. Consumers read the published range without taking an append lock, so they cannot observe a
slot before the item has been written. After enqueue succeeds, the partition notifies every
registered consumer.

## Read and Commit

Each consumer group has a reader for each partition. The reader keeps:

- its current segment;
- its current read position;
- the last read count.

`TryPull` returns up to `BatchSize` items. The reader's committed position
moves only when `Commit` is called. An uncommitted batch can therefore be delivered
again while the queue instance remains alive.

## Segment Recycling

Memory mode can recycle old segments. A segment is eligible only after every consumer group has
consumed past that segment's end. This protects a slow group from seeing data overwritten before it
has read it.

## Bounded Capacity

Memory mode supports optional bounded capacity through
`MemoryBufferQueueOptions.BoundedCapacity`.

When capacity is configured and the queue is full:

- `ProduceAsync` throws `BufferQueueFullException`.
- `TryProduceAsync` returns `false`.

For a batch, bounded-capacity admission reserves the complete item count before any item is
appended. If the remaining capacity is insufficient, `ProduceAsync` throws and
`TryProduceAsync` returns `false`; neither appends a partial batch. This applies to both batch
input forms after an `IEnumerable<T>` has been materialized.

MemoryMappedFile currently does not support bounded capacity. See
[MemoryMappedFile storage](memory-mapped-file.md) for durable-storage behavior.
