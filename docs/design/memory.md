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

The internal `MemoryBufferPartitionOffset` contains one `ulong Index`. Offset arithmetic is
unchecked, so incrementing the maximum value wraps to zero and subtraction wraps in the same
64-bit domain.
Ordering uses the signed difference of two modular positions; this is unambiguous while the
live window being compared is strictly smaller than `2^63` positions. The live window includes
retained segment data and all reader positions. A Memory queue cannot retain `2^63` items, so
normal reads, commits, replay of uncommitted batches, bounded-capacity release, and segment
recycling continue across the wrap boundary.

The former `Generation` field tracked how many times `Index` had wrapped. Memory storage only
needs to order positions within the currently retained window of one partition; it does not
compare them with arbitrary positions from discarded history. A slow group keeps its unread
segments resident, so its lag is part of that same memory-limited window. Modular ordering
therefore makes `Generation` unnecessary, even when `Index` wraps. Removing it reduces each
offset from 16 to 8 bytes and removes generation comparisons and carry/borrow handling. This
decision relies on the live-window invariant, not on assuming that a 64-bit counter never wraps.

This is an internal Memory offset representation. It does not change public APIs or the
MemoryMappedFile record and offset format.

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

For default round-robin routing, a short topic-level coordinator serializes partition selection and
batch-range reservation. After its complete bounded-capacity admission succeeds, a batch reserves
one contiguous selection range, visits each partition's strided slice, and appends that slice under
the partition's own append lock. This avoids materializing a `List<T>` for every partition. A
single round-robin write remains on the coordinator-only path when no round-robin batch is
appending; while a batch is appending, it also takes its target partition lock. Consumer state
changes use the same coordinator, so they cannot overlap an unsynchronized append. Partition-key
routing selects a partition before taking that partition's append lock. Appends within one
partition remain serialized.

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

Memory mode supports optional topic-wide bounded capacity through
`MemoryBufferQueueOptions.BoundedCapacity`. The limit is shared by every partition in the topic.

`MemoryBufferQueueOptions.FullMode` controls both producer methods when capacity is
unavailable. Its default is `BufferQueueFullMode.Wait`:

- `Wait` makes both `ProduceAsync` and `TryProduceAsync` asynchronously wait for
  capacity. `TryProduceAsync` returns `true` after the complete input is accepted.
  Either call can be canceled through its `CancellationToken`.
- `Fail` makes `ProduceAsync` throw `BufferQueueFullException` immediately and
  makes `TryProduceAsync` return `false`.

For a batch no larger than the configured capacity, admission reserves the complete
item count before any item is appended. In `Fail` mode, insufficient capacity therefore
rejects the complete batch without appending a partial batch. In `Wait` mode, both
methods wait for the complete batch capacity and then append it as one admission.

In `Wait` mode, a batch larger than the configured capacity is split into consecutive
input slices of at most `BoundedCapacity` items. Each slice waits for and reserves its
complete capacity before it appends, then the next slice begins. This rule applies to
both `ProduceAsync` and `TryProduceAsync`; `TryProduceAsync` does not return `false`
because a `Wait` queue is full. Cancellation can interrupt the operation only between
slices, so previously completed slices remain visible while no admitted slice is only
partially appended. In `Fail` mode, an oversized batch is rejected as a whole. These
rules apply to both batch input forms after an `IEnumerable<T>` has been materialized.

Each partition returns capacity to the topic-wide gate only when the minimum committed position
across all known consumer groups advances. The release can cover only part of a segment; a group
that has not committed past the same position continues to hold that capacity. A topic with no
consumer group cannot release capacity.

A consumer group registered after capacity has already been released starts at the current logical
earliest position. Records before that position are no longer available to the new group, even if a
physical segment has not yet been recycled.

MemoryMappedFile currently does not support bounded capacity. See
[MemoryMappedFile storage](memory-mapped-file.md) for durable-storage behavior.
