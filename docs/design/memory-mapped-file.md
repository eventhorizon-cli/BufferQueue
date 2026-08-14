# MemoryMappedFile Storage

[English](memory-mapped-file.md) | [Simplified Chinese](memory-mapped-file.zh-CN.md)

[Design index](../README.md)

## Scope and Options

MemoryMappedFile mode persists produced data in memory-mapped segment files. It also persists the
producer offset and committed consumer offsets. It is local durable buffering for one active queue
instance with simple recovery; the configured flush strategy determines when appended records reach
an explicit durability boundary.

`MemoryMappedFileBufferQueueOptions<T>.SegmentSizeInBytes` configures the segment
size and defaults to `256L * 1024 * 1024` (256 MiB).

`MaxRetainedConsumedSegments` controls per-partition deletion of fully consumed segments:

- `null`, the default, disables deletion.
- `0` retains no reclaimable consumed segments.
- A positive value retains that many of the newest reclaimable segments.

MMF does not support bounded capacity or multi-process writer coordination. It preserves ordering
within a partition, not across all partitions. The selected partition-key routing behavior and
`PartitionNumber` must stay compatible across restarts; see
[Partitioning and concurrency](partitioning-and-concurrency.md).

MMF implements the same four `IBufferProducer<T>` core methods as Memory storage, and every
method accepts an optional `CancellationToken`. It is unbounded, so neither `TryProduceAsync`
nor `ProduceAsync` waits for capacity and there is no `BufferQueueFullMode` option. The
`IEnumerable<T>` batch convenience overloads remain extensions and materialize non-array
inputs before calling the core `ReadOnlyMemory<T>` methods.

## Directory Layout

Each topic partition is stored under:

~~~text
{DataDirectory}/{TopicName}/partition-{PartitionId:D5}/
~~~

Data segments are named by segment index:

~~~text
00000000000000000000.log
00000000000000000001.log
...
~~~

Consumer offsets are stored below:

~~~text
{DataDirectory}/{TopicName}/partition-{PartitionId:D5}/offsets/
~~~

Each consumer group has a readable directory under `offsets`. The directory name is
`{escaped-group-name}`. A group name is used directly when it is valid as one folder
name. Only characters that cannot be used in a path component, such as `/`, are
percent-encoded. The percent sign is also encoded to prevent collisions with already escaped names.

~~~text
{DataDirectory}/{TopicName}/partition-{PartitionId:D5}/offsets/{escaped-group-name}/consumer.offset
~~~

For topic `orders`, partition `0`, and group
`billing-worker-1`:

~~~text
bufferqueue/orders/partition-00000/offsets/billing-worker-1/consumer.offset
~~~

For group `orders/worker 1`, the slash is encoded but the space remains readable:

~~~text
bufferqueue/orders/partition-00000/offsets/orders%2Fworker 1/consumer.offset
~~~

The partition producer offset and earliest retained segment boundary are stored at:

~~~text
{DataDirectory}/{TopicName}/partition-{PartitionId:D5}/producer.offset
{DataDirectory}/{TopicName}/partition-{PartitionId:D5}/earliest.offset
~~~

For example:

~~~text
bufferqueue/orders/partition-00000/producer.offset
~~~

`earliest.offset`, `producer.offset`, and consumer offset files contain one
8-byte little-endian integer.

## Record Format

Each record is stored as:

~~~text
4 bytes  payload length, little-endian int32
N bytes  payload
1 byte   record end marker
~~~

The record end marker detects incomplete or corrupted records during reads and recovery.

When the remaining space cannot hold the next record, the partition writes a segment-end marker if
at least four bytes remain, then continues in the next segment. When fewer than four bytes remain,
the unused tail is segment padding. The segment-end marker is an `int32` length of
`-1`.

## Serialization and Schema Compatibility

`MemoryMappedFileBufferQueueOptions<T>` exposes one pluggable property:

- `Serializer: IMemoryMappedFileSerializer<T>`

`IMemoryMappedFileSerializer<T>` contains both operations:
`Serialize(T)` returns `byte[]`, and
`Deserialize(ReadOnlyMemory<byte>)` returns `T`.

Available implementations are:

- An internal `System.Text.Json` implementation used by default.
- `MessagePackMemoryMappedFileSerializer<T>`, backed by MessagePack for C#.
  Its parameterless constructor uses `MessagePackSerializerOptions.Standard`; another
  constructor accepts explicit options. Custom resolvers and formatters must be safe for concurrent
  use.
- `UnmanagedMemoryMappedFileSerializer<T>`, which requires
  `T : unmanaged` and copies the value's native in-memory representation.
  Deserialization requires an exact `Unsafe.SizeOf<T>()` payload length.

With standard MessagePack options, custom types should use
`[MessagePackObject]` and stable numeric `[Key]` values. The application
project should reference MessagePack directly because the MMF package's transitive runtime
dependency does not provide MessagePack's analyzer and source generator. Contractless serialization
can be enabled with custom options, but it writes member names into the persisted format and is not
the preferred MMF schema. Resolver, key, compression, and security choices are part of the
persisted format; a removed numeric key must not be reused.

The unmanaged serializer avoids format encoding and decoding, but is not zero-copy because the
serializer contract and partition still materialize payload byte arrays. Native endianness, padding,
field order, packing, runtime, and process architecture are part of its wire format.
`[StructLayout]` is optional, but an explicit sequential or explicit layout and packing
are recommended. Do not persist pointer-sized or process-specific fields.

The serializer and its wire schema are part of the persisted topic format and must remain compatible
with existing records across queue restarts and application upgrades. One configured serializer
instance is shared by all topic partitions and can be called concurrently. Implementations must be
thread-safe, and neither operation may return `null`.

## Flush Boundaries and Append

`MemoryMappedFileBufferQueueOptions<T>` provides two flush strategies:

- `MemoryMappedFileFlushStrategy.Immediate` is the default and explicitly flushes every
  record.
- `MemoryMappedFileFlushStrategy.Batch` explicitly flushes after
  `FlushBatchSize` records have been appended to one partition.
  `FlushBatchSize` defaults to `100`.

A segment rollover and a consumer commit are unconditional flush boundaries in either strategy.
Batch mode can therefore flush before reaching `FlushBatchSize`. A partial tail batch
without later production, rollover, or consumer commit is not guaranteed to have been explicitly
flushed.

`MemoryMappedFileBufferProducer<T>` uses the shared partitioner. It chooses
round-robin routing by default or the configured key selector. Numeric and string routing are
deterministic, and string routing does not use `string.GetHashCode()`.

Batch production is still per record. Each item follows normal partition selection, serialization,
record writing, and flush-strategy handling. A batch is neither a combined MMF record nor a
single-partition grouping or flush unit; round-robin routing advances for each item, and key-based
ordering remains per partition.

After selecting a partition, it serializes the item, calculates record size, finds the active
segment, writes the record, advances the in-process write offset, applies the configured flush
strategy, and notifies consumers.

At each flush boundary, the partition flushes the memory-mapped accessor before writing the
corresponding offset to `producer.offset`. A rollover flushes the completed segment
before writing to the next one. A consumer commit also flushes pending log data and advances
`producer.offset` before its consumer offset is persisted.

Production fails with `InvalidOperationException` when a serialized item is larger than
the segment size.

## Recovery

At startup, a partition reads `earliest.offset`, using `0` when that file is
absent, then attempts to read `producer.offset`. If the producer checkpoint is missing,
startup scans from the earliest retained offset. If it is valid and points to a real record boundary
at or after the earliest retained offset, startup scans forward from it to find the final valid write
offset.

The scan stops at:

- an empty length;
- a non-positive length other than the segment-end marker;
- a record that would cross a segment boundary;
- a missing record end marker.

This keeps normal startup fast and tolerates expected crash windows:

- Data was flushed, but `producer.offset` was not updated.
- The operating system persisted complete records from a pending batch before an explicit flush.
- Trailing data was only partially written.

The scan finds the last valid record boundary in those cases. Since `producer.offset`
advances only after the related log flush succeeds, recovery can use it as a safe checkpoint and
scan forward for additional complete records. In Batch mode, records from a partial tail batch that
was not explicitly flushed can be absent after abnormal termination.

Clearly inconsistent checkpoint state is corruption and fails fast. `earliest.offset`
must be segment-aligned and no greater than the producer offset. Retained segment files must be
contiguous. Invalid offsets, missing retained segments, incorrectly sized segment files,
non-record-boundary checkpoints, an existing consumer-group directory without
`consumer.offset`, and a configured partition count below existing partition directories
throw rather than create replacement files or reset progress.

## Consumer Checkpoints

MMF persists committed offsets per partition and consumer group. On `Commit`, a partition
first forces pending log data to flush and advances `producer.offset`. It then writes the
committed offset as an 8-byte little-endian integer to that group's
`consumer.offset` checkpoint. This ordering prevents a persisted consumer checkpoint
from advancing beyond successfully flushed log data.

Checkpoint writes first use a temporary file, then replace or move it, so readers do not observe a
partially written file.

When a consumer group is assigned partitions, each partition creates an initial checkpoint at the
earliest retained offset if the group does not already have one. The group therefore participates in
retention before its first pull. When creating a reader:

- A valid existing checkpoint starts reading at its stored offset.
- A new checkpoint starts reading at the earliest retained offset.
- An invalid-length checkpoint or negative offset throws `InvalidDataException`.
- An offset before the earliest retained offset, beyond the current write offset, or not on a record
  boundary throws instead of silently resetting progress.

Initial and subsequent committed offsets are persisted. If a batch is read but not committed, the
checkpoint does not advance and the next queue instance reads that batch again.

## Segment Retention

A segment is reclaimable only after every known consumer group has committed past its end. The
partition calculates the minimum committed offset across all persisted group checkpoints, including
groups inactive in the current process. It normalizes offsets across segment-end markers and padding
without skipping records. No segment is deleted when there are no known groups.

For retention value `N`, the partition retains the newest `N` reclaimable
segments and every segment at or after the first segment not fully consumed by every group. Slow,
uncommitted, offline, and obsolete group checkpoints therefore block reclamation.

Removing an obsolete group is an explicit administrative action: stop the queue and delete the
complete `offsets/{escaped-group-name}/` directory from every partition, not just its
`consumer.offset` file.

Deletion runs after a successful consumer commit and at startup to retry incomplete cleanup. Its
durable order is:

1. Flush log data and advance `producer.offset`.
2. Persist the consumer offset.
3. Atomically advance `earliest.offset` to the new segment boundary.
4. Dispose mapped views for older segments and delete their files.

If the process stops after step three, old files can remain outside the logical retained range and
are removed on a later startup or commit. If a segment cannot be deleted after
`earliest.offset` advances, the consumer commit remains persisted and the operation
throws an `IOException` that states cleanup can be retried.

## Producer Checkpoint

At a flush boundary, the partition flushes its memory-mapped accessor and writes the latest
successfully flushed producer offset to `producer.offset` as an 8-byte little-endian
integer. Immediate creates the boundary for every record. Batch creates it after
`FlushBatchSize` records; segment rollover and consumer commit create it regardless of
the pending count.

Producer-offset writes use the same temporary-file plus replace-or-move pattern as consumer
checkpoints. The producer checkpoint is an optimization and recovery hint, not the only source of
truth. It can lag the in-process write offset while Batch mode has a partial batch pending.

On startup, the partition validates records forward from the persisted producer offset. The scan
catches up when that checkpoint is behind complete data. If the checkpoint file is absent, it scans
from the earliest retained offset. If it exists but is inconsistent with the log, recovery fails
fast.
