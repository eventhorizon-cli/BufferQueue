# Partitioning and Concurrency

[English](partitioning-and-concurrency.md) | [Simplified Chinese](partitioning-and-concurrency.zh-CN.md)

[Design index](../README.md)

## Partitions and Consumer Groups

Each topic has one or more partitions. Producers use round-robin selection by default. Both Memory
and MemoryMappedFile topics can enable `UsePartitionKey` with a key-selector delegate so
equal keys route to the same partition. Selectors must be deterministic and safe for concurrent
calls.

Consumers are created per consumer group. A group may contain multiple consumers, and its
partitions are distributed evenly among them:

- Consumer count must be greater than zero.
- Consumer count cannot exceed partition count.
- Each group has an independent read position.
- Each group receives all topic messages, while partitions are load-balanced within that group.

For five partitions and two consumers:

~~~text
consumer-0: partition-0, partition-1, partition-2
consumer-1: partition-3, partition-4
~~~

Groups are independent. Two groups consuming the same topic retain separate progress. The consumer
count is fixed when a group is created, and duplicate group names in one queue instance are
rejected.

Memory consumer-group registration may run concurrently with production. Existing group assignments
remain unchanged. A group created while production continues starts at each partition's earliest
still-readable position; there is no topic-wide atomic registration time or guarantee that the group can
read all historical records.

Ordering is preserved within a partition, not globally across partitions. Multiple consumers in a
group receive distinct partition assignments, so no group member competes with another member for
the same partition.

## Partition-Key Routing

Round-robin routing distributes appends across partitions. With partition-key routing:

- A numeric selector result must be a finite integer. It maps through the normalized mathematical
  modulo of `(key - 1)` and `PartitionNumber`; zero and negative keys are
  accepted.
- A string selector hashes the complete key with XXH3-64 and seed zero. It does not use
  `string.GetHashCode()`. The canonical input is the UTF-16 code-unit sequence serialized as
  little-endian bytes, including surrogate code units and embedded nulls; no normalization, case
  folding, BOM, terminator, or replacement of unpaired surrogates is performed. The unsigned 64-bit
  hash modulo `PartitionNumber` selects the partition. Empty strings are valid and use the non-zero
  XXH3 empty-input hash; null strings are rejected. A single partition always selects zero after
  validation.

The fixed byte order and seed make mapping independent of process hash randomization, machine
endianness, target framework, and hardware. On little-endian machines, keys up to
`int.MaxValue / 2` UTF-16 code units are passed through a `MemoryMarshal` span directly to
`System.IO.Hashing` without an encoding buffer or managed allocation. A portable streaming path also
handles keys whose canonical byte length exceeds the maximum span length. The big-endian fallback
feeds canonical little-endian bytes incrementally through `XxHash3`, using a bounded stack buffer and
allocating the hash state object; it produces the same canonical result, so string routing is not
universally allocation-free. XXH3 uses SIMD for inputs longer than 240 bytes (more than 120 UTF-16
code units) when supported by the hardware; shorter inputs use specialized scalar paths, and the
scalar fallback produces the same hash. The direct dependency is `System.IO.Hashing` 10.0.9, with
assets for net8.0 and net10.0 and no additional transitive production dependencies. The implementation
uses the mature `System.IO.Hashing` XXH3 implementation
instead of copying a large hash implementation into this repository. See the [runtime source](https://github.com/dotnet/runtime/tree/v10.0.9/src/libraries/System.IO.Hashing)
and [xxHash reference](https://github.com/Cyan4973/xxHash).

This replaces the previous four-code-unit mapping without a legacy mode.
Upgrading changes string partition assignments; existing MMF logs remain readable, but new writes for
the same key may enter another partition. Per-key order across that upgrade is not preserved.

This mapping is not Kafka-compatible. Kafka's Java keyed partitioner hashes serialized key bytes with
Murmur2, masks with `0x7fffffff`, and takes `% partitionCount`; that is a separate mapping, not a
SIMD alternative to this UTF-16LE XXH3-64 contract.

Equal keys therefore retain their order in one partition, while distinct keys may still collide on
the same partition.

For a persisted MemoryMappedFile topic, do not change the selector, partition-key-routing behavior,
or `PartitionNumber` across restarts. Reducing an existing MMF topic's partition count
also fails startup when stored partition directories exceed the configured count. See
[MemoryMappedFile storage](memory-mapped-file.md) for the recovery rule.

## Batch Routing

The `ProduceAsync` and `TryProduceAsync` batch overloads apply normal routing to every item.
A batch is not assigned to one partition:

- With round-robin routing, partition selection advances for each item in the batch.
- With partition-key routing, the selector runs for each item. Equal keys retain their input order
  within the selected partition.

Items routed to different partitions do not gain a global batch order. The queue continues to
preserve order per partition only.

For a bounded Memory topic in `Wait` mode, an input batch larger than the configured capacity is
processed as consecutive capacity-sized slices. Each slice applies the same per-item routing, so
round-robin selection continues across slice boundaries and equal partition keys retain input order
within their selected partition.

## In-Process Concurrency

The queue is designed for concurrent production and consumption within one process:

- Producers select a partition through round-robin counters by default, or through the configured
  partition-key selector. Selector implementations must be safe for concurrent calls.
- In Memory mode, default round-robin selection and batch-range reservation use a short topic-level
  coordinator. Each admitted batch or batch slice obtains its complete capacity before it reserves
  the range, then appends each partition slice under that partition's append lock. A single
  round-robin append uses its target partition lock while a batch append is active; consumer state
  changes use the same coordinator. Partition-key routing selects a partition first, then uses that
  partition's append lock. Appends to one partition remain serialized.
- A Memory partition publishes its readable segment cursor only after it stores the item. Consumers
  never observe an unwritten slot and do not take the append lock while reading.
- Consumer-group creation is guarded by a queue-level lock.
- Memory consumer registration and unregistration are serialized by the existing partition append
  lock. Registration builds a notification set copied from the current `HashSet`, initializes the
  reader, and then publishes the set with a volatile write. Published sets are never modified.
  Producers use a volatile read and enumerate the selected set outside the lock, so production does
  not copy the set or take an additional notification lock for each item. An in-flight notification
  may use an older set and notify a consumer that has just been unregistered; unregistration does not
  wait for such notifications to finish. A new consumer reads available data before waiting, so
  missing an older notification set does not by itself lose a wake-up.
- Consumer wait and wake-up state is protected by `ReaderWriterLockSlim`.
- MemoryMappedFile producer and consumer checkpoints use replace-or-move semantics, so readers do
  not observe partially written offset files.

MemoryMappedFile does not provide cross-process coordination for writers to one topic directory.
Treat it as local persistence for one active queue instance unless external coordination is added.

## Related Notes

The [consumer model](consumer-model.md) covers batch pulls, waiting, commits, and delivery. The
[Memory storage](memory.md) and [MemoryMappedFile storage](memory-mapped-file.md) articles describe
what happens after a partition is selected.
