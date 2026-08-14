# Consumer Model and Delivery

[English](consumer-model.md) | [Simplified Chinese](consumer-model.zh-CN.md)

[Design index](../README.md)

## Pull Consumers

`BufferPullConsumer<TItem>` is the one shared pull-consumer implementation. It is
not storage-specific. It:

- keeps the assigned partition list;
- selects partitions in round-robin order;
- pulls batches from partitions;
- tries other assigned partitions when the selected partition has no data;
- waits asynchronously when no assigned partition has data;
- commits manually consumed batches;
- auto-commits when `AutoCommit` is enabled.

Consumption returns an asynchronous stream of batches:

~~~csharp
await foreach (var batch in consumer.ConsumeAsync(cancellationToken))
{
    foreach (var item in batch)
    {
        // Process item.
    }

    await consumer.CommitAsync();
}
~~~

When `AutoCommit` is false, the consumer read position does not advance until
`CommitAsync` is called. This gives at-least-once behavior within the current process.
In MemoryMappedFile mode it also applies across process restarts for records that have reached a
flush boundary; `CommitAsync` itself forces that boundary.

The relationship between groups, consumers, partitions, ordering, and assignment is described in
[Partitioning and concurrency](partitioning-and-concurrency.md).

## Waiting and Wake-Up

Consumers do not spin when no data is available. The common consumer waits through
`PendingDataValueTaskSource<T>`:

1. The consumer tries the selected partition.
2. If it finds no data, it tries every other assigned partition.
3. If none has data, it resets the pending-data value-task source and waits.
4. A producer appends data to a partition.
5. The partition notifies registered consumers through
   `IBufferPartitionConsumer<TItem>`.
6. The consumer increments its pending-data version and completes the pending value task.
7. The consumer resumes and tries the partition that sent the notification.

The pending-data version prevents a lost wake-up when data arrives between the final pull attempt
and the transition into the waiting state.

## Push Consumers

Push-consumer mode is built on pull consumers. The host service discovers push consumers by
attribute, creates the corresponding pull consumers, and passes batches to the push-consumer
implementation.

Auto-commit push consumers advance progress after a successful pull and before application code
processes the batch. A handler failure therefore does not make that batch eligible for replay.
Manual-commit push consumers receive `IBufferConsumerCommitter` and decide when to
commit; an uncommitted batch may be delivered again.

The configured `ServiceLifetime` controls push-consumer resolution:

- A `Singleton` consumer is resolved from the root provider and reused across batches
  and concurrent consumer loops, so it must be thread-safe.
- `Scoped` and `Transient` consumers are resolved in a new asynchronous DI
  scope for every delivered batch. The scope and captured services are asynchronously disposed
  after the handler completes or throws. Scoped dependencies must not escape that handler call.

## Delivery Semantics

Manual commit provides at-least-once delivery:

- A batch that is read but not committed can be delivered again.
- Auto commit advances progress immediately after a successful pull.
- Manual commit advances progress when application code calls `CommitAsync`.

Memory keeps offsets only for the lifetime of the process. MemoryMappedFile persists producer
offsets and committed consumer offsets to disk. A consumer commit first forces pending MMF log data
to a flush boundary. In Batch mode, an uncommitted partial tail batch is not guaranteed to survive
an abnormal termination.

See [Memory storage](memory.md) and
[MemoryMappedFile storage](memory-mapped-file.md) for storage-specific behavior.
