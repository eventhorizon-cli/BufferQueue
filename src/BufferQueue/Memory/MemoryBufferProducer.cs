using System;
using System.Threading.Tasks;

namespace BufferQueue.Memory;

internal sealed class MemoryBufferProducer<T>(
    MemoryBufferQueueOptions options,
    MemoryBufferPartition<T>[] partitions,
    IPartitioner<T> partitioner)
    : IBufferProducer<T>
{
    public MemoryBufferProducer(
        MemoryBufferQueueOptions options,
        MemoryBufferPartition<T>[] partitions)
        : this(
            options,
            partitions,
            options is MemoryBufferQueueOptions<T> { PartitionIndexSelector: { } selector }
                ? new KeyPartitioner<T>(selector)
                : new RoundRobinPartitioner<T>())
    {
    }

    private readonly MemoryBufferCapacityGate? _capacityGate = options.BoundedCapacity is { } capacity
        ? new(capacity)
        : null;
    private readonly object _appendLock = GetSharedAppendLock(partitions);

    public string TopicName { get; } = options.TopicName!;

    public ValueTask ProduceAsync(T item)
    {
        if (!TryEnqueue(item))
        {
            throw new MemoryBufferQueueFullException(
                $"The queue '{TopicName}' is full, and the item cannot be produced.");
        }
        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> TryProduceAsync(T item)
    {
        var succeeded = TryEnqueue(item);
        return new(succeeded);
    }

    private bool TryEnqueue(T item)
    {
        var capacityGate = _capacityGate;
        if (capacityGate == null)
        {
            MemoryBufferPartition<T> unboundedPartition;
            lock (_appendLock)
            {
                unboundedPartition = SelectPartition(item);
                unboundedPartition.AppendFromSerializedProducer(item);
            }

            unboundedPartition.NotifyConsumers();
            return true;
        }

        MemoryBufferPartition<T> partition;
        lock (_appendLock)
        {
            if (!capacityGate.TryAcquire())
            {
                return false;
            }

            ulong reclaimedCount;
            try
            {
                partition = SelectPartition(item);
                reclaimedCount = partition.AppendFromSerializedProducer(item);
            }
            catch
            {
                capacityGate.Release();
                throw;
            }

            capacityGate.Release(reclaimedCount);
        }

        partition.NotifyConsumers();
        return true;
    }

    private MemoryBufferPartition<T> SelectPartition(T item) =>
        partitions[partitioner.SelectPartition(item, partitions.Length)];

    private static object GetSharedAppendLock(MemoryBufferPartition<T>[] bufferPartitions)
    {
        if (bufferPartitions.Length == 0)
        {
            throw new ArgumentException("At least one partition is required.", nameof(bufferPartitions));
        }

        var appendLock = bufferPartitions[0].AppendLock;
        for (var i = 1; i < bufferPartitions.Length; i++)
        {
            if (!ReferenceEquals(appendLock, bufferPartitions[i].AppendLock))
            {
                throw new ArgumentException("All partitions must share the same append lock.", nameof(bufferPartitions));
            }
        }

        return appendLock;
    }
}
