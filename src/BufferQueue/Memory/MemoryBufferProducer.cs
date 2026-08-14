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
    private readonly object? _serializedAppendLock = partitioner.SupportsConcurrentSelection
        ? null
        : GetSharedAppendLock(partitions);

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
            var unboundedPartition = Append(item, out _);

            unboundedPartition.NotifyConsumers();
            return true;
        }

        if (_serializedAppendLock is { } serializedAppendLock)
        {
            return TryEnqueueSerialized(item, capacityGate, serializedAppendLock);
        }

        return TryEnqueueConcurrent(item, capacityGate);
    }

    private bool TryEnqueueSerialized(T item, MemoryBufferCapacityGate capacityGate, object appendLock)
    {
        MemoryBufferPartition<T> partition;
        lock (appendLock)
        {
            if (!capacityGate.TryAcquire())
            {
                return false;
            }

            ulong reclaimedCount;
            try
            {
                partition = AppendSelectedPartition(item, out reclaimedCount);
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

    private bool TryEnqueueConcurrent(T item, MemoryBufferCapacityGate capacityGate)
    {
        if (!capacityGate.TryAcquire())
        {
            return false;
        }

        MemoryBufferPartition<T> partition;
        var appended = false;
        try
        {
            partition = Append(item, out var reclaimedCount);
            appended = true;
            capacityGate.Release(reclaimedCount);
        }
        catch
        {
            if (!appended)
            {
                capacityGate.Release();
            }

            throw;
        }

        partition.NotifyConsumers();
        return true;
    }

    private MemoryBufferPartition<T> Append(T item, out ulong reclaimedCount)
    {
        if (_serializedAppendLock is { } serializedAppendLock)
        {
            lock (serializedAppendLock)
            {
                return AppendSelectedPartition(item, out reclaimedCount);
            }
        }

        var partition = SelectPartition(item);
        lock (partition.AppendLock)
        {
            reclaimedCount = partition.AppendFromSerializedProducer(item);
        }

        return partition;
    }

    private MemoryBufferPartition<T> AppendSelectedPartition(T item, out ulong reclaimedCount)
    {
        var partition = SelectPartition(item);
        reclaimedCount = partition.AppendFromSerializedProducer(item);
        return partition;
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
