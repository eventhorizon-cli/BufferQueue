using System;
using System.Collections.Generic;
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

    public ValueTask<bool> TryProduceAsync(T item)
    {
        var succeeded = TryEnqueue(item);
        return new(succeeded);
    }

    public ValueTask<bool> TryProduceAsync(ReadOnlyMemory<T> items)
    {
        var succeeded = TryEnqueueBatch(items.Span);
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

    private bool TryEnqueueBatch(ReadOnlySpan<T> items)
    {
        if (items.IsEmpty)
        {
            return true;
        }

        if (items.Length == 1)
        {
            return TryEnqueue(items[0]);
        }

        return _serializedAppendLock is { } serializedAppendLock
            ? TryEnqueueBatchSerialized(items, serializedAppendLock)
            : TryEnqueueBatchConcurrent(items);
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

    private bool TryEnqueueBatchSerialized(ReadOnlySpan<T> items, object appendLock)
    {
        var modifiedPartitions = partitions.Length <= 128
            ? stackalloc bool[partitions.Length]
            : new bool[partitions.Length];
        var capacityGate = _capacityGate;

        try
        {
            lock (appendLock)
            {
                if (capacityGate != null && !capacityGate.TryAcquire((ulong)items.Length))
                {
                    return false;
                }

                var appendedCount = 0;
                ulong reclaimedCount = 0;
                try
                {
                    foreach (var item in items)
                    {
                        var partitionIndex = SelectPartitionIndex(item);
                        reclaimedCount += partitions[partitionIndex].AppendFromSerializedProducer(item);
                        modifiedPartitions[partitionIndex] = true;
                        appendedCount++;
                    }
                }
                catch
                {
                    capacityGate?.Release((ulong)(items.Length - appendedCount) + reclaimedCount);
                    throw;
                }

                capacityGate?.Release(reclaimedCount);
            }
        }
        finally
        {
            NotifyConsumers(modifiedPartitions);
        }

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

    private bool TryEnqueueBatchConcurrent(ReadOnlySpan<T> items)
    {
        var itemsByPartition = new List<T>?[partitions.Length];
        foreach (var item in items)
        {
            var partitionIndex = SelectPartitionIndex(item);
            var partitionItems = itemsByPartition[partitionIndex];
            if (partitionItems == null)
            {
                partitionItems = [];
                itemsByPartition[partitionIndex] = partitionItems;
            }

            partitionItems.Add(item);
        }

        var capacityGate = _capacityGate;
        if (capacityGate != null && !capacityGate.TryAcquire((ulong)items.Length))
        {
            return false;
        }

        var modifiedPartitions = partitions.Length <= 128
            ? stackalloc bool[partitions.Length]
            : new bool[partitions.Length];
        var appendedCount = 0;
        ulong reclaimedCount = 0;

        try
        {
            for (var partitionIndex = 0; partitionIndex < partitions.Length; partitionIndex++)
            {
                var partitionItems = itemsByPartition[partitionIndex];
                if (partitionItems == null)
                {
                    continue;
                }

                var partition = partitions[partitionIndex];
                modifiedPartitions[partitionIndex] = true;
                lock (partition.AppendLock)
                {
                    foreach (var item in partitionItems)
                    {
                        reclaimedCount += partition.AppendFromSerializedProducer(item);
                        appendedCount++;
                    }
                }
            }

            capacityGate?.Release(reclaimedCount);
        }
        catch
        {
            capacityGate?.Release((ulong)(items.Length - appendedCount) + reclaimedCount);
            throw;
        }
        finally
        {
            NotifyConsumers(modifiedPartitions);
        }

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

    private int SelectPartitionIndex(T item) => partitioner.SelectPartition(item, partitions.Length);

    private MemoryBufferPartition<T> SelectPartition(T item) => partitions[SelectPartitionIndex(item)];

    private void NotifyConsumers(ReadOnlySpan<bool> modifiedPartitions)
    {
        for (var partitionIndex = 0; partitionIndex < modifiedPartitions.Length; partitionIndex++)
        {
            if (modifiedPartitions[partitionIndex])
            {
                partitions[partitionIndex].NotifyConsumers();
            }
        }
    }

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
