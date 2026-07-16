// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;

namespace BufferQueue.Memory;

internal sealed class MemoryBufferProducer<T>(
    MemoryBufferQueueOptions options,
    MemoryBufferPartition<T>[] partitions)
    : IBufferProducer<T>
{
    private uint _partitionIndex;
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
                unboundedPartition = SelectPartition();
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
                partition = SelectPartition();
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

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private MemoryBufferPartition<T> SelectPartition()
    {
        var index = _partitionIndex;
        _partitionIndex = index + 1 == partitions.Length ? 0 : index + 1;
        return partitions[index];
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
