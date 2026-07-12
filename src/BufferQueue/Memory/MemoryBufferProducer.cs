// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;
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
    private readonly object _boundedAppendLock = new();

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
            Enqueue(item);
            return true;
        }

        if (!capacityGate.TryAcquire())
        {
            return false;
        }

        var appended = false;
        var reclaimedCount = 0UL;
        try
        {
            lock (_boundedAppendLock)
            {
                var partition = SelectPartition();
                partition.Enqueue(item, out reclaimedCount, out appended);
            }
        }
        catch
        {
            capacityGate.Release(appended ? reclaimedCount : 1);
            throw;
        }

        capacityGate.Release(reclaimedCount);
        return true;
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Enqueue(T item)
    {
        var partition = SelectPartition();
        partition.Enqueue(item);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private MemoryBufferPartition<T> SelectPartition()
    {
        var index = (Interlocked.Increment(ref _partitionIndex) - 1) % partitions.Length;
        return partitions[index];
    }
}
