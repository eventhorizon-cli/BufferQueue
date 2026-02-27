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
    private int _checkingCapacity;

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
        return new ValueTask<bool>(succeeded);
    }

    private bool TryEnqueue(T item)
    {
        if (!options.BoundedCapacity.HasValue)
        {
            Enqueue(item);
            return true;
        }

        while (true)
        {
            if (Interlocked.CompareExchange(ref _checkingCapacity, 1, 0) != 0)
            {
                continue;
            }

            try
            {
                var count = 0UL;
                foreach (var partition in partitions)
                {
                    count += partition.Count;
                    if (count >= options.BoundedCapacity.Value)
                    {
                        return false;
                    }
                }

                Enqueue(item);
                return true;
            }
            finally
            {
                Interlocked.Exchange(ref _checkingCapacity, 0);
            }
        }
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
