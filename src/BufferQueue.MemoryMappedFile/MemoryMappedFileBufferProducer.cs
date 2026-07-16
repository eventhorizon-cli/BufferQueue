// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace BufferQueue.MemoryMappedFile;

internal sealed class MemoryMappedFileBufferProducer<T>(
    MemoryMappedFileBufferQueueOptions<T> options,
    MemoryMappedFileBufferPartition<T>[] partitions)
    : IBufferProducer<T>
    where T : notnull
{
    private uint _partitionIndex;

    public string TopicName { get; } = options.TopicName!;

    public ValueTask ProduceAsync(T item)
    {
        Enqueue(item);
        return ValueTask.CompletedTask;
    }

    public ValueTask<bool> TryProduceAsync(T item)
    {
        Enqueue(item);
        return new(true);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private void Enqueue(T item)
    {
        var partition = SelectPartition();
        partition.Enqueue(item);
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    private MemoryMappedFileBufferPartition<T> SelectPartition()
    {
        var index = (Interlocked.Increment(ref _partitionIndex) - 1) % partitions.Length;
        return partitions[index];
    }
}
