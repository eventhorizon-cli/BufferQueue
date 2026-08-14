using System;
using System.Threading.Tasks;

namespace BufferQueue.MemoryMappedFile;

internal sealed class MemoryMappedFileBufferProducer<T>(
    MemoryMappedFileBufferQueueOptions<T> options,
    MemoryMappedFileBufferPartition<T>[] partitions,
    IPartitioner<T> partitioner)
    : IBufferProducer<T>
    where T : notnull
{
    public string TopicName { get; } = options.TopicName!;

    public ValueTask<bool> TryProduceAsync(T item)
    {
        Enqueue(item);
        return new(true);
    }

    public ValueTask<bool> TryProduceAsync(ReadOnlyMemory<T> items)
    {
        foreach (var item in items.Span)
        {
            Enqueue(item);
        }

        return new(true);
    }

    private void Enqueue(T item)
    {
        var partition = partitions[partitioner.SelectPartition(item, partitions.Length)];
        partition.Enqueue(item);
    }
}
