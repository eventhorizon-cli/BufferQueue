namespace BufferQueue.Memory;

internal sealed class MemoryBufferQueue<T> : BufferQueue<T>
{
    public MemoryBufferQueue(MemoryBufferQueueOptions options)
        : this(
            options,
            options is MemoryBufferQueueOptions<T> { PartitionIndexSelector: { } selector }
                ? new KeyPartitioner<T>(selector)
                : new RoundRobinPartitioner<T>())
    {
    }

    internal MemoryBufferQueue(MemoryBufferQueueOptions options, IPartitioner<T> partitioner)
        : this(options, partitioner, new object())
    {
    }

    private MemoryBufferQueue(
        MemoryBufferQueueOptions options,
        IPartitioner<T> partitioner,
        object appendLock)
        : this(options, partitioner, CreatePartitions(options, appendLock))
    {
    }

    private MemoryBufferQueue(
        MemoryBufferQueueOptions options,
        IPartitioner<T> partitioner,
        MemoryBufferPartition<T>[] partitions)
        : base(options.TopicName!, partitions, new MemoryBufferProducer<T>(options, partitions, partitioner))
    {
    }

    private static MemoryBufferPartition<T>[] CreatePartitions(MemoryBufferQueueOptions options, object appendLock)
    {
        var partitions = new MemoryBufferPartition<T>[options.PartitionNumber];
        for (var i = 0; i < partitions.Length; i++)
        {
            partitions[i] = new MemoryBufferPartition<T>(i, options.SegmentSize, appendLock);
        }

        return partitions;
    }
}
