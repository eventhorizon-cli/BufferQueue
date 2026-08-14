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
        : this(
            options.Validate(),
            partitioner,
            CreatePartitions(options, partitioner.SupportsConcurrentSelection))
    {
    }

    private MemoryBufferQueue(
        MemoryBufferQueueOptions options,
        IPartitioner<T> partitioner,
        MemoryBufferPartition<T>[] partitions)
        : base(options.TopicName!, partitions, new MemoryBufferProducer<T>(options, partitions, partitioner))
    {
    }

    private static MemoryBufferPartition<T>[] CreatePartitions(
        MemoryBufferQueueOptions options,
        bool useIndependentAppendLocks)
    {
        var partitions = new MemoryBufferPartition<T>[options.PartitionNumber];
        var appendLock = useIndependentAppendLocks ? null : new object();
        for (var i = 0; i < partitions.Length; i++)
        {
            partitions[i] = appendLock == null
                ? new MemoryBufferPartition<T>(i, options.SegmentSize)
                : new MemoryBufferPartition<T>(i, options.SegmentSize, appendLock);
        }

        return partitions;
    }
}
