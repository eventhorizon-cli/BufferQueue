// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

namespace BufferQueue.Memory;

internal sealed class MemoryBufferQueue<T> : BufferQueue<T>
{
    public MemoryBufferQueue(MemoryBufferQueueOptions options)
        : this(options, CreatePartitions(options))
    {
    }

    private MemoryBufferQueue(MemoryBufferQueueOptions options, MemoryBufferPartition<T>[] partitions)
        : base(options.TopicName!, partitions, new MemoryBufferProducer<T>(options, partitions))
    {
    }

    private static MemoryBufferPartition<T>[] CreatePartitions(MemoryBufferQueueOptions options)
    {
        var partitions = new MemoryBufferPartition<T>[options.PartitionNumber];
        for (var i = 0; i < partitions.Length; i++)
        {
            partitions[i] = new MemoryBufferPartition<T>(i, options.SegmentSize);
        }

        return partitions;
    }
}
