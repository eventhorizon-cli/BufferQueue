using System;
using System.Numerics;

namespace BufferQueue.Memory;

public class MemoryBufferQueueOptions
{
    /// <summary>
    /// The topic name for the buffer queue.
    /// </summary>
    public string? TopicName { get; set; }

    /// <summary>
    /// The number of partitions for the topic. Default is 1.
    /// </summary>
    public int PartitionNumber { get; set; } = 1;

    /// <summary>
    /// The segment size for each segment. Default is 1024.
    /// </summary>
    public int SegmentSize { get; set; } = 1024;

    /// <summary>
    /// The maximum capacity of the bounded memory buffer queue. Default is null, which means unbounded.
    /// </summary>
    /// <remarks>
    /// If set, <see cref="IBufferProducer{T}.ProduceAsync(T)"/> will throw a <see cref="MemoryBufferQueueFullException"/>
    /// when the queue is full, and
    /// <see cref="IBufferProducer{T}.TryProduceAsync(T)"/> will return false when the queue is full.
    /// </remarks>
    public ulong? BoundedCapacity { get; set; }
}

public class MemoryBufferQueueOptions<T> : MemoryBufferQueueOptions
{
    internal Func<T, int, int>? PartitionIndexSelector { get; private set; }

    /// <summary>
    /// Enables partition-key routing for a numeric key. Items with equal keys are written to the same partition.
    /// </summary>
    /// <param name="partitionKeySelector">Selects the integer-valued partition key from an item.</param>
    /// <remarks>
    /// Values must be finite integers and route with <c>(key - 1) mod partitionCount</c>.
    /// The selector should be deterministic and safe for concurrent calls.
    /// </remarks>
    public void UsePartitionKey<TNumber>(Func<T, TNumber> partitionKeySelector)
        where TNumber : INumber<TNumber>
    {
        ArgumentNullException.ThrowIfNull(partitionKeySelector);
        PartitionIndexSelector = PartitionKeyRouting.CreateNumericPartitionIndexSelector(partitionKeySelector);
    }

    /// <summary>
    /// Enables partition-key routing for a string key. The first four UTF-16 characters determine the partition.
    /// </summary>
    /// <param name="partitionKeySelector">Selects the string partition key from an item.</param>
    /// <remarks>The selector should be deterministic and safe for concurrent calls.</remarks>
    public void UsePartitionKey(Func<T, string> partitionKeySelector)
    {
        ArgumentNullException.ThrowIfNull(partitionKeySelector);
        PartitionIndexSelector = (item, partitionCount) =>
            PartitionKeyRouting.SelectStringPartition(partitionKeySelector(item), partitionCount);
    }
}
