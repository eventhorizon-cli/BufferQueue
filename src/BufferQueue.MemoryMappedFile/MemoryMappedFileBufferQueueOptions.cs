using System;
using System.IO;
using System.Numerics;

namespace BufferQueue.MemoryMappedFile;

public class MemoryMappedFileBufferQueueOptions<T>
    where T : notnull
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
    /// The memory-mapped file segment size in bytes. Default is 256 MiB.
    /// </summary>
    public long SegmentSizeInBytes { get; set; } = 256L * 1024 * 1024;

    /// <summary>
    /// The maximum number of segments retained per partition after every known consumer group
    /// has committed past them. A null value disables deletion, and zero retains no reclaimable
    /// consumed segments. This is not a limit on unconsumed segments or total disk usage. Default is null.
    /// </summary>
    public int? MaxRetainedConsumedSegments { get; set; }

    /// <summary>
    /// The directory used to store topic partition files.
    /// </summary>
    public string DataDirectory { get; set; } = Path.Combine(AppContext.BaseDirectory, "bufferqueue");

    /// <summary>
    /// Specifies when memory-mapped-file writes are flushed. Default is Immediate.
    /// </summary>
    public MemoryMappedFileFlushStrategy FlushStrategy { get; set; } = MemoryMappedFileFlushStrategy.Immediate;

    /// <summary>
    /// The number of records appended to a partition before a batch flush. Default is 100.
    /// </summary>
    public int FlushBatchSize { get; set; } = 100;

    /// <summary>
    /// Serializes and deserializes items stored in memory-mapped files.
    /// </summary>
    public IMemoryMappedFileSerializer<T> Serializer { get; set; } =
        SystemTextJsonMemoryMappedFileSerializer<T>.Instance;

    internal Func<T, int, int>? PartitionIndexSelector { get; private set; }

    /// <summary>
    /// Enables partition-key routing for a numeric key. Items with equal keys are written to the same partition.
    /// </summary>
    /// <param name="partitionKeySelector">Selects the integer-valued partition key from an item.</param>
    /// <remarks>
    /// Values must be finite integers and route with <c>(key - 1) mod partitionCount</c>.
    /// The selector should be deterministic and safe for concurrent calls. Keep the selector and partition count
    /// unchanged across process restarts to preserve routing.
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
    /// <remarks>
    /// The selector should be deterministic and safe for concurrent calls. Keep the selector and partition count
    /// unchanged across process restarts to preserve routing.
    /// </remarks>
    public void UsePartitionKey(Func<T, string> partitionKeySelector)
    {
        ArgumentNullException.ThrowIfNull(partitionKeySelector);
        PartitionIndexSelector = (item, partitionCount) =>
            PartitionKeyRouting.SelectStringPartition(partitionKeySelector(item), partitionCount);
    }

    internal long GetSegmentSizeInBytes()
    {
        if (SegmentSizeInBytes <= MemoryMappedFileBufferPartition<T>.MaxRecordOverhead)
        {
            throw new ArgumentOutOfRangeException(nameof(SegmentSizeInBytes),
                "Segment size must be large enough to contain at least one record.");
        }

        return SegmentSizeInBytes;
    }

    internal int? GetMaxRetainedConsumedSegments()
    {
        if (MaxRetainedConsumedSegments < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(MaxRetainedConsumedSegments),
                "The maximum number of retained consumed segments must be greater than or equal to zero.");
        }

        return MaxRetainedConsumedSegments;
    }
}
