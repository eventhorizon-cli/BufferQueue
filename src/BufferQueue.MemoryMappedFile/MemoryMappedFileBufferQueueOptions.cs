// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;
using System.IO;

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
