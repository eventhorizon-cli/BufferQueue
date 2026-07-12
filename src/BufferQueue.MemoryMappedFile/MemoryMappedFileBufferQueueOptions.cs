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
    /// The memory-mapped file segment size in bytes. Default is 64 MB.
    /// </summary>
    public long SegmentSize { get; set; } = 64L * 1024 * 1024;

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
}
