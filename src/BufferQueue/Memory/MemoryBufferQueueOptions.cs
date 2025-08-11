// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;

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
    public long SegmentSize { get; set; } = 1024;

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
