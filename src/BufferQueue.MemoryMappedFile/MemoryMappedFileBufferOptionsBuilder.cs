// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;
using Microsoft.Extensions.DependencyInjection;

namespace BufferQueue.MemoryMappedFile;

public class MemoryMappedFileBufferOptionsBuilder(IServiceCollection services)
{
    public MemoryMappedFileBufferOptionsBuilder AddTopic<T>(
        Action<MemoryMappedFileBufferQueueOptions<T>> configure)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(configure);

        var options = new MemoryMappedFileBufferQueueOptions<T>();
        configure(options);

        var topicName = options.TopicName;
        var partitionNumber = options.PartitionNumber;

        if (string.IsNullOrWhiteSpace(topicName))
        {
            throw new ArgumentException("Topic name cannot be null or whitespace.", nameof(options.TopicName));
        }

        if (partitionNumber <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.PartitionNumber),
                "Partition number must be greater than zero.");
        }

        if (options.SegmentSize <= MemoryMappedFileBufferPartition<T>.MaxRecordOverhead)
        {
            throw new ArgumentOutOfRangeException(nameof(options.SegmentSize),
                "Segment size must be large enough to contain at least one record.");
        }

        if (!Enum.IsDefined(options.FlushStrategy))
        {
            throw new ArgumentOutOfRangeException(nameof(options.FlushStrategy),
                "The flush strategy is not supported.");
        }

        if (options.FlushStrategy == MemoryMappedFileFlushStrategy.Batch && options.FlushBatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.FlushBatchSize),
                "Flush batch size must be greater than zero when using the batch flush strategy.");
        }

        ArgumentNullException.ThrowIfNull(options.Serializer, nameof(options.Serializer));

        services.AddKeyedSingleton<IBufferQueue<T>>(
            topicName, (_, _) => new MemoryMappedFileBufferQueue<T>(options));
        return this;
    }
}
