// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;
using Microsoft.Extensions.DependencyInjection;

namespace BufferQueue.Memory;

public class MemoryBufferOptionsBuilder(IServiceCollection services)
{
    public MemoryBufferOptionsBuilder AddTopic<T>(
        Action<MemoryBufferQueueOptions> configure)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(configure);

        var options = new MemoryBufferQueueOptions();
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

        services.AddKeyedSingleton<IBufferQueue<T>>(
            topicName, new MemoryBufferQueue<T>(options));
        return this;
    }
}
