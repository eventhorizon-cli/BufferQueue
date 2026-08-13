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

        return AddTopic<T>((MemoryBufferQueueOptions<T> options) => configure(options));
    }

    public MemoryBufferOptionsBuilder AddTopic<T>(
        Action<MemoryBufferQueueOptions<T>> configure)
        where T : notnull
    {
        ArgumentNullException.ThrowIfNull(configure);

        var options = new MemoryBufferQueueOptions<T>();
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

        if (options.PartitionIndexSelector is { } partitionIndexSelector)
        {
            services.AddKeyedSingleton<IPartitioner<T>>(
                topicName, new KeyPartitioner<T>(partitionIndexSelector));
        }
        else
        {
            services.AddKeyedSingleton<IPartitioner<T>, RoundRobinPartitioner<T>>(topicName);
        }

        services.AddKeyedSingleton<IBufferQueue<T>>(
            topicName,
            (serviceProvider, serviceKey) => new MemoryBufferQueue<T>(
                options,
                serviceProvider.GetRequiredKeyedService<IPartitioner<T>>(serviceKey)));
        services.AddKeyedSingleton<IBufferProducer<T>>(
            topicName,
            (serviceProvider, serviceKey) =>
                serviceProvider.GetRequiredKeyedService<IBufferQueue<T>>(serviceKey).GetProducer());
        return this;
    }
}
