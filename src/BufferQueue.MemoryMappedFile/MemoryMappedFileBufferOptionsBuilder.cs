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

        options.Validate();
        var topicName = options.TopicName!;

        if (options.PartitionIndexSelector is { } partitionIndexSelector)
        {
            services.AddKeyedSingleton<IPartitioner<T>>(
                topicName, new KeyPartitioner<T>(partitionIndexSelector));
        }
        else
        {
            services.AddKeyedSingleton<IPartitioner<T>, ConcurrentRoundRobinPartitioner<T>>(topicName);
        }

        services.AddKeyedSingleton<IBufferQueue<T>>(
            topicName,
            (serviceProvider, serviceKey) => new MemoryMappedFileBufferQueue<T>(
                options,
                serviceProvider.GetRequiredKeyedService<IPartitioner<T>>(serviceKey)));
        services.AddKeyedSingleton<IBufferProducer<T>>(
            topicName,
            (serviceProvider, serviceKey) =>
                serviceProvider.GetRequiredKeyedService<IBufferQueue<T>>(serviceKey).GetProducer());
        return this;
    }
}
