// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;
using System.Collections.Generic;
using Microsoft.Extensions.DependencyInjection;

namespace BufferQueue;

internal class BufferQueue(IServiceProvider serviceProvider) : IBufferQueue
{
    public IBufferProducer<T> GetProducer<T>(string topicName)
    {
        if (string.IsNullOrWhiteSpace(topicName))
        {
            throw new ArgumentException("The topic name cannot be null or empty.", nameof(topicName));
        }

        var queue = serviceProvider.GetKeyedService<IBufferQueue<T>>(topicName) ??
                    throw new ArgumentException($"The topic '{topicName}' has not been registered.");
        return queue.GetProducer();
    }

    public IBufferPullConsumer<T> CreatePullConsumer<T>(BufferPullConsumerOptions options)
    {
        if (string.IsNullOrWhiteSpace(options.TopicName))
        {
            throw new ArgumentException("The topic name cannot be null or empty.", nameof(options.TopicName));
        }

        var queue = serviceProvider.GetKeyedService<IBufferQueue<T>>(options.TopicName) ??
                    throw new ArgumentException($"The topic '{options.TopicName}' has not been registered.");
        return queue.CreateConsumer(options);
    }

    public IEnumerable<IBufferPullConsumer<T>> CreatePullConsumers<T>(BufferPullConsumerOptions options, int consumerNumber)
    {
        if (string.IsNullOrWhiteSpace(options.TopicName))
        {
            throw new ArgumentException("The topic name cannot be null or empty.", nameof(options.TopicName));
        }

        if (consumerNumber < 1)
        {
            throw new ArgumentOutOfRangeException(nameof(consumerNumber),
                "The number of consumers must be greater than 0.");
        }

        var queue = serviceProvider.GetKeyedService<IBufferQueue<T>>(options.TopicName) ??
                    throw new ArgumentException($"The topic '{options.TopicName}' has not been registered.");
        return queue.CreateConsumers(options, consumerNumber);
    }
}
