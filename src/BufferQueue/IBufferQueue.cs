// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;
using System.Collections.Generic;

namespace BufferQueue;

public interface IBufferQueue
{
    /// <summary>
    /// Create a producer for the specified topic.
    /// </summary>
    /// <param name="topicName">The topic name.</param>
    /// <typeparam name="T">The type of the item.</typeparam>
    /// <returns>The producer.</returns>
    IBufferProducer<T> CreateProducer<T>(string topicName);

    /// <summary>
    /// Create a pull consumer for the specified topic.
    /// This method can only be called once for each consumer group within the same topic.
    /// Use the <see cref="CreatePullConsumers{T}"/> method to create multiple consumers.
    /// </summary>
    /// <param name="options">The consumer options.</param>
    /// <typeparam name="T">The type of the item.</typeparam>
    /// <returns>The consumer.</returns>
    /// <exception cref="ArgumentException">The topic name is null or empty.</exception>
    /// <exception cref="ArgumentException">The group name is null or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The batch size must be greater than 0.</exception>
    /// <exception cref="InvalidOperationException">The consumer group has been created.</exception>
    IBufferPullConsumer<T> CreatePullConsumer<T>(BufferPullConsumerOptions options);

    /// <summary>
    /// Create multiple pull consumers for the specified topic.
    /// This method can only be called once for each consumer group within the same topic.
    /// </summary>
    /// <param name="options">The consumer options.</param>
    /// <param name="consumerNumber">The number of consumers.</param>
    /// <typeparam name="T">The type of the item.</typeparam>
    /// <returns>The consumers.</returns>
    /// <exception cref="ArgumentOutOfRangeException">The number of consumers must be greater than 0 and cannot be greater than the number of partitions.</exception>
    /// <exception cref="ArgumentException">The topic name is null or empty.</exception>
    /// <exception cref="ArgumentException">The group name is null or empty.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The batch size must be greater than 0.</exception>
    /// <exception cref="ArgumentOutOfRangeException">The number of consumers must be greater than 0.</exception>
    /// <exception cref="InvalidOperationException">The consumer group has been created.</exception>
    IEnumerable<IBufferPullConsumer<T>> CreatePullConsumers<T>(BufferPullConsumerOptions options, int consumerNumber);
}
