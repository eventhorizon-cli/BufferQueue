// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.Collections.Generic;

namespace BufferQueue;

internal interface IBufferQueue<T>
{
    string TopicName { get; }

    IBufferProducer<T> GetProducer();

    IBufferPullConsumer<T> CreateConsumer(BufferPullConsumerOptions options);

    IEnumerable<IBufferPullConsumer<T>> CreateConsumers(BufferPullConsumerOptions options, int consumerNumber);
}
