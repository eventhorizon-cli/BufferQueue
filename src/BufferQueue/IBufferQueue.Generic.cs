using System.Collections.Generic;

namespace BufferQueue;

internal interface IBufferQueue<T>
{
    string TopicName { get; }

    IBufferProducer<T> GetProducer();

    IBufferPullConsumer<T> CreateConsumer(BufferPullConsumerOptions options);

    IEnumerable<IBufferPullConsumer<T>> CreateConsumers(BufferPullConsumerOptions options, int consumerNumber);
}
