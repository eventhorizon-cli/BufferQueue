using System;
using System.Threading.Tasks;

namespace BufferQueue;

public interface IBufferProducer<T>
{
    string TopicName { get; }

    ValueTask<bool> TryProduceAsync(T item);

    ValueTask<bool> TryProduceAsync(ReadOnlyMemory<T> items);
}
