using System;
using System.Threading;
using System.Threading.Tasks;

namespace BufferQueue;

public interface IBufferProducer<T>
{
    string TopicName { get; }

    ValueTask<bool> TryProduceAsync(T item, CancellationToken cancellationToken = default);

    ValueTask<bool> TryProduceAsync(
        ReadOnlyMemory<T> items,
        CancellationToken cancellationToken = default);

    ValueTask ProduceAsync(T item, CancellationToken cancellationToken = default);

    ValueTask ProduceAsync(
        ReadOnlyMemory<T> items,
        CancellationToken cancellationToken = default);
}
