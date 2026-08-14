using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace BufferQueue;

public static class BufferProducerExtensions
{
    public static ValueTask ProduceAsync<T>(
        this IBufferProducer<T> producer,
        IEnumerable<T> items,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(producer);
        ArgumentNullException.ThrowIfNull(items);
        cancellationToken.ThrowIfCancellationRequested();
        return producer.ProduceAsync(GetItems(items), cancellationToken);
    }

    public static ValueTask<bool> TryProduceAsync<T>(
        this IBufferProducer<T> producer,
        IEnumerable<T> items,
        CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(producer);
        ArgumentNullException.ThrowIfNull(items);
        cancellationToken.ThrowIfCancellationRequested();
        return producer.TryProduceAsync(GetItems(items), cancellationToken);
    }

    private static ReadOnlyMemory<T> GetItems<T>(IEnumerable<T> items)
    {
        return items is T[] array ? array : items.ToArray();
    }
}
