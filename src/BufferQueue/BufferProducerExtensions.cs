using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;

namespace BufferQueue;

public static class BufferProducerExtensions
{
    public static ValueTask ProduceAsync<T>(this IBufferProducer<T> producer, T item)
    {
        ArgumentNullException.ThrowIfNull(producer);
        return ProduceAsync(producer.TryProduceAsync(item), producer.TopicName, "item");
    }

    public static ValueTask ProduceAsync<T>(this IBufferProducer<T> producer, ReadOnlyMemory<T> items)
    {
        ArgumentNullException.ThrowIfNull(producer);
        return ProduceAsync(producer.TryProduceAsync(items), producer.TopicName, "batch");
    }

    public static ValueTask ProduceAsync<T>(this IBufferProducer<T> producer, IEnumerable<T> items)
    {
        ArgumentNullException.ThrowIfNull(producer);
        return ProduceAsync(producer, GetItems(items));
    }

    public static ValueTask<bool> TryProduceAsync<T>(this IBufferProducer<T> producer, IEnumerable<T> items)
    {
        ArgumentNullException.ThrowIfNull(producer);
        return producer.TryProduceAsync(GetItems(items));
    }

    private static ReadOnlyMemory<T> GetItems<T>(IEnumerable<T> items)
    {
        ArgumentNullException.ThrowIfNull(items);
        return items is T[] array ? array : items.ToArray();
    }

    private static ValueTask ProduceAsync(ValueTask<bool> result, string topicName, string subject)
    {
        if (result.IsCompletedSuccessfully)
        {
            if (result.Result)
            {
                return ValueTask.CompletedTask;
            }

            throw CreateQueueFullException(topicName, subject);
        }

        return AwaitProduceAsync(result, topicName, subject);
    }

    private static async ValueTask AwaitProduceAsync(ValueTask<bool> result, string topicName, string subject)
    {
        if (!await result.ConfigureAwait(false))
        {
            throw CreateQueueFullException(topicName, subject);
        }
    }

    private static BufferQueueFullException CreateQueueFullException(string topicName, string subject) =>
        new($"The queue '{topicName}' is full, and the {subject} cannot be produced.");
}
