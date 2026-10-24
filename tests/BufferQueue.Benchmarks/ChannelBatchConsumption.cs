using System.Runtime.CompilerServices;
using System.Threading.Channels;

namespace BufferQueue.Benchmarks;

internal static class ChannelBatchConsumption
{
    public static async Task ConsumeAsync(
        Channel<int> channel,
        int consumerCount,
        int batchSize,
        int expectedTotal)
    {
        var tasks = Enumerable.Range(0, consumerCount)
            .Select(_ => Task.Run(async () =>
            {
                var consumedCount = 0;
                await foreach (var batch in ReadBatchesAsync(channel.Reader, batchSize))
                {
                    consumedCount += batch.Length;
                }

                return consumedCount;
            }))
            .ToArray();

        var consumedCounts = await Task.WhenAll(tasks);
        if (consumedCounts.Sum() != expectedTotal)
        {
            throw new InvalidOperationException(
                $"Consumed {consumedCounts.Sum()} items; expected {expectedTotal}.");
        }
    }

    // Each yielded batch owns a fresh fixed-size array. The final short batch exposes a prefix
    // of its array so the comparator has no trim-copy or pooled-buffer lifetime to hide.
    private static async IAsyncEnumerable<ReadOnlyMemory<int>> ReadBatchesAsync(
        ChannelReader<int> reader,
        int batchSize,
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (batchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(batchSize));
        }

        int[]? buffer = null;
        var count = 0;

        await foreach (var item in reader.ReadAllAsync(cancellationToken).ConfigureAwait(false))
        {
            buffer ??= new int[batchSize];
            buffer[count++] = item;
            if (count != batchSize)
            {
                continue;
            }

            yield return buffer;
            buffer = null;
            count = 0;
        }

        if (count != 0)
        {
            yield return buffer!.AsMemory(0, count);
        }
    }
}
