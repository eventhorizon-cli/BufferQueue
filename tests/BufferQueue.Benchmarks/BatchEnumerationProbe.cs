using System.Diagnostics;
using System.Threading.Channels;
using BufferQueue.Memory;

namespace BufferQueue.Benchmarks;

// An isolated synchronous drain probe, separate from the end-to-end BenchmarkDotNet suite.
internal static class BatchEnumerationProbe
{
    private const int ItemCount = 8192;
    private const int PartitionCount = 12;

    public static void Run()
    {
        Console.WriteLine("payload,backend,traversal,batchSize,actualItems,bounded,autoCommit,consumers,sample,drains,nsPerDrain,bytesPerDrain");
        foreach (var batchSize in new[] { 1, 10, 1000 })
        {
            RunCase(batchSize, ItemCount, false, true, 1, static value => value, static value => value);
        }

        RunCase(1, ItemCount, false, true, 1, static value => new ReferenceItem(value), static item => item.Value);
        RunCase(1, ItemCount, false, false, 1, static value => value, static value => value);
        RunCase(1, ItemCount, true, true, 1, static value => value, static value => value);
        RunCase(1, ItemCount, false, true, 12, static value => value, static value => value);
        RunCase(1000, PartitionCount, false, true, 1, static value => value, static value => value);
    }

    private static void RunCase<T>(int batchSize, int itemCount, bool bounded, bool autoCommit, int consumerCount,
        Func<int, T> create, Func<T, int> value)
    {
        var payload = Enumerable.Range(0, itemCount).Select(create).ToArray();
        foreach (var traversal in new[] { "foreach", "count" })
        {
            Measure(typeof(T).Name, "Memory", traversal, batchSize, itemCount, bounded, autoCommit, consumerCount,
                () => PrepareQueue(payload, value, batchSize, bounded, autoCommit, consumerCount, traversal == "foreach"));
        }

        // One channel for the same total payload; consumer loops share its completed reader.
        Measure(typeof(T).Name, "Channel", "foreach", batchSize, itemCount, bounded, autoCommit, consumerCount,
            () => PrepareChannel(payload, value, bounded, consumerCount));
    }

    private static void Measure(string payload, string backend, string traversal, int batchSize, int itemCount,
        bool bounded, bool autoCommit, int consumers, Func<Action> prepare)
    {
        for (var warmup = 0; warmup < 6; warmup++)
        {
            prepare()();
        }

        for (var sample = 0; sample < 20; sample++)
        {
            long elapsed = 0;
            long allocated = 0;
            var drains = 0;
            do
            {
                var drain = prepare();
                var beforeBytes = GC.GetAllocatedBytesForCurrentThread();
                var before = Stopwatch.GetTimestamp();
                drain();
                elapsed += Stopwatch.GetTimestamp() - before;
                allocated += GC.GetAllocatedBytesForCurrentThread() - beforeBytes;
                drains++;
            } while (elapsed < Stopwatch.Frequency / 100);

            var nanoseconds = elapsed * (1_000_000_000.0 / Stopwatch.Frequency) / drains;
            Console.WriteLine(FormattableString.Invariant(
                $"{payload},{backend},{traversal},{batchSize},{itemCount},{bounded},{autoCommit},{consumers},{sample},{drains},{nanoseconds:F3},{allocated / (double)drains:F3}"));
        }
    }

    private static Action PrepareQueue<T>(T[] payload, Func<T, int> value, int batchSize, bool bounded,
        bool autoCommit, int consumerCount, bool enumerate)
    {
        var queue = new MemoryBufferQueue<T>(new MemoryBufferQueueOptions
        {
            TopicName = "enumeration-probe",
            PartitionNumber = PartitionCount,
            BoundedCapacity = bounded ? (ulong)payload.Length : null
        });
        Complete(queue.GetProducer().ProduceAsync(payload));
        var consumers = queue.CreateConsumers(new BufferPullConsumerOptions
        {
            TopicName = "enumeration-probe",
            GroupName = "group",
            AutoCommit = autoCommit,
            BatchSize = batchSize
        }, consumerCount).ToArray();

        return () =>
        {
            var count = 0;
            long checksum = 0;
            for (var consumerIndex = 0; consumerIndex < consumers.Length; consumerIndex++)
            {
                var consumer = consumers[consumerIndex];
                var iterator = consumer.ConsumeAsync().GetAsyncEnumerator();
                var expected = consumerCount == 1 ? payload.Length : (payload.Length + 11 - consumerIndex) / 12;
                var consumed = 0;
                while (consumed < expected)
                {
                    var move = iterator.MoveNextAsync();
                    if (!move.IsCompletedSuccessfully || !move.Result)
                    {
                        throw new InvalidOperationException("Unexpected queue suspension or end.");
                    }

                    if (enumerate)
                    {
                        foreach (var item in iterator.Current)
                        {
                            checksum += value(item);
                            consumed++;
                        }
                    }
                    else
                    {
                        consumed += iterator.Current.Count();
                    }

                    if (!autoCommit)
                    {
                        Complete(consumer.CommitAsync());
                    }
                }

                Complete(iterator.DisposeAsync());
                count += consumed;
            }

            Validate(payload.Length, count, checksum, enumerate);
        };
    }

    private static Action PrepareChannel<T>(T[] payload, Func<T, int> value, bool bounded, int consumers)
    {
        var channel = bounded ? Channel.CreateBounded<T>(payload.Length) : Channel.CreateUnbounded<T>();
        foreach (var item in payload)
        {
            if (!channel.Writer.TryWrite(item))
            {
                throw new InvalidOperationException("Channel prefill failed.");
            }
        }

        channel.Writer.Complete();
        return () =>
        {
            var count = 0;
            long checksum = 0;
            for (var consumer = 0; consumer < consumers; consumer++)
            {
                var iterator = channel.Reader.ReadAllAsync().GetAsyncEnumerator();
                var expected = (payload.Length + consumers - 1 - consumer) / consumers;
                for (var consumed = 0; consumed < expected; consumed++)
                {
                    var move = iterator.MoveNextAsync();
                    if (!move.IsCompletedSuccessfully || !move.Result)
                    {
                        throw new InvalidOperationException("Unexpected channel suspension.");
                    }

                    count++;
                    checksum += value(iterator.Current);
                }

                Complete(iterator.DisposeAsync());
            }

            Validate(payload.Length, count, checksum, true);
        };
    }

    private static void Validate(int expected, int count, long checksum, bool enumerate)
    {
        if (count != expected || (enumerate && checksum != (long)expected * (expected - 1) / 2))
        {
            throw new InvalidOperationException("Incorrect item count or checksum.");
        }
    }

    private static void Complete(ValueTask task)
    {
        if (!task.IsCompletedSuccessfully)
        {
            throw new InvalidOperationException("Unexpected asynchronous operation.");
        }

        task.GetAwaiter().GetResult();
    }

    private sealed record ReferenceItem(int Value);
}
