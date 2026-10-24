using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BufferQueue.Memory;

namespace BufferQueue.Benchmarks;

[SimpleJob(RuntimeMoniker.Net10_0, launchCount: 1, warmupCount: 6, iterationCount: 20, invocationCount: 1)]
public class MemoryPartialBatchAllocationBenchmark
{
    private const int MessageCount = 8192;
    private IBufferProducer<int> _producer = null!;
    private IBufferPullConsumer<int>[] _consumers = null!;

    [ParamsSource(nameof(Scenarios))]
    public Scenario Case { get; set; } = null!;

    public IEnumerable<Scenario> Scenarios()
    {
        foreach (var bounded in new[] { false, true })
        {
            foreach (var autoCommit in new[] { false, true })
            {
                foreach (var available in new[] { 1, 2, 10 })
                {
                    yield return new(1000, available, bounded, autoCommit, 1);
                }

                yield return new(100, 0, bounded, autoCommit, 12);
            }
        }

        foreach (var batchSize in new[] { 10, 50, 100 })
        {
            yield return new(batchSize, 2, false, true, 1);
        }

        foreach (var batchSize in new[] { 1, 10, 100, 1000 })
        {
            yield return new(batchSize, 0, false, true, 1);
        }

        yield return new(100, 0, true, false, 1);
    }

    [IterationSetup]
    public void Setup()
    {
        var queue = new MemoryBufferQueue<int>(new MemoryBufferQueueOptions
        {
            TopicName = "partial-allocation",
            PartitionNumber = Case.Consumers,
            BoundedCapacity = Case.Bounded ? (Case.Available == 0 ? MessageCount : 64UL) : null
        });
        _producer = queue.GetProducer();
        _consumers = queue.CreateConsumers(new BufferPullConsumerOptions
        {
            TopicName = "partial-allocation",
            GroupName = "Group",
            BatchSize = Case.BatchSize,
            AutoCommit = Case.AutoCommit
        }, Case.Consumers).ToArray();
        if (Case.Available == 0)
        {
            for (var item = 0; item < MessageCount; item++)
            {
                _producer.ProduceAsync(item).GetAwaiter().GetResult();
            }
        }
    }

    [Benchmark]
    public Task<long> Consume() => Case.Available == 0 ? ConsumePrefilledAsync() : ConsumeSparseAsync();

    private async Task<long> ConsumeSparseAsync()
    {
        // Produce one available slice, then consume it. Production is included;
        // this does not measure concurrent throughput.
        var consumer = _consumers[0];
        await using var batches = consumer.ConsumeAsync().GetAsyncEnumerator();
        var consumed = 0;
        var checksum = 0L;
        while (consumed < MessageCount)
        {
            var available = Math.Min(Case.Available, MessageCount - consumed);
            for (var item = consumed; item < consumed + available; item++)
            {
                await _producer.ProduceAsync(item);
            }

            if (!await batches.MoveNextAsync())
            {
                throw new InvalidOperationException("Expected a sparse batch.");
            }

            var actual = 0;
            foreach (var item in batches.Current)
            {
                actual++;
                checksum += item;
            }

            if (actual != available)
            {
                throw new InvalidOperationException($"Expected {available} available items, got {actual}.");
            }

            consumed += actual;
            if (!Case.AutoCommit)
            {
                await consumer.CommitAsync();
            }
        }

        Validate(consumed, checksum);
        return checksum;
    }

    private async Task<long> ConsumePrefilledAsync()
    {
        var tasks = _consumers.Select((consumer, index) => Task.Run(async () =>
        {
            var expected = MessageCount / Case.Consumers + (index < MessageCount % Case.Consumers ? 1 : 0);
            var consumed = 0;
            var checksum = 0L;
            await foreach (var batch in consumer.ConsumeAsync())
            {
                foreach (var item in batch)
                {
                    consumed++;
                    checksum += item;
                }

                if (!Case.AutoCommit)
                {
                    await consumer.CommitAsync();
                }

                if (consumed >= expected)
                {
                    break;
                }
            }

            if (consumed != expected)
            {
                throw new InvalidOperationException($"Expected {expected} items, got {consumed}.");
            }

            return (Count: consumed, Checksum: checksum);
        })).ToArray();
        var results = await Task.WhenAll(tasks);
        var total = results.Sum(result => result.Count);
        var checksum = results.Sum(result => result.Checksum);
        Validate(total, checksum);
        return checksum;
    }

    private static void Validate(int count, long checksum)
    {
        if (count != MessageCount || checksum != (long)MessageCount * (MessageCount - 1) / 2)
        {
            throw new InvalidOperationException($"Unexpected count/checksum: {count}/{checksum}.");
        }
    }

    public sealed record Scenario(int BatchSize, int Available, bool Bounded, bool AutoCommit, int Consumers)
    {
        public override string ToString() =>
            $"{(Available == 0 ? "full" : $"sparse{Available}")}-batch{BatchSize}-{(Bounded ? "bounded" : "unbounded")}-{(AutoCommit ? "auto" : "manual")}-{Consumers}c";
    }
}
