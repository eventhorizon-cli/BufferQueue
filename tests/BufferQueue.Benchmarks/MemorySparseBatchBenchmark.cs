using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BufferQueue.Memory;

namespace BufferQueue.Benchmarks;

[SimpleJob(RuntimeMoniker.Net10_0, launchCount: 1, warmupCount: 6, iterationCount: 20, invocationCount: 1)]
public class MemorySparseBatchBenchmark
{
    private const int MessageSize = 8192;
    private IBufferProducer<int> _producer = null!;
    private IBufferPullConsumer<int> _consumer = null!;

    [Params(1, 10, 100, 1000)]
    public int BatchSize { get; set; }

    [Params(false, true)]
    public bool Bounded { get; set; }

    [IterationSetup(Target = nameof(AutoCommit))]
    public void SetupAutoCommit() => SetupConsumer(autoCommit: true);

    [IterationSetup(Target = nameof(ManualCommit))]
    public void SetupManualCommit() => SetupConsumer(autoCommit: false);

    [Benchmark(Baseline = true)]
    public Task<int> AutoCommit() => ConsumeSparseBatchAsync(autoCommit: true);

    [Benchmark]
    public Task<int> ManualCommit() => ConsumeSparseBatchAsync(autoCommit: false);

    private void SetupConsumer(bool autoCommit)
    {
        var queue = new MemoryBufferQueue<int>(new MemoryBufferQueueOptions
        {
            TopicName = "sparse-batch-benchmark",
            PartitionNumber = 1,
            BoundedCapacity = Bounded ? 64UL : null
        });
        _producer = queue.GetProducer();
        _consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "sparse-batch-benchmark",
            GroupName = "sparse-batch-benchmark-group",
            AutoCommit = autoCommit,
            BatchSize = BatchSize
        });
    }

    private async Task<int> ConsumeSparseBatchAsync(bool autoCommit)
    {
        // This measures sequential production plus consumption. The producer does not run concurrently
        // with the consumer, so the result isolates the sparse-batch allocation path rather than throughput.
        var consumedCount = 0;
        await using var batches = _consumer.ConsumeAsync().GetAsyncEnumerator();
        for (var value = 0; value < MessageSize; value++)
        {
            await _producer.ProduceAsync(value);
            if (!await batches.MoveNextAsync())
            {
                throw new InvalidOperationException("The consumer returned no data.");
            }

            var batch = batches.Current;
            var batchCount = batch.Count();
            if (batchCount != 1)
            {
                throw new InvalidOperationException("The sparse batch contained an unexpected item count.");
            }

            consumedCount += batchCount;
            if (!autoCommit)
            {
                await _consumer.CommitAsync();
            }
        }

        if (consumedCount != MessageSize)
        {
            throw new InvalidOperationException(
                $"Expected to consume {MessageSize} items, but consumed {consumedCount}.");
        }

        return consumedCount;
    }
}
