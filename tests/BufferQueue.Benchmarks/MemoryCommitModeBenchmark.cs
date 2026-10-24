using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BufferQueue.Memory;

namespace BufferQueue.Benchmarks;

[SimpleJob(RuntimeMoniker.Net10_0, launchCount: 1, warmupCount: 6, iterationCount: 20, invocationCount: 1)]
public class MemoryCommitModeBenchmark
{
    private const int MessageSize = 8192;
    private IBufferPullConsumer<int> _consumer = null!;

    [Params(1, 10, 100)]
    public int BatchSize { get; set; }

    [Params(1, 12)]
    public int PartitionNumber { get; set; }

    [Params(false, true)]
    public bool Bounded { get; set; }

    [Params(1, 2)]
    public int GroupCount { get; set; }

    [IterationSetup(Target = nameof(AutoCommit))]
    public void SetupAutoCommit() => SetupConsumer(autoCommit: true);

    [IterationSetup(Target = nameof(ManualCommit))]
    public void SetupManualCommit() => SetupConsumer(autoCommit: false);

    [Benchmark(Baseline = true)]
    public Task<int> AutoCommit() => ConsumeAsync(autoCommit: true);

    [Benchmark]
    public Task<int> ManualCommit() => ConsumeAsync(autoCommit: false);

    private void SetupConsumer(bool autoCommit)
    {
        var queue = new MemoryBufferQueue<int>(new MemoryBufferQueueOptions
        {
            TopicName = "commit-mode-benchmark",
            PartitionNumber = PartitionNumber,
            BoundedCapacity = Bounded ? MessageSize : null
        });
        var producer = queue.GetProducer();
        for (var i = 0; i < MessageSize; i++)
        {
            producer.ProduceAsync(i).GetAwaiter().GetResult();
        }

        _consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "commit-mode-benchmark",
            GroupName = "commit-mode-benchmark-group",
            AutoCommit = autoCommit,
            BatchSize = BatchSize
        });

        // The additional group stays idle to exercise shared retention and the multiple-group lookup path.
        // Queues are prefilled; no production or capacity waiting occurs during measurement.
        if (GroupCount == 2)
        {
            queue.CreateConsumer(new BufferPullConsumerOptions
            {
                TopicName = "commit-mode-benchmark",
                GroupName = "idle-group",
                AutoCommit = false,
                BatchSize = BatchSize
            });
        }
    }

    private async Task<int> ConsumeAsync(bool autoCommit)
    {
        var consumedCount = 0;
        await foreach (var batch in _consumer.ConsumeAsync())
        {
            consumedCount += batch.Count();
            if (!autoCommit)
            {
                await _consumer.CommitAsync();
            }

            if (consumedCount >= MessageSize)
            {
                break;
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
