using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BufferQueue.Memory;

namespace BufferQueue.Benchmarks;

[SimpleJob(RuntimeMoniker.Net10_0, launchCount: 1, warmupCount: 3, iterationCount: 10, invocationCount: 1)]
public class PullConsumerSchedulingBenchmark
{
    private const int MessageCount = 65536;
    private IBufferPullConsumer<int> _consumer = null!;

    [Params("Single", "All", "Sparse", "Last")]
    public string Layout { get; set; } = "Single";

    [Params(1, 64)]
    public int BatchSize { get; set; }

    [IterationSetup]
    public void Setup()
    {
        var count = Layout == "Single" ? 1 : 8;
        var partitions = Enumerable.Range(0, count)
            .Select(index => new MemoryBufferPartition<int>(index, MessageCount)).ToArray();
        foreach (var partition in partitions)
        {
            if (Layout == "Sparse" && partition.PartitionId is not (0 or 7) ||
                Layout == "Last" && partition.PartitionId != 7)
            {
                continue;
            }

            for (var i = 0; i < MessageCount; i++)
            {
                partition.Enqueue(i);
            }
        }
        var consumer = new BufferPullConsumer<int>(new BufferPullConsumerOptions
        {
            TopicName = "benchmark",
            GroupName = "group",
            BatchSize = BatchSize,
            AutoCommit = true
        });
        consumer.AssignPartitions(partitions);
        _consumer = consumer;
    }

    [Benchmark(OperationsPerInvoke = MessageCount)]
    public async Task Consume()
    {
        var remaining = MessageCount / BatchSize;
        await foreach (var batch in _consumer.ConsumeAsync())
        {
            // TryPull prepares each batch; measure scheduling without application handler work.
            if (--remaining == 0)
            {
                break;
            }
        }
    }
}
