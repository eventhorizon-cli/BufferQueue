using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BufferQueue.Memory;

namespace BufferQueue.Benchmarks;

[SimpleJob(RuntimeMoniker.Net10_0, launchCount: 1, warmupCount: 3, iterationCount: 5)]
public class MemoryConsumerLagSnapshotQueryBenchmark
{
    private const int ItemCount = 32768;
    private IBufferPullConsumer<int> _consumer = null!;

    [Params(1, 8)]
    public int PartitionCount { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var queue = new MemoryBufferQueue<int>(new MemoryBufferQueueOptions<int>
        {
            TopicName = "lag-query-benchmark",
            PartitionNumber = PartitionCount
        });
        queue.GetProducer().ProduceAsync(Enumerable.Range(0, ItemCount).ToArray().AsMemory())
            .GetAwaiter().GetResult();
        _consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "lag-query-benchmark",
            GroupName = "lag-query-benchmark-group",
            BatchSize = 64,
            AutoCommit = false
        });
    }

    [Benchmark]
    public BufferConsumerLagSnapshot Query()
    {
        if (!_consumer.TryGetLagSnapshot(out var snapshot))
        {
            throw new InvalidOperationException("The consumer lag snapshot is unavailable.");
        }

        return snapshot;
    }
}

[SimpleJob(RuntimeMoniker.Net10_0, launchCount: 1, warmupCount: 3, iterationCount: 5, invocationCount: 1024)]
public class MemoryConsumerLagSnapshotSamplingBenchmark
{
    private const int BatchSize = 64;
    private const int BatchCount = 512;
    private IBufferProducer<int> _producer = null!;
    private IBufferPullConsumer<int> _consumer = null!;
    private int[][] _batches = null!;

    [Params(1, 8)]
    public int PartitionCount { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _batches = Enumerable.Range(0, BatchCount)
            .Select(batch => Enumerable.Range(batch * BatchSize, BatchSize).ToArray())
            .ToArray();
    }

    [IterationSetup]
    public void IterationSetup()
    {
        var queue = new MemoryBufferQueue<int>(new MemoryBufferQueueOptions<int>
        {
            TopicName = "lag-sampling-benchmark",
            PartitionNumber = PartitionCount
        });
        _producer = queue.GetProducer();
        _consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "lag-sampling-benchmark",
            GroupName = "lag-sampling-benchmark-group",
            BatchSize = BatchSize,
            AutoCommit = false
        });
    }

    [Benchmark(Baseline = true)]
    public Task<int> ProducePullAndCommit() => ProducePullAndCommitAsync(sampleLag: false);

    [Benchmark]
    public Task<int> ProducePullCommitAndSample() => ProducePullAndCommitAsync(sampleLag: true);

    private async Task<int> ProducePullAndCommitAsync(bool sampleLag)
    {
        await using var batches = _consumer.ConsumeAsync().GetAsyncEnumerator();
        var producedCount = 0;
        var consumedCount = 0;
        var consumedBatches = 0;
        ulong sampledLag = 0;

        foreach (var batch in _batches)
        {
            await _producer.ProduceAsync(batch.AsMemory());
            producedCount += batch.Length;

            while (consumedCount < producedCount)
            {
                if (!await batches.MoveNextAsync())
                {
                    throw new InvalidOperationException("The consumer completed before consuming all produced items.");
                }

                consumedCount += batches.Current.Count;
                consumedBatches++;
                await _consumer.CommitAsync();

                if (sampleLag && consumedBatches % 64 == 0)
                {
                    if (!_consumer.TryGetLagSnapshot(out var snapshot))
                    {
                        throw new InvalidOperationException("The consumer lag snapshot is unavailable.");
                    }

                    sampledLag += snapshot.OffsetLag;
                }
            }
        }

        if (producedCount != BatchCount * BatchSize || consumedCount != producedCount)
        {
            throw new InvalidOperationException(
                $"Produced {producedCount} items and consumed {consumedCount}; expected {BatchCount * BatchSize}.");
        }

        return consumedCount + (int)sampledLag;
    }
}
