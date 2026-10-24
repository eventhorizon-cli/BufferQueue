using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using BufferQueue.Memory;

namespace BufferQueue.Benchmarks;

public class UnboundedChannelVsMemoryBufferQueueConsumeBenchmark
{

    private Channel<int> _unboundedChannel = null!;
    private IEnumerable<IBufferPullConsumer<int>> _unboundedMemoryBufferQueueConsumers = null!;
    private IEnumerable<IBufferPullConsumer<int>> _unboundedManualCommitConsumers = null!;

    [Params(8192)] public int MessageSize { get; set; }

    [Params(1, 10, 50, 100, 1000)] public int BatchSize { get; set; }

    public IEnumerable<int> ConsumerCounts => new[] { 1, Environment.ProcessorCount }.Distinct();

    [ParamsSource(nameof(ConsumerCounts))]
    public int Consumers { get; set; }

    [IterationSetup]
    public void Setup()
    {
        _unboundedChannel = Channel.CreateUnbounded<int>();

        var unboundedMemoryBufferQueue = new MemoryBufferQueue<int>(new MemoryBufferQueueOptions
        {
            TopicName = "test-unbounded",
            PartitionNumber = Environment.ProcessorCount
        });
        var unboundedMemoryBufferQueueProducer = unboundedMemoryBufferQueue.GetProducer();
        var unboundedManualCommitQueue = new MemoryBufferQueue<int>(new MemoryBufferQueueOptions
        {
            TopicName = "test-unbounded-manual-commit",
            PartitionNumber = Environment.ProcessorCount
        });
        var unboundedManualCommitProducer = unboundedManualCommitQueue.GetProducer();

        for (var i = 0; i < MessageSize; i++)
        {
            _unboundedChannel.Writer.TryWrite(i);
            unboundedMemoryBufferQueueProducer.ProduceAsync(i).GetAwaiter().GetResult();
            unboundedManualCommitProducer.ProduceAsync(i).GetAwaiter().GetResult();
        }

        _unboundedChannel.Writer.TryComplete();
        _unboundedMemoryBufferQueueConsumers = unboundedMemoryBufferQueue.CreateConsumers(
            new BufferPullConsumerOptions
            {
                GroupName = "TestGroup",
                TopicName = "test-unbounded",
                AutoCommit = true,
                BatchSize = BatchSize,
            },
            Consumers);
        _unboundedManualCommitConsumers = unboundedManualCommitQueue.CreateConsumers(
            new BufferPullConsumerOptions
            {
                GroupName = "TestGroup",
                TopicName = "test-unbounded-manual-commit",
                AutoCommit = false,
                BatchSize = BatchSize,
            },
            Consumers);
    }

    [Benchmark]
    public void Channel_TryRead()
    {
        ValidateTotal(ConsumeChannelTryRead(_unboundedChannel), MessageSize);
    }

    [Benchmark(Baseline = true)]
    public Task Channel_ReadAllAsync() => ConsumeChannelReadAllAsync(_unboundedChannel, MessageSize);

    [Benchmark]
    public Task BufferQueue_ConsumeAsync_AutoCommit() =>
        ConsumeMemoryBufferQueueAsync(_unboundedMemoryBufferQueueConsumers, MessageSize, manualCommit: false);

    [Benchmark]
    public Task BufferQueue_ConsumeAsync_ManualCommit() =>
        ConsumeMemoryBufferQueueAsync(_unboundedManualCommitConsumers, MessageSize, manualCommit: true);

    private int[] ConsumeChannelTryRead(Channel<int> channel)
    {
        var tasks = Enumerable.Range(0, Consumers)
            .Select(_ => Task.Run(() =>
            {
                var reader = channel.Reader;
                var consumedCount = 0;
                while (reader.TryRead(out _))
                {
                    consumedCount++;
                }

                return consumedCount;
            }))
            .ToArray();

        return Task.WhenAll(tasks).GetAwaiter().GetResult();
    }

    private async Task ConsumeChannelReadAllAsync(Channel<int> channel, int expectedTotal)
    {
        var tasks = Enumerable.Range(0, Consumers)
            .Select(_ => Task.Run(async () =>
            {
                var consumedCount = 0;
                await foreach (var _ in channel.Reader.ReadAllAsync())
                {
                    consumedCount++;
                }

                return consumedCount;
            }))
            .ToArray();

        ValidateTotal(await Task.WhenAll(tasks), expectedTotal);
    }

    private async Task ConsumeMemoryBufferQueueAsync(
        IEnumerable<IBufferPullConsumer<int>> consumers,
        int expectedTotal,
        bool manualCommit)
    {
        var consumerList = consumers.ToList();
        var expectedCounts = GetExpectedCounts(consumerList.Count, expectedTotal);
        var tasks = consumerList.Select((consumer, index) => Task.Run(async () =>
        {
            var consumedCount = 0;
            await foreach (var items in consumer.ConsumeAsync())
            {
                consumedCount += items.Count();
                if (manualCommit)
                {
                    await consumer.CommitAsync();
                }

                if (consumedCount >= expectedCounts[index])
                {
                    break;
                }
            }

            return consumedCount;
        })).ToArray();

        var consumedCounts = await Task.WhenAll(tasks);
        for (var index = 0; index < consumedCounts.Length; index++)
        {
            if (consumedCounts[index] != expectedCounts[index])
            {
                throw new InvalidOperationException(
                    $"Consumer {index} consumed {consumedCounts[index]} items; expected {expectedCounts[index]}.");
            }
        }

        ValidateTotal(consumedCounts, expectedTotal);
    }

    private static int[] GetExpectedCounts(int consumerCount, int messageSize)
    {
        var baseMessageCount = messageSize / consumerCount;
        var remainder = messageSize % consumerCount;
        return Enumerable.Range(0, consumerCount)
            .Select(index => baseMessageCount + (index < remainder ? 1 : 0))
            .ToArray();
    }

    private static void ValidateTotal(IReadOnlyCollection<int> counts, int expectedTotal)
    {
        if (counts.Sum() != expectedTotal)
        {
            throw new InvalidOperationException(
                $"Consumed {counts.Sum()} items; expected {expectedTotal}.");
        }
    }
}
