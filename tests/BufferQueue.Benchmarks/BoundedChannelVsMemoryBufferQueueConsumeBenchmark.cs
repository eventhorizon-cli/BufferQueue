using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using BufferQueue.Memory;

namespace BufferQueue.Benchmarks;

public class BoundedChannelVsMemoryBufferQueueConsumeBenchmark
{

    private Channel<int> _boundedChannel = null!;
    private IEnumerable<IBufferPullConsumer<int>> _boundedMemoryBufferQueueConsumers = null!;
    private IEnumerable<IBufferPullConsumer<int>> _boundedManualCommitConsumers = null!;

    [Params(8192)] public int MessageSize { get; set; }

    [Params(1, 10, 50, 100, 1000)] public int BatchSize { get; set; }

    public IEnumerable<int> ConsumerCounts => new[] { 1, Environment.ProcessorCount }.Distinct();

    [ParamsSource(nameof(ConsumerCounts))]
    public int Consumers { get; set; }

    [IterationSetup]
    public void Setup()
    {
        _boundedChannel = Channel.CreateBounded<int>(new BoundedChannelOptions(MessageSize)
        {
            SingleReader = false,
            SingleWriter = false
        });

        var boundedMemoryBufferQueue = new MemoryBufferQueue<int>(new MemoryBufferQueueOptions
        {
            TopicName = "test-bounded",
            PartitionNumber = Environment.ProcessorCount,
            BoundedCapacity = (ulong)MessageSize
        });
        var boundedMemoryBufferQueueProducer = boundedMemoryBufferQueue.GetProducer();
        var boundedManualCommitQueue = new MemoryBufferQueue<int>(new MemoryBufferQueueOptions
        {
            TopicName = "test-bounded-manual-commit",
            PartitionNumber = Environment.ProcessorCount,
            BoundedCapacity = (ulong)MessageSize
        });
        var boundedManualCommitProducer = boundedManualCommitQueue.GetProducer();

        for (var i = 0; i < MessageSize; i++)
        {
            _boundedChannel.Writer.TryWrite(i);
            boundedMemoryBufferQueueProducer.ProduceAsync(i).GetAwaiter().GetResult();
            boundedManualCommitProducer.ProduceAsync(i).GetAwaiter().GetResult();
        }

        _boundedChannel.Writer.TryComplete();
        _boundedMemoryBufferQueueConsumers = boundedMemoryBufferQueue.CreateConsumers(
            new BufferPullConsumerOptions
            {
                GroupName = "TestGroup",
                TopicName = "test-bounded",
                AutoCommit = true,
                BatchSize = BatchSize,
            },
            Consumers);
        _boundedManualCommitConsumers = boundedManualCommitQueue.CreateConsumers(
            new BufferPullConsumerOptions
            {
                GroupName = "TestGroup",
                TopicName = "test-bounded-manual-commit",
                AutoCommit = false,
                BatchSize = BatchSize,
            },
            Consumers);
    }

    [Benchmark]
    public void Channel_TryRead()
    {
        ValidateTotal(ConsumeChannelTryRead(_boundedChannel), MessageSize);
    }

    [Benchmark(Baseline = true)]
    public Task Channel_ReadAllAsync() => ConsumeChannelReadAllAsync(_boundedChannel, MessageSize);

    [Benchmark]
    public Task BufferQueue_ConsumeAsync_AutoCommit() =>
        ConsumeMemoryBufferQueueAsync(_boundedMemoryBufferQueueConsumers, MessageSize, manualCommit: false);

    [Benchmark]
    public Task BufferQueue_ConsumeAsync_ManualCommit() =>
        ConsumeMemoryBufferQueueAsync(_boundedManualCommitConsumers, MessageSize, manualCommit: true);

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
