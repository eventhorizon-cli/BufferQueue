using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using BufferQueue.Memory;

namespace BufferQueue.Benchmarks;

public class SingleReaderChannelVsMemoryBufferQueueConsumeBenchmark
{
    private const int ConsumerCount = 1;

    private Channel<int> _defaultChannel = null!;
    private Channel<int> _singleReaderChannel = null!;
    private IEnumerable<IBufferPullConsumer<int>> _memoryBufferQueueConsumers = null!;

    [Params(8192)] public int MessageSize { get; set; }

    [Params(1, 10, 50, 100, 1000)] public int BatchSize { get; set; }

    [IterationSetup]
    public void Setup()
    {
        _defaultChannel = Channel.CreateUnbounded<int>();
        _singleReaderChannel = Channel.CreateUnbounded<int>(new UnboundedChannelOptions
        {
            SingleReader = true,
            SingleWriter = false,
            AllowSynchronousContinuations = false
        });

        var memoryBufferQueue = new MemoryBufferQueue<int>(new MemoryBufferQueueOptions
        {
            TopicName = "test-single-reader",
            PartitionNumber = Environment.ProcessorCount
        });
        var producer = memoryBufferQueue.GetProducer();

        for (var i = 0; i < MessageSize; i++)
        {
            _defaultChannel.Writer.TryWrite(i);
            _singleReaderChannel.Writer.TryWrite(i);
            producer.ProduceAsync(i).GetAwaiter().GetResult();
        }

        _defaultChannel.Writer.TryComplete();
        _singleReaderChannel.Writer.TryComplete();
        _memoryBufferQueueConsumers = memoryBufferQueue.CreateConsumers(
            new BufferPullConsumerOptions
            {
                GroupName = "TestGroup",
                TopicName = "test-single-reader",
                AutoCommit = true,
                BatchSize = BatchSize
            },
            ConsumerCount);
    }

    [Benchmark]
    public void Channel_Default_TryRead()
    {
        ValidateTotal(ConsumeChannelTryRead(_defaultChannel), MessageSize);
    }

    [Benchmark(Baseline = true)]
    public Task Channel_Default_ReadAllAsync() =>
        ConsumeChannelReadAllAsync(_defaultChannel, MessageSize);

    [Benchmark]
    public void Channel_SingleReader_TryRead()
    {
        ValidateTotal(ConsumeChannelTryRead(_singleReaderChannel), MessageSize);
    }

    [Benchmark]
    public Task Channel_SingleReader_ReadAllAsync() =>
        ConsumeChannelReadAllAsync(_singleReaderChannel, MessageSize);

    [Benchmark]
    public Task BufferQueue_ConsumeAsync_AutoCommit() =>
        ConsumeMemoryBufferQueueAsync(_memoryBufferQueueConsumers, MessageSize);

    private static int[] ConsumeChannelTryRead(Channel<int> channel)
    {
        var tasks = Enumerable.Range(0, ConsumerCount)
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

    private static async Task ConsumeChannelReadAllAsync(Channel<int> channel, int expectedTotal)
    {
        var tasks = Enumerable.Range(0, ConsumerCount)
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

    private static async Task ConsumeMemoryBufferQueueAsync(
        IEnumerable<IBufferPullConsumer<int>> consumers,
        int expectedTotal)
    {
        var consumer = consumers.Single();
        var task = Task.Run(async () =>
        {
            var consumedCount = 0;
            await foreach (var items in consumer.ConsumeAsync())
            {
                consumedCount += items.Count();
                if (consumedCount >= expectedTotal)
                {
                    break;
                }
            }

            return consumedCount;
        });

        var consumedCount = await task;
        if (consumedCount != expectedTotal)
        {
            throw new InvalidOperationException(
                $"Consumed {consumedCount} items; expected {expectedTotal}.");
        }
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
