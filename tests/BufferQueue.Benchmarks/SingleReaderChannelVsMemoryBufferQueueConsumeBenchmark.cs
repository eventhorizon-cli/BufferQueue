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

    [Params(1, 10, 100, 1000)] public int BatchSize { get; set; }

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

    [Benchmark(Baseline = true)]
    public void Channel_Default()
    {
        ConsumeChannel(_defaultChannel);
    }

    [Benchmark]
    public void Channel_SingleReader()
    {
        ConsumeChannel(_singleReaderChannel);
    }

    [Benchmark]
    public void MemoryBufferQueue_Consume()
    {
        ConsumeMemoryBufferQueue(_memoryBufferQueueConsumers);
    }

    private void ConsumeChannel(Channel<int> channel)
    {
        var remaining = MessageSize;
        var task = Task.Run(() =>
        {
            var reader = channel.Reader;
            while (Volatile.Read(ref remaining) > 0 && reader.TryRead(out _))
            {
                Interlocked.Decrement(ref remaining);
            }
        });

        task.Wait();
    }

    private void ConsumeMemoryBufferQueue(IEnumerable<IBufferPullConsumer<int>> consumers)
    {
        var consumer = consumers.Single();
        var task = Task.Run(async () =>
        {
            var consumedCount = 0;
            await foreach (var items in consumer.ConsumeAsync())
            {
                consumedCount += items.Count();
                if (consumedCount >= MessageSize)
                {
                    break;
                }
            }
        });

        task.Wait();
    }
}
