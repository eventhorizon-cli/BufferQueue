using System.Reflection;
using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BufferQueue.Memory;

namespace BufferQueue.Benchmarks;

[SimpleJob(RuntimeMoniker.Net10_0, launchCount: 1, warmupCount: 6, iterationCount: 20, invocationCount: 1)]
public class ChannelReadPathBenchmark
{
    private const int MessageSize = 8192;
    private const string TopicName = "channel-read-path-benchmark";
    private const string GroupName = "channel-read-path-benchmark-group";

    private Channel<int> _channel = null!;
    private IBufferPullConsumer<int> _consumer = null!;
    private MemoryBufferPartition<int> _partition = null!;

    [Params(1, 10, 50, 100)]
    public int BatchSize { get; set; }

    [Params(false, true)]
    public bool Bounded { get; set; }

    [IterationSetup(Target = nameof(Channel_TryRead))]
    public void SetupChannelTryRead() => SetupChannel(singleReader: false);

    [IterationSetup(Target = nameof(Channel_ReadAllAsync))]
    public void SetupChannelReadAllAsync() => SetupChannel(singleReader: false);

    [IterationSetup(Target = nameof(ChannelSingleReader_TryRead))]
    public void SetupChannelSingleReaderTryRead() => SetupChannel(singleReader: true);

    [IterationSetup(Target = nameof(ChannelSingleReader_ReadAllAsync))]
    public void SetupChannelSingleReaderReadAllAsync() => SetupChannel(singleReader: true);

    [IterationSetup(Target = nameof(BufferQueue_AutoCommit))]
    public void SetupBufferQueueAutoCommit() => SetupMemoryQueue(autoCommit: true);

    [IterationSetup(Target = nameof(MemoryPartition_PullAndCommit))]
    public void SetupMemoryPartitionPullAndCommit() => SetupMemoryQueue(autoCommit: false);

    [Benchmark(Baseline = true)]
    public void Channel_TryRead()
    {
        var consumedCount = 0;
        while (_channel.Reader.TryRead(out _))
        {
            consumedCount++;
        }

        ValidateConsumedCount(consumedCount);
    }

    [Benchmark]
    public async Task Channel_ReadAllAsync()
    {
        var consumedCount = 0;
        await foreach (var _ in _channel.Reader.ReadAllAsync())
        {
            consumedCount++;
            if (consumedCount == MessageSize)
            {
                break;
            }
        }

        ValidateConsumedCount(consumedCount);
    }

    [Benchmark]
    public void ChannelSingleReader_TryRead()
    {
        var consumedCount = 0;
        while (_channel.Reader.TryRead(out _))
        {
            consumedCount++;
        }

        ValidateConsumedCount(consumedCount);
    }

    [Benchmark]
    public async Task ChannelSingleReader_ReadAllAsync()
    {
        var consumedCount = 0;
        await foreach (var _ in _channel.Reader.ReadAllAsync())
        {
            consumedCount++;
            if (consumedCount == MessageSize)
            {
                break;
            }
        }

        ValidateConsumedCount(consumedCount);
    }

    [Benchmark]
    public async Task BufferQueue_AutoCommit()
    {
        var consumedCount = 0;
        await foreach (var items in _consumer.ConsumeAsync())
        {
            consumedCount += items.Count();
            if (consumedCount >= MessageSize)
            {
                break;
            }
        }

        ValidateConsumedCount(consumedCount);
    }

    [Benchmark]
    public void MemoryPartition_PullAndCommit()
    {
        // This synchronous partition path includes different scheduling and state-machine layers
        // than BufferQueue_AutoCommit; the measurements are not additive profiling attribution.
        var consumedCount = 0;
        while (consumedCount < MessageSize)
        {
            if (!_partition.TryPull(GroupName, BatchSize, out var items))
            {
                throw new InvalidOperationException("The memory partition returned no data.");
            }

            consumedCount += items.Count();
            _partition.Commit(GroupName);
        }

        ValidateConsumedCount(consumedCount);
    }

    private void SetupChannel(bool singleReader)
    {
        _channel = Bounded
            ? Channel.CreateBounded<int>(new BoundedChannelOptions(MessageSize)
            {
                FullMode = BoundedChannelFullMode.Wait,
                SingleReader = singleReader,
                SingleWriter = false
            })
            : Channel.CreateUnbounded<int>(new UnboundedChannelOptions
            {
                SingleReader = singleReader,
                SingleWriter = false
            });

        for (var i = 0; i < MessageSize; i++)
        {
            if (!_channel.Writer.TryWrite(i))
            {
                throw new InvalidOperationException("The channel could not be prefilled.");
            }
        }
    }

    private void SetupMemoryQueue(bool autoCommit)
    {
        var queue = new MemoryBufferQueue<int>(new MemoryBufferQueueOptions
        {
            TopicName = TopicName,
            PartitionNumber = 1,
            BoundedCapacity = Bounded ? (ulong)MessageSize : null
        });
        var producer = queue.GetProducer();
        for (var i = 0; i < MessageSize; i++)
        {
            producer.ProduceAsync(i).GetAwaiter().GetResult();
        }

        _consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = TopicName,
            GroupName = GroupName,
            AutoCommit = autoCommit,
            BatchSize = BatchSize
        });
        _partition = GetAssignedPartition(_consumer);
    }

    private static MemoryBufferPartition<int> GetAssignedPartition(IBufferPullConsumer<int> consumer)
    {
        var field = consumer.GetType().GetField("_assignedPartitions", BindingFlags.Instance | BindingFlags.NonPublic)
                    ?? throw new InvalidOperationException("Could not locate the assigned partitions.");
        var partitions = (IBufferPartition<int>[])field.GetValue(consumer)!;
        if (partitions.Length != 1 || partitions[0] is not MemoryBufferPartition<int> partition)
        {
            throw new InvalidOperationException("Expected exactly one memory partition.");
        }

        return partition;
    }

    private static void ValidateConsumedCount(int consumedCount)
    {
        if (consumedCount != MessageSize)
        {
            throw new InvalidOperationException(
                $"Expected to consume {MessageSize} items, but consumed {consumedCount}.");
        }
    }
}
