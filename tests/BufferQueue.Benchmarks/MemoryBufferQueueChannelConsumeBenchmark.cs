// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using BufferQueue.Memory;

namespace BufferQueue.Benchmarks;

public class MemoryBufferQueueChannelConsumeBenchmark
{
    private Channel<int> _unboundedChannel = default!;
    private Channel<int> _boundedChannel = default!;
    private IEnumerable<IBufferPullConsumer<int>> _unboundedMemoryBufferQueueConsumers = default!;
    private IEnumerable<IBufferPullConsumer<int>> _boundedMemoryBufferQueueConsumers = default!;

    [Params(4096, 8192)] public int MessageSize { get; set; }

    [Params(1, 10, 100, 1000)] public int BatchSize { get; set; }

    [IterationSetup]
    public void Setup()
    {
        _unboundedChannel = Channel.CreateUnbounded<int>();
        _boundedChannel = Channel.CreateBounded<int>(new BoundedChannelOptions(MessageSize)
        {
            SingleReader = false,
            SingleWriter = false
        });

        var unboundedMemoryBufferQueue = new MemoryBufferQueue<int>(new MemoryBufferQueueOptions
        {
            TopicName = "test-unbounded",
            PartitionNumber = Environment.ProcessorCount
        });
        var boundedMemoryBufferQueue = new MemoryBufferQueue<int>(new MemoryBufferQueueOptions
        {
            TopicName = "test-bounded",
            PartitionNumber = Environment.ProcessorCount,
            BoundedCapacity = (ulong)MessageSize
        });
        var unboundedMemoryBufferQueueProducer = unboundedMemoryBufferQueue.GetProducer();
        var boundedMemoryBufferQueueProducer = boundedMemoryBufferQueue.GetProducer();

        for (var i = 0; i < MessageSize; i++)
        {
            _unboundedChannel.Writer.TryWrite(i);
            _boundedChannel.Writer.TryWrite(i);
            unboundedMemoryBufferQueueProducer.ProduceAsync(i);
            boundedMemoryBufferQueueProducer.ProduceAsync(i);
        }

        _unboundedMemoryBufferQueueConsumers = unboundedMemoryBufferQueue.CreateConsumers(
            new BufferPullConsumerOptions
            {
                GroupName = "TestGroup",
                TopicName = "test-unbounded",
                AutoCommit = true,
                BatchSize = BatchSize,
            },
            Environment.ProcessorCount);
        _boundedMemoryBufferQueueConsumers = boundedMemoryBufferQueue.CreateConsumers(
            new BufferPullConsumerOptions
            {
                GroupName = "TestGroup",
                TopicName = "test-bounded",
                AutoCommit = true,
                BatchSize = BatchSize,
            },
            Environment.ProcessorCount);
    }

    [Benchmark(Baseline = true)]
    public void Channel_Unbounded_Concurrent_Consuming()
    {
        ConsumeChannel(_unboundedChannel);
    }

    [Benchmark]
    public void MemoryBufferQueue_Unbounded_Concurrent_Consuming()
    {
        ConsumeMemoryBufferQueue(_unboundedMemoryBufferQueueConsumers);
    }

    [Benchmark]
    public void Channel_Bounded_Concurrent_Consuming()
    {
        ConsumeChannel(_boundedChannel);
    }

    [Benchmark]
    public void MemoryBufferQueue_Bounded_Concurrent_Consuming()
    {
        ConsumeMemoryBufferQueue(_boundedMemoryBufferQueueConsumers);
    }

    private void ConsumeChannel(Channel<int> channel)
    {
        var remaining = MessageSize;
        var tasks = Enumerable.Range(0, Environment.ProcessorCount).Select(_ => Task.Run(() =>
        {
            var reader = channel.Reader;
            while (Volatile.Read(ref remaining) > 0 && reader.TryRead(out _))
            {
                Interlocked.Decrement(ref remaining);
            }
        })).ToArray();

        Task.WaitAll(tasks);
    }

    private void ConsumeMemoryBufferQueue(IEnumerable<IBufferPullConsumer<int>> consumers)
    {
        var consumerList = consumers.ToList();
        var partitionCount = consumerList.Count;
        var baseMessageCount = MessageSize / partitionCount;
        var remainder = MessageSize % partitionCount;
        var tasks = consumerList.Select((consumer, index) => Task.Run(async () =>
        {
            var expectedCount = baseMessageCount + (index < remainder ? 1 : 0);
            var consumedCount = 0;
            await foreach (var items in consumer.ConsumeAsync())
            {
                consumedCount += items.Count();
                if (consumedCount >= expectedCount)
                {
                    break;
                }
            }
        })).ToArray();

        Task.WaitAll(tasks);
    }
}
