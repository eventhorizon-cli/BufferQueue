// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.Collections.Concurrent;
using BenchmarkDotNet.Attributes;
using BufferQueue.Memory;

namespace BufferQueue.Benchmarks;

public class BlockingCollectionVsMemoryBufferQueueConsumeBenchmark
{
    private BlockingCollection<int>? _blockingCollection;
    private MemoryBufferQueue<int>? _memoryBufferQueue;
    private IEnumerable<IBufferPullConsumer<int>> _consumers = null!;

    [Params(8192)] public int MessageSize { get; set; }
    [Params(1, 10, 100, 1000)] public int BatchSize { get; set; }

    [IterationSetup]
    public void Setup()
    {
        _blockingCollection = new BlockingCollection<int>();
        _memoryBufferQueue = new MemoryBufferQueue<int>(new MemoryBufferQueueOptions
        {
            TopicName = "test",
            PartitionNumber = Environment.ProcessorCount
        });
        var producer = _memoryBufferQueue.GetProducer();

        for (var i = 0; i < MessageSize; i++)
        {
            _blockingCollection.Add(i);
            producer.ProduceAsync(i);
        }

        _consumers = _memoryBufferQueue!.CreateConsumers(
            new BufferPullConsumerOptions
            {
                GroupName = "TestGroup",
                TopicName = "test",
                AutoCommit = true,
                BatchSize = BatchSize,
            },
            Environment.ProcessorCount);
    }

    [Benchmark]
    public void BlockingCollection_Consume_Concurrent()
    {
        var collection = _blockingCollection!;
        var remaining = MessageSize;
        var tasks = Enumerable.Range(0, Environment.ProcessorCount).Select(_ => Task.Run(() =>
        {
            while (Volatile.Read(ref remaining) > 0 && collection.TryTake(out _))
            {
                Interlocked.Decrement(ref remaining);
            }
        })).ToArray();

        Task.WaitAll(tasks);
        if (remaining != 0)
        {
            throw new InvalidOperationException($"Expected to consume {MessageSize} items, but {remaining} remain.");
        }
    }

    [Benchmark]
    public void MemoryBufferQueue_Consume_ConcurrentProcessorCountPartitions()
    {
        var consumerList = _consumers.ToList();
        var partitionCount = consumerList.Count;
        var baseMessageCount = MessageSize / partitionCount;
        var remainder = MessageSize % partitionCount;
        var tasks = consumerList.Select((consumer, index) => Task.Run(async () =>
        {
            var expectedCount = baseMessageCount + (index < remainder ? 1 : 0);
            if (expectedCount == 0)
            {
                return;
            }

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
