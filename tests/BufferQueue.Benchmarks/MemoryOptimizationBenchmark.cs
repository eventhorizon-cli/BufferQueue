using System.Diagnostics.CodeAnalysis;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BufferQueue.Memory;

namespace BufferQueue.Benchmarks;

[SimpleJob(RuntimeMoniker.Net10_0, launchCount: 1, warmupCount: 4, iterationCount: 10, invocationCount: 1)]
public class MemoryAutoCommitOptimizationBenchmark
{
    private const int MessageSize = 8192;
    private IBufferPullConsumer<int> _consumer = null!;

    [Params(1, 10, 100)]
    public int BatchSize { get; set; }

    [Params(false, true)]
    public bool Bounded { get; set; }

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
            TopicName = "auto-commit-optimization",
            PartitionNumber = 1,
            BoundedCapacity = Bounded ? MessageSize : null
        });
        var producer = queue.GetProducer();
        for (var value = 0; value < MessageSize; value++)
        {
            producer.ProduceAsync(value).GetAwaiter().GetResult();
        }

        _consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "auto-commit-optimization",
            GroupName = "auto-commit-optimization-group",
            AutoCommit = autoCommit,
            BatchSize = BatchSize
        });
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
            throw new InvalidOperationException($"Expected {MessageSize} items, got {consumedCount}.");
        }

        return consumedCount;
    }
}

[SimpleJob(RuntimeMoniker.Net10_0, launchCount: 1, warmupCount: 4, iterationCount: 10, invocationCount: 1)]
public class MemoryNotificationOptimizationBenchmark
{
    private const int NotificationsPerProducer = 8192;
    private FakePartition _partition = null!;
    private BufferPullConsumer<int> _consumer = null!;

    [Params(1, 4)]
    public int ProducerCount { get; set; }

    [IterationSetup]
    public void Setup()
    {
        _partition = new FakePartition();
        _consumer = new BufferPullConsumer<int>(new BufferPullConsumerOptions
        {
            TopicName = "notification-optimization",
            GroupName = "notification-optimization-group",
            AutoCommit = true,
            BatchSize = 1
        });
        _consumer.AssignPartitions(_partition);
    }

    [Benchmark(Baseline = true)]
    public Task NotifyWithoutWaiters()
    {
        var tasks = new Task[ProducerCount];
        for (var producer = 0; producer < ProducerCount; producer++)
        {
            tasks[producer] = Task.Run(() =>
            {
                for (var i = 0; i < NotificationsPerProducer; i++)
                {
                    _consumer.NotifyNewDataAvailable(_partition);
                }
            });
        }

        return Task.WhenAll(tasks);
    }

    private sealed class FakePartition : IBufferPartition<int>
    {
        public int PartitionId => 0;

        public void RegisterConsumer(IBufferPartitionConsumer<int> consumer)
        {
        }

        public void UnregisterConsumer(IBufferPartitionConsumer<int> consumer)
        {
        }

        public void Enqueue(int item)
        {
        }

        public bool TryPull(string groupName, int batchSize, [NotNullWhen(true)] out IReadOnlyList<int>? items)
        {
            items = null;
            return false;
        }

        public void Commit(string groupName)
        {
        }
    }
}

[SimpleJob(RuntimeMoniker.Net10_0, launchCount: 1, warmupCount: 4, iterationCount: 10, invocationCount: 1)]
public class MemoryGroupProgressOptimizationBenchmark
{
    private const int MessageSize = 8192;
    private MemoryBufferPartition<int> _partition = null!;
    private MemoryBufferProducer<int> _producer = null!;
    private BufferPullConsumer<int>[] _consumers = null!;

    [Params(2, 16)]
    public int GroupCount { get; set; }

    [IterationSetup(Target = nameof(FastReaderWithIdleGroups))]
    public void SetupFastReader() => Setup();

    [IterationSetup(Target = nameof(LockstepReaders))]
    public void SetupLockstepReaders() => Setup();

    [Benchmark(Baseline = true)]
    public int FastReaderWithIdleGroups()
    {
        const string activeGroup = "group-0";
        var consumed = DrainGroup(activeGroup);
        if (consumed != MessageSize)
        {
            throw new InvalidOperationException($"Expected {MessageSize} items, got {consumed}.");
        }

        return consumed;
    }

    [Benchmark]
    public int LockstepReaders()
    {
        var consumed = 0;
        for (var value = 0; value < MessageSize; value++)
        {
            for (var group = 0; group < _consumers.Length; group++)
            {
                var groupName = _consumers[group].GroupName;
                if (!_partition.TryPull(groupName, 1, out var items) || items.Count() != 1)
                {
                    throw new InvalidOperationException("A group did not receive exactly one item.");
                }

                _partition.Commit(groupName);
            }

            consumed++;
        }

        return consumed;
    }

    private void Setup()
    {
        _partition = new MemoryBufferPartition<int>(0, 256);
        _producer = new MemoryBufferProducer<int>(new MemoryBufferQueueOptions
        {
            TopicName = "group-progress-optimization",
            PartitionNumber = 1,
            SegmentSize = 256,
            BoundedCapacity = MessageSize,
            FullMode = BufferQueueFullMode.Fail
        }, [_partition]);
        _consumers = new BufferPullConsumer<int>[GroupCount];
        for (var group = 0; group < GroupCount; group++)
        {
            var consumer = new BufferPullConsumer<int>(new BufferPullConsumerOptions
            {
                TopicName = "group-progress-optimization",
                GroupName = $"group-{group}",
                AutoCommit = false,
                BatchSize = 1
            });
            consumer.AssignPartitions(_partition);
            _consumers[group] = consumer;
        }

        for (var value = 0; value < MessageSize; value++)
        {
            if (!_producer.TryProduceAsync(value).GetAwaiter().GetResult())
            {
                throw new InvalidOperationException("The bounded benchmark could not prefill the queue.");
            }
        }
    }

    private int DrainGroup(string groupName)
    {
        var consumed = 0;
        while (consumed < MessageSize)
        {
            if (!_partition.TryPull(groupName, 1, out var items) || items.Count() != 1)
            {
                throw new InvalidOperationException("The active group did not receive exactly one item.");
            }

            _partition.Commit(groupName);
            consumed++;
        }

        return consumed;
    }
}
