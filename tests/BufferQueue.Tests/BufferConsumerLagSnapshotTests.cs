using System.Diagnostics.CodeAnalysis;
using BufferQueue.Memory;

namespace BufferQueue.Tests;

public sealed class BufferConsumerLagSnapshotTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Lag_Follows_Commit_Mode_And_Does_Not_Change_Replay(bool autoCommit)
    {
        var queue = CreateQueue();
        var consumer = queue.CreateConsumer(Options(autoCommit: autoCommit));
        AssertLag(consumer, 0);
        await queue.GetProducer().ProduceAsync(new[] { 10, 20, 30 });
        AssertLag(consumer, 3);

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var reader = consumer.ConsumeAsync(cancellation.Token).GetAsyncEnumerator();
        Assert.True(await reader.MoveNextAsync());
        Assert.Equal(new[] { 10, 20 }, reader.Current);
        AssertLag(consumer, autoCommit ? 1UL : 3UL);
        if (!autoCommit)
        {
            Assert.True(await reader.MoveNextAsync());
            Assert.Equal(new[] { 10, 20 }, reader.Current);
            await consumer.CommitAsync();
            AssertLag(consumer, 1);
        }
    }

    [Fact]
    public async Task Each_Consumer_Reports_Only_Its_Assignments_And_Group_Progress()
    {
        var queue = CreateQueue(partitions: 4);
        var consumers = queue.CreateConsumers(Options(), 2).ToArray();
        var other = queue.CreateConsumer(Options("other"));
        await queue.GetProducer().ProduceAsync(Enumerable.Range(0, 8).ToArray());
        Assert.All(consumers, consumer => AssertLag(consumer, 4));
        AssertLag(other, 8);

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var reader = consumers[0].ConsumeAsync(cancellation.Token).GetAsyncEnumerator();
        Assert.True(await reader.MoveNextAsync());
        await consumers[0].CommitAsync();
        AssertLag(consumers[0], 2);
        AssertLag(consumers[1], 4);
        AssertLag(other, 8);
    }

    [Fact]
    public async Task Sampling_Does_Not_Release_Bounded_Capacity_Or_Count_Retained_Committed_Items()
    {
        var queue = CreateQueue(capacity: 2);
        var producer = queue.GetProducer();
        var consumer = queue.CreateConsumer(Options());
        await producer.ProduceAsync(new[] { 1, 2 });
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var waiting = producer.ProduceAsync(3, cancellation.Token).AsTask();
        Assert.False(waiting.IsCompleted);
        AssertLag(consumer, 2);
        Assert.False(waiting.IsCompleted);

        await using var reader = consumer.ConsumeAsync(cancellation.Token).GetAsyncEnumerator();
        Assert.True(await reader.MoveNextAsync());
        AssertLag(consumer, 2);
        Assert.False(waiting.IsCompleted);
        await consumer.CommitAsync();
        await waiting;
        AssertLag(consumer, 1);
        Assert.True(await reader.MoveNextAsync());
        Assert.Equal(new[] { 3 }, reader.Current);
        await consumer.CommitAsync();
        AssertLag(consumer, 0);
    }

    [Fact]
    public void Unassigned_Or_Unavailable_Partition_Returns_False_Without_Partial_Result()
    {
        var consumer = new BufferPullConsumer<int>(Options());
        IBufferPullConsumer<int> api = consumer;
        Assert.False(api.TryGetLagSnapshot(out var snapshot));
        Assert.Equal(default, snapshot);

        var first = new MemoryBufferPartition<int>(0, 2);
        var second = new MemoryBufferPartition<int>(1, 2);
        consumer.AssignPartitions(first, second);
        first.Enqueue(42);
        AssertLag(api, 1);
        second.UnregisterConsumer(consumer);
        Assert.False(api.TryGetLagSnapshot(out snapshot));
        Assert.Equal(default, snapshot);
        consumer.UnassignPartitions();
        Assert.False(api.TryGetLagSnapshot(out snapshot));
        Assert.Equal(default, snapshot);
    }

    [Fact]
    public void Custom_Consumer_Default_Implementation_Returns_False()
    {
        IBufferPullConsumer<int> consumer = new CustomConsumer();
        Assert.False(consumer.TryGetLagSnapshot(out var snapshot));
        Assert.Equal(default, snapshot);
    }

    [Fact]
    public void Aggregate_Saturates_And_Rejects_Mixed_Units()
    {
        var consumer = new BufferPullConsumer<int>(Options());
        IBufferPullConsumer<int> api = consumer;
        consumer.AssignPartitions(new LagPartition(ulong.MaxValue, BufferOffsetUnit.Items),
            new LagPartition(1, BufferOffsetUnit.Items));
        AssertLag(api, ulong.MaxValue);
        consumer.UnassignPartitions();
        consumer.AssignPartitions(new LagPartition(1, BufferOffsetUnit.Items),
            new LagPartition(1, BufferOffsetUnit.Bytes));
        Assert.False(api.TryGetLagSnapshot(out var snapshot));
        Assert.Equal(default, snapshot);
    }

    [Fact]
    public async Task Concurrent_Sampling_Preserves_Delivery_And_Final_Progress()
    {
        const int itemCount = 5000;
        var queue = CreateQueue(capacity: 64);
        var consumer = queue.CreateConsumer(Options(autoCommit: true));
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var producer = Task.Run(async () =>
        {
            for (var i = 0; i < itemCount; i++)
            {
                await queue.GetProducer().ProduceAsync(i, cancellation.Token);
            }
        }, cancellation.Token);
        var reader = Task.Run(async () =>
        {
            var expected = 0;
            await foreach (var batch in consumer.ConsumeAsync(cancellation.Token))
            {
                foreach (var item in batch)
                {
                    Assert.Equal(expected++, item);
                }

                if (expected == itemCount)
                {
                    break;
                }
            }

            Assert.Equal(itemCount, expected);
        }, cancellation.Token);
        var completion = Task.WhenAll(producer, reader);
        do
        {
            Assert.True(consumer.TryGetLagSnapshot(out var snapshot));
            Assert.InRange(snapshot.OffsetLag, 0UL, (ulong)itemCount);
            await Task.Yield();
        }
        while (!completion.IsCompleted && !cancellation.IsCancellationRequested);

        await completion;
        AssertLag(consumer, 0);
    }

    [Fact]
    public async Task Sampling_Allocates_No_Result_Objects()
    {
        var queue = CreateQueue(partitions: 4);
        var consumer = queue.CreateConsumer(Options());
        await queue.GetProducer().ProduceAsync(new[] { 1, 2, 3, 4 });
        for (var i = 0; i < 1000; i++)
        {
            consumer.TryGetLagSnapshot(out _);
        }

        var before = GC.GetAllocatedBytesForCurrentThread();
        ulong total = 0;
        for (var i = 0; i < 1000; i++)
        {
            if (consumer.TryGetLagSnapshot(out var snapshot))
            {
                total += snapshot.OffsetLag;
            }
        }

        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        Assert.Equal(4000UL, total);
        Assert.Equal(0, allocated);
    }

    private static MemoryBufferQueue<int> CreateQueue(int partitions = 1, ulong? capacity = null) =>
        new(new MemoryBufferQueueOptions<int>
        {
            TopicName = "lag",
            PartitionNumber = partitions,
            SegmentSize = 2,
            BoundedCapacity = capacity
        });

    private static BufferPullConsumerOptions Options(string group = "group", bool autoCommit = false) => new()
    {
        TopicName = "lag",
        GroupName = group,
        BatchSize = 2,
        AutoCommit = autoCommit
    };

    private static void AssertLag(IBufferPullConsumer<int> consumer, ulong expected)
    {
        Assert.True(consumer.TryGetLagSnapshot(out var snapshot));
        Assert.Equal(expected, snapshot.OffsetLag);
        Assert.Equal(BufferOffsetUnit.Items, snapshot.Unit);
    }

    private sealed class CustomConsumer : IBufferPullConsumer<int>
    {
        public string TopicName => "lag";
        public string GroupName => "custom";
        public IAsyncEnumerable<IReadOnlyList<int>> ConsumeAsync(CancellationToken cancellationToken = default)
            => throw new NotSupportedException();
        public ValueTask CommitAsync() => throw new NotSupportedException();
    }

    private sealed class LagPartition(ulong lag, BufferOffsetUnit unit) : IBufferPartition<int>
    {
        public int PartitionId => 0;
        public bool TryGetLagSnapshot(string groupName, out BufferConsumerLagSnapshot snapshot)
        {
            snapshot = new(lag, unit);
            return true;
        }
        public void RegisterConsumer(IBufferPartitionConsumer<int> consumer) { }
        public void UnregisterConsumer(IBufferPartitionConsumer<int> consumer) { }
        public void Enqueue(int item) => throw new NotSupportedException();
        public void Commit(string groupName) => throw new NotSupportedException();
        public bool TryPull(string groupName, int batchSize, [NotNullWhen(true)] out IReadOnlyList<int>? items)
            => throw new NotSupportedException();
    }
}
