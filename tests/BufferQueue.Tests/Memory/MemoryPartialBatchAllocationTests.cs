using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public sealed class MemoryPartialBatchAllocationTests
{
    [Fact]
    public void Sparse_Tail_Allocation_Does_Not_Scale_With_Configured_Batch_Limit()
    {
        var partition = new MemoryBufferPartition<int>(0, 32);
        partition.Enqueue(10);
        partition.Enqueue(11);
        Assert.True(partition.TryPull("Group", 10, out _));
        // Warm the same read path before comparing the allocation requested by two upper limits.
        Assert.True(partition.TryPull("Group", 1000, out _));

        var before = GC.GetAllocatedBytesForCurrentThread();
        Assert.True(partition.TryPull("Group", 10, out var smallLimit));
        var smallAllocation = GC.GetAllocatedBytesForCurrentThread() - before;
        before = GC.GetAllocatedBytesForCurrentThread();
        Assert.True(partition.TryPull("Group", 1000, out var largeLimit));
        var largeAllocation = GC.GetAllocatedBytesForCurrentThread() - before;

        Assert.Equal(new[] { 10, 11 }, smallLimit);
        Assert.Equal(smallLimit, largeLimit);
        Assert.Equal(smallAllocation, largeAllocation);
    }

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    [InlineData(32)]
    public void Partial_Snapshot_Survives_Tail_Append_Replay_Commit_And_Recycling(int segmentSize)
    {
        var partition = new MemoryBufferPartition<string>(0, segmentSize);
        partition.Enqueue("first");
        partition.Enqueue("second");
        Assert.True(partition.TryPull("Group", 1000, out var held));
        Assert.Equal(new[] { "first", "second" }, held);

        partition.Enqueue("third");
        partition.Enqueue("fourth");
        Assert.True(partition.TryPull("Group", 1000, out var replay));
        Assert.Equal(new[] { "first", "second", "third", "fourth" }, replay);
        partition.Commit("Group");

        for (var index = 0; index < segmentSize * 4; index++)
        {
            partition.Enqueue($"next-{index}");
            Assert.True(partition.TryPull("Group", 1000, out var next));
            Assert.Equal(new[] { $"next-{index}" }, next);
            partition.Commit("Group");
        }

        Assert.Equal(new[] { "first", "second" }, held);
        Assert.Equal(new[] { "first", "second", "third", "fourth" }, replay);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Partial_Batch_Commits_Release_Only_Delivered_Bounded_Capacity(bool autoCommit)
    {
        var queue = new MemoryBufferQueue<int>(new MemoryBufferQueueOptions
        {
            TopicName = "partial-capacity",
            PartitionNumber = 1,
            SegmentSize = 2,
            BoundedCapacity = 4,
            FullMode = BufferQueueFullMode.Fail
        });
        var producer = queue.GetProducer();
        var consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "partial-capacity",
            GroupName = "Group",
            BatchSize = 1000,
            AutoCommit = autoCommit
        });
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var batches = consumer.ConsumeAsync(cancellation.Token).GetAsyncEnumerator();

        await producer.ProduceAsync(new[] { 0, 1 }.AsMemory(), cancellation.Token);
        Assert.True(await batches.MoveNextAsync());
        var held = batches.Current;
        await producer.ProduceAsync(new[] { 2, 3 }.AsMemory(), cancellation.Token);
        if (!autoCommit)
        {
            Assert.False(await producer.TryProduceAsync(4, cancellation.Token));
            await consumer.CommitAsync();
        }

        Assert.True(await producer.TryProduceAsync(4, cancellation.Token));
        Assert.True(await producer.TryProduceAsync(5, cancellation.Token));
        Assert.False(await producer.TryProduceAsync(6, cancellation.Token));
        Assert.True(await batches.MoveNextAsync());
        Assert.Equal(new[] { 2, 3, 4, 5 }, batches.Current);
        Assert.Equal(new[] { 0, 1 }, held);
    }

    [Fact]
    public void Cross_Segment_Partial_Batch_Exposes_Only_Delivered_Items()
    {
        var partition = new MemoryBufferPartition<int>(0, 3);
        foreach (var value in new[] { 10, 11, 12, 13, 14 })
        {
            partition.Enqueue(value);
        }

        Assert.True(partition.TryPull("Group", 10, out var items));
        var list = Assert.IsAssignableFrom<IReadOnlyList<int>>(items);
        Assert.Equal(5, list.Count);
        Assert.Equal(10, list[0]);
        Assert.Equal(14, list[4]);
        Assert.Throws<ArgumentOutOfRangeException>(() => list[-1]);
        Assert.Throws<ArgumentOutOfRangeException>(() => list[5]);
        Assert.Equal(new[] { 10, 11, 12, 13, 14 }, list);

        var collection = Assert.IsAssignableFrom<ICollection<int>>(items);
        Assert.True(collection.IsReadOnly);
        Assert.True(collection.Contains(14));
        Assert.False(collection.Contains(0));
        var copy = Enumerable.Repeat(-1, 7).ToArray();
        collection.CopyTo(copy, 1);
        Assert.Equal(new[] { -1, 10, 11, 12, 13, 14, -1 }, copy);
        Assert.Throws<NotSupportedException>(() => collection.Add(15));
        Assert.Throws<NotSupportedException>(() => collection.Remove(10));
        Assert.Throws<NotSupportedException>(() => collection.Clear());
    }

}
