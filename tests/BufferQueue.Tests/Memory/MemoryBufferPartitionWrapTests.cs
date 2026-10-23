using System.Reflection;
using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public sealed class MemoryBufferPartitionWrapTests
{
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Consumer_Commit_Releases_Bounded_Producer_Capacity_Across_Wrap(bool autoCommit)
    {
        var segment = CreateSegment(new(ulong.MaxValue - 2), 4);
        var partition = SeedPartition(segment, segment);
        IBufferProducer<int> producer = new MemoryBufferProducer<int>(new MemoryBufferQueueOptions
        {
            TopicName = "Wrap",
            PartitionNumber = 1,
            SegmentSize = 4,
            BoundedCapacity = 4,
            FullMode = BufferQueueFullMode.Fail
        }, [partition]);
        var consumer = new BufferPullConsumer<int>(new BufferPullConsumerOptions
        {
            TopicName = "Wrap",
            GroupName = "Group",
            BatchSize = 2,
            AutoCommit = autoCommit
        });
        consumer.AssignPartitions(partition);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var batches = consumer.ConsumeAsync(cancellation.Token).GetAsyncEnumerator();
        IEnumerable<int>? firstBatch = null;

        for (var cycle = 0; cycle < 5; cycle++)
        {
            for (var item = cycle * 4; item < cycle * 4 + 4; item++)
            {
                Assert.True(await producer.TryProduceAsync(item));
            }

            Assert.False(await producer.TryProduceAsync(-1));
            for (var batch = 0; batch < 2; batch++)
            {
                var expected = Enumerable.Range(cycle * 4 + batch * 2, 2).ToArray();
                Assert.True(await batches.MoveNextAsync());
                Assert.Equal(expected, batches.Current);
                firstBatch ??= batches.Current;
                if (!autoCommit)
                {
                    if (batch == 0)
                    {
                        Assert.False(await producer.TryProduceAsync(-1));
                    }

                    Assert.True(await batches.MoveNextAsync());
                    Assert.Equal(expected, batches.Current);
                    await consumer.CommitAsync();
                }
            }
        }

        Assert.Equal(new[] { 0, 1 }, firstBatch);
        Assert.True(GetHead(partition).StartOffset > segment.StartOffset);
        Assert.Equal(8UL, partition.Capacity);
    }

    [Fact]
    public void TryPull_Replays_Uncommitted_Batch_Across_Index_Wrap()
    {
        var segment = CreateSegment(new(ulong.MaxValue - 1), 3, 10, 11, 12);
        var partition = SeedPartition(segment, segment);

        Assert.True(partition.TryPull("Group", 3, out var firstBatch));
        Assert.Equal(new[] { 10, 11, 12 }, firstBatch);

        Assert.True(partition.TryPull("Group", 3, out var replayedBatch));
        Assert.Equal(new[] { 10, 11, 12 }, replayedBatch);

        partition.Commit("Group");

        Assert.False(partition.TryPull("Group", 1, out _));
    }

    [Fact]
    public void Slow_Reader_Blocks_Recycle_Until_Commit_Across_Index_Wrap()
    {
        var first = CreateSegment(new(ulong.MaxValue - 1), 2, 0, 1);
        var second = CreateSegment(new(0), 2, 2, 3);
        var third = CreateSegment(new(2), 2, 4, 5);
        Link(first, second, third);
        var partition = SeedPartition(first, third);

        Assert.True(partition.TryPull("Fast", 4, out var fastBatch));
        Assert.Equal(new[] { 0, 1, 2, 3 }, fastBatch);
        partition.Commit("Fast");

        Assert.True(partition.TryPull("Slow", 2, out var slowBatch));
        Assert.Equal(new[] { 0, 1 }, slowBatch);

        partition.Enqueue(6);

        Assert.Equal(new MemoryBufferPartitionOffset(ulong.MaxValue - 1), GetHead(partition).StartOffset);
        Assert.Equal(4, GetSegments(partition).Count);
        Assert.True(partition.TryPull("Slow", 2, out slowBatch));
        Assert.Equal(new[] { 0, 1 }, slowBatch);
        partition.Commit("Slow");

        Assert.True(partition.TryPull("Slow", 2, out slowBatch));
        Assert.Equal(new[] { 2, 3 }, slowBatch);
        partition.Commit("Slow");

        partition.Enqueue(7);
        partition.Enqueue(8);

        Assert.Equal(new MemoryBufferPartitionOffset(2), GetHead(partition).StartOffset);
        Assert.Equal(3, GetSegments(partition).Count);
        Assert.True(partition.TryPull("Fast", 5, out fastBatch));
        Assert.Equal(new[] { 4, 5, 6, 7, 8 }, fastBatch);
        Assert.True(partition.TryPull("Slow", 5, out slowBatch));
        Assert.Equal(new[] { 4, 5, 6, 7, 8 }, slowBatch);
    }

    [Fact]
    public void Partial_Commit_Releases_Capacity_And_New_Group_Starts_After_Released_Items()
    {
        var segment = CreateSegment(new(ulong.MaxValue - 1), 4, 0, 1, 2, 3);
        var releasedCounts = new List<ulong>();
        var partition = SeedPartition(segment, segment, releaseCounts: releasedCounts);

        Assert.True(partition.TryPull("First", 2, out var firstBatch));
        Assert.Equal(new[] { 0, 1 }, firstBatch);
        Assert.True(partition.TryPull("Fast", 4, out _));
        partition.Commit("Fast");
        Assert.Equal(new[] { 0UL }, releasedCounts);
        partition.Commit("First");

        Assert.Equal(new[] { 0UL, 2UL }, releasedCounts);
        Assert.True(partition.TryPull("RegisteredAfterRelease", 2, out var remainingBatch));
        Assert.Equal(new[] { 2, 3 }, remainingBatch);
    }

    [Fact]
    public void Count_And_Capacity_Remain_Correct_When_Appending_Across_Index_Wrap()
    {
        var segment = CreateSegment(new(ulong.MaxValue - 1), 4, 0, 1, 2, 3);
        var partition = SeedPartition(segment, segment);

        Assert.Equal(4UL, partition.Capacity);
        Assert.Equal(4UL, partition.Count);

        partition.Enqueue(4);
        partition.Enqueue(5);

        Assert.Equal(8UL, partition.Capacity);
        Assert.Equal(6UL, partition.Count);
    }

    [Fact]
    public void Unbounded_New_Group_Ignores_Stale_Release_Offset_After_Index_Wrap()
    {
        var segment = CreateSegment(new(ulong.MaxValue - 1), 2, 10, 11);
        var partition = SeedPartition(
            segment,
            segment,
            releasedPosition: new MemoryBufferPartitionOffset(0));

        Assert.True(partition.TryPull("NewGroup", 2, out var items));
        Assert.Equal(new[] { 10, 11 }, items);
    }

    private static MemoryBufferSegment<int> CreateSegment(
        MemoryBufferPartitionOffset startOffset,
        int capacity,
        params int[] items)
    {
        var segment = new MemoryBufferSegment<int>(capacity, startOffset);
        foreach (var item in items)
        {
            Assert.True(segment.TryEnqueueSingleWriter(item));
        }

        return segment;
    }

    private static void Link(params MemoryBufferSegment<int>[] segments)
    {
        for (var index = 1; index < segments.Length; index++)
        {
            segments[index - 1].NextSegment = segments[index];
        }
    }

    private static MemoryBufferPartition<int> SeedPartition(
        MemoryBufferSegment<int> head,
        MemoryBufferSegment<int> tail,
        MemoryBufferPartitionOffset? releasedPosition = null,
        List<ulong>? releaseCounts = null)
    {
        var partition = new MemoryBufferPartition<int>(0, head.Capacity);
        SetPrivateField(partition, "_head", head);
        SetPrivateField(partition, "_tail", tail);
        SetPrivateField(partition, "_releasedCapacityPosition", releasedPosition ?? head.StartOffset);

        if (releaseCounts != null)
        {
            partition.SetCapacityReleaseHandler(releaseCounts.Add);
        }

        return partition;
    }

    private static MemoryBufferSegment<int> GetHead(MemoryBufferPartition<int> partition) =>
        (MemoryBufferSegment<int>)typeof(MemoryBufferPartition<int>)
            .GetField("_head", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(partition)!;

    private static List<MemoryBufferSegment<int>> GetSegments(MemoryBufferPartition<int> partition)
    {
        var segments = new List<MemoryBufferSegment<int>>();
        var segment = GetHead(partition);
        while (segment != null)
        {
            segments.Add(segment);
            segment = segment.NextSegment;
        }

        return segments;
    }

    private static void SetPrivateField<T>(MemoryBufferPartition<T> partition, string name, object value) =>
        typeof(MemoryBufferPartition<T>)
            .GetField(name, BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(partition, value);
}
