using System.Reflection;
using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public sealed class MemoryBufferPartitionWrapTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(3)]
    public async Task Concurrent_Readers_And_Writer_Preserve_Backpressure_And_Replay_Across_Wrap(int batchSize)
    {
        const int itemCount = 512;
        var segment = CreateSegment(new(ulong.MaxValue - 3), 4);
        var partition = SeedPartition(segment, segment);
        var producer = new MemoryBufferProducer<int>(new MemoryBufferQueueOptions
        {
            TopicName = "Wrap",
            PartitionNumber = 1,
            SegmentSize = 4,
            BoundedCapacity = 8,
            FullMode = BufferQueueFullMode.Wait
        }, [partition]);
        var fast = new BufferPullConsumer<int>(new BufferPullConsumerOptions
        {
            TopicName = "Wrap",
            GroupName = "Fast",
            BatchSize = batchSize,
            AutoCommit = true
        });
        var slow = new BufferPullConsumer<int>(new BufferPullConsumerOptions
        {
            TopicName = "Wrap",
            GroupName = "Slow",
            BatchSize = batchSize,
            AutoCommit = false
        });
        fast.AssignPartitions(partition);
        slow.AssignPartitions(partition);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var token = cancellation.Token;
        await using var fastBatches = fast.ConsumeAsync(token).GetAsyncEnumerator();
        await using var slowBatches = slow.ConsumeAsync(token).GetAsyncEnumerator();
        var fastItems = new List<int>();
        var slowItems = new List<int>();

        for (var item = 0; item < 8; item++)
        {
            await producer.ProduceAsync(item, token);
        }

        var blockedWrite = producer.ProduceAsync(8, token).AsTask();
        Assert.False(blockedWrite.IsCompleted);
        while (fastItems.Count < 8)
        {
            Assert.True(await fastBatches.MoveNextAsync());
            fastItems.AddRange(fastBatches.Current);
        }

        // The fast group's auto commits must not release the slow group's retained capacity.
        Assert.False(blockedWrite.IsCompleted);
        Assert.True(await slowBatches.MoveNextAsync());
        var heldBatch = slowBatches.Current;
        Assert.Equal(Enumerable.Range(0, batchSize), heldBatch);
        Assert.True(await slowBatches.MoveNextAsync());
        Assert.Equal(heldBatch, slowBatches.Current);
        Assert.False(blockedWrite.IsCompleted);
        slowItems.AddRange(heldBatch);
        await slow.CommitAsync();
        await blockedWrite.WaitAsync(token);

        var writing = Task.Run(async () =>
        {
            for (var item = 9; item < itemCount; item++)
            {
                await producer.ProduceAsync(item, token);
                await Task.Yield();
            }
        }, token);
        var fastReading = Task.Run(() => DrainAsync(fastBatches, fastItems, null), token);
        var slowReading = Task.Run(() => DrainAsync(slowBatches, slowItems, slow), token);
        await Task.WhenAll(writing, fastReading, slowReading).WaitAsync(token);

        Assert.Equal(Enumerable.Range(0, itemCount), fastItems);
        Assert.Equal(Enumerable.Range(0, itemCount), slowItems);
        Assert.Equal(Enumerable.Range(0, batchSize), heldBatch);
        Assert.True(GetHead(partition).StartOffset > segment.StartOffset);

        async Task DrainAsync(
            IAsyncEnumerator<IReadOnlyList<int>> batches,
            List<int> consumed,
            IBufferPullConsumer<int>? manualConsumer)
        {
            while (consumed.Count < itemCount)
            {
                Assert.True(await batches.MoveNextAsync());
                consumed.AddRange(batches.Current);
                if (manualConsumer != null)
                {
                    await Task.Yield();
                    await manualConsumer.CommitAsync();
                }
            }
        }
    }

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
        IReadOnlyList<int>? firstBatch = null;

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

    [Theory]
    [InlineData(2)]
    [InlineData(3)]
    public void Partial_Tail_Snapshot_Replays_Across_Wrap_And_Preserves_Later_Appends(int segmentSize)
    {
        var segment = CreateSegment(new(ulong.MaxValue), segmentSize, 10, 11);
        var partition = SeedPartition(segment, segment);
        Assert.True(partition.TryPull("Group", 1000, out var held));
        Assert.Equal(new[] { 10, 11 }, held);

        partition.Enqueue(12);
        Assert.True(partition.TryPull("Group", 1000, out var replay));
        Assert.Equal(new[] { 10, 11, 12 }, replay);
        partition.Commit("Group");
        partition.Enqueue(13);
        Assert.True(partition.TryPull("Group", 1000, out var next));
        Assert.Equal(new[] { 13 }, next);
        Assert.Equal(new[] { 10, 11 }, held);
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

    [Theory]
    [InlineData(0UL)]
    [InlineData(ulong.MaxValue)]
    public void TryPull_Does_Not_Read_Unpublished_Next_Segment(ulong initialStart)
    {
        var first = CreateSegment(new(initialStart), 1, 42);
        var tail = CreateSegment(new(unchecked(initialStart + 1)), 1);
        Link(first, tail);
        var partition = SeedPartition(first, tail);

        Assert.True(partition.TryPull("Group", 16, out var firstBatch));
        Assert.Equal(new[] { 42 }, firstBatch);
        Assert.True(partition.TryPull("Group", 16, out var replayedBatch));
        Assert.Equal(new[] { 42 }, replayedBatch);

        partition.Commit("Group");
        Assert.False(partition.TryPull("Group", 16, out _));

        partition.Enqueue(43);
        Assert.True(partition.TryPull("Group", 16, out var nextBatch));
        Assert.Equal(new[] { 43 }, nextBatch);
        Assert.Equal(new[] { 42 }, firstBatch);
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

    [Fact]
    public void Lag_Snapshot_Preserves_Wrap_And_Excludes_Committed_Retained_Items()
    {
        var segment = CreateSegment(new(ulong.MaxValue - 1), 2, 10, 11);
        var partition = SeedPartition(segment, segment);
        var consumer = new BufferPullConsumer<int>(new BufferPullConsumerOptions
        {
            TopicName = "lag",
            GroupName = "group"
        });
        consumer.AssignPartitions(partition);
        Assert.True(partition.TryGetLagSnapshot("group", out var snapshot));
        Assert.Equal(2UL, snapshot.OffsetLag);
        Assert.True(partition.TryPull("group", 2, out _));
        partition.Commit("group");
        Assert.True(partition.TryGetLagSnapshot("group", out snapshot));
        Assert.Equal(0UL, snapshot.OffsetLag);

        for (var item = 0; item < 8; item++)
        {
            partition.Enqueue(item);
            Assert.True(partition.TryGetLagSnapshot("group", out snapshot));
            Assert.Equal(1UL, snapshot.OffsetLag);
            Assert.True(partition.TryPull("group", 1, out var items));
            Assert.Equal(new[] { item }, items);
            partition.Commit("group");
        }

        Assert.True(partition.TryGetLagSnapshot("group", out snapshot));
        Assert.Equal(0UL, snapshot.OffsetLag);
    }

    [Fact]
    public void Lag_Snapshot_Clamps_An_Older_Published_Position_To_Zero()
    {
        var oldTail = CreateSegment(new(ulong.MaxValue - 1), 2, 10, 11);
        var newTail = CreateSegment(new(0), 2, 12);
        Link(oldTail, newTail);
        var partition = SeedPartition(oldTail, newTail);
        Assert.True(partition.TryPull("group", 3, out _));
        partition.Commit("group");
        // Model sampling a previous tail before observing a newer commit across the wrap boundary.
        SetPrivateField(partition, "_tail", oldTail);
        Assert.True(partition.TryGetLagSnapshot("group", out var snapshot));
        Assert.Equal(0UL, snapshot.OffsetLag);
    }

    [Fact]
    public void Lag_Snapshot_Does_Not_Create_An_Absent_Reader()
    {
        var partition = new MemoryBufferPartition<int>(0, 2);
        Assert.False(partition.TryGetLagSnapshot("missing", out var snapshot));
        Assert.Equal(default, snapshot);
        Assert.Throws<InvalidOperationException>(() => partition.Commit("missing"));
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
