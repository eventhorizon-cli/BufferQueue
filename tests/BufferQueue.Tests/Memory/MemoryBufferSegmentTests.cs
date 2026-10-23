using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public class MemoryBufferSegmentTests
{
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);

    [Fact]
    public void TryEnqueueSingleWriter_Publishes_Stored_Items()
    {
        var segment = new MemoryBufferSegment<int>(2, default);

        Assert.Equal(0, segment.Count);
        Assert.False(segment.TryGet(default, 2, out _));

        Assert.True(segment.TryEnqueueSingleWriter(1));
        Assert.Equal(1, segment.Count);
        Assert.True(segment.TryGet(default, 2, out var firstItems));
        Assert.Equal(new[] { 1 }, firstItems.ToArray());

        Assert.True(segment.TryEnqueueSingleWriter(2));
        Assert.Equal(2, segment.Count);
        Assert.True(segment.TryGet(default, 2, out var allItems));
        Assert.Equal(new[] { 1, 2 }, allItems.ToArray());

        Assert.False(segment.TryEnqueueSingleWriter(3));
    }

    [Fact]
    public void RecycleSlots_Clears_Items_And_Published_Position()
    {
        var segment = new MemoryBufferSegment<int>(2, default);
        Assert.True(segment.TryEnqueueSingleWriter(1));
        Assert.True(segment.TryEnqueueSingleWriter(2));

        var startOffset = new MemoryBufferPartitionOffset(2);
        var recycledSegment = segment.RecycleSlots(startOffset);

        Assert.Equal(0, recycledSegment.Count);
        Assert.False(recycledSegment.TryGet(startOffset, 2, out _));
        Assert.True(recycledSegment.TryEnqueueSingleWriter(3));
        Assert.Equal(1, recycledSegment.Count);
        Assert.True(recycledSegment.TryGet(startOffset, 2, out var items));
        Assert.Equal(new[] { 3 }, items.ToArray());
    }

    [Fact]
    public void TryGet_Reads_Each_Slot_Across_Index_Wrap()
    {
        var segment = new MemoryBufferSegment<int>(3, new(ulong.MaxValue - 1));
        Assert.True(segment.TryEnqueueSingleWriter(10));
        Assert.True(segment.TryEnqueueSingleWriter(11));
        Assert.True(segment.TryEnqueueSingleWriter(12));

        var offsets = new[]
        {
            new MemoryBufferPartitionOffset(ulong.MaxValue - 1),
            new MemoryBufferPartitionOffset(ulong.MaxValue),
            new MemoryBufferPartitionOffset(0)
        };
        var expected = new[] { 10, 11, 12 };

        for (var index = 0; index < offsets.Length; index++)
        {
            Assert.True(segment.TryGet(offsets[index], 1, out var items));
            Assert.Equal(new[] { expected[index] }, items.ToArray());
        }

        Assert.Equal(new MemoryBufferPartitionOffset(0), segment.EndOffset);
    }

    [Fact]
    public void TryGet_Rejects_Offsets_Outside_Wrapped_Segment()
    {
        var segment = new MemoryBufferSegment<int>(3, new(ulong.MaxValue - 1));
        Assert.True(segment.TryEnqueueSingleWriter(10));
        Assert.True(segment.TryEnqueueSingleWriter(11));
        Assert.True(segment.TryEnqueueSingleWriter(12));

        Assert.False(segment.TryGet(new MemoryBufferPartitionOffset(ulong.MaxValue - 2), 1, out _));
        Assert.False(segment.TryGet(new MemoryBufferPartitionOffset(2), 1, out _));
    }

    [Fact]
    public void RecycleSlots_Restarts_At_Wrapped_Offset()
    {
        var segment = new MemoryBufferSegment<int>(2, new(ulong.MaxValue));
        Assert.True(segment.TryEnqueueSingleWriter(1));
        Assert.True(segment.TryEnqueueSingleWriter(2));

        var recycledSegment = segment.RecycleSlots(new MemoryBufferPartitionOffset(1));

        Assert.Equal(new MemoryBufferPartitionOffset(1), recycledSegment.StartOffset);
        Assert.Equal(new MemoryBufferPartitionOffset(2), recycledSegment.EndOffset);
        Assert.Equal(0, recycledSegment.Count);
        Assert.False(recycledSegment.TryGet(new MemoryBufferPartitionOffset(1), 1, out _));
        Assert.True(recycledSegment.TryEnqueueSingleWriter(3));
        Assert.True(recycledSegment.TryEnqueueSingleWriter(4));
        Assert.True(recycledSegment.TryGet(new MemoryBufferPartitionOffset(1), 2, out var items));
        Assert.Equal(new[] { 3, 4 }, items.ToArray());
    }

    [Fact]
    public async Task Concurrent_Partition_Enqueue_And_Pull_Preserves_Every_Item()
    {
        const int itemCount = 4096;
        const int producerCount = 8;
        var partition = new MemoryBufferPartition<int>(0, 4);
        var consumedItems = new List<int>(itemCount);
        using var cancellationTokenSource = new CancellationTokenSource(_timeout);

        var consumer = Task.Run(async () =>
        {
            while (consumedItems.Count < itemCount)
            {
                cancellationTokenSource.Token.ThrowIfCancellationRequested();
                if (!partition.TryPull("TestGroup", 16, out var items))
                {
                    await Task.Yield();
                    continue;
                }

                consumedItems.AddRange(items);
                partition.Commit("TestGroup");
            }
        }, cancellationTokenSource.Token);

        var producers = Enumerable.Range(1, itemCount)
            .Chunk(itemCount / producerCount)
            .Select(chunk => Task.Run(() =>
            {
                foreach (var item in chunk)
                {
                    partition.Enqueue(item);
                }
            }))
            .ToArray();

        await Task.WhenAll(producers).WaitAsync(_timeout);
        await consumer.WaitAsync(_timeout);

        Assert.Equal(itemCount, consumedItems.Count);
        Assert.Equal(Enumerable.Range(1, itemCount), consumedItems.Order());
    }
}
