using System.Reflection;
using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public sealed class MemoryAutoCommitTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AutoCommit_RecyclesOnlyAfterSlowGroupCommits(bool bounded)
    {
        const int segmentSize = 2;
        var partition = new MemoryBufferPartition<int>(0, segmentSize);
        var options = new MemoryBufferQueueOptions
        {
            TopicName = "auto-commit",
            PartitionNumber = 1,
            SegmentSize = segmentSize,
            BoundedCapacity = bounded ? 4UL : null,
            FullMode = BufferQueueFullMode.Fail
        };
        var producer = bounded
            ? new MemoryBufferProducer<int>(options, [partition])
            : null;
        var autoConsumer = CreateConsumer("auto", autoCommit: true, segmentSize);
        var manualConsumer = CreateConsumer("manual", autoCommit: false, segmentSize);
        autoConsumer.AssignPartitions(partition);
        manualConsumer.AssignPartitions(partition);

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var autoBatches = autoConsumer.ConsumeAsync(cancellation.Token).GetAsyncEnumerator();
        await using var manualBatches = manualConsumer.ConsumeAsync(cancellation.Token).GetAsyncEnumerator();

        Produce(partition, producer, 0, 1);
        Assert.True(await autoBatches.MoveNextAsync());
        var heldAutoBatch = autoBatches.Current;
        Assert.Equal(new[] { 0, 1 }, heldAutoBatch);
        Assert.True(await manualBatches.MoveNextAsync());
        Assert.Equal(new[] { 0, 1 }, manualBatches.Current);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await autoConsumer.CommitAsync());

        Produce(partition, producer, 2, 3);
        Assert.True(await autoBatches.MoveNextAsync());
        Assert.Equal(new[] { 2, 3 }, autoBatches.Current);
        if (bounded)
        {
            Assert.False(await producer!.TryProduceAsync(4));
        }

        // The manual group has not committed its first batch, so the first segment cannot recycle.
        Assert.Equal(new MemoryBufferPartitionOffset(0), GetHead(partition).StartOffset);
        Assert.True(await manualBatches.MoveNextAsync());
        Assert.Equal(new[] { 0, 1 }, manualBatches.Current);
        await manualConsumer.CommitAsync();

        Assert.True(await manualBatches.MoveNextAsync());
        Assert.Equal(new[] { 2, 3 }, manualBatches.Current);
        await manualConsumer.CommitAsync();

        for (var start = 4; start < 16; start += 2)
        {
            Produce(partition, producer, start, start + 1);
            Assert.True(await autoBatches.MoveNextAsync());
            Assert.Equal(new[] { start, start + 1 }, autoBatches.Current);
            Assert.True(await manualBatches.MoveNextAsync());
            Assert.Equal(new[] { start, start + 1 }, manualBatches.Current);
            await manualConsumer.CommitAsync();
        }

        Assert.Equal(new[] { 0, 1 }, heldAutoBatch);
        Assert.True(GetHead(partition).StartOffset > new MemoryBufferPartitionOffset(0));
    }

    private static BufferPullConsumer<int> CreateConsumer(
        string groupName,
        bool autoCommit,
        int segmentSize) =>
        new(new BufferPullConsumerOptions
        {
            TopicName = "auto-commit",
            GroupName = groupName,
            AutoCommit = autoCommit,
            BatchSize = segmentSize
        });

    private static void Produce(
        MemoryBufferPartition<int> partition,
        MemoryBufferProducer<int>? producer,
        int first,
        int second)
    {
        if (producer != null)
        {
            Assert.True(producer.TryProduceAsync(first).Result);
            Assert.True(producer.TryProduceAsync(second).Result);
            return;
        }

        partition.Enqueue(first);
        partition.Enqueue(second);
    }

    private static MemoryBufferSegment<int> GetHead(MemoryBufferPartition<int> partition) =>
        (MemoryBufferSegment<int>)typeof(MemoryBufferPartition<int>)
            .GetField("_head", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(partition)!;
}
