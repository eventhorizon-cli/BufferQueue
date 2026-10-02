using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public class PullConsumerSchedulingTests
{
    public static IEnumerable<object[]> Cases()
    {
        foreach (var autoCommit in new[] { true, false })
        {
            foreach (var active in new[] { new[] { 0, 7 }, new[] { 2, 5 }, new[] { 7 }, Enumerable.Range(0, 8).ToArray() })
            {
                yield return new object[] { autoCommit, active };
            }
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Readable_Partitions_Share_Pulls_And_Preserve_Local_Order(bool autoCommit, int[] active)
    {
        var options = new MemoryBufferQueueOptions<int>
        {
            TopicName = "fairness",
            PartitionNumber = 8,
            SegmentSize = 128
        };
        options.UsePartitionKey(item => item % 8 + 1);
        var queue = new MemoryBufferQueue<int>(options);
        var producer = queue.GetProducer();
        var consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "fairness",
            GroupName = "group",
            BatchSize = 2,
            AutoCommit = autoCommit
        });
        for (var item = 0; item < 100; item++)
        {
            foreach (var partition in active)
            {
                await producer.ProduceAsync(item * 8 + partition);
            }
        }

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var reader = consumer.ConsumeAsync(cancellation.Token).GetAsyncEnumerator();
        for (var pull = 0; pull < 40; pull++)
        {
            Assert.True(await reader.MoveNextAsync());
            var partition = active[pull % active.Length];
            var item = pull / active.Length * 2;
            Assert.Equal(new[] { item * 8 + partition, (item + 1) * 8 + partition }, reader.Current);
            if (!autoCommit)
            {
                await consumer.CommitAsync();
            }
        }
    }

    [Fact]
    public async Task Manual_Commit_Advances_Only_The_Last_Delivered_Partition()
    {
        var options = new MemoryBufferQueueOptions<int>
        {
            TopicName = "fairness",
            PartitionNumber = 8,
            SegmentSize = 128
        };
        options.UsePartitionKey(item => item % 8 + 1);
        var queue = new MemoryBufferQueue<int>(options);
        var producer = queue.GetProducer();
        foreach (var item in new[] { 0, 7, 8, 15 })
        {
            await producer.ProduceAsync(item);
        }

        var consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "fairness",
            GroupName = "group",
            BatchSize = 1,
            AutoCommit = false
        });
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var reader = consumer.ConsumeAsync(cancellation.Token).GetAsyncEnumerator();
        foreach (var expected in new[] { 0, 7, 0, 15, 0, 0 })
        {
            Assert.True(await reader.MoveNextAsync());
            Assert.Equal(new[] { expected }, reader.Current);
            if (expected is 7 or 15)
            {
                await consumer.CommitAsync();
            }
        }
        await consumer.CommitAsync();
        Assert.True(await reader.MoveNextAsync());
        Assert.Equal(new[] { 8 }, reader.Current);
    }

    [Fact]
    public async Task Notification_And_Newly_Readable_Partitions_Join_The_Rotation()
    {
        var options = new MemoryBufferQueueOptions<int>
        {
            TopicName = "fairness",
            PartitionNumber = 8,
            SegmentSize = 128
        };
        options.UsePartitionKey(item => item + 1);
        var queue = new MemoryBufferQueue<int>(options);
        var producer = queue.GetProducer();
        var consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "fairness",
            GroupName = "group",
            BatchSize = 1,
            AutoCommit = true
        });
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var reader = consumer.ConsumeAsync(cancellation.Token).GetAsyncEnumerator();
        var waiting = reader.MoveNextAsync().AsTask();
        Assert.False(waiting.IsCompleted);
        await producer.ProduceAsync(5);
        Assert.True(await waiting);
        Assert.Equal(new[] { 5 }, reader.Current);
        await producer.ProduceAsync(2);
        await producer.ProduceAsync(7);
        foreach (var expected in new[] { 7, 2 })
        {
            Assert.True(await reader.MoveNextAsync());
            Assert.Equal(new[] { expected }, reader.Current);
        }
        waiting = reader.MoveNextAsync().AsTask();
        Assert.False(waiting.IsCompleted);
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
    }
}
