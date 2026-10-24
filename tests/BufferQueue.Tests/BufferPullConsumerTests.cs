using System.Diagnostics.CodeAnalysis;
using BufferQueue.Memory;

namespace BufferQueue.Tests;

public class BufferPullConsumerTests
{
    [Fact]
    public async Task Notification_Between_Empty_Scan_And_Wait_Is_Not_Lost()
    {
        var first = new TestPartition(0);
        var last = new TestPartition(1);
        var consumer = new BufferPullConsumer<int>(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "group",
            BatchSize = 1,
            AutoCommit = true
        });
        consumer.AssignPartitions(first, last);
        last.OnEmpty = () => first.Enqueue(42);

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var reader = consumer.ConsumeAsync(cancellation.Token).GetAsyncEnumerator();
        Assert.True(await reader.MoveNextAsync());
        Assert.Equal(new[] { 42 }, reader.Current);
        Assert.Equal(1, first.CommitCount);
        Assert.Equal(0, last.CommitCount);
    }

    [Theory]
    [InlineData(true, false, 1)]
    [InlineData(false, false, 1)]
    [InlineData(true, true, 2)]
    [InlineData(false, true, 2)]
    [InlineData(true, false, 4)]
    [InlineData(false, false, 4)]
    public async Task ConsumeAsync_Exposes_Count_And_Index_For_A_Batch(bool autoCommit, bool bounded, int batchSize)
    {
        var queue = new MemoryBufferQueue<int>(new MemoryBufferQueueOptions<int>
        {
            TopicName = "readonly-batch",
            PartitionNumber = 1,
            SegmentSize = 2,
            BoundedCapacity = bounded ? 2UL : null
        });
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        var producer = queue.GetProducer();
        await producer.ProduceAsync(42, cancellation.Token);
        await producer.ProduceAsync(43, cancellation.Token);
        var consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "readonly-batch",
            GroupName = "group",
            BatchSize = batchSize,
            AutoCommit = autoCommit
        });

        await using var reader = consumer.ConsumeAsync(cancellation.Token).GetAsyncEnumerator();
        Assert.True(await reader.MoveNextAsync());
        var batch = reader.Current;
        var expectedCount = Math.Min(2, batchSize);
        Assert.Equal(expectedCount, batch.Count);
        Assert.Equal(42, batch[0]);
        if (expectedCount == 2)
        {
            Assert.Equal(43, batch[1]);
        }

        if (!autoCommit)
        {
            await consumer.CommitAsync();
        }

        await producer.ProduceAsync(44, cancellation.Token);
        Assert.True(await reader.MoveNextAsync());
        Assert.Equal(expectedCount == 1 ? 43 : 44, reader.Current[0]);
        if (!autoCommit)
        {
            await consumer.CommitAsync();
        }

        Assert.Equal(expectedCount, batch.Count);
        Assert.Equal(42, batch[0]);
        if (expectedCount == 2)
        {
            Assert.Equal(43, batch[1]);
        }
    }

    [Fact]
    public async Task Empty_Scan_Tries_Each_Assigned_Partition_Once()
    {
        var partitions = Enumerable.Range(0, 8).Select(index => new TestPartition(index)).ToArray();
        var consumer = new BufferPullConsumer<int>(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "group",
            BatchSize = 1
        });
        consumer.AssignPartitions(partitions);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var reader = consumer.ConsumeAsync(cancellation.Token).GetAsyncEnumerator();
        var waiting = reader.MoveNextAsync().AsTask();
        Assert.False(waiting.IsCompleted);
        Assert.All(partitions, partition => Assert.Equal(1, partition.PullCount));
        await cancellation.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
    }

    private sealed class TestPartition(int partitionId) : IBufferPartition<int>
    {
        private readonly Queue<int> _items = new();
        private IBufferPartitionConsumer<int>? _consumer;
        public int PartitionId => partitionId;
        public int PullCount { get; private set; }
        public int CommitCount { get; private set; }
        public Action? OnEmpty { get; set; }

        public void RegisterConsumer(IBufferPartitionConsumer<int> consumer) => _consumer = consumer;
        public void UnregisterConsumer(IBufferPartitionConsumer<int> consumer) => _consumer = null;

        public void Enqueue(int item)
        {
            _items.Enqueue(item);
            _consumer?.NotifyNewDataAvailable(this);
        }

        public bool TryPull(string groupName, int batchSize, [NotNullWhen(true)] out IReadOnlyList<int>? items)
        {
            PullCount++;
            if (_items.TryPeek(out var item))
            {
                items = new[] { item };
                return true;
            }

            var onEmpty = OnEmpty;
            OnEmpty = null;
            onEmpty?.Invoke();
            items = null;
            return false;
        }

        public void Commit(string groupName)
        {
            _items.Dequeue();
            CommitCount++;
        }
    }
}
