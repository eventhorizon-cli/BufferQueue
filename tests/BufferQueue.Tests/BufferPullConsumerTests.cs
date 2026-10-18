using System.Diagnostics.CodeAnalysis;
using System.Reflection;

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

    [Fact]
    public async Task Notification_During_Wait_Registration_Completes_New_Wait()
    {
        var consumer = new BufferPullConsumer<int>(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "group",
            BatchSize = 1
        });
        var partition = new TestPartition(0);
        var pendingDataSource = (PendingDataValueTaskSource<IBufferPartition<int>>)typeof(BufferPullConsumer<int>)
            .GetField("_pendingDataValueTaskSource", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(consumer)!;
        var pendingDataLock = (ReaderWriterLockSlim)typeof(BufferPullConsumer<int>)
            .GetField("_pendingDataLock", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(consumer)!;

        pendingDataLock.EnterWriteLock();
        Task notification;
        try
        {
            notification = Task.Run(() => consumer.NotifyNewDataAvailable(partition));
            Assert.True(
                SpinWait.SpinUntil(
                    () => notification.IsCompleted || pendingDataLock.WaitingUpgradeCount > 0,
                    TimeSpan.FromSeconds(10)));
            pendingDataSource.Reset();
        }
        finally
        {
            pendingDataLock.ExitWriteLock();
        }

        await notification.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(pendingDataSource.ValueTask.IsCompletedSuccessfully);
        Assert.Same(partition, await pendingDataSource.ValueTask);
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

        public bool TryPull(string groupName, int batchSize, [NotNullWhen(true)] out IEnumerable<int>? items)
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
