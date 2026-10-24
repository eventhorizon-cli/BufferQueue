using System.Diagnostics.CodeAnalysis;
using System.Reflection;
using BufferQueue.Memory;

namespace BufferQueue.Tests;

public sealed class ConsumerNotificationConcurrencyTests
{
    private static readonly FieldInfo _pendingDataLockField =
        typeof(BufferPullConsumer<int>).GetField(
            "_pendingDataLock",
            BindingFlags.Instance | BindingFlags.NonPublic)!;

    private static readonly FieldInfo _pendingDataSourceField =
        typeof(BufferPullConsumer<int>).GetField(
            "_pendingDataValueTaskSource",
            BindingFlags.Instance | BindingFlags.NonPublic)!;

    private static readonly FieldInfo _waitingForDataField =
        typeof(BufferPullConsumer<int>).GetField(
            "_waitingForData",
            BindingFlags.Instance | BindingFlags.NonPublic)!;

    [Fact]
    public async Task Notification_During_Wait_Registration_Completes_Exactly_Once()
    {
        var consumer = CreateConsumer("window");
        var partition = new TestPartition(0);
        var gate = _pendingDataLockField.GetValue(consumer)!;
        var source = (PendingDataValueTaskSource<IBufferPartition<int>>)_pendingDataSourceField
            .GetValue(consumer)!;

        var notifiers = new List<Thread>(4);
        var notifierExceptions = new Exception?[4];
        try
        {
            lock (gate)
            {
                _waitingForDataField.SetValue(consumer, 1);
                for (var index = 0; index < notifierExceptions.Length; index++)
                {
                    var notifierIndex = index;
                    var notifier = new Thread(() =>
                    {
                        try
                        {
                            consumer.NotifyNewDataAvailable(partition);
                        }
                        catch (Exception exception)
                        {
                            notifierExceptions[notifierIndex] = exception;
                        }
                    })
                    {
                        IsBackground = true
                    };
                    notifiers.Add(notifier);
                    notifier.Start();
                }

                Assert.True(SpinWait.SpinUntil(
                    () => notifiers.All(notifier =>
                        (notifier.ThreadState & ThreadState.WaitSleepJoin) != 0),
                    TimeSpan.FromSeconds(5)));

                source.Reset();
            }

            foreach (var notifier in notifiers)
            {
                Assert.True(notifier.Join(TimeSpan.FromSeconds(5)));
            }

            Assert.All(notifierExceptions, exception => Assert.Null(exception));
            Assert.True(source.ValueTask.IsCompletedSuccessfully);
            var result = await source.ValueTask;
            Assert.Same(partition, result);

            // A second notification must observe that the waiter was already completed.
            consumer.NotifyNewDataAvailable(partition);
            Assert.True(source.ValueTask.IsCompletedSuccessfully);
        }
        finally
        {
            foreach (var notifier in notifiers)
            {
                if (notifier.IsAlive)
                {
                    notifier.Join(TimeSpan.FromSeconds(5));
                }
            }
        }
    }

    [Fact]
    public async Task Concurrent_Duplicate_Notifications_Wake_One_Consumer_Per_Item()
    {
        const int itemCount = 64;
        var partition = new MemoryBufferPartition<int>(0, 4);
        var consumer = CreateConsumer("notifications");
        consumer.AssignPartitions(partition);
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(15));
        await using var batches = consumer.ConsumeAsync(cancellation.Token).GetAsyncEnumerator();

        for (var value = 0; value < itemCount; value++)
        {
            var pending = batches.MoveNextAsync().AsTask();
            Assert.True(
                SpinWait.SpinUntil(
                    () => (int)_waitingForDataField.GetValue(consumer)! != 0,
                    TimeSpan.FromSeconds(5)),
                "The consumer should publish its waiting state before this item is published.");
            Assert.False(pending.IsCompleted);

            partition.Enqueue(value);
            var notifications = Enumerable.Range(0, 4)
                .Select(_ => Task.Run(() => consumer.NotifyNewDataAvailable(partition), cancellation.Token))
                .ToArray();
            await Task.WhenAll(notifications).WaitAsync(cancellation.Token);

            Assert.True(await pending.WaitAsync(cancellation.Token));
            Assert.Equal([value], batches.Current);
        }
    }

    private static BufferPullConsumer<int> CreateConsumer(string groupName) =>
        new(new BufferPullConsumerOptions
        {
            TopicName = "notification-concurrency",
            GroupName = groupName,
            BatchSize = 1,
            AutoCommit = true
        });

    private sealed class TestPartition(int partitionId) : IBufferPartition<int>
    {
        public int PartitionId => partitionId;

        public void RegisterConsumer(IBufferPartitionConsumer<int> consumer)
        {
        }

        public void UnregisterConsumer(IBufferPartitionConsumer<int> consumer)
        {
        }

        public void Enqueue(int item)
        {
        }

        public bool TryPull(
            string groupName,
            int batchSize,
            [NotNullWhen(true)] out IReadOnlyList<int>? items)
        {
            items = null;
            return false;
        }

        public void Commit(string groupName)
        {
        }
    }
}
