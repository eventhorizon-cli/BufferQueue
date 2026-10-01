using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public class MemoryBufferPartitionNotificationTests
{
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(10);

    [Theory]
    [InlineData(false, false, false)]
    [InlineData(false, false, true)]
    [InlineData(false, true, false)]
    [InlineData(false, true, true)]
    [InlineData(true, false, false)]
    [InlineData(true, false, true)]
    [InlineData(true, true, false)]
    [InlineData(true, true, true)]
    public async Task ProduceAsync_Concurrent_Registration_Preserves_WakeUp_And_Late_Consumption(
        bool keyed, bool bounded, bool batch)
    {
        var options = new MemoryBufferQueueOptions<int>
        {
            TopicName = "test",
            PartitionNumber = 1,
            SegmentSize = 8,
            BoundedCapacity = bounded ? 4 : null
        };
        if (keyed)
        {
            options.UsePartitionKey(item => item);
        }

        var partition = new MemoryBufferPartition<int>(0, 8, new object(), keyed ? null : new object());
        IBufferProducer<int> producer = new MemoryBufferProducer<int>(options, [partition]);
        var notificationStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var resumeNotification = new ManualResetEventSlim();
        partition.RegisterConsumer(new TestConsumer("blocking", _ =>
        {
            notificationStarted.TrySetResult();
            Assert.True(resumeNotification.Wait(_timeout));
        }));
        var waitingConsumer = CreateConsumer("waiting");
        waitingConsumer.AssignPartitions(partition);
        var lateConsumer = CreateConsumer("late");
        using var cancellation = new CancellationTokenSource(_timeout);
        await using var waitingEnumerator = waitingConsumer.ConsumeAsync(cancellation.Token).GetAsyncEnumerator();
        var waitingRead = waitingEnumerator.MoveNextAsync().AsTask();
        Assert.False(waitingRead.IsCompleted);
        var expected = batch ? new[] { 42, 43 } : [42];

        var production = Task.Run(async () =>
        {
            if (batch)
            {
                await producer.ProduceAsync(expected.AsMemory());
            }
            else
            {
                await producer.ProduceAsync(expected[0]);
            }
        });

        try
        {
            try
            {
                await notificationStarted.Task.WaitAsync(_timeout);
                await Task.Run(() => lateConsumer.AssignPartitions(partition)).WaitAsync(_timeout);
            }
            finally
            {
                resumeNotification.Set();
                await production.WaitAsync(_timeout);
            }

            // The original write must wake the existing reader without another write.
            Assert.True(await waitingRead.WaitAsync(_timeout));
            Assert.Equal(expected, waitingEnumerator.Current);

            await using var lateEnumerator = lateConsumer.ConsumeAsync(cancellation.Token).GetAsyncEnumerator();
            Assert.True(await lateEnumerator.MoveNextAsync());
            Assert.Equal(expected, lateEnumerator.Current);
            await lateConsumer.CommitAsync();

            var nextRead = lateEnumerator.MoveNextAsync().AsTask();
            Assert.False(nextRead.IsCompleted);
            await producer.ProduceAsync(99);
            Assert.True(await nextRead.WaitAsync(_timeout));
            Assert.Equal(new[] { 99 }, lateEnumerator.Current);
        }
        finally
        {
            cancellation.Cancel();
            try
            {
                await waitingRead;
            }
            catch (OperationCanceledException)
            {
            }
        }
    }

    [Fact]
    public async Task Enqueue_Concurrent_Unregistration_Preserves_InFlight_Notifications()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        var notificationStarted = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var resumeNotification = new ManualResetEventSlim();
        partition.RegisterConsumer(new TestConsumer("blocking", _ =>
        {
            notificationStarted.TrySetResult();
            Assert.True(resumeNotification.Wait(_timeout));
        }));
        var removedConsumer = new TestConsumer("removed");
        var remainingConsumer = new TestConsumer("remaining");
        partition.RegisterConsumer(removedConsumer);
        partition.RegisterConsumer(remainingConsumer);

        var production = Task.Run(() => partition.Enqueue(42));
        try
        {
            await notificationStarted.Task.WaitAsync(_timeout);
            await Task.Run(() => partition.UnregisterConsumer(removedConsumer)).WaitAsync(_timeout);
        }
        finally
        {
            resumeNotification.Set();
            await production.WaitAsync(_timeout);
        }

        Assert.Equal(1, removedConsumer.NotificationCount);
        Assert.Equal(1, remainingConsumer.NotificationCount);

        partition.Enqueue(43);

        Assert.Equal(1, removedConsumer.NotificationCount);
        Assert.Equal(2, remainingConsumer.NotificationCount);
    }

    [Fact]
    public async Task RegisterConsumer_Concurrent_Groups_All_Receive_Notifications_Once()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        var consumers = Enumerable.Range(0, 16).Select(i => new TestConsumer($"group-{i}")).ToArray();
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var registrations = consumers.Select(consumer => Task.Run(async () =>
        {
            await start.Task;
            partition.RegisterConsumer(consumer);
            partition.RegisterConsumer(consumer);
        })).ToArray();

        start.SetResult();
        await Task.WhenAll(registrations).WaitAsync(_timeout);
        partition.Enqueue(42);

        Assert.All(consumers, consumer => Assert.Equal(1, consumer.NotificationCount));
    }

    private static BufferPullConsumer<int> CreateConsumer(string groupName) => new(new BufferPullConsumerOptions
    {
        TopicName = "test",
        GroupName = groupName,
        BatchSize = 8
    });

    private sealed class TestConsumer(string groupName, Action<IBufferPartition<int>>? onNotification = null)
        : IBufferPartitionConsumer<int>
    {
        private int _notificationCount;

        public string GroupName => groupName;

        public int NotificationCount => Volatile.Read(ref _notificationCount);

        public void NotifyNewDataAvailable(IBufferPartition<int> partition)
        {
            Interlocked.Increment(ref _notificationCount);
            onNotification?.Invoke(partition);
        }
    }
}
