using System.Reflection;
using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public sealed class MemoryProducerHandoffTests
{
    private static readonly FieldInfo _activeBatchCountField =
        typeof(MemoryBufferProducer<int>).GetField(
            "_activeRoundRobinBatchAppendCount",
            BindingFlags.Instance | BindingFlags.NonPublic)!;

    private static readonly MethodInfo _endBatchAppendMethod =
        typeof(MemoryBufferProducer<int>).GetMethod(
            "EndRoundRobinBatchAppend",
            BindingFlags.Instance | BindingFlags.NonPublic)!;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ScalarAppend_RetainsCoordinator_WhileWaitingForPartitionLock(bool bounded)
    {
        var coordinator = new object();
        var partitionLock = new object();
        var partitions = new[]
        {
            new MemoryBufferPartition<int>(0, 8, partitionLock, coordinator),
            new MemoryBufferPartition<int>(1, 8, new object(), coordinator)
        };
        var options = new MemoryBufferQueueOptions
        {
            TopicName = "producer-handoff",
            PartitionNumber = partitions.Length,
            BoundedCapacity = bounded ? 8UL : null,
            FullMode = BufferQueueFullMode.Fail
        };
        using var selectionStarted = new ManualResetEventSlim();
        var partitioner = new HandoffPartitioner(selectionStarted);
        var producer = new MemoryBufferProducer<int>(options, partitions, partitioner);

        // The first scalar was selected while a batch was active, so it must use the
        // partition lock. The simulated batch completion changes the fast-path state
        // while that scalar is blocked on the partition lock.
        _activeBatchCountField.SetValue(producer, 1);
        Exception? firstException = null;
        var firstThread = new Thread(() =>
        {
            try
            {
                ProduceSingle(producer, bounded, 1);
            }
            catch (Exception exception)
            {
                firstException = exception;
            }
        });
        firstThread.IsBackground = true;

        var firstStarted = false;
        try
        {
            lock (partitionLock)
            {
                firstThread.Start();
                firstStarted = true;
                Assert.True(selectionStarted.Wait(TimeSpan.FromSeconds(5)));
                Assert.True(SpinWait.SpinUntil(
                    () => (firstThread.ThreadState & ThreadState.WaitSleepJoin) != 0,
                    TimeSpan.FromSeconds(5)));

                AssertCoordinatorHeld(coordinator);

                // EndRoundRobinBatchAppend uses the same atomic decrement as a real batch.
                _endBatchAppendMethod.Invoke(producer, null);
                AssertCoordinatorHeld(coordinator);
            }
        }
        finally
        {
            if (firstStarted)
            {
                firstThread.Join(TimeSpan.FromSeconds(5));
            }
        }

        Assert.False(firstThread.IsAlive);
        Assert.Null(firstException);
        Assert.True(ProduceSingle(producer, bounded, 2));

        Assert.True(partitions[0].TryPull("handoff", 2, out var items));
        Assert.Equal(new[] { 1, 2 }, items);
    }

    private static void AssertCoordinatorHeld(object coordinator)
    {
        var entered = Monitor.TryEnter(coordinator);
        try
        {
            Assert.False(entered, "The scalar append released the coordinator while waiting for its partition lock.");
        }
        finally
        {
            if (entered)
            {
                Monitor.Exit(coordinator);
            }
        }
    }

    private static bool ProduceSingle(MemoryBufferProducer<int> producer, bool bounded, int item)
    {
        if (bounded)
        {
            return producer.TryProduceAsync(item).GetAwaiter().GetResult();
        }

        producer.ProduceAsync(item).GetAwaiter().GetResult();
        return true;
    }

    private sealed class HandoffPartitioner : IPartitioner<int>, IRoundRobinBatchPartitioner
    {
        public HandoffPartitioner(ManualResetEventSlim selectionStarted)
        {
            SelectionStarted = selectionStarted;
        }

        public ManualResetEventSlim SelectionStarted { get; }

        public bool SupportsConcurrentSelection => false;

        public int SelectPartition(int item, int partitionCount)
        {
            SelectionStarted.Set();
            return 0;
        }

        public int ReserveBatch(int itemCount, int partitionCount) => 0;
    }
}
