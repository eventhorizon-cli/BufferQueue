using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public sealed class MemoryBufferProducerFailureTests
{
    [Fact]
    public async Task TryProduceAsync_SerializedSingle_RollsBackCapacity_WhenSelectionFails()
    {
        var options = new MemoryBufferQueueOptions
        {
            TopicName = "test",
            BoundedCapacity = 1,
            FullMode = BufferQueueFullMode.Fail
        };
        var partitioner = new ControlledPartitioner { ThrowOnItem = 1 };
        var partition = new MemoryBufferPartition<int>(0, 4);
        var producer = new MemoryBufferProducer<int>(options, [partition], partitioner);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await producer.TryProduceAsync(1));

        partitioner.ThrowOnItem = null;
        Assert.True(await producer.TryProduceAsync(2));
        Assert.Equal(1UL, partition.Count);
    }

    [Fact]
    public async Task TryProduceAsync_SerializedSingle_RollsBackCapacity_WhenAppendFails()
    {
        var options = new MemoryBufferQueueOptions
        {
            TopicName = "test",
            BoundedCapacity = 1,
            FullMode = BufferQueueFullMode.Fail
        };
        var partition = new MemoryBufferPartition<int>(0, 0);
        var producer = new MemoryBufferProducer<int>(options, [partition], new ControlledPartitioner());

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await producer.TryProduceAsync(1));
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await producer.TryProduceAsync(2));

    }

    [Fact]
    public async Task ProduceAsync_Wait_SerializedSingle_RollsBackCapacity_WhenAppendFails()
    {
        var options = new MemoryBufferQueueOptions
        {
            TopicName = "test",
            BoundedCapacity = 1,
            FullMode = BufferQueueFullMode.Wait
        };
        var partitioner = new ControlledPartitioner
        {
            PartitionSelector = item => item == 1 ? 1 : 0
        };
        var goodPartition = new MemoryBufferPartition<int>(0, 4);
        var badPartition = new MemoryBufferPartition<int>(1, 0);
        var producer = new MemoryBufferProducer<int>(
            options,
            [goodPartition, badPartition],
            partitioner);

        await producer.ProduceAsync(0);
        var failedWrite = producer.ProduceAsync(1).AsTask();
        Assert.False(failedWrite.IsCompleted);

        Assert.True(goodPartition.TryPull("TestGroup", 1, out var items));
        Assert.Equal(new[] { 0 }, items);
        goodPartition.Commit("TestGroup");

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await failedWrite);
        Assert.True(await producer.TryProduceAsync(2));
        Assert.Equal(2UL, goodPartition.Count);
    }

    [Fact]
    public async Task ProduceAsync_Uses_SerializedBatchAppend_For_NonConcurrent_Partitioner()
    {
        var options = new MemoryBufferQueueOptions
        {
            TopicName = "test",
            PartitionNumber = 2,
            BoundedCapacity = 3,
            FullMode = BufferQueueFullMode.Fail
        };
        var partitions = new[] {
            new MemoryBufferPartition<int>(0, 4),
            new MemoryBufferPartition<int>(1, 4)
        };
        var producer = new MemoryBufferProducer<int>(
            options,
            partitions,
            new ControlledPartitioner { PartitionSelector = item => item % 2 });

        await producer.ProduceAsync(new[] { 1, 2, 3 }.AsMemory());

        AssertPartitionItems(partitions[0], 2);
        AssertPartitionItems(partitions[1], 1, 3);
    }

    [Fact]
    public async Task TryProduceAsync_Uses_SerializedBatchAppend_For_NonConcurrent_Partitioner()
    {
        var options = new MemoryBufferQueueOptions
        {
            TopicName = "test",
            PartitionNumber = 2,
            BoundedCapacity = 3,
            FullMode = BufferQueueFullMode.Fail
        };
        var partitions = new[] {
            new MemoryBufferPartition<int>(0, 4),
            new MemoryBufferPartition<int>(1, 4)
        };
        var producer = new MemoryBufferProducer<int>(
            options,
            partitions,
            new ControlledPartitioner { PartitionSelector = item => item % 2 });

        Assert.True(await producer.TryProduceAsync(new[] { 1, 2, 3 }.AsMemory()));

        AssertPartitionItems(partitions[0], 2);
        AssertPartitionItems(partitions[1], 1, 3);
    }

    [Fact]
    public async Task ProduceAsync_Wait_SerializedBatch_Appends_After_Capacity_Is_Committed()
    {
        var options = new MemoryBufferQueueOptions
        {
            TopicName = "test",
            BoundedCapacity = 2,
            FullMode = BufferQueueFullMode.Wait
        };
        var partition = new MemoryBufferPartition<int>(0, 8);
        var producer = new MemoryBufferProducer<int>(options, [partition], new ControlledPartitioner());

        await producer.ProduceAsync(new[] { 0, 1 }.AsMemory());
        var waitingBatch = producer.ProduceAsync(new[] { 2, 3 }.AsMemory()).AsTask();
        Assert.False(waitingBatch.IsCompleted);

        Assert.True(partition.TryPull("TestGroup", 2, out var firstBatch));
        Assert.Equal(new[] { 0, 1 }, firstBatch);
        partition.Commit("TestGroup");

        await waitingBatch.WaitAsync(TimeSpan.FromSeconds(10));
        AssertPartitionItems(partition, 2, 3);
    }

    [Fact]
    public async Task TryProduceAsync_SerializedBatch_RollsBackRemainingCapacity_WhenSelectionFails()
    {
        var options = new MemoryBufferQueueOptions
        {
            TopicName = "test",
            BoundedCapacity = 2,
            FullMode = BufferQueueFullMode.Fail
        };
        var partitioner = new ControlledPartitioner { ThrowOnItem = 2 };
        var partition = new MemoryBufferPartition<int>(0, 8);
        var producer = new MemoryBufferProducer<int>(options, [partition], partitioner);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await producer.TryProduceAsync(new[] { 1, 2 }.AsMemory()));

        partitioner.ThrowOnItem = null;
        Assert.True(await producer.TryProduceAsync(3));
        Assert.Equal(2UL, partition.Count);
        AssertPartitionItems(partition, 1, 3);
    }

    [Fact]
    public async Task TryProduceAsync_ConcurrentBatch_RollsBackRemainingCapacity_WhenAppendFails()
    {
        var options = new MemoryBufferQueueOptions<int>
        {
            TopicName = "test",
            PartitionNumber = 2,
            BoundedCapacity = 2,
            FullMode = BufferQueueFullMode.Fail
        };
        options.UsePartitionKey(static item => item);
        var partitions = new[] {
            new MemoryBufferPartition<int>(0, 8),
            new MemoryBufferPartition<int>(1, 0)
        };
        var producer = new MemoryBufferProducer<int>(options, partitions);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await producer.TryProduceAsync(new[] { 1, 2 }.AsMemory()));

        Assert.True(await producer.TryProduceAsync(3));
        Assert.Equal(2UL, partitions[0].Count);
        AssertPartitionItems(partitions[0], 1, 3);
    }

    [Fact]
    public async Task TryProduceAsync_RoundRobinBatch_RollsBackRemainingCapacity_WhenAppendFails()
    {
        var options = new MemoryBufferQueueOptions
        {
            TopicName = "test",
            PartitionNumber = 2,
            BoundedCapacity = 2,
            FullMode = BufferQueueFullMode.Fail
        };
        var partitions = new[] {
            new MemoryBufferPartition<int>(0, 8),
            new MemoryBufferPartition<int>(1, 0)
        };
        var producer = new MemoryBufferProducer<int>(options, partitions);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await producer.TryProduceAsync(new[] { 1, 2 }.AsMemory()));

        Assert.True(await producer.TryProduceAsync(3));
        Assert.Equal(2UL, partitions[0].Count);
        AssertPartitionItems(partitions[0], 1, 3);
    }

    [Fact]
    public async Task TryProduceAsync_RoundRobinBatch_ReleasesCapacity_WhenReservationFails()
    {
        var options = new MemoryBufferQueueOptions
        {
            TopicName = "test",
            BoundedCapacity = 2,
            FullMode = BufferQueueFullMode.Fail
        };
        var partitioner = new ControlledRoundRobinPartitioner { ThrowOnReservation = true };
        var partition = new MemoryBufferPartition<int>(0, 8);
        var producer = new MemoryBufferProducer<int>(options, [partition], partitioner);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await producer.TryProduceAsync(new[] { 1, 2 }.AsMemory()));

        Assert.True(await producer.TryProduceAsync(3));
        Assert.Equal(1UL, partition.Count);
        AssertPartitionItems(partition, 3);
    }

    [Fact]
    public async Task Producer_Uses_Heap_Notification_Buffer_For_More_Than_128_Serialized_Partitions()
    {
        const int partitionCount = 129;
        var options = new MemoryBufferQueueOptions
        {
            TopicName = "test",
            PartitionNumber = partitionCount,
            BoundedCapacity = 2,
            FullMode = BufferQueueFullMode.Fail
        };
        var partitions = Enumerable.Range(0, partitionCount)
            .Select(index => new MemoryBufferPartition<int>(index, 2))
            .ToArray();
        var producer = new MemoryBufferProducer<int>(
            options,
            partitions,
            new ControlledPartitioner { PartitionSelector = item => item });

        Assert.True(await producer.TryProduceAsync(new[] { 0, 128 }.AsMemory()));

        AssertPartitionItems(partitions[0], 0);
        AssertPartitionItems(partitions[128], 128);
    }

    [Fact]
    public async Task Producer_Uses_Heap_Notification_Buffer_For_More_Than_128_Concurrent_Partitions()
    {
        const int partitionCount = 129;
        var options = new MemoryBufferQueueOptions<int>
        {
            TopicName = "test",
            PartitionNumber = partitionCount,
            BoundedCapacity = 2,
            FullMode = BufferQueueFullMode.Fail
        };
        options.UsePartitionKey(static item => item + 1);
        var partitions = Enumerable.Range(0, partitionCount)
            .Select(index => new MemoryBufferPartition<int>(index, 2))
            .ToArray();
        var producer = new MemoryBufferProducer<int>(options, partitions);

        Assert.True(await producer.TryProduceAsync(new[] { 0, 127 }.AsMemory()));

        AssertPartitionItems(partitions[0], 0);
        AssertPartitionItems(partitions[127], 127);
    }

    [Fact]
    public void Producer_Rejects_An_Empty_Partition_List()
    {
        var exception = Assert.Throws<ArgumentException>(() =>
            new MemoryBufferProducer<int>(
                new MemoryBufferQueueOptions { TopicName = "test" },
                []));

        Assert.Equal("bufferPartitions", exception.ParamName);
    }

    private static void AssertPartitionItems<T>(
        MemoryBufferPartition<T> partition,
        params T[] expectedItems)
    {
        Assert.True(partition.TryPull($"TestGroup-{partition.PartitionId}", expectedItems.Length, out var items));
        Assert.Equal(expectedItems, items);
    }

    private sealed class ControlledPartitioner : IPartitioner<int>
    {
        public Func<int, int>? PartitionSelector { get; init; }

        public int? ThrowOnItem { get; set; }

        public bool SupportsConcurrentSelection => false;

        public int SelectPartition(int item, int partitionCount)
        {
            if (item == ThrowOnItem)
            {
                throw new InvalidOperationException("The partition selection failed.");
            }

            return PartitionSelector?.Invoke(item) ?? 0;
        }
    }

    private sealed class ControlledRoundRobinPartitioner : IPartitioner<int>, IRoundRobinBatchPartitioner
    {
        public bool ThrowOnReservation { get; set; }

        public bool SupportsConcurrentSelection => false;

        public int SelectPartition(int item, int partitionCount) => 0;

        public int ReserveBatch(int itemCount, int partitionCount)
        {
            if (ThrowOnReservation)
            {
                ThrowOnReservation = false;
                throw new InvalidOperationException("The batch reservation failed.");
            }

            return 0;
        }
    }
}
