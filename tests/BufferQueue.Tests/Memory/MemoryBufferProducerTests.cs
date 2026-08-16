using System.Numerics;
using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public class MemoryBufferProducerTests
{
    [Fact]
    public async Task ProduceAsync_ReadOnlyMemory_Batch_Appends_All_Items_In_Order()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        IBufferProducer<int> producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test"
            },
            [partition]);
        var expectedItems = new[] { 1, 2, 3, 4 };

        await producer.ProduceAsync(expectedItems.AsMemory());

        Assert.True(partition.TryPull("TestGroup", expectedItems.Length, out var items));
        Assert.Equal(expectedItems, items);
    }

    [Fact]
    public async Task ProduceAsync_IEnumerable_Batch_Appends_All_Items_In_Order()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        IBufferProducer<int> producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test"
            },
            [partition]);
        var expectedItems = Enumerable.Range(1, 4);

        await producer.ProduceAsync(expectedItems);

        Assert.True(partition.TryPull("TestGroup", 4, out var items));
        Assert.Equal(expectedItems, items);
    }

    [Fact]
    public async Task TryProduceAsync_IEnumerable_Batch_Appends_All_Items_In_Order()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        IBufferProducer<int> producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions { TopicName = "test" },
            [partition]);
        var expectedItems = Enumerable.Range(1, 4);

        Assert.True(await producer.TryProduceAsync(expectedItems));

        Assert.True(partition.TryPull("TestGroup", 4, out var items));
        Assert.Equal(expectedItems, items);
    }

    [Fact]
    public async Task ReadOnlyMemory_Empty_And_Single_Item_Batches_Are_Supported()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        IBufferProducer<int> producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions { TopicName = "test" },
            [partition]);

        await producer.ProduceAsync(ReadOnlyMemory<int>.Empty);
        Assert.True(await producer.TryProduceAsync(ReadOnlyMemory<int>.Empty));
        await producer.ProduceAsync(new[] { 1 }.AsMemory());
        Assert.True(await producer.TryProduceAsync(new[] { 2 }.AsMemory()));

        Assert.True(partition.TryPull("TestGroup", 2, out var items));
        Assert.Equal(new[] { 1, 2 }, items);
    }

    [Fact]
    public async Task Canceled_IEnumerable_Batch_Does_Not_Enumerate_The_Input()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        IBufferProducer<int> producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions { TopicName = "test" },
            [partition]);
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await producer.ProduceAsync(ThrowWhenEnumerated(), cancellationTokenSource.Token));
        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await producer.TryProduceAsync(ThrowWhenEnumerated(), cancellationTokenSource.Token));

        Assert.Equal(0UL, partition.Count);
    }

    [Fact]
    public async Task ProduceAsync_Batch_RoundRobin_Reserves_The_Complete_Selection_Range()
    {
        var partitions = Enumerable.Range(0, 4)
            .Select(index => new MemoryBufferPartition<int>(index, 8))
            .ToArray();
        IBufferProducer<int> producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                PartitionNumber = partitions.Length
            },
            partitions);

        await producer.ProduceAsync(0);
        await producer.ProduceAsync(new[] { 1, 2, 3, 4, 5 }.AsMemory());

        AssertPartitionItems(partitions[0], 0, 4);
        AssertPartitionItems(partitions[1], 1, 5);
        AssertPartitionItems(partitions[2], 2);
        AssertPartitionItems(partitions[3], 3);
    }

    [Fact]
    public async Task TryProduceAsync_Batch_In_Fail_Mode_WithoutEnoughBoundedCapacity_DoesNotAppend_A_PartialBatch()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        IBufferProducer<int> producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                BoundedCapacity = 2,
                FullMode = BufferQueueFullMode.Fail
            },
            [partition]);
        var items = new[] { 1, 2, 3 };

        var result = await producer.TryProduceAsync(items.AsMemory());

        Assert.False(result);
        Assert.Equal(0UL, partition.Count);
    }

    [Fact]
    public async Task TryProduceAsync_Failed_RoundRobin_Batch_Does_Not_Advance_Partition_Selection()
    {
        var partitions = Enumerable.Range(0, 4)
            .Select(index => new MemoryBufferPartition<int>(index, 8))
            .ToArray();
        IBufferProducer<int> producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                PartitionNumber = partitions.Length,
                BoundedCapacity = 2,
                FullMode = BufferQueueFullMode.Fail
            },
            partitions);

        Assert.False(await producer.TryProduceAsync(new[] { 1, 2, 3 }.AsMemory()));
        Assert.True(await producer.TryProduceAsync(new[] { 1, 2 }.AsMemory()));

        AssertPartitionItems(partitions[0], 1);
        AssertPartitionItems(partitions[1], 2);
        Assert.Equal(0UL, partitions[2].Count);
        Assert.Equal(0UL, partitions[3].Count);
    }

    [Fact]
    public async Task ProduceAsync_Batch_In_Fail_Mode_WithoutEnoughCapacity_Throws_WithoutAppending_A_PartialBatch()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        IBufferProducer<int> producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                BoundedCapacity = 2,
                FullMode = BufferQueueFullMode.Fail
            },
            [partition]);
        var items = new[] { 1, 2, 3 };

        await Assert.ThrowsAsync<BufferQueueFullException>(async () =>
            await producer.ProduceAsync(items.AsMemory()));

        Assert.Equal(0UL, partition.Count);
    }

    [Fact]
    public async Task ProduceAsync_Single_In_Fail_Mode_Throws_When_The_Queue_Is_Full()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        IBufferProducer<int> producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                BoundedCapacity = 1,
                FullMode = BufferQueueFullMode.Fail
            },
            [partition]);

        await producer.ProduceAsync(1);

        await Assert.ThrowsAsync<BufferQueueFullException>(async () => await producer.ProduceAsync(2));
        Assert.Equal(1UL, partition.Count);
    }

    [Fact]
    public async Task ProduceAsync_PartitionKey_Batch_In_Fail_Mode_Requires_The_Whole_Capacity()
    {
        var options = new MemoryBufferQueueOptions<int>
        {
            TopicName = "test",
            PartitionNumber = 2,
            BoundedCapacity = 2,
            FullMode = BufferQueueFullMode.Fail
        };
        options.UsePartitionKey(static item => item);
        var partitions = Enumerable.Range(0, options.PartitionNumber)
            .Select(index => new MemoryBufferPartition<int>(index, 8))
            .ToArray();
        IBufferProducer<int> producer = new MemoryBufferProducer<int>(options, partitions);

        await producer.ProduceAsync(1);

        await Assert.ThrowsAsync<BufferQueueFullException>(async () =>
            await producer.ProduceAsync(new[] { 2, 3 }.AsMemory()));
        Assert.Equal(1UL, partitions.Aggregate(0UL, (count, partition) => count + partition.Count));
    }

    [Fact]
    public async Task ProduceAsync_Wait_Mode_Completes_After_Exact_Full_Capacity_Is_Committed()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                BoundedCapacity = 1,
                FullMode = BufferQueueFullMode.Wait
            },
            [partition]);

        await producer.ProduceAsync(1);
        var waitingTask = producer.ProduceAsync(2).AsTask();

        Assert.False(waitingTask.IsCompleted);
        Assert.True(partition.TryPull("TestGroup", 1, out var firstBatch));
        Assert.Equal(new[] { 1 }, firstBatch);

        partition.Commit("TestGroup");

        await waitingTask.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(partition.TryPull("TestGroup", 1, out var secondBatch));
        Assert.Equal(new[] { 2 }, secondBatch);
    }

    [Fact]
    public async Task ProduceAsync_Wait_Mode_Cancellation_Does_Not_Write_Or_Lose_Capacity()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                BoundedCapacity = 1,
                FullMode = BufferQueueFullMode.Wait
            },
            [partition]);
        using var cancellationTokenSource = new CancellationTokenSource();

        await producer.ProduceAsync(1);
        var canceledWrite = producer.ProduceAsync(2, cancellationTokenSource.Token).AsTask();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAsync<TaskCanceledException>(async () => await canceledWrite);
        Assert.True(partition.TryPull("TestGroup", 1, out var firstBatch));
        Assert.Equal(new[] { 1 }, firstBatch);
        partition.Commit("TestGroup");

        Assert.True(await producer.TryProduceAsync(3));
        Assert.True(partition.TryPull("TestGroup", 1, out var secondBatch));
        Assert.Equal(new[] { 3 }, secondBatch);
    }

    [Fact]
    public async Task TryProduceAsync_Wait_Mode_Waits_And_Returns_True()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                BoundedCapacity = 1,
                FullMode = BufferQueueFullMode.Wait
            },
            [partition]);

        await producer.ProduceAsync(1);
        var waitingWrite = producer.TryProduceAsync(2).AsTask();

        Assert.False(waitingWrite.IsCompleted);
        Assert.True(partition.TryPull("TestGroup", 1, out var firstBatch));
        Assert.Equal(new[] { 1 }, firstBatch);
        partition.Commit("TestGroup");

        Assert.True(await waitingWrite.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.True(partition.TryPull("TestGroup", 1, out var secondBatch));
        Assert.Equal(new[] { 2 }, secondBatch);
    }

    [Fact]
    public async Task TryProduceAsync_Wait_Mode_Cancellation_Does_Not_Write_Or_Lose_Capacity()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                BoundedCapacity = 1,
                FullMode = BufferQueueFullMode.Wait
            },
            [partition]);
        using var cancellationTokenSource = new CancellationTokenSource();

        await producer.ProduceAsync(1);
        var canceledWrite = producer.TryProduceAsync(2, cancellationTokenSource.Token).AsTask();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAsync<TaskCanceledException>(async () => await canceledWrite);
        Assert.True(partition.TryPull("TestGroup", 1, out var firstBatch));
        Assert.Equal(new[] { 1 }, firstBatch);
        partition.Commit("TestGroup");

        Assert.True(await producer.TryProduceAsync(3));
        Assert.True(partition.TryPull("TestGroup", 1, out var secondBatch));
        Assert.Equal(new[] { 3 }, secondBatch);
    }

    [Fact]
    public async Task ProduceAsync_Wait_Mode_Batch_Waits_For_The_Whole_Capacity()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                BoundedCapacity = 3,
                FullMode = BufferQueueFullMode.Wait
            },
            [partition]);

        await producer.ProduceAsync(new[] { 1, 2 }.AsMemory());
        var waitingBatch = producer.ProduceAsync(new[] { 3, 4 }.AsMemory()).AsTask();
        Assert.False(waitingBatch.IsCompleted);

        Assert.True(partition.TryPull("TestGroup", 1, out var firstBatch));
        Assert.Equal(new[] { 1 }, firstBatch);
        partition.Commit("TestGroup");

        await waitingBatch.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(partition.TryPull("TestGroup", 3, out var remainingItems));
        Assert.Equal(new[] { 2, 3, 4 }, remainingItems);
    }

    [Fact]
    public async Task ProduceAsync_Wait_Mode_RoundRobin_Batch_Reserves_Partitions_After_Capacity_Is_Available()
    {
        var partitions = Enumerable.Range(0, 4)
            .Select(index => new MemoryBufferPartition<int>(index, 8))
            .ToArray();
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                PartitionNumber = partitions.Length,
                BoundedCapacity = 2,
                FullMode = BufferQueueFullMode.Wait
            },
            partitions);

        await producer.ProduceAsync(1);
        var waitingBatch = producer.ProduceAsync(new[] { 2, 3 }.AsMemory()).AsTask();
        Assert.False(waitingBatch.IsCompleted);

        Assert.True(partitions[0].TryPull("TestGroup", 1, out var firstBatch));
        Assert.Equal(new[] { 1 }, firstBatch);
        partitions[0].Commit("TestGroup");

        await waitingBatch.WaitAsync(TimeSpan.FromSeconds(10));
        AssertPartitionItems(partitions[1], 2);
        AssertPartitionItems(partitions[2], 3);
    }

    [Fact]
    public async Task ProduceAsync_Wait_Mode_PartitionKey_Batch_Resumes_Across_Partitions()
    {
        var options = new MemoryBufferQueueOptions<int>
        {
            TopicName = "test",
            PartitionNumber = 2,
            BoundedCapacity = 3,
            FullMode = BufferQueueFullMode.Wait
        };
        options.UsePartitionKey(static item => item);
        var partitions = Enumerable.Range(0, options.PartitionNumber)
            .Select(index => new MemoryBufferPartition<int>(index, 8))
            .ToArray();
        var producer = new MemoryBufferProducer<int>(options, partitions);

        await producer.ProduceAsync(new[] { 1, 2 }.AsMemory());
        var waitingBatch = producer.ProduceAsync(new[] { 3, 4 }.AsMemory()).AsTask();
        Assert.False(waitingBatch.IsCompleted);

        Assert.True(partitions[0].TryPull("TestGroup", 1, out var firstBatch));
        Assert.Equal(new[] { 1 }, firstBatch);
        partitions[0].Commit("TestGroup");

        await waitingBatch.WaitAsync(TimeSpan.FromSeconds(10));
        AssertPartitionItems(partitions[0], 3);
        AssertPartitionItems(partitions[1], 2, 4);
    }

    [Fact]
    public async Task ProduceAsync_Wait_Mode_Batch_Cancellation_Does_Not_Write_Or_Lose_Capacity()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                BoundedCapacity = 2,
                FullMode = BufferQueueFullMode.Wait
            },
            [partition]);
        using var cancellationTokenSource = new CancellationTokenSource();

        await producer.ProduceAsync(1);
        var canceledBatch = producer
            .ProduceAsync(new[] { 2, 3 }.AsMemory(), cancellationTokenSource.Token)
            .AsTask();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAsync<TaskCanceledException>(async () => await canceledBatch);
        Assert.True(partition.TryPull("TestGroup", 1, out var firstBatch));
        Assert.Equal(new[] { 1 }, firstBatch);
        partition.Commit("TestGroup");

        Assert.True(await producer.TryProduceAsync(new[] { 4, 5 }.AsMemory()));
        Assert.True(partition.TryPull("TestGroup", 2, out var secondBatch));
        Assert.Equal(new[] { 4, 5 }, secondBatch);
    }

    [Fact]
    public async Task ProduceAsync_Wait_Mode_Splits_A_Batch_Larger_Than_Capacity()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                BoundedCapacity = 2,
                FullMode = BufferQueueFullMode.Wait
            },
            [partition]);

        var write = producer.ProduceAsync(new[] { 1, 2, 3 }.AsMemory()).AsTask();

        Assert.False(write.IsCompleted);
        Assert.True(partition.TryPull("TestGroup", 2, out var firstBatch));
        Assert.Equal(new[] { 1, 2 }, firstBatch);
        partition.Commit("TestGroup");

        await write.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.True(partition.TryPull("TestGroup", 1, out var secondBatch));
        Assert.Equal(new[] { 3 }, secondBatch);
    }

    [Fact]
    public async Task TryProduceAsync_Wait_Mode_Splits_A_Batch_Larger_Than_Capacity()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                BoundedCapacity = 2,
                FullMode = BufferQueueFullMode.Wait
            },
            [partition]);

        var write = producer.TryProduceAsync(new[] { 1, 2, 3 }.AsMemory()).AsTask();

        Assert.False(write.IsCompleted);
        Assert.True(partition.TryPull("TestGroup", 2, out var firstBatch));
        Assert.Equal(new[] { 1, 2 }, firstBatch);
        partition.Commit("TestGroup");

        Assert.True(await write.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.True(partition.TryPull("TestGroup", 1, out var secondBatch));
        Assert.Equal(new[] { 3 }, secondBatch);
    }

    [Fact]
    public async Task ProduceAsync_Wait_Mode_Splits_A_Large_Batch_Into_Multiple_Chunks()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                BoundedCapacity = 1,
                FullMode = BufferQueueFullMode.Wait
            },
            [partition]);

        var write = producer.ProduceAsync(new[] { 1, 2, 3 }.AsMemory()).AsTask();

        Assert.Equal(new[] { 1 }, await PullWhenAvailableAsync(partition, "TestGroup", 1));
        partition.Commit("TestGroup");
        Assert.Equal(new[] { 2 }, await PullWhenAvailableAsync(partition, "TestGroup", 1));
        partition.Commit("TestGroup");

        await write.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(new[] { 3 }, await PullWhenAvailableAsync(partition, "TestGroup", 1));
    }

    [Fact]
    public async Task TryProduceAsync_Wait_Mode_Cancellation_After_A_Completed_Chunk_Preserves_That_Chunk()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                BoundedCapacity = 1,
                FullMode = BufferQueueFullMode.Wait
            },
            [partition]);
        using var cancellationTokenSource = new CancellationTokenSource();

        var write = producer
            .TryProduceAsync(new[] { 1, 2 }.AsMemory(), cancellationTokenSource.Token)
            .AsTask();

        Assert.False(write.IsCompleted);
        Assert.True(partition.TryPull("TestGroup", 1, out var firstBatch));
        Assert.Equal(new[] { 1 }, firstBatch);
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAsync<TaskCanceledException>(async () => await write);
        partition.Commit("TestGroup");

        Assert.True(await producer.TryProduceAsync(3));
        Assert.True(partition.TryPull("TestGroup", 1, out var secondBatch));
        Assert.Equal(new[] { 3 }, secondBatch);
    }

    [Fact]
    public async Task ProduceAsync_Wait_Mode_Split_RoundRobin_Batch_Continues_Partition_Selection()
    {
        var partitions = Enumerable.Range(0, 4)
            .Select(index => new MemoryBufferPartition<int>(index, 8))
            .ToArray();
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                PartitionNumber = partitions.Length,
                BoundedCapacity = 2,
                FullMode = BufferQueueFullMode.Wait
            },
            partitions);

        var write = producer.ProduceAsync(new[] { 1, 2, 3, 4 }.AsMemory()).AsTask();

        Assert.False(write.IsCompleted);
        Assert.True(partitions[0].TryPull("TestGroup", 1, out var firstPartitionItems));
        Assert.Equal(new[] { 1 }, firstPartitionItems);
        partitions[0].Commit("TestGroup");
        Assert.True(partitions[1].TryPull("TestGroup", 1, out var secondPartitionItems));
        Assert.Equal(new[] { 2 }, secondPartitionItems);
        partitions[1].Commit("TestGroup");

        await write.WaitAsync(TimeSpan.FromSeconds(10));
        AssertPartitionItems(partitions[2], 3);
        AssertPartitionItems(partitions[3], 4);
    }

    [Fact]
    public async Task TryProduceAsync_Wait_Mode_Invokes_A_PartitionKey_Selector_Once_Per_Item()
    {
        var selectorCallCount = 0;
        var options = new MemoryBufferQueueOptions<int>
        {
            TopicName = "test",
            PartitionNumber = 2,
            BoundedCapacity = 1,
            FullMode = BufferQueueFullMode.Wait
        };
        options.UsePartitionKey(item =>
        {
            Interlocked.Increment(ref selectorCallCount);
            return item;
        });
        var partitions = Enumerable.Range(0, options.PartitionNumber)
            .Select(index => new MemoryBufferPartition<int>(index, 8))
            .ToArray();
        var producer = new MemoryBufferProducer<int>(options, partitions);

        var write = producer.TryProduceAsync(new[] { 1, 2 }.AsMemory()).AsTask();

        Assert.False(write.IsCompleted);
        Assert.True(partitions[0].TryPull("TestGroup", 1, out var firstBatch));
        Assert.Equal(new[] { 1 }, firstBatch);
        partitions[0].Commit("TestGroup");

        Assert.True(await write.WaitAsync(TimeSpan.FromSeconds(10)));
        Assert.Equal(2, selectorCallCount);
        AssertPartitionItems(partitions[1], 2);
    }

    [Fact]
    public async Task ProduceAsync_Wait_Mode_Split_PartitionKey_Batch_Retains_PerKey_Order_Across_Chunks()
    {
        var options = new MemoryBufferQueueOptions<int>
        {
            TopicName = "test",
            PartitionNumber = 2,
            BoundedCapacity = 2,
            FullMode = BufferQueueFullMode.Wait
        };
        options.UsePartitionKey(static item => item);
        var partitions = Enumerable.Range(0, options.PartitionNumber)
            .Select(index => new MemoryBufferPartition<int>(index, 8))
            .ToArray();
        var producer = new MemoryBufferProducer<int>(options, partitions);

        var write = producer.ProduceAsync(new[] { 1, 2, 1, 2, 1 }.AsMemory()).AsTask();

        Assert.Equal(new[] { 1 }, await PullWhenAvailableAsync(partitions[0], "TestGroup", 1));
        Assert.Equal(new[] { 2 }, await PullWhenAvailableAsync(partitions[1], "TestGroup", 1));
        partitions[0].Commit("TestGroup");
        partitions[1].Commit("TestGroup");

        Assert.Equal(new[] { 1 }, await PullWhenAvailableAsync(partitions[0], "TestGroup", 1));
        Assert.Equal(new[] { 2 }, await PullWhenAvailableAsync(partitions[1], "TestGroup", 1));
        partitions[0].Commit("TestGroup");

        await write.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.Equal(new[] { 1 }, await PullWhenAvailableAsync(partitions[0], "TestGroup", 1));
    }

    [Fact]
    public async Task TryProduceAsync_Wait_Mode_Split_PartitionKey_Batch_Cancellation_Does_Not_Write_The_Next_Chunk()
    {
        var options = new MemoryBufferQueueOptions<int>
        {
            TopicName = "test",
            PartitionNumber = 2,
            BoundedCapacity = 2,
            FullMode = BufferQueueFullMode.Wait
        };
        options.UsePartitionKey(static item => item);
        var partitions = Enumerable.Range(0, options.PartitionNumber)
            .Select(index => new MemoryBufferPartition<int>(index, 8))
            .ToArray();
        var producer = new MemoryBufferProducer<int>(options, partitions);
        using var cancellationTokenSource = new CancellationTokenSource();

        var write = producer
            .TryProduceAsync(new[] { 1, 2, 1 }.AsMemory(), cancellationTokenSource.Token)
            .AsTask();

        Assert.False(write.IsCompleted);
        Assert.Equal(new[] { 1 }, await PullWhenAvailableAsync(partitions[0], "TestGroup", 1));
        Assert.Equal(new[] { 2 }, await PullWhenAvailableAsync(partitions[1], "TestGroup", 1));
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAsync<TaskCanceledException>(async () => await write);
        Assert.Equal(1UL, partitions[0].Count);
        Assert.Equal(1UL, partitions[1].Count);
    }

    [Fact]
    public async Task Capacity_Is_Released_Only_After_All_Known_Groups_Commit()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                BoundedCapacity = 1,
                FullMode = BufferQueueFullMode.Wait
            },
            [partition]);

        await producer.ProduceAsync(1);
        Assert.True(partition.TryPull("Group1", 1, out _));
        Assert.True(partition.TryPull("Group2", 1, out _));
        var waitingTask = producer.ProduceAsync(2).AsTask();

        partition.Commit("Group1");
        Assert.False(waitingTask.IsCompleted);

        partition.Commit("Group2");
        await waitingTask.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task New_Group_Starts_After_Capacity_Already_Released()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                BoundedCapacity = 1
            },
            [partition]);

        await producer.ProduceAsync(1);
        Assert.True(partition.TryPull("Group1", 1, out _));
        partition.Commit("Group1");
        await producer.ProduceAsync(2);

        Assert.True(partition.TryPull("Group2", 1, out var items));
        Assert.Equal(new[] { 2 }, items);
    }

    [Fact]
    public async Task New_Group_Skips_Multiple_Segments_Before_The_Released_Position()
    {
        var partition = new MemoryBufferPartition<int>(0, 2);
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                BoundedCapacity = 5
            },
            [partition]);

        await producer.ProduceAsync(new[] { 1, 2, 3, 4, 5 }.AsMemory());
        Assert.True(partition.TryPull("Group1", 5, out _));
        partition.Commit("Group1");
        await producer.ProduceAsync(6);

        Assert.True(partition.TryPull("Group2", 1, out var items));
        Assert.Equal(new[] { 6 }, items);
    }

    [Fact]
    public async Task Canceled_TryProduceAsync_Does_Not_Write()
    {
        var partition = new MemoryBufferPartition<int>(0, 8);
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions { TopicName = "test" },
            [partition]);
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await producer.TryProduceAsync(1, cancellationTokenSource.Token));

        Assert.Equal(0UL, partition.Count);
    }

    [Fact]
    public async Task ProduceAsync_Batch_PartitionKey_Retains_PerKey_Order()
    {
        var options = new MemoryBufferQueueOptions<KeyedItem>
        {
            TopicName = "test",
            PartitionNumber = 4
        };
        options.UsePartitionKey(item => item.Key);
        var partitions = Enumerable.Range(0, options.PartitionNumber)
            .Select(index => new MemoryBufferPartition<KeyedItem>(index, 8))
            .ToArray();
        IBufferProducer<KeyedItem> producer = new MemoryBufferProducer<KeyedItem>(options, partitions);
        var items = new[]
        {
            new KeyedItem(1, "one-1"),
            new KeyedItem(2, "two-1"),
            new KeyedItem(1, "one-2"),
            new KeyedItem(4, "four-1"),
            new KeyedItem(2, "two-2")
        };

        await producer.ProduceAsync(items.AsMemory());

        AssertPartitionItems(partitions[0], new KeyedItem(1, "one-1"), new KeyedItem(1, "one-2"));
        AssertPartitionItems(partitions[1], new KeyedItem(2, "two-1"), new KeyedItem(2, "two-2"));
        Assert.Equal(0UL, partitions[2].Count);
        AssertPartitionItems(partitions[3], new KeyedItem(4, "four-1"));
    }

    [Fact]
    public async Task Partition_Key_Selector_Routes_Equal_Keys_To_The_Same_Partition()
    {
        var options = new MemoryBufferQueueOptions<KeyedItem>
        {
            TopicName = "test",
            PartitionNumber = 4
        };
        options.UsePartitionKey(item => item.Key);
        var partitions = Enumerable.Range(0, options.PartitionNumber)
            .Select(index => new MemoryBufferPartition<KeyedItem>(index, 16))
            .ToArray();
        var producer = new MemoryBufferProducer<KeyedItem>(options, partitions);

        await producer.ProduceAsync(new KeyedItem(1, "one-1"));
        await producer.ProduceAsync(new KeyedItem(2, "two-1"));
        await producer.ProduceAsync(new KeyedItem(1, "one-2"));
        await producer.ProduceAsync(new KeyedItem(4, "four-1"));
        await producer.ProduceAsync(new KeyedItem(2, "two-2"));

        AssertPartitionItems(partitions[0], new KeyedItem(1, "one-1"), new KeyedItem(1, "one-2"));
        AssertPartitionItems(partitions[1], new KeyedItem(2, "two-1"), new KeyedItem(2, "two-2"));
        Assert.Equal(0UL, partitions[2].Count);
        AssertPartitionItems(partitions[3], new KeyedItem(4, "four-1"));
    }

    [Fact]
    public async Task Partition_Key_Selector_Exception_Does_Not_Consume_Bounded_Capacity()
    {
        var options = new MemoryBufferQueueOptions<int>
        {
            TopicName = "test",
            BoundedCapacity = 1,
            FullMode = BufferQueueFullMode.Fail
        };
        options.UsePartitionKey(item => item >= 0
            ? item
            : throw new InvalidOperationException("Invalid partition key."));
        var partition = new MemoryBufferPartition<int>(0, 4);
        var producer = new MemoryBufferProducer<int>(options, [partition]);

        await Assert.ThrowsAsync<InvalidOperationException>(async () => await producer.ProduceAsync(-1));

        Assert.True(await producer.TryProduceAsync(1));
        Assert.False(await producer.TryProduceAsync(2));
        Assert.Equal(1UL, partition.Count);
    }

    [Fact]
    public async Task TryProduceAsync_Fail_Mode_PartitionKey_Batch_Does_Not_Append_A_Partial_Batch()
    {
        var options = new MemoryBufferQueueOptions<int>
        {
            TopicName = "test",
            PartitionNumber = 2,
            BoundedCapacity = 3,
            FullMode = BufferQueueFullMode.Fail
        };
        options.UsePartitionKey(static item => item);
        var partitions = Enumerable.Range(0, options.PartitionNumber)
            .Select(index => new MemoryBufferPartition<int>(index, 8))
            .ToArray();
        var producer = new MemoryBufferProducer<int>(options, partitions);

        await producer.ProduceAsync(1);

        Assert.False(await producer.TryProduceAsync(new[] { 2, 1, 2 }.AsMemory()));
        AssertPartitionItems(partitions[0], 1);
        Assert.Equal(0UL, partitions[1].Count);
    }

    [Fact]
    public async Task Fail_Mode_Oversized_PartitionKey_Batch_Does_Not_Invoke_The_Selector()
    {
        var selectorCallCount = 0;
        var options = new MemoryBufferQueueOptions<int>
        {
            TopicName = "test",
            PartitionNumber = 2,
            BoundedCapacity = 2,
            FullMode = BufferQueueFullMode.Fail
        };
        options.UsePartitionKey(item =>
        {
            Interlocked.Increment(ref selectorCallCount);
            return item;
        });
        var partitions = Enumerable.Range(0, options.PartitionNumber)
            .Select(index => new MemoryBufferPartition<int>(index, 8))
            .ToArray();
        var producer = new MemoryBufferProducer<int>(options, partitions);
        var oversizedBatch = new[] { 1, 2, 3 }.AsMemory();

        Assert.False(await producer.TryProduceAsync(oversizedBatch));
        await Assert.ThrowsAsync<BufferQueueFullException>(async () =>
            await producer.ProduceAsync(oversizedBatch));

        Assert.Equal(0, selectorCallCount);
        Assert.Equal(0UL, partitions[0].Count);
        Assert.Equal(0UL, partitions[1].Count);
    }

    [Fact]
    public async Task Numeric_Partition_Key_Accepts_Negative_Values_Without_Consuming_Extra_Bounded_Capacity()
    {
        var options = new MemoryBufferQueueOptions<int>
        {
            TopicName = "test",
            BoundedCapacity = 1,
            FullMode = BufferQueueFullMode.Fail
        };
        options.UsePartitionKey(static item => item);
        var partition = new MemoryBufferPartition<int>(0, 4);
        var producer = new MemoryBufferProducer<int>(options, [partition]);

        Assert.True(await producer.TryProduceAsync(-1));
        Assert.False(await producer.TryProduceAsync(2));
        Assert.Equal(1UL, partition.Count);
    }

    [Fact]
    public void Use_Partition_Key_Requires_A_Selector()
    {
        var options = new MemoryBufferQueueOptions<KeyedItem>();

        var exception = Assert.Throws<ArgumentNullException>(() =>
            options.UsePartitionKey<int>(null!));

        Assert.Equal("partitionKeySelector", exception.ParamName);
    }

    [Fact]
    public void Numeric_Partition_Keys_Use_Normalized_One_Based_Modulo_For_All_Built_In_Numeric_Types()
    {
        AssertNumericPartition(15, 14);
        AssertNumericPartition(16, 15);
        AssertNumericPartition(17, 0);
        AssertNumericPartition(0, 15);
        AssertNumericPartition(-1, 14);
        AssertNumericPartition(-16, 15);
        AssertNumericPartition(int.MinValue, 15);
        AssertNumericPartition(int.MaxValue, 14);
        AssertNumericPartition((sbyte)-1, 14);
        AssertNumericPartition((sbyte)15, 14);
        AssertNumericPartition((byte)16, 15);
        AssertNumericPartition((short)17, 0);
        AssertNumericPartition((ushort)15, 14);
        AssertNumericPartition(17U, 0);
        AssertNumericPartition(15L, 14);
        AssertNumericPartition(16UL, 15);
        AssertNumericPartition(-1L, 14);
        AssertNumericPartition(long.MinValue, 15);
        AssertNumericPartition(long.MaxValue, 14);
        AssertNumericPartition(uint.MaxValue, 14);
        AssertNumericPartition(ulong.MaxValue, 14);
        AssertNumericPartition((nint)17, 0);
        AssertNumericPartition((nuint)15, 14);
        AssertNumericPartition((Int128)16, 15);
        AssertNumericPartition((UInt128)17, 0);
        AssertNumericPartition(new BigInteger(15), 14);
        AssertNumericPartition(new BigInteger(-1), 14);
        AssertNumericPartition((char)16, 15);
        AssertNumericPartition((Half)16, 15);
        AssertNumericPartition(17F, 0);
        AssertNumericPartition(15D, 14);
        AssertNumericPartition(-1D, 14);
        AssertNumericPartition(16M, 15);
        AssertNumericPartition(-1M, 14);
    }

    [Theory]
    [InlineData(1.5D)]
    [InlineData(double.NaN)]
    [InlineData(double.PositiveInfinity)]
    [InlineData(double.NegativeInfinity)]
    public void Numeric_Partition_Keys_Reject_Invalid_Values(double partitionKey)
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            PartitionKeyRouting.SelectNumericPartition(partitionKey, 16));

        Assert.Throws<ArgumentOutOfRangeException>(() =>
            PartitionKeyRouting.SelectNumericPartition(1.5M, 16));
    }

    [Fact]
    public void String_Partition_Keys_Use_Only_The_First_Four_Characters()
    {
        var options = new MemoryBufferQueueOptions<string>
        {
            TopicName = "test",
            PartitionNumber = 16
        };
        options.UsePartitionKey(static item => item);

        var partitioner = new KeyPartitioner<string>(options.PartitionIndexSelector!);

        Assert.Equal(
            partitioner.SelectPartition("cust-0001", options.PartitionNumber),
            partitioner.SelectPartition("cust-9999", options.PartitionNumber));
        Assert.Equal(0, partitioner.SelectPartition(string.Empty, options.PartitionNumber));
    }

    [Fact]
    public async Task Producer_And_Direct_Partition_Enqueue_Share_Append_Serialization()
    {
        const int workerCount = 8;
        const int itemsPerWorker = 512;
        var partition = new MemoryBufferPartition<int>(0, 4);
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test"
            },
            [partition]);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ready = new CountdownEvent(workerCount);

        var tasks = Enumerable.Range(0, workerCount)
            .Select(workerIndex => Task.Run(async () =>
            {
                ready.Signal();
                await start.Task;

                for (var i = 0; i < itemsPerWorker; i++)
                {
                    var item = workerIndex * itemsPerWorker + i;
                    if (workerIndex % 2 == 0)
                    {
                        await producer.ProduceAsync(item);
                    }
                    else
                    {
                        partition.Enqueue(item);
                    }
                }
            }))
            .ToArray();

        Assert.True(ready.Wait(TimeSpan.FromSeconds(10)));
        start.SetResult();
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));

        var itemCount = workerCount * itemsPerWorker;
        Assert.True(partition.TryPull("TestGroup", itemCount, out var items));
        Assert.Equal(Enumerable.Range(0, itemCount), items.Order());
    }

    [Fact]
    public async Task Concurrent_RoundRobin_Batch_Producers_Store_All_Items()
    {
        const int partitionCount = 4;
        const int workerCount = 16;
        const int batchesPerWorker = 16;
        const int itemsPerBatch = 17;
        var partitions = Enumerable.Range(0, partitionCount)
            .Select(index => new MemoryBufferPartition<int>(index, 512))
            .ToArray();
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                PartitionNumber = partitionCount
            },
            partitions);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ready = new CountdownEvent(workerCount);

        var tasks = Enumerable.Range(0, workerCount)
            .Select(workerIndex => Task.Run(async () =>
            {
                ready.Signal();
                await start.Task;

                var workerStart = workerIndex * batchesPerWorker * itemsPerBatch;
                for (var batch = 0; batch < batchesPerWorker; batch++)
                {
                    var items = Enumerable.Range(workerStart + batch * itemsPerBatch, itemsPerBatch).ToArray();
                    await producer.ProduceAsync(items.AsMemory());
                }
            }))
            .ToArray();

        Assert.True(ready.Wait(TimeSpan.FromSeconds(10)));
        start.SetResult();
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));

        var itemCount = workerCount * batchesPerWorker * itemsPerBatch;
        var producedItems = partitions.SelectMany((partition, partitionIndex) =>
        {
            Assert.True(partition.TryPull($"TestGroup-{partitionIndex}", itemCount, out var items));
            return items;
        });
        Assert.Equal(Enumerable.Range(0, itemCount), producedItems.Order());
    }

    [Fact]
    public async Task Concurrent_Unbounded_Producers_Distribute_All_Items_Evenly()
    {
        const int partitionCount = 4;
        const int workerCount = 32;
        const int itemsPerWorker = 128;
        var appendLock = new object();
        var partitions = Enumerable.Range(0, partitionCount)
            .Select(index => new MemoryBufferPartition<int>(index, 512, appendLock))
            .ToArray();
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test"
            },
            partitions);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ready = new CountdownEvent(workerCount);

        var tasks = Enumerable.Range(0, workerCount)
            .Select(workerIndex => Task.Run(async () =>
            {
                ready.Signal();
                await start.Task;

                for (var i = 0; i < itemsPerWorker; i++)
                {
                    await producer.ProduceAsync(workerIndex * itemsPerWorker + i);
                }
            }))
            .ToArray();

        Assert.True(ready.Wait(TimeSpan.FromSeconds(10)));
        start.SetResult();
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));

        var expectedPartitionCount = workerCount * itemsPerWorker / partitionCount;
        Assert.All(partitions, partition => Assert.Equal((ulong)expectedPartitionCount, partition.Count));

        var producedItems = partitions.SelectMany((partition, partitionIndex) =>
        {
            Assert.True(partition.TryPull($"TestGroup-{partitionIndex}", expectedPartitionCount, out var items));
            return items;
        });
        Assert.Equal(Enumerable.Range(0, workerCount * itemsPerWorker), producedItems.Order());
    }

    [Fact]
    public async Task Concurrent_Partition_Key_Producers_With_Independent_Partition_Locks_Distribute_All_Items_Evenly()
    {
        const int partitionCount = 4;
        const int workerCount = 32;
        const int itemsPerWorker = 128;
        var options = new MemoryBufferQueueOptions<int>
        {
            TopicName = "test",
            PartitionNumber = partitionCount
        };
        options.UsePartitionKey(static item => item + 1);
        var partitions = Enumerable.Range(0, partitionCount)
            .Select(index => new MemoryBufferPartition<int>(index, 512))
            .ToArray();
        var producer = new MemoryBufferProducer<int>(options, partitions);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ready = new CountdownEvent(workerCount);

        var tasks = Enumerable.Range(0, workerCount)
            .Select(workerIndex => Task.Run(async () =>
            {
                ready.Signal();
                await start.Task;

                for (var i = 0; i < itemsPerWorker; i++)
                {
                    await producer.ProduceAsync(workerIndex * itemsPerWorker + i);
                }
            }))
            .ToArray();

        Assert.True(ready.Wait(TimeSpan.FromSeconds(10)));
        start.SetResult();
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));

        var expectedPartitionCount = workerCount * itemsPerWorker / partitionCount;
        Assert.All(partitions, partition => Assert.Equal((ulong)expectedPartitionCount, partition.Count));

        var producedItems = partitions.SelectMany((partition, partitionIndex) =>
        {
            Assert.True(partition.TryPull($"TestGroup-{partitionIndex}", expectedPartitionCount, out var items));
            return items;
        });
        Assert.Equal(Enumerable.Range(0, workerCount * itemsPerWorker), producedItems.Order());
    }

    [Fact]
    public async Task Concurrent_Producers_Store_Exactly_The_Bounded_Capacity()
    {
        const int capacity = 257;
        const int workerCount = 32;
        const int attemptsPerWorker = 32;
        var options = new MemoryBufferQueueOptions<int>
        {
            TopicName = "test",
            PartitionNumber = 4,
            BoundedCapacity = capacity,
            FullMode = BufferQueueFullMode.Fail
        };
        options.UsePartitionKey(static item => item);
        var partitions = Enumerable.Range(0, 4)
            .Select(index => new MemoryBufferPartition<int>(index, 512))
            .ToArray();
        var producer = new MemoryBufferProducer<int>(options, partitions);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ready = new CountdownEvent(workerCount);
        var producedCount = 0;

        var tasks = Enumerable.Range(0, workerCount)
            .Select(workerIndex => Task.Run(async () =>
            {
                ready.Signal();
                await start.Task;

                for (var i = 0; i < attemptsPerWorker; i++)
                {
                    var item = workerIndex * attemptsPerWorker + i + 1;
                    if (await producer.TryProduceAsync(item))
                    {
                        Interlocked.Increment(ref producedCount);
                    }
                }
            }))
            .ToArray();

        Assert.True(ready.Wait(TimeSpan.FromSeconds(10)));
        start.SetResult();
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(capacity, producedCount);
        Assert.Equal((ulong)capacity, partitions.Aggregate(0UL, (count, partition) => count + partition.Count));
        Assert.False(await producer.TryProduceAsync(-1));
    }

    [Fact]
    public async Task Recycled_Items_Return_Capacity_To_The_Gate()
    {
        var partition = new MemoryBufferPartition<int>(0, 2);
        var options = new MemoryBufferQueueOptions<int>
        {
            TopicName = "test",
            BoundedCapacity = 5
        };
        options.UsePartitionKey(static item => item);
        var producer = new MemoryBufferProducer<int>(options, [partition]);

        for (var i = 0; i < 4; i++)
        {
            Assert.True(await producer.TryProduceAsync(i));
        }

        Assert.True(partition.TryPull("TestGroup", 4, out var items));
        Assert.Equal(Enumerable.Range(0, 4), items);
        partition.Commit("TestGroup");

        for (var i = 4; i < 7; i++)
        {
            Assert.True(await producer.TryProduceAsync(i));
        }

        Assert.Equal(3UL, partition.Count);
    }

    private static void AssertPartitionItems<T>(
        MemoryBufferPartition<T> partition,
        params T[] expectedItems)
    {
        Assert.True(partition.TryPull($"TestGroup-{partition.PartitionId}", 16, out var items));
        Assert.Equal(expectedItems, items);
    }

    private static async Task<IEnumerable<T>> PullWhenAvailableAsync<T>(
        MemoryBufferPartition<T> partition,
        string groupName,
        int batchSize)
    {
        for (var attempt = 0; attempt < 1000; attempt++)
        {
            if (partition.TryPull(groupName, batchSize, out var items))
            {
                return items;
            }

            await Task.Delay(10);
        }

        throw new TimeoutException("The expected batch was not produced within the test timeout.");
    }

    private static IEnumerable<int> ThrowWhenEnumerated()
    {
        yield return Throw();

        static int Throw() => throw new InvalidOperationException("The input must not be enumerated.");
    }

    private static void AssertNumericPartition<TNumber>(TNumber partitionKey, int expectedPartitionIndex)
        where TNumber : INumber<TNumber>
    {
        var options = new MemoryBufferQueueOptions<TNumber>
        {
            TopicName = "test",
            PartitionNumber = 16
        };
        options.UsePartitionKey(static item => item);

        var partitioner = new KeyPartitioner<TNumber>(options.PartitionIndexSelector!);
        Assert.Equal(expectedPartitionIndex, partitioner.SelectPartition(partitionKey, options.PartitionNumber));
    }

    private sealed record KeyedItem(int Key, string Value);
}
