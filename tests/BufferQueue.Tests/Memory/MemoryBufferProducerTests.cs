// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public class MemoryBufferProducerTests
{
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
    public async Task Concurrent_Producers_Store_Exactly_The_Bounded_Capacity()
    {
        const int capacity = 257;
        const int workerCount = 32;
        const int attemptsPerWorker = 32;
        var appendLock = new object();
        var partitions = Enumerable.Range(0, 4)
            .Select(index => new MemoryBufferPartition<int>(index, 512, appendLock))
            .ToArray();
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                BoundedCapacity = capacity
            },
            partitions);
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
        var producer = new MemoryBufferProducer<int>(
            new MemoryBufferQueueOptions
            {
                TopicName = "test",
                BoundedCapacity = 5
            },
            [partition]);

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
}
