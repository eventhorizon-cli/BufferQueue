// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public class MemoryBufferProducerTests
{
    [Fact]
    public async Task Concurrent_Producers_Store_Exactly_The_Bounded_Capacity()
    {
        const int capacity = 257;
        const int workerCount = 32;
        const int attemptsPerWorker = 32;
        var partitions = Enumerable.Range(0, 4)
            .Select(index => new MemoryBufferPartition<int>(index, 512))
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
