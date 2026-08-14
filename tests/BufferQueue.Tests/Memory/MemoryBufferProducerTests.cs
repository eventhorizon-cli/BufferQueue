using System.Numerics;
using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public class MemoryBufferProducerTests
{
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
            BoundedCapacity = 1
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
    public async Task Numeric_Partition_Key_Accepts_Negative_Values_Without_Consuming_Extra_Bounded_Capacity()
    {
        var options = new MemoryBufferQueueOptions<int>
        {
            TopicName = "test",
            BoundedCapacity = 1
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
            BoundedCapacity = capacity
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

    private static void AssertPartitionItems(
        MemoryBufferPartition<KeyedItem> partition,
        params KeyedItem[] expectedItems)
    {
        Assert.True(partition.TryPull($"TestGroup-{partition.PartitionId}", 16, out var items));
        Assert.Equal(expectedItems, items);
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
