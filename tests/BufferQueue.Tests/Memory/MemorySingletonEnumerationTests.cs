using System.Collections;
using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public class MemorySingletonEnumerationTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void Enumerator_Lifecycle_Preserves_Yield_Behavior(bool referenceItem)
    {
        if (referenceItem)
        {
            AssertLifecycle("item");
        }
        else
        {
            AssertLifecycle(42);
        }
    }

    [Theory]
    [InlineData(1)]
    [InlineData(16)]
    public async Task Snapshot_Supports_Independent_Enumeration_After_Replay_Commit_And_Recycling(int batchSize)
    {
        var partition = new MemoryBufferPartition<string>(0, 1);
        partition.Enqueue("original");
        Assert.True(partition.TryPull("group", batchSize, out var snapshot));
        Assert.True(partition.TryPull("group", batchSize, out var replay));
        Assert.Equal(snapshot, replay);
        using var heldEnumerator = snapshot.GetEnumerator();

        for (var i = 0; i < 8; i++)
        {
            partition.Commit("group");
            partition.Enqueue($"replacement-{i}");
            Assert.True(partition.TryPull("group", batchSize, out _));
        }

        Assert.True(heldEnumerator.MoveNext());
        Assert.Equal("original", heldEnumerator.Current);
        Assert.False(heldEnumerator.MoveNext());
        foreach (var outer in snapshot)
        {
            foreach (var inner in snapshot)
            {
                Assert.Equal("original", outer);
                Assert.Equal(outer, inner);
            }
        }

        await Task.WhenAll(Enumerable.Range(0, 8).Select(_ => Task.Run(() =>
        {
            for (var i = 0; i < 100; i++)
            {
                Assert.Equal(new[] { "original" }, snapshot);
            }
        })));
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Twelve_Concurrent_Consumers_Enumerate_Each_Value_Once(bool bounded, bool autoCommit)
    {
        await AssertConcurrentConsumers(bounded, autoCommit, static value => value, static value => value);
        await AssertConcurrentConsumers(bounded, autoCommit, static value => value.ToString(), int.Parse);
    }

    private static async Task AssertConcurrentConsumers<T>(bool bounded, bool autoCommit,
        Func<int, T> create, Func<T, int> value)
    {
        const int total = 384;
        var queue = new MemoryBufferQueue<T>(new MemoryBufferQueueOptions
        {
            TopicName = "enumeration-concurrency",
            PartitionNumber = 12,
            SegmentSize = 2,
            BoundedCapacity = bounded ? total : null
        });
        await queue.GetProducer().ProduceAsync(Enumerable.Range(0, total).Select(create).ToArray());
        var consumers = queue.CreateConsumers(new BufferPullConsumerOptions
        {
            TopicName = "enumeration-concurrency",
            GroupName = "group",
            AutoCommit = autoCommit,
            BatchSize = 1
        }, 12);
        var results = await Task.WhenAll(consumers.Select(consumer => Task.Run(async () =>
        {
            var values = new List<int>();
            await using var iterator = consumer.ConsumeAsync().GetAsyncEnumerator();
            while (values.Count < total / 12)
            {
                Assert.True(await iterator.MoveNextAsync());
                foreach (var item in iterator.Current)
                {
                    values.Add(value(item));
                }

                if (!autoCommit)
                {
                    await consumer.CommitAsync();
                }
            }

            return values;
        }))).WaitAsync(TimeSpan.FromSeconds(15));
        Assert.Equal(Enumerable.Range(0, total), results.SelectMany(values => values).Order());
    }

    private static void AssertLifecycle<T>(T item)
    {
        var partition = new MemoryBufferPartition<T>(0, 1);
        partition.Enqueue(item);
        Assert.True(partition.TryPull("group", 1, out var snapshot));
        using var first = snapshot.GetEnumerator();
        using var second = snapshot.GetEnumerator();
        Assert.NotSame(first, second);
        Assert.Equal(default, first.Current);
        Assert.Equal(default(T), ((IEnumerator)first).Current);
        Assert.Throws<NotSupportedException>(() => ((IEnumerator)first).Reset());
        Assert.True(first.MoveNext());
        Assert.Equal(item, first.Current);
        Assert.Equal(item, ((IEnumerator)first).Current);
        Assert.Equal(default, second.Current);
        Assert.True(second.MoveNext());
        Assert.Equal(item, second.Current);
        first.Dispose();
        first.Dispose();
        Assert.False(first.MoveNext());
        Assert.Equal(item, first.Current);
        Assert.Equal(item, ((IEnumerator)first).Current);
        Assert.False(second.MoveNext());
        Assert.False(second.MoveNext());
        Assert.Equal(item, second.Current);
        Assert.Throws<NotSupportedException>(() => ((IEnumerator)second).Reset());
        using var disposedBeforeStart = snapshot.GetEnumerator();
        disposedBeforeStart.Dispose();
        Assert.False(disposedBeforeStart.MoveNext());
        Assert.Equal(default, disposedBeforeStart.Current);
        Assert.Equal(default(T), ((IEnumerator)disposedBeforeStart).Current);
    }
}
