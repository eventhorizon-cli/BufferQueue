using System.Collections.Concurrent;
using System.Reflection;
using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public sealed class MemoryConsumptionConcurrencyTests
{
    private const int PartitionCount = 4;
    private const int ProducerCount = 4;
    private const int ItemsPerProducer = 256;
    private const int TotalItemCount = ProducerCount * ItemsPerProducer;
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(15);

    public static IEnumerable<object[]> Cases()
    {
        foreach (var bounded in new[] { false, true })
        {
            foreach (var keyed in new[] { false, true })
            {
                foreach (var batchSize in new[] { 1, 5, 1000 })
                {
                    // Smaller segments exercise recycling most aggressively for single-item pulls;
                    // the larger batch uses the other segment layout.
                    yield return new object[] { bounded, keyed, batchSize, batchSize == 1 ? 2 : 4 };
                }
            }
        }
    }

    [Theory]
    [MemberData(nameof(Cases))]
    public async Task Concurrent_Producers_And_Independent_Groups_Preserve_All_Items(
        bool bounded,
        bool keyed,
        int batchSize,
        int segmentSize)
    {
        await AssertManualReplayAsync(bounded, segmentSize);

        var options = new MemoryBufferQueueOptions<int>
        {
            TopicName = "consumption-concurrency",
            PartitionNumber = PartitionCount,
            SegmentSize = segmentSize,
            BoundedCapacity = bounded ? 64UL : null
        };
        if (keyed)
        {
            options.UsePartitionKey(static item => item);
        }

        var queue = new MemoryBufferQueue<int>(options);
        var producer = queue.GetProducer();
        var autoConsumers = queue.CreateConsumers(
            new BufferPullConsumerOptions
            {
                TopicName = options.TopicName!,
                GroupName = "auto-group",
                AutoCommit = true,
                BatchSize = batchSize
            },
            consumerNumber: 2).ToArray();
        var manualConsumers = queue.CreateConsumers(
            new BufferPullConsumerOptions
            {
                TopicName = options.TopicName!,
                GroupName = "manual-group",
                AutoCommit = false,
                BatchSize = batchSize
            },
            consumerNumber: 2).ToArray();

        using var cancellation = new CancellationTokenSource(_timeout);
        using var autoStop = CancellationTokenSource.CreateLinkedTokenSource(cancellation.Token);
        using var manualStop = CancellationTokenSource.CreateLinkedTokenSource(cancellation.Token);
        var autoItems = new ConcurrentBag<int>();
        var manualItems = new ConcurrentBag<int>();
        var heldSnapshots = new ConcurrentBag<(IEnumerable<int> Batch, int[] Values)>();

        var autoTasks = autoConsumers.Select(consumer =>
            ConsumeAsync(consumer, autoItems, autoStop, manualCommit: false, heldSnapshots)).ToArray();
        var manualTasks = manualConsumers.Select(consumer =>
            ConsumeAsync(consumer, manualItems, manualStop, manualCommit: true, heldSnapshots)).ToArray();
        var producerTasks = Enumerable.Range(0, ProducerCount)
            .Select(index => ProduceAsync(producer, index, cancellation.Token))
            .ToArray();

        await Task.WhenAll(producerTasks.Concat(autoTasks).Concat(manualTasks)).WaitAsync(_timeout);

        AssertReceivedAllItems(autoItems);
        AssertReceivedAllItems(manualItems);
        // Force another append to each full tail after both groups have committed everything.
        // Recycling must be exercised even if the scheduler let producers finish ahead of readers.
        for (var item = TotalItemCount + 1; item <= TotalItemCount + PartitionCount; item++)
        {
            await producer.ProduceAsync(item, cancellation.Token);
        }

        AssertHeldSnapshots(heldSnapshots);

        var partitions = GetAssignedPartitions(autoConsumers);
        Assert.Equal(PartitionCount, partitions.Length);
        Assert.Contains(partitions, partition => GetHead(partition).StartOffset > new MemoryBufferPartitionOffset(0));
    }

    private static async Task ProduceAsync(
        IBufferProducer<int> producer,
        int producerIndex,
        CancellationToken cancellationToken)
    {
        var firstItem = producerIndex * ItemsPerProducer + 1;
        for (var index = 0; index < ItemsPerProducer;)
        {
            if ((index & 7) == 0 && index + 3 <= ItemsPerProducer)
            {
                var batch = new[] { firstItem + index, firstItem + index + 1, firstItem + index + 2 };
                await producer.ProduceAsync(batch.AsMemory(), cancellationToken);
                index += batch.Length;
            }
            else
            {
                await producer.ProduceAsync(firstItem + index, cancellationToken);
                index++;
            }

            if ((index & 15) == 0)
            {
                await Task.Yield();
            }
        }
    }

    private static async Task ConsumeAsync(
        IBufferPullConsumer<int> consumer,
        ConcurrentBag<int> receivedItems,
        CancellationTokenSource stop,
        bool manualCommit,
        ConcurrentBag<(IEnumerable<int> Batch, int[] Values)> heldSnapshots)
    {
        var capturedSnapshot = false;
        try
        {
            await foreach (var batch in consumer.ConsumeAsync(stop.Token))
            {
                var values = batch.ToArray();
                foreach (var value in values)
                {
                    receivedItems.Add(value);
                }

                if (!capturedSnapshot)
                {
                    heldSnapshots.Add((batch, values));
                    capturedSnapshot = true;
                }

                if (manualCommit)
                {
                    await consumer.CommitAsync();
                }

                if (receivedItems.Count >= TotalItemCount)
                {
                    stop.Cancel();
                }
            }
        }
        catch (OperationCanceledException) when (stop.IsCancellationRequested)
        {
        }
    }

    private static async Task AssertManualReplayAsync(bool bounded, int segmentSize)
    {
        var options = new MemoryBufferQueueOptions
        {
            TopicName = "manual-replay",
            PartitionNumber = 1,
            SegmentSize = segmentSize,
            BoundedCapacity = bounded ? 2UL : null
        };
        var queue = new MemoryBufferQueue<int>(options);
        var producer = queue.GetProducer();
        await producer.ProduceAsync(1);
        await producer.ProduceAsync(2);
        var consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = options.TopicName!,
            GroupName = "replay-group",
            AutoCommit = false,
            BatchSize = 2
        });

        using var cancellation = new CancellationTokenSource(_timeout);
        await using var batches = consumer.ConsumeAsync(cancellation.Token).GetAsyncEnumerator();
        Assert.True(await batches.MoveNextAsync());
        var firstBatch = batches.Current.ToArray();
        Assert.True(await batches.MoveNextAsync());
        Assert.Equal(firstBatch, batches.Current);
        await consumer.CommitAsync();
    }

    private static void AssertReceivedAllItems(ConcurrentBag<int> receivedItems)
    {
        Assert.Equal(TotalItemCount, receivedItems.Count);
        Assert.Equal(TotalItemCount, receivedItems.Distinct().Count());
        Assert.Equal(Enumerable.Range(1, TotalItemCount), receivedItems.Order());
    }

    private static void AssertHeldSnapshots(
        ConcurrentBag<(IEnumerable<int> Batch, int[] Values)> heldSnapshots)
    {
        Assert.NotEmpty(heldSnapshots);
        foreach (var (batch, values) in heldSnapshots)
        {
            Assert.Equal(values, batch.ToArray());
        }
    }

    private static MemoryBufferPartition<int>[] GetAssignedPartitions(
        IEnumerable<IBufferPullConsumer<int>> consumers)
    {
        var field = typeof(BufferPullConsumer<int>).GetField(
            "_assignedPartitions",
            BindingFlags.Instance | BindingFlags.NonPublic)!;
        return consumers
            .SelectMany(consumer => (IBufferPartition<int>[])field.GetValue(consumer)!)
            .Cast<MemoryBufferPartition<int>>()
            .Distinct()
            .ToArray();
    }

    private static MemoryBufferSegment<int> GetHead(MemoryBufferPartition<int> partition) =>
        (MemoryBufferSegment<int>)typeof(MemoryBufferPartition<int>)
            .GetField("_head", BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(partition)!;
}
