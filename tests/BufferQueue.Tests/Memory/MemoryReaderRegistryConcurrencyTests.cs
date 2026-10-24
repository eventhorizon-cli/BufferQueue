using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public sealed class MemoryReaderRegistryConcurrencyTests
{
    private const int SegmentSize = 2;
    private const int ChurnRounds = 16;
    private const int TotalItems = 128;
    private static readonly TimeSpan _timeout = TimeSpan.FromSeconds(15);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Sole_Group_ReRegistration_Replays_From_Physical_Head(bool autoCommit)
    {
        var partition = new MemoryBufferPartition<int>(0, SegmentSize);
        var firstConsumer = CreateConsumer("group", autoCommit, batchSize: 1);
        firstConsumer.AssignPartitions(partition);
        partition.Enqueue(1);
        partition.Enqueue(2);

        using var cancellation = new CancellationTokenSource(_timeout);
        await ReadOneBatchAsync(firstConsumer, cancellation.Token, expected: [1]);
        if (!autoCommit)
        {
            await firstConsumer.CommitAsync();
        }
        firstConsumer.UnassignPartitions();

        var replacementConsumer = CreateConsumer("group", autoCommit, batchSize: 1);
        replacementConsumer.AssignPartitions(partition);
        await ReadOneBatchAsync(replacementConsumer, cancellation.Token, expected: [1]);
        replacementConsumer.UnassignPartitions();
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public async Task Long_Lived_Consumer_Survives_Concurrent_Transient_Group_Churn(bool bounded, bool autoCommit)
    {
        var options = new MemoryBufferQueueOptions
        {
            TopicName = "reader-registry-concurrency",
            PartitionNumber = 1,
            SegmentSize = SegmentSize,
            BoundedCapacity = bounded ? 32UL : null
        };
        var partition = new MemoryBufferPartition<int>(0, SegmentSize);
        var producer = new MemoryBufferProducer<int>(options, [partition]);
        var longLivedConsumer = CreateConsumer("long-lived", autoCommit, batchSize: 1);
        longLivedConsumer.AssignPartitions(partition);

        var roundSignals = Enumerable.Range(0, ChurnRounds)
            .Select(_ => new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously))
            .ToArray();
        using var cancellation = new CancellationTokenSource(_timeout);
        var longLivedTask = ConsumeSequenceAsync(longLivedConsumer, autoCommit, cancellation.Token);
        var producerTask = ProduceSequenceAsync(producer, roundSignals, cancellation.Token);
        var churnTask = ChurnTransientGroupAsync(partition, roundSignals, cancellation.Token);

        await Task.WhenAll(producerTask, churnTask, longLivedTask).WaitAsync(_timeout);

        Assert.Equal(Enumerable.Range(1, TotalItems), await longLivedTask);
        longLivedConsumer.UnassignPartitions();
    }

    private static BufferPullConsumer<int> CreateConsumer(string groupName, bool autoCommit, int batchSize) =>
        new(new BufferPullConsumerOptions
        {
            TopicName = "reader-registry-concurrency",
            GroupName = groupName,
            AutoCommit = autoCommit,
            BatchSize = batchSize
        });

    private static async Task ReadOneBatchAsync(
        BufferPullConsumer<int> consumer,
        CancellationToken cancellationToken,
        int[] expected)
    {
        await using var batches = consumer.ConsumeAsync(cancellationToken).GetAsyncEnumerator();
        Assert.True(await batches.MoveNextAsync());
        Assert.Equal(expected, batches.Current);
    }

    private static async Task<int[]> ConsumeSequenceAsync(
        BufferPullConsumer<int> consumer,
        bool autoCommit,
        CancellationToken cancellationToken)
    {
        var values = new List<int>(TotalItems);
        await using var batches = consumer.ConsumeAsync(cancellationToken).GetAsyncEnumerator();
        while (values.Count < TotalItems && await batches.MoveNextAsync())
        {
            values.AddRange(batches.Current);
            if (!autoCommit)
            {
                await consumer.CommitAsync();
            }
        }

        return values.ToArray();
    }

    private static async Task ProduceSequenceAsync(
        MemoryBufferProducer<int> producer,
        TaskCompletionSource[] roundSignals,
        CancellationToken cancellationToken)
    {
        for (var value = 1; value <= TotalItems; value++)
        {
            if (value <= roundSignals.Length)
            {
                await roundSignals[value - 1].Task.WaitAsync(cancellationToken);
            }

            await producer.ProduceAsync(value, cancellationToken);
            if ((value & 3) == 0)
            {
                await Task.Yield();
            }
        }
    }

    private static async Task ChurnTransientGroupAsync(
        MemoryBufferPartition<int> partition,
        TaskCompletionSource[] roundSignals,
        CancellationToken cancellationToken)
    {
        for (var round = 0; round < roundSignals.Length; round++)
        {
            var transientConsumer = CreateConsumer("transient", autoCommit: false, batchSize: 1);
            transientConsumer.AssignPartitions(partition);
            await using var batches = transientConsumer.ConsumeAsync(cancellationToken).GetAsyncEnumerator();
            roundSignals[round].SetResult();

            Assert.True(await batches.MoveNextAsync());
            var values = batches.Current.ToArray();
            Assert.NotEmpty(values);
            Assert.All(values, value => Assert.True(value > 0));
            await transientConsumer.CommitAsync();
            transientConsumer.UnassignPartitions();
            await Task.Yield();
        }
    }
}
