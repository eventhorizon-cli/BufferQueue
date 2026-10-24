namespace BufferQueue.MemoryMappedFile.Tests;

public class ReadOnlyBatchContractTests
{
    [Theory]
    [InlineData(true, 1)]
    [InlineData(false, 1)]
    [InlineData(true, 4)]
    [InlineData(false, 4)]
    public async Task ConsumeAsync_Exposes_Count_And_Index_For_A_Batch(bool autoCommit, int batchSize)
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "readonly-batch",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 1,
            SegmentSizeInBytes = 1024
        };
        using var queue = new MemoryMappedFileBufferQueue<int>(options);
        await queue.GetProducer().ProduceAsync(42);

        var consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = options.TopicName,
            GroupName = "group",
            BatchSize = batchSize,
            AutoCommit = autoCommit
        });

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var reader = consumer.ConsumeAsync(cancellation.Token).GetAsyncEnumerator();
        Assert.True(await reader.MoveNextAsync());
        var batch = reader.Current;

        var count = batch.Count;
        Assert.Equal(1, count);
        Assert.Equal(42, batch[0]);

        if (!autoCommit)
        {
            await consumer.CommitAsync();
        }

        await queue.GetProducer().ProduceAsync(43, cancellation.Token);
        Assert.True(await reader.MoveNextAsync());
        Assert.Equal(43, reader.Current[0]);
        if (!autoCommit)
        {
            await consumer.CommitAsync();
        }

        count = batch.Count;
        Assert.Equal(1, count);
        Assert.Equal(42, batch[0]);
    }
}
