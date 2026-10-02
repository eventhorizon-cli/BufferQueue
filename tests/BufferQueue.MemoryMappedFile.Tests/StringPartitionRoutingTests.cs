namespace BufferQueue.MemoryMappedFile.Tests;

public class StringPartitionRoutingTests
{
    [Theory]
    [InlineData(0, 9)]
    [InlineData(1, 9)]
    [InlineData(2, 9)]
    [InlineData(0, 121)]
    [InlineData(1, 121)]
    [InlineData(2, 121)]
    [InlineData(0, 256)]
    [InlineData(1, 256)]
    [InlineData(2, 256)]
    public async Task Single_And_Batch_Production_Preserve_Key_Order_Across_Restart(int mode, int keyLength)
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = new MemoryMappedFileBufferQueueOptions<string>
        {
            TopicName = "routing",
            PartitionNumber = 8,
            DataDirectory = temporaryDirectory.Path,
            SegmentSizeInBytes = 1024
        };
        options.UsePartitionKey(static item => item.Split('|')[0]);
        var prefix = new string('x', keyLength - 9);
        var before = Enumerable.Range(0, 64).Select(i => $"{prefix}user:{i:D4}|first").ToArray();
        var after = Enumerable.Range(0, 64).Select(i => $"{prefix}user:{i:D4}|second").ToArray();
        using (var initial = new MemoryMappedFileBufferQueue<string>(options))
        {
            await Produce(initial.GetProducer(), before, mode);
        }
        using var queue = new MemoryMappedFileBufferQueue<string>(options);
        await Produce(queue.GetProducer(), after, mode);
        var consumers = queue.CreateConsumers(new BufferPullConsumerOptions
        {
            TopicName = "routing",
            GroupName = "group",
            BatchSize = 256,
            AutoCommit = true
        }, 8).ToArray();
        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        for (var partition = 0; partition < 8; partition++)
        {
            var expected = before.Concat(after)
                .Where(item => PartitionKeyRouting.SelectStringPartition(item.Split('|')[0], 8) == partition).ToArray();
            Assert.NotEmpty(expected);
            var actual = new List<string>();
            await foreach (var batch in consumers[partition].ConsumeAsync(cancellation.Token))
            {
                actual.AddRange(batch);
                if (actual.Count >= expected.Length)
                {
                    break;
                }
            }
            Assert.Equal(expected, actual);
        }
    }

    private static async Task Produce(IBufferProducer<string> producer, string[] items, int mode)
    {
        if (mode == 1)
        {
            await producer.ProduceAsync(items.AsMemory());
        }
        else if (mode == 2)
        {
            await producer.ProduceAsync((IEnumerable<string>)items);
        }
        else
        {
            foreach (var item in items)
            {
                await producer.ProduceAsync(item);
            }
        }
    }
}
