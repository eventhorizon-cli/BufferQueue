using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public class StringPartitionProductionTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task Single_And_Batch_Production_Preserve_Key_Order(int mode)
    {
        var options = new MemoryBufferQueueOptions<string>
        {
            TopicName = "routing",
            PartitionNumber = 8,
            SegmentSize = 128
        };
        options.UsePartitionKey(static item => item.Split('|')[0]);
        var before = Enumerable.Range(0, 64).Select(i => $"user:{i:D4}|first").ToArray();
        var after = Enumerable.Range(0, 64).Select(i => $"user:{i:D4}|second").ToArray();
        var queue = new MemoryBufferQueue<string>(options);
        await Produce(queue.GetProducer(), before, mode);
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
