using System.Text.Json;

namespace BufferQueue.MemoryMappedFile.Tests;

public class MemoryMappedFileBufferProducerTests
{
    [Fact]
    public async Task TryProduceAsync_Batch_Writes_All_Items()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        using var queue = CreateQueue(temporaryDirectory.Path);
        var producer = queue.GetProducer();

        Assert.True(await producer.TryProduceAsync(new[] { 1, 2, 3 }.AsMemory()));

        Assert.Equal(new[] { 1, 2, 3 }, await ConsumeOneBatchAsync(queue));
    }

    [Fact]
    public async Task TryProduceAsync_Canceled_Single_Does_Not_Write()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        using var queue = CreateQueue(temporaryDirectory.Path);
        var producer = queue.GetProducer();
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await producer.TryProduceAsync(1, cancellationTokenSource.Token));

        await producer.ProduceAsync(2);
        Assert.Equal(new[] { 2 }, await ConsumeOneBatchAsync(queue));
    }

    [Fact]
    public async Task TryProduceAsync_Canceled_Batch_Does_Not_Write()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        using var queue = CreateQueue(temporaryDirectory.Path);
        var producer = queue.GetProducer();
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await producer.TryProduceAsync(new[] { 1, 2, 3 }.AsMemory(), cancellationTokenSource.Token));

        await producer.ProduceAsync(4);
        Assert.Equal(new[] { 4 }, await ConsumeOneBatchAsync(queue));
    }

    [Fact]
    public async Task ProduceAsync_Canceled_Single_Does_Not_Write()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        using var queue = CreateQueue(temporaryDirectory.Path);
        var producer = queue.GetProducer();
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await producer.ProduceAsync(1, cancellationTokenSource.Token));

        await producer.ProduceAsync(2);
        Assert.Equal(new[] { 2 }, await ConsumeOneBatchAsync(queue));
    }

    [Fact]
    public async Task ProduceAsync_Canceled_Batch_Does_Not_Write()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        using var queue = CreateQueue(temporaryDirectory.Path);
        var producer = queue.GetProducer();
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(async () =>
            await producer.ProduceAsync(new[] { 1, 2, 3 }.AsMemory(), cancellationTokenSource.Token));

        await producer.ProduceAsync(4);
        Assert.Equal(new[] { 4 }, await ConsumeOneBatchAsync(queue));
    }

    [Fact]
    public async Task ProduceAsync_Batch_Does_Not_Recheck_Cancellation_During_Write()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        using var cancellationTokenSource = new CancellationTokenSource();
        var options = CreateOptions(temporaryDirectory.Path);
        options.Serializer = new CancelOnFirstSerializeMemoryMappedFileSerializer(cancellationTokenSource);
        using var queue = new MemoryMappedFileBufferQueue<int>(options);
        var producer = queue.GetProducer();

        await producer.ProduceAsync(new[] { 1, 2, 3 }.AsMemory(), cancellationTokenSource.Token);

        Assert.Equal(new[] { 1, 2, 3 }, await ConsumeOneBatchAsync(queue));
    }

    private static MemoryMappedFileBufferQueue<int> CreateQueue(string dataDirectory) =>
        new(new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = dataDirectory,
            SegmentSizeInBytes = 1024
        });

    private static MemoryMappedFileBufferQueueOptions<int> CreateOptions(string dataDirectory) =>
        new()
        {
            TopicName = "test",
            DataDirectory = dataDirectory,
            SegmentSizeInBytes = 1024
        };

    private static async Task<int[]> ConsumeOneBatchAsync(MemoryMappedFileBufferQueue<int> queue)
    {
        var consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "test-group",
            AutoCommit = true,
            BatchSize = 10
        });

        await foreach (var items in consumer.ConsumeAsync())
        {
            return items.ToArray();
        }

        throw new InvalidOperationException("The consumer completed without returning a batch.");
    }

    private sealed class CancelOnFirstSerializeMemoryMappedFileSerializer(
        CancellationTokenSource cancellationTokenSource)
        : IMemoryMappedFileSerializer<int>
    {
        private int _serializeCount;

        public byte[] Serialize(int item)
        {
            if (Interlocked.Increment(ref _serializeCount) == 1)
            {
                cancellationTokenSource.Cancel();
            }

            return JsonSerializer.SerializeToUtf8Bytes(item);
        }

        public int Deserialize(ReadOnlyMemory<byte> payload) =>
            JsonSerializer.Deserialize<int>(payload.Span);
    }
}
