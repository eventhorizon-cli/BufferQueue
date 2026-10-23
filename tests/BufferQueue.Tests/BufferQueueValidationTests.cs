using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;

namespace BufferQueue.Tests;

public class BufferQueueValidationTests
{
    [Fact]
    public void GenericQueue_Validates_Consumer_Options_And_Duplicate_Groups()
    {
        var queue = new TestQueue(CreatePartitions(3));

        Assert.Throws<ArgumentOutOfRangeException>(() => queue.CreateConsumers(Options("group"), 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => queue.CreateConsumers(Options("group"), 4));
        Assert.Throws<ArgumentException>(() => queue.CreateConsumers(Options(" "), 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => queue.CreateConsumers(Options("group", batchSize: 0), 1));

        _ = queue.CreateConsumer(Options("group"));
        var duplicate = Assert.Throws<InvalidOperationException>(() => queue.CreateConsumer(Options("group")));
        Assert.Contains("already exists", duplicate.Message);
    }

    [Fact]
    public void GenericQueue_Assigns_Remainder_Partitions_Evenly()
    {
        var partitions = CreatePartitions(5);
        var queue = new TestQueue(partitions);

        var consumers = queue.CreateConsumers(Options("group"), 2).ToArray();

        Assert.Equal(3, partitions.Count(partition => ReferenceEquals(partition.RegisteredConsumer, consumers[0])));
        Assert.Equal(2, partitions.Count(partition => ReferenceEquals(partition.RegisteredConsumer, consumers[1])));
        Assert.Equal(new[] { 0, 1, 2 }, partitions
            .Where(partition => ReferenceEquals(partition.RegisteredConsumer, consumers[0]))
            .Select(partition => partition.PartitionId));
        Assert.Equal(new[] { 3, 4 }, partitions
            .Where(partition => ReferenceEquals(partition.RegisteredConsumer, consumers[1]))
            .Select(partition => partition.PartitionId));
    }

    [Fact]
    public void GenericQueue_Rolls_Back_Assigned_Consumers_When_Assignment_Fails()
    {
        var first = new TestPartition(0);
        var failing = new TestPartition(1) { ThrowOnRegister = true };
        var queue = new TestQueue(new IBufferPartition<int>[] { first, failing });

        Assert.Throws<InvalidOperationException>(() => queue.CreateConsumers(Options("group"), 2).ToArray());
        Assert.Equal(1, first.UnregisterCount);
        Assert.Null(first.RegisteredConsumer);
    }

    [Fact]
    public void PublicQueue_Validates_Topic_And_Consumer_Number()
    {
        var services = new ServiceCollection();
        services.AddBufferQueue(builder => builder.UseMemory(memory => memory.AddTopic<int>(options =>
        {
            options.TopicName = "topic";
            options.PartitionNumber = 2;
        })));
        using var provider = services.BuildServiceProvider();
        var queue = provider.GetRequiredService<IBufferQueue>();

        Assert.Throws<ArgumentException>(() => queue.GetProducer<int>(" "));
        Assert.Throws<ArgumentException>(() => queue.GetProducer<int>("missing"));
        Assert.Throws<ArgumentException>(() => queue.CreatePullConsumer<int>(Options("group", topicName: " ")));
        Assert.Throws<ArgumentException>(() => queue.CreatePullConsumer<int>(Options("group", topicName: "missing")));
        Assert.Throws<ArgumentException>(() => queue.CreatePullConsumers<int>(Options("group", topicName: " "), 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => queue.CreatePullConsumers<int>(Options("group"), 0));

        Assert.Equal("topic", queue.GetProducer<int>("topic").TopicName);
    }

    private static BufferPullConsumerOptions Options(string groupName, string topicName = "test", int batchSize = 1) =>
        new()
        {
            TopicName = topicName,
            GroupName = groupName,
            BatchSize = batchSize
        };

    private static TestPartition[] CreatePartitions(int count) =>
        Enumerable.Range(0, count).Select(index => new TestPartition(index)).ToArray();

    private sealed class TestQueue(IBufferPartition<int>[] partitions)
        : BufferQueue<int>("test", partitions, new TestProducer());

    private sealed class TestPartition(int partitionId) : IBufferPartition<int>
    {
        public int PartitionId { get; } = partitionId;
        public bool ThrowOnRegister { get; init; }
        public IBufferPartitionConsumer<int>? RegisteredConsumer { get; private set; }
        public int UnregisterCount { get; private set; }

        public void RegisterConsumer(IBufferPartitionConsumer<int> consumer)
        {
            if (ThrowOnRegister)
            {
                throw new InvalidOperationException("registration failed");
            }

            RegisteredConsumer = consumer;
        }

        public void UnregisterConsumer(IBufferPartitionConsumer<int> consumer)
        {
            Assert.Same(consumer, RegisteredConsumer);
            RegisteredConsumer = null;
            UnregisterCount++;
        }

        public void Enqueue(int item) => throw new NotSupportedException();

        public bool TryPull(string groupName, int batchSize, [NotNullWhen(true)] out IEnumerable<int>? items)
        {
            items = null;
            return false;
        }

        public void Commit(string groupName) => throw new NotSupportedException();
    }

    private sealed class TestProducer : IBufferProducer<int>
    {
        public string TopicName => "test";

        public ValueTask<bool> TryProduceAsync(int item, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask<bool> TryProduceAsync(ReadOnlyMemory<int> items, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask ProduceAsync(int item, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();

        public ValueTask ProduceAsync(ReadOnlyMemory<int> items, CancellationToken cancellationToken = default) =>
            throw new NotSupportedException();
    }
}
