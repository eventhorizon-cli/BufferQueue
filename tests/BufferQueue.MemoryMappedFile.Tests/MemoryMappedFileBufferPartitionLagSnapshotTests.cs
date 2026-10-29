using System.Text;

namespace BufferQueue.MemoryMappedFile.Tests;

public class MemoryMappedFileBufferPartitionLagSnapshotTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Consumer_Lag_Uses_Byte_Units_And_Follows_Commit_Mode(bool autoCommit)
    {
        using var temporaryDirectory = new TemporaryDirectory();
        using var queue = new MemoryMappedFileBufferQueue<int>(new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 1,
            SegmentSizeInBytes = 64
        });
        var consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "group",
            BatchSize = 2,
            AutoCommit = autoCommit
        });
        await queue.GetProducer().ProduceAsync(new[] { 1, 2, 3 });

        Assert.True(consumer.TryGetLagSnapshot(out var beforePull));
        Assert.Equal(18UL, beforePull.OffsetLag);
        Assert.Equal(BufferOffsetUnit.Bytes, beforePull.Unit);

        using var cancellation = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        await using var reader = consumer.ConsumeAsync(cancellation.Token).GetAsyncEnumerator();
        Assert.True(await reader.MoveNextAsync());
        Assert.Equal(new[] { 1, 2 }, reader.Current);

        if (autoCommit)
        {
            AssertLag(consumer, 6);
        }
        else
        {
            AssertLag(consumer, 18);
            await consumer.CommitAsync();
            AssertLag(consumer, 6);
        }
    }

    [Fact]
    public void TryGetLagSnapshot_Returns_False_Without_Creating_A_Reader_Or_After_Disposal()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        using var partition = new MemoryMappedFileBufferPartition<string>(0, options);
        partition.Enqueue("value");

        Assert.False(partition.TryGetLagSnapshot("unknown", out var absentSnapshot));
        Assert.Equal(default, absentSnapshot);
        Assert.Empty(Directory.EnumerateDirectories(GetOffsetDirectory(options)));

        partition.Dispose();

        Assert.False(partition.TryGetLagSnapshot("unknown", out var disposedSnapshot));
        Assert.Equal(default, disposedSnapshot);
    }

    [Fact]
    public void TryGetLagSnapshot_Uses_Byte_Offsets_And_Does_Not_Advance_On_Uncommitted_Pull()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        using var partition = new MemoryMappedFileBufferPartition<string>(0, options);
        Assert.False(partition.TryGetLagSnapshot("group", out _));

        Assert.False(partition.TryPull("group", 1, out _));
        Assert.True(partition.TryGetLagSnapshot("group", out var emptySnapshot));
        Assert.Equal(0UL, emptySnapshot.OffsetLag);
        Assert.Equal(BufferOffsetUnit.Bytes, emptySnapshot.Unit);

        partition.Enqueue("a");
        partition.Enqueue("long");

        Assert.True(partition.TryGetLagSnapshot("group", out var beforePull));
        Assert.Equal(5UL + 1UL + 5UL + 4UL, beforePull.OffsetLag);
        Assert.Equal(BufferOffsetUnit.Bytes, beforePull.Unit);

        Assert.True(partition.TryPull("group", 1, out _));
        Assert.True(partition.TryGetLagSnapshot("group", out var afterUncommittedPull));
        Assert.Equal(beforePull, afterUncommittedPull);

        partition.Commit("group");

        Assert.True(partition.TryGetLagSnapshot("group", out var afterCommit));
        Assert.Equal(5UL + 4UL, afterCommit.OffsetLag);
        Assert.Equal(BufferOffsetUnit.Bytes, afterCommit.Unit);
    }

    [Fact]
    public void TryGetLagSnapshot_Includes_Unflushed_Batch_Tail_In_The_Current_Process()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        options.FlushStrategy = MemoryMappedFileFlushStrategy.Batch;
        options.FlushBatchSize = 100;
        using var partition = new MemoryMappedFileBufferPartition<string>(0, options);
        Assert.False(partition.TryPull("group", 1, out _));

        partition.Enqueue("tail");

        Assert.True(partition.TryGetLagSnapshot("group", out var snapshot));
        Assert.Equal(9UL, snapshot.OffsetLag);
        Assert.Equal(BufferOffsetUnit.Bytes, snapshot.Unit);
    }

    [Fact]
    public void TryGetLagSnapshot_Counts_Offset_Bytes_Across_Segment_Padding()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        options.SegmentSizeInBytes = 16;
        using var partition = new MemoryMappedFileBufferPartition<string>(0, options);
        Assert.False(partition.TryPull("group", 1, out _));

        partition.Enqueue("12345");
        partition.Enqueue("abcde");

        Assert.True(partition.TryGetLagSnapshot("group", out var snapshot));
        Assert.Equal(26UL, snapshot.OffsetLag);
        Assert.Equal(BufferOffsetUnit.Bytes, snapshot.Unit);
    }

    [Fact]
    public void TryGetLagSnapshot_Restores_The_Backlog_From_Persisted_Offsets()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        using (var partition = new MemoryMappedFileBufferPartition<string>(0, options))
        {
            Assert.False(partition.TryPull("group", 1, out _));
            partition.Enqueue("first");
            partition.Enqueue("second");
        }

        using var restored = new MemoryMappedFileBufferPartition<string>(0, options);
        Assert.True(restored.TryPull("group", 1, out _));

        Assert.True(restored.TryGetLagSnapshot("group", out var snapshot));
        Assert.Equal((5UL + 5UL) + (5UL + 6UL), snapshot.OffsetLag);
        Assert.Equal(BufferOffsetUnit.Bytes, snapshot.Unit);
    }

    [Fact]
    public void TryGetLagSnapshot_Returns_False_After_Reader_Removal()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        using var partition = new MemoryMappedFileBufferPartition<string>(0, options);
        var consumer = new TestPartitionConsumer("group");
        partition.RegisterConsumer(consumer);

        Assert.True(partition.TryGetLagSnapshot("group", out _));

        partition.UnregisterConsumer(consumer);

        Assert.False(partition.TryGetLagSnapshot("group", out var snapshot));
        Assert.Equal(default, snapshot);
        Assert.False(Directory.Exists(Path.Combine(GetOffsetDirectory(options), "group")));
    }

    private static MemoryMappedFileBufferQueueOptions<string> CreateOptions(string dataDirectory) => new()
    {
        TopicName = "test",
        DataDirectory = dataDirectory,
        PartitionNumber = 1,
        SegmentSizeInBytes = 64,
        Serializer = new Utf8StringSerializer()
    };

    private static string GetOffsetDirectory(MemoryMappedFileBufferQueueOptions<string> options) =>
        Path.Combine(options.DataDirectory, options.TopicName!, "partition-00000", "offsets");

    private static void AssertLag(IBufferPullConsumer<int> consumer, ulong expected)
    {
        Assert.True(consumer.TryGetLagSnapshot(out var snapshot));
        Assert.Equal(expected, snapshot.OffsetLag);
        Assert.Equal(BufferOffsetUnit.Bytes, snapshot.Unit);
    }

    private sealed class Utf8StringSerializer : IMemoryMappedFileSerializer<string>
    {
        public byte[] Serialize(string item) => Encoding.UTF8.GetBytes(item);

        public string Deserialize(ReadOnlyMemory<byte> payload) => Encoding.UTF8.GetString(payload.Span);
    }

    private sealed class TestPartitionConsumer(string groupName) : IBufferPartitionConsumer<string>
    {
        public string GroupName { get; } = groupName;

        public void NotifyNewDataAvailable(IBufferPartition<string> partition)
        {
        }
    }
}
