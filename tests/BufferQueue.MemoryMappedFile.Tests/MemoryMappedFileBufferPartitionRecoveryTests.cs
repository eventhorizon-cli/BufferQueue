using System.Buffers.Binary;

namespace BufferQueue.MemoryMappedFile.Tests;

public class MemoryMappedFileBufferPartitionRecoveryTests
{
    [Theory]
    [InlineData(0, 0)]
    [InlineData(-2, 0)]
    [InlineData(30, 0)]
    [InlineData(4, 0)]
    public void Recovery_Rejects_Checkpoint_After_An_Invalid_Record(int length, byte endMarker)
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        WriteSegment(options, length, endMarker);
        WriteOffset(options, "producer.offset", 9);

        var exception = Assert.Throws<InvalidDataException>(
            () => new MemoryMappedFileBufferPartition<int>(0, options));

        Assert.Contains("record boundary", exception.Message);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-2)]
    [InlineData(30)]
    [InlineData(4)]
    public void Recovery_Without_Checkpoint_Replaces_An_Incomplete_Tail(int length)
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        WriteSegment(options, length, endMarker: 0);

        using (var partition = new MemoryMappedFileBufferPartition<int>(0, options))
        {
            Assert.False(partition.TryPull("group", 1, out _));
            partition.Enqueue(42);
        }

        using var restored = new MemoryMappedFileBufferPartition<int>(0, options);
        Assert.True(restored.TryPull("group", 2, out var items));
        Assert.Equal(new[] { 42 }, items);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(31)]
    [InlineData(33)]
    public void Recovery_Rejects_Segments_With_An_Unexpected_Size(int size)
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        Directory.CreateDirectory(GetPartitionDirectory(options));
        File.WriteAllBytes(GetSegmentPath(options), new byte[size]);

        var exception = Assert.Throws<InvalidDataException>(
            () => new MemoryMappedFileBufferPartition<int>(0, options));

        Assert.Contains("must be exactly 32 bytes", exception.Message);
        Assert.Equal(size, new FileInfo(GetSegmentPath(options)).Length);
    }

    [Fact]
    public void Recovery_Rejects_A_Producer_Checkpoint_After_The_Log_Files()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        WriteSegment(options, 4, 0xBB);
        WriteOffset(options, "producer.offset", 33);

        var exception = Assert.Throws<InvalidDataException>(
            () => new MemoryMappedFileBufferPartition<int>(0, options));

        Assert.Contains("greater than the memory-mapped-file log size", exception.Message);
    }

    [Fact]
    public void Recovery_Rejects_A_Producer_Checkpoint_Without_Log_Files()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        WriteOffset(options, "producer.offset", 9);

        var exception = Assert.Throws<InvalidDataException>(
            () => new MemoryMappedFileBufferPartition<int>(0, options));

        Assert.Contains("log that does not exist", exception.Message);
        Assert.False(File.Exists(GetSegmentPath(options)));
    }

    [Fact]
    public void Recovery_Rejects_A_Producer_Checkpoint_Before_Retained_Data()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        WriteOffset(options, "earliest.offset", 32);
        WriteOffset(options, "producer.offset", 0);

        var exception = Assert.Throws<InvalidDataException>(
            () => new MemoryMappedFileBufferPartition<int>(0, options));

        Assert.Contains("less than the earliest offset", exception.Message);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void A_Group_Directory_Without_A_Checkpoint_Is_Rejected(bool beforeStartup)
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        var groupDirectory = Path.Combine(GetPartitionDirectory(options), "offsets", "group");
        if (beforeStartup)
        {
            Directory.CreateDirectory(groupDirectory);
            var exception = Assert.Throws<InvalidDataException>(
                () => new MemoryMappedFileBufferPartition<int>(0, options));
            Assert.Contains("without an offset checkpoint", exception.Message);
        }
        else
        {
            using var partition = new MemoryMappedFileBufferPartition<int>(0, options);
            Directory.CreateDirectory(groupDirectory);
            var exception = Assert.Throws<InvalidDataException>(() => partition.TryPull("group", 1, out _));
            Assert.Contains("without an offset checkpoint", exception.Message);
        }

        Assert.False(File.Exists(Path.Combine(groupDirectory, "consumer.offset")));
    }

    [Fact]
    public void Dispose_Is_Idempotent_And_Rejects_Further_Operations()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        using var partition = new MemoryMappedFileBufferPartition<int>(0, options);
        partition.Enqueue(1);

        partition.Dispose();
        partition.Dispose();

        Assert.Throws<ObjectDisposedException>(() => partition.Enqueue(2));
        Assert.Throws<ObjectDisposedException>(() => partition.TryPull("group", 1, out _));
        Assert.Throws<ObjectDisposedException>(() => partition.Commit("group"));
    }

    [Fact]
    public void Commit_Rejects_An_Unknown_Group_Without_Creating_A_Checkpoint()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        using var partition = new MemoryMappedFileBufferPartition<int>(0, options);

        Assert.Throws<InvalidOperationException>(() => partition.Commit("unknown"));
        Assert.Empty(Directory.EnumerateDirectories(Path.Combine(GetPartitionDirectory(options), "offsets")));
    }

    private static MemoryMappedFileBufferQueueOptions<int> CreateOptions(string dataDirectory) => new()
    {
        TopicName = "test",
        DataDirectory = dataDirectory,
        PartitionNumber = 1,
        SegmentSizeInBytes = 32,
        Serializer = new Int32Serializer()
    };

    private static string GetPartitionDirectory(MemoryMappedFileBufferQueueOptions<int> options) =>
        Path.Combine(options.DataDirectory, options.TopicName!, "partition-00000");

    private static string GetSegmentPath(MemoryMappedFileBufferQueueOptions<int> options) =>
        Path.Combine(GetPartitionDirectory(options), "00000000000000000000.log");

    private static void WriteSegment(MemoryMappedFileBufferQueueOptions<int> options, int length, byte endMarker)
    {
        Directory.CreateDirectory(GetPartitionDirectory(options));
        var bytes = new byte[options.SegmentSizeInBytes];
        BinaryPrimitives.WriteInt32LittleEndian(bytes, length);
        bytes[8] = endMarker;
        File.WriteAllBytes(GetSegmentPath(options), bytes);
    }

    private static void WriteOffset(MemoryMappedFileBufferQueueOptions<int> options, string fileName, long offset)
    {
        Directory.CreateDirectory(GetPartitionDirectory(options));
        var bytes = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, offset);
        File.WriteAllBytes(Path.Combine(GetPartitionDirectory(options), fileName), bytes);
    }

    private sealed class Int32Serializer : IMemoryMappedFileSerializer<int>
    {
        public byte[] Serialize(int item)
        {
            var bytes = new byte[sizeof(int)];
            BinaryPrimitives.WriteInt32LittleEndian(bytes, item);
            return bytes;
        }

        public int Deserialize(ReadOnlyMemory<byte> payload) => BinaryPrimitives.ReadInt32LittleEndian(payload.Span);
    }
}
