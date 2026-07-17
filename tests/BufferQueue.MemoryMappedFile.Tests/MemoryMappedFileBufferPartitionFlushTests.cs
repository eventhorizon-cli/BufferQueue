// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.Buffers.Binary;
using System.IO.MemoryMappedFiles;
using System.Text.Json;
using BufferQueue.MemoryMappedFile;

namespace BufferQueue.MemoryMappedFile.Tests;

public class MemoryMappedFileBufferPartitionFlushTests
{
    [Fact]
    public void Options_Have_Expected_Defaults()
    {
        var options = new MemoryMappedFileBufferQueueOptions<int>();

        Assert.Equal(256L * 1024 * 1024, options.SegmentSizeInBytes);
        Assert.Null(options.MaxRetainedConsumedSegments);
        Assert.Equal(MemoryMappedFileFlushStrategy.Immediate, options.FlushStrategy);
        Assert.Equal(100, options.FlushBatchSize);

        var payload = options.Serializer.Serialize(42);
        Assert.Equal(42, options.Serializer.Deserialize(payload));
    }

    [Fact]
    public void Partition_Uses_Configured_Serializer_For_Write_And_Read()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        var serializer = new Int32MemoryMappedFileSerializer();
        options.Serializer = serializer;
        using var partition = new MemoryMappedFileBufferPartition<int>(
            0,
            options,
            new RecordingMemoryMappedFileFlusher());

        partition.Enqueue(42);
        Assert.True(partition.TryPull("TestGroup", 1, out var items));

        Assert.Equal(new[] { 42 }, items);
        Assert.Equal(1, serializer.SerializeCount);
        Assert.Equal(1, serializer.DeserializeCount);
    }

    [Fact]
    public void Default_Serializer_Will_Throw_If_Payload_Deserializes_To_Null()
    {
        var options = new MemoryMappedFileBufferQueueOptions<string>();

        Assert.Throws<JsonException>(() => options.Serializer.Deserialize("null"u8.ToArray()));
    }

    [Fact]
    public void Enqueue_Will_Throw_If_Serializer_Returns_Null_Payload()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        options.Serializer = new NullPayloadMemoryMappedFileSerializer();
        using var partition = new MemoryMappedFileBufferPartition<int>(
            0,
            options,
            new RecordingMemoryMappedFileFlusher());

        var exception = Assert.Throws<InvalidOperationException>(() => partition.Enqueue(42));

        Assert.Contains("returned a null payload", exception.Message);
    }

    [Fact]
    public void Pull_Will_Throw_If_Serializer_Returns_Null_Item()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = new MemoryMappedFileBufferQueueOptions<string>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            SegmentSizeInBytes = 1024,
            Serializer = new NullItemMemoryMappedFileSerializer()
        };
        using var partition = new MemoryMappedFileBufferPartition<string>(
            0,
            options,
            new RecordingMemoryMappedFileFlusher());
        partition.Enqueue("value");

        var exception = Assert.Throws<InvalidDataException>(() =>
        {
            partition.TryPull("TestGroup", 1, out _);
        });

        Assert.Contains("deserialized a record to null", exception.Message);
    }

    [Fact]
    public void Immediate_Strategy_Flushes_Every_Record()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        var flusher = new RecordingMemoryMappedFileFlusher();
        using var partition = new MemoryMappedFileBufferPartition<int>(0, options, flusher);

        partition.Enqueue(1);
        partition.Enqueue(2);

        Assert.Equal(2, flusher.FlushCount);
        Assert.Equal(18, ReadInt64(GetProducerOffsetFilePath(options)));
    }

    [Fact]
    public void Batch_Strategy_Flushes_When_Threshold_Is_Reached()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        options.FlushStrategy = MemoryMappedFileFlushStrategy.Batch;
        options.FlushBatchSize = 3;
        var flusher = new RecordingMemoryMappedFileFlusher();
        using var partition = new MemoryMappedFileBufferPartition<int>(0, options, flusher);

        partition.Enqueue(1);
        partition.Enqueue(2);

        Assert.Equal(0, flusher.FlushCount);
        Assert.False(File.Exists(GetProducerOffsetFilePath(options)));

        partition.Enqueue(3);

        Assert.Equal(1, flusher.FlushCount);
        Assert.Equal(27, ReadInt64(GetProducerOffsetFilePath(options)));

        partition.Enqueue(4);
        partition.Enqueue(5);

        Assert.Equal(1, flusher.FlushCount);

        partition.Enqueue(6);

        Assert.Equal(2, flusher.FlushCount);
        Assert.Equal(54, ReadInt64(GetProducerOffsetFilePath(options)));
    }

    [Fact]
    public void Batch_Strategy_Counts_Records_Per_Partition()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        options.FlushStrategy = MemoryMappedFileFlushStrategy.Batch;
        options.FlushBatchSize = 2;
        var firstFlusher = new RecordingMemoryMappedFileFlusher();
        var secondFlusher = new RecordingMemoryMappedFileFlusher();
        using var firstPartition = new MemoryMappedFileBufferPartition<int>(0, options, firstFlusher);
        using var secondPartition = new MemoryMappedFileBufferPartition<int>(1, options, secondFlusher);

        firstPartition.Enqueue(1);
        secondPartition.Enqueue(2);

        Assert.Equal(0, firstFlusher.FlushCount);
        Assert.Equal(0, secondFlusher.FlushCount);

        firstPartition.Enqueue(3);

        Assert.Equal(1, firstFlusher.FlushCount);
        Assert.Equal(0, secondFlusher.FlushCount);
    }

    [Fact]
    public void Segment_Rotation_Forces_Pending_Records_To_Flush()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        options.FlushStrategy = MemoryMappedFileFlushStrategy.Batch;
        options.FlushBatchSize = 3;
        options.SegmentSizeInBytes = 22;
        var flusher = new RecordingMemoryMappedFileFlusher();
        using var partition = new MemoryMappedFileBufferPartition<int>(0, options, flusher);

        partition.Enqueue(1);
        partition.Enqueue(2);
        partition.Enqueue(3);

        Assert.Equal(1, flusher.FlushCount);
        Assert.Equal(22, ReadInt64(GetProducerOffsetFilePath(options)));
    }

    [Fact]
    public void Segment_Rotation_Flushes_When_No_End_Marker_Fits()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        options.FlushStrategy = MemoryMappedFileFlushStrategy.Batch;
        options.FlushBatchSize = 100;
        options.SegmentSizeInBytes = 20;
        var flusher = new RecordingMemoryMappedFileFlusher();
        using var partition = new MemoryMappedFileBufferPartition<int>(0, options, flusher);

        partition.Enqueue(1);
        partition.Enqueue(2);
        partition.Enqueue(3);

        Assert.Equal(1, flusher.FlushCount);
        Assert.Equal(20, ReadInt64(GetProducerOffsetFilePath(options)));
        Assert.True(partition.TryPull("TestGroup", 3, out var items));
        Assert.Equal(new[] { 1, 2, 3 }, items);
    }

    [Fact]
    public void Filling_A_Segment_Forces_Pending_Records_To_Flush()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        options.FlushStrategy = MemoryMappedFileFlushStrategy.Batch;
        options.FlushBatchSize = 100;
        options.SegmentSizeInBytes = 18;
        var flusher = new RecordingMemoryMappedFileFlusher();
        using var partition = new MemoryMappedFileBufferPartition<int>(0, options, flusher);

        partition.Enqueue(1);
        partition.Enqueue(2);

        Assert.Equal(1, flusher.FlushCount);
        Assert.Equal(18, ReadInt64(GetProducerOffsetFilePath(options)));
    }

    [Fact]
    public void Commit_Flushes_Pending_Records_Before_Persisting_Consumer_Offset()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        options.FlushStrategy = MemoryMappedFileFlushStrategy.Batch;
        options.FlushBatchSize = 100;
        var flusher = new RecordingMemoryMappedFileFlusher();
        using var partition = new MemoryMappedFileBufferPartition<int>(0, options, flusher);

        partition.Enqueue(1);
        partition.Enqueue(2);
        Assert.True(partition.TryPull("TestGroup", 2, out var items));

        partition.Commit("TestGroup");

        Assert.Equal(new[] { 1, 2 }, items);
        Assert.Equal(1, flusher.FlushCount);
        Assert.Equal(18, ReadInt64(GetProducerOffsetFilePath(options)));
        Assert.Equal(18, ReadInt64(GetConsumerOffsetFilePath(options, "TestGroup")));
        Assert.False(partition.TryPull("TestGroup", 2, out _));
    }

    [Fact]
    public void Commit_Does_Not_Advance_Checkpoints_If_Flush_Fails()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        options.FlushStrategy = MemoryMappedFileFlushStrategy.Batch;
        options.FlushBatchSize = 100;
        using var partition = new MemoryMappedFileBufferPartition<int>(
            0,
            options,
            new ThrowingMemoryMappedFileFlusher());

        partition.Enqueue(1);
        Assert.True(partition.TryPull("TestGroup", 1, out _));

        Assert.Throws<IOException>(() => partition.Commit("TestGroup"));
        Assert.False(File.Exists(GetProducerOffsetFilePath(options)));
        Assert.Equal(0, ReadInt64(GetConsumerOffsetFilePath(options, "TestGroup")));
        Assert.True(partition.TryPull("TestGroup", 1, out var replayedItems));
        Assert.Equal(new[] { 1 }, replayedItems);
    }

    [Fact]
    public void Commit_Flushes_Records_Scanned_Past_The_Producer_Checkpoint()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path);
        options.FlushStrategy = MemoryMappedFileFlushStrategy.Batch;
        options.FlushBatchSize = 100;
        using var originalPartition = new MemoryMappedFileBufferPartition<int>(
            0,
            options,
            new RecordingMemoryMappedFileFlusher());
        originalPartition.Enqueue(1);
        originalPartition.Enqueue(2);
        Assert.False(File.Exists(GetProducerOffsetFilePath(options)));

        var restoredFlusher = new RecordingMemoryMappedFileFlusher();
        using var restoredPartition = new MemoryMappedFileBufferPartition<int>(0, options, restoredFlusher);
        Assert.True(restoredPartition.TryPull("TestGroup", 2, out var items));

        restoredPartition.Commit("TestGroup");

        Assert.Equal(new[] { 1, 2 }, items);
        Assert.Equal(1, restoredFlusher.FlushCount);
        Assert.Equal(18, ReadInt64(GetProducerOffsetFilePath(options)));
        Assert.Equal(18, ReadInt64(GetConsumerOffsetFilePath(options, "TestGroup")));
    }

    private static MemoryMappedFileBufferQueueOptions<int> CreateOptions(string dataDirectory) =>
        new()
        {
            TopicName = "test",
            DataDirectory = dataDirectory,
            PartitionNumber = 1,
            SegmentSizeInBytes = 1024,
            Serializer = new Int32MemoryMappedFileSerializer()
        };

    private static string GetProducerOffsetFilePath(MemoryMappedFileBufferQueueOptions<int> options) =>
        Path.Combine(options.DataDirectory, options.TopicName!, "partition-00000", "producer.offset");

    private static string GetConsumerOffsetFilePath(
        MemoryMappedFileBufferQueueOptions<int> options,
        string groupName) =>
        Path.Combine(
            options.DataDirectory,
            options.TopicName!,
            "partition-00000",
            "offsets",
            groupName,
            "consumer.offset");

    private static long ReadInt64(string filePath) =>
        BinaryPrimitives.ReadInt64LittleEndian(File.ReadAllBytes(filePath));

    private sealed class RecordingMemoryMappedFileFlusher : IMemoryMappedFileFlusher
    {
        public int FlushCount { get; private set; }

        public void Flush(MemoryMappedViewAccessor accessor) => FlushCount++;
    }

    private sealed class ThrowingMemoryMappedFileFlusher : IMemoryMappedFileFlusher
    {
        public void Flush(MemoryMappedViewAccessor accessor) =>
            throw new IOException("Simulated flush failure.");
    }

    private sealed class Int32MemoryMappedFileSerializer : IMemoryMappedFileSerializer<int>
    {
        public int SerializeCount { get; private set; }

        public int DeserializeCount { get; private set; }

        public byte[] Serialize(int item)
        {
            SerializeCount++;
            return BitConverter.GetBytes(item);
        }

        public int Deserialize(ReadOnlyMemory<byte> payload)
        {
            DeserializeCount++;
            return BitConverter.ToInt32(payload.Span);
        }
    }

    private sealed class NullPayloadMemoryMappedFileSerializer : IMemoryMappedFileSerializer<int>
    {
        public byte[] Serialize(int item) => null!;

        public int Deserialize(ReadOnlyMemory<byte> payload) => 0;
    }

    private sealed class NullItemMemoryMappedFileSerializer : IMemoryMappedFileSerializer<string>
    {
        public byte[] Serialize(string item) => [1];

        public string Deserialize(ReadOnlyMemory<byte> payload) => null!;
    }
}
