// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.Buffers.Binary;
using BufferQueue.MemoryMappedFile;

namespace BufferQueue.MemoryMappedFile.Tests;

public class MemoryMappedFileBufferPartitionRetentionTests
{
    [Fact]
    public async Task Auto_Commit_Consumer_Deletes_Eligible_Segments_Before_Yielding_Materialized_Batch()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path, maxRetainedConsumedSegments: 0);
        using var queue = new MemoryMappedFileBufferQueue<int>(options);
        var producer = queue.GetProducer();
        for (var value = 1; value <= 4; value++)
        {
            await producer.ProduceAsync(value);
        }

        var consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "group",
            AutoCommit = true,
            BatchSize = 4
        });

        await foreach (var items in consumer.ConsumeAsync())
        {
            Assert.Equal(Enumerable.Range(1, 4), items);
            break;
        }

        Assert.Empty(GetSegmentIndices(options));
    }

    [Fact]
    public void Creating_A_Group_Persists_Its_Initial_Offset_For_Every_Assigned_Partition()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path, maxRetainedConsumedSegments: 0);
        options.PartitionNumber = 2;
        using var queue = new MemoryMappedFileBufferQueue<int>(options);

        queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "group",
            BatchSize = 1
        });

        Assert.Equal(0, ReadInt64(GetConsumerOffsetFilePath(options, 0, "group")));
        Assert.Equal(0, ReadInt64(GetConsumerOffsetFilePath(options, 1, "group")));
    }

    [Fact]
    public void Failed_Multi_Partition_Group_Creation_Rolls_Back_New_Checkpoints()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path, maxRetainedConsumedSegments: 0);
        options.PartitionNumber = 2;
        using var queue = new MemoryMappedFileBufferQueue<int>(options);
        var blockedGroupPath = Path.Combine(
            options.DataDirectory,
            options.TopicName!,
            "partition-00001",
            "offsets",
            "group");
        File.WriteAllText(blockedGroupPath, "blocked");

        Assert.Throws<IOException>(() => queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "group",
            BatchSize = 1
        }));
        Assert.False(Directory.Exists(Path.GetDirectoryName(GetConsumerOffsetFilePath(options, 0, "group"))));

        File.Delete(blockedGroupPath);
        queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "group",
            BatchSize = 1
        });
        Assert.Equal(0, ReadInt64(GetConsumerOffsetFilePath(options, 0, "group")));
        Assert.Equal(0, ReadInt64(GetConsumerOffsetFilePath(options, 1, "group")));
    }

    [Fact]
    public void Null_Retention_Does_Not_Delete_Consumed_Segments()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path, maxRetainedConsumedSegments: null);
        using var partition = new MemoryMappedFileBufferPartition<int>(0, options);
        Enqueue(partition, 4);

        Assert.True(partition.TryPull("group", 4, out _));
        partition.Commit("group");

        Assert.Equal(new long[] { 0, 1 }, GetSegmentIndices(options));
        Assert.False(File.Exists(GetEarliestOffsetFilePath(options)));
    }

    [Fact]
    public void Startup_Applies_Retention_To_Existing_Committed_Segments()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path, maxRetainedConsumedSegments: null);
        using (var partition = new MemoryMappedFileBufferPartition<int>(0, options))
        {
            Enqueue(partition, 4);
            Assert.True(partition.TryPull("group", 4, out _));
            partition.Commit("group");
        }

        options.MaxRetainedConsumedSegments = 0;
        using var restoredPartition = new MemoryMappedFileBufferPartition<int>(0, options);

        Assert.Empty(GetSegmentIndices(options));
        Assert.Equal(36, ReadInt64(GetEarliestOffsetFilePath(options)));
    }

    [Theory]
    [InlineData(20)]
    [InlineData(22)]
    public void Commit_Normalizes_Segment_Padding_Without_Skipping_Records(long segmentSize)
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path, maxRetainedConsumedSegments: 0);
        options.SegmentSizeInBytes = segmentSize;
        using var partition = new MemoryMappedFileBufferPartition<int>(0, options);
        Enqueue(partition, 3);

        Assert.True(partition.TryPull("group", 2, out var firstBatch));
        partition.Commit("group");

        Assert.Equal(new[] { 1, 2 }, firstBatch);
        Assert.Equal(new long[] { 1 }, GetSegmentIndices(options));
        Assert.Equal(segmentSize, ReadInt64(GetEarliestOffsetFilePath(options)));
        Assert.True(partition.TryPull("group", 1, out var secondBatch));
        Assert.Equal(new[] { 3 }, secondBatch);
    }

    [Fact]
    public void Other_Group_Commit_Does_Not_Reset_An_Uncommitted_Batch_During_Normalization()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path, maxRetainedConsumedSegments: 0);
        options.SegmentSizeInBytes = 22;
        using var partition = new MemoryMappedFileBufferPartition<int>(0, options);
        Enqueue(partition, 2);
        Assert.True(partition.TryPull("manual", 2, out _));
        partition.Commit("manual");

        partition.Enqueue(3);
        Assert.True(partition.TryPull("manual", 1, out var uncommittedBatch));
        Assert.Equal(new[] { 3 }, uncommittedBatch);
        Assert.True(partition.TryPull("other", 3, out _));
        partition.Commit("other");

        partition.Commit("manual");

        Assert.Equal(31, ReadInt64(GetConsumerOffsetFilePath(options, 0, "manual")));
        Assert.False(partition.TryPull("manual", 1, out _));
    }

    [Fact]
    public void Commit_Retains_Configured_Number_Of_Consumed_Segments()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path, maxRetainedConsumedSegments: 1);
        using var partition = new MemoryMappedFileBufferPartition<int>(0, options);
        Enqueue(partition, 8);

        Assert.True(partition.TryPull("existing", 8, out var consumedItems));
        partition.Commit("existing");

        Assert.Equal(Enumerable.Range(1, 8), consumedItems);
        Assert.Equal(new long[] { 3 }, GetSegmentIndices(options));
        Assert.Equal(54, ReadInt64(GetEarliestOffsetFilePath(options)));
        Assert.True(partition.TryPull("new", 8, out var retainedItems));
        Assert.Equal(new[] { 7, 8 }, retainedItems);
    }

    [Fact]
    public void Zero_Retention_Recovers_Without_Log_Files_And_Continues_Appending()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path, maxRetainedConsumedSegments: 0);
        using (var partition = new MemoryMappedFileBufferPartition<int>(0, options))
        {
            Enqueue(partition, 4);
            Assert.True(partition.TryPull("existing", 4, out _));
            partition.Commit("existing");

            Assert.Empty(GetSegmentIndices(options));
            Assert.Equal(36, ReadInt64(GetEarliestOffsetFilePath(options)));
        }

        using var restoredPartition = new MemoryMappedFileBufferPartition<int>(0, options);
        restoredPartition.Enqueue(5);

        Assert.Equal(new long[] { 2 }, GetSegmentIndices(options));
        Assert.True(restoredPartition.TryPull("new", 1, out var items));
        Assert.Equal(new[] { 5 }, items);
    }

    [Fact]
    public void Empty_Commit_After_Retention_Does_Not_Move_Offset_Before_Earliest()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path, maxRetainedConsumedSegments: 0);
        using var partition = new MemoryMappedFileBufferPartition<int>(0, options);
        Enqueue(partition, 4);
        Assert.True(partition.TryPull("existing", 4, out _));
        partition.Commit("existing");
        Assert.False(partition.TryPull("new", 1, out _));

        partition.Commit("new");

        Assert.Equal(36, ReadInt64(GetConsumerOffsetFilePath(options, 0, "new")));
    }

    [Fact]
    public void Uncommitted_Slow_Group_Prevents_Deletion()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path, maxRetainedConsumedSegments: 0);
        using var partition = new MemoryMappedFileBufferPartition<int>(0, options);
        Enqueue(partition, 4);

        Assert.True(partition.TryPull("slow", 2, out var uncommittedItems));
        Assert.Equal(new[] { 1, 2 }, uncommittedItems);
        Assert.True(partition.TryPull("fast", 4, out _));
        partition.Commit("fast");

        Assert.Equal(new long[] { 0, 1 }, GetSegmentIndices(options));
        Assert.True(partition.TryPull("slow", 4, out var replayedItems));
        Assert.Equal(Enumerable.Range(1, 4), replayedItems);
        partition.Commit("slow");
        Assert.Empty(GetSegmentIndices(options));
    }

    [Fact]
    public void Persisted_Offline_Group_Prevents_Deletion_After_Restart()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path, maxRetainedConsumedSegments: 0);
        using (var partition = new MemoryMappedFileBufferPartition<int>(0, options))
        {
            Enqueue(partition, 4);
            Assert.True(partition.TryPull("offline", 1, out _));
            Assert.True(partition.TryPull("fast", 4, out _));
            partition.Commit("fast");
        }

        using (var restoredPartition = new MemoryMappedFileBufferPartition<int>(0, options))
        {
            Assert.Equal(new long[] { 0, 1 }, GetSegmentIndices(options));
        }

        Directory.Delete(Path.GetDirectoryName(GetConsumerOffsetFilePath(options, 0, "offline"))!,
            recursive: true);
        using var secondRestoredPartition = new MemoryMappedFileBufferPartition<int>(0, options);
        Assert.Empty(GetSegmentIndices(options));
    }

    [Fact]
    public void Recovery_Fails_If_A_Retained_Segment_Is_Missing()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path, maxRetainedConsumedSegments: 2);
        using (var partition = new MemoryMappedFileBufferPartition<int>(0, options))
        {
            Enqueue(partition, 8);
            Assert.True(partition.TryPull("group", 8, out _));
            partition.Commit("group");
        }

        var missingSegmentPath = GetSegmentFilePath(options, 2);
        File.Delete(missingSegmentPath);

        Assert.Throws<InvalidDataException>(() => new MemoryMappedFileBufferPartition<int>(0, options));
        Assert.False(File.Exists(missingSegmentPath));
    }

    [Fact]
    public void Recovery_Scans_From_Earliest_Offset_If_Producer_Checkpoint_Is_Missing()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path, maxRetainedConsumedSegments: 1);
        using (var partition = new MemoryMappedFileBufferPartition<int>(0, options))
        {
            Enqueue(partition, 8);
            Assert.True(partition.TryPull("existing", 8, out _));
            partition.Commit("existing");
        }

        File.Delete(GetProducerOffsetFilePath(options));
        using var restoredPartition = new MemoryMappedFileBufferPartition<int>(0, options);

        Assert.False(File.Exists(GetSegmentFilePath(options, 0)));
        Assert.True(restoredPartition.TryPull("new", 8, out var items));
        Assert.Equal(new[] { 7, 8 }, items);
    }

    [Theory]
    [InlineData(20)]
    [InlineData(22)]
    public void Recovery_Without_Producer_Checkpoint_Crosses_Segment_Padding(long segmentSize)
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path, maxRetainedConsumedSegments: null);
        options.SegmentSizeInBytes = segmentSize;
        using (var partition = new MemoryMappedFileBufferPartition<int>(0, options))
        {
            Enqueue(partition, 3);
        }

        File.Delete(GetProducerOffsetFilePath(options));
        using var restoredPartition = new MemoryMappedFileBufferPartition<int>(0, options);

        Assert.True(restoredPartition.TryPull("group", 3, out var items));
        Assert.Equal(Enumerable.Range(1, 3), items);
    }

    [Theory]
    [InlineData(20)]
    [InlineData(22)]
    public void Startup_Retention_Does_Not_Advance_Past_A_Stale_Producer_Checkpoint(long segmentSize)
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path, maxRetainedConsumedSegments: 0);
        options.SegmentSizeInBytes = segmentSize;
        using (var partition = new MemoryMappedFileBufferPartition<int>(0, options))
        {
            Enqueue(partition, 3);
        }

        WriteInt64(GetProducerOffsetFilePath(options), 18);
        WriteInt64(GetConsumerOffsetFilePath(options, 0, "group"), 18);
        using (var restoredPartition = new MemoryMappedFileBufferPartition<int>(0, options))
        {
            Assert.False(File.Exists(GetEarliestOffsetFilePath(options)));
            Assert.Equal(18, ReadInt64(GetConsumerOffsetFilePath(options, 0, "group")));
        }

        using var secondRestoredPartition = new MemoryMappedFileBufferPartition<int>(0, options);
        Assert.True(secondRestoredPartition.TryPull("group", 1, out var items));
        Assert.Equal(new[] { 3 }, items);
    }

    [Fact]
    public void Recovery_Deletes_A_Segment_Left_Behind_Before_Earliest_Offset()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path, maxRetainedConsumedSegments: 1);
        using (var partition = new MemoryMappedFileBufferPartition<int>(0, options))
        {
            Enqueue(partition, 8);
            Assert.True(partition.TryPull("existing", 8, out _));
            partition.Commit("existing");
        }

        var staleSegmentPath = GetSegmentFilePath(options, 0);
        using (var stream = new FileStream(staleSegmentPath, FileMode.CreateNew, FileAccess.Write))
        {
            stream.SetLength(options.SegmentSizeInBytes);
        }

        using var restoredPartition = new MemoryMappedFileBufferPartition<int>(0, options);

        Assert.False(File.Exists(staleSegmentPath));
        Assert.True(restoredPartition.TryPull("new", 8, out var items));
        Assert.Equal(new[] { 7, 8 }, items);
    }

    [Fact]
    public void Recovery_Fails_If_Earliest_Offset_Is_Not_Segment_Aligned()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path, maxRetainedConsumedSegments: 1);
        using (var partition = new MemoryMappedFileBufferPartition<int>(0, options))
        {
            Enqueue(partition, 4);
            Assert.True(partition.TryPull("group", 4, out _));
            partition.Commit("group");
        }

        WriteInt64(GetEarliestOffsetFilePath(options), 19);

        Assert.Throws<InvalidDataException>(() => new MemoryMappedFileBufferPartition<int>(0, options));
    }

    [Fact]
    public void Recovery_Fails_If_Consumer_Offset_Is_Before_Earliest_Offset()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path, maxRetainedConsumedSegments: 1);
        using (var partition = new MemoryMappedFileBufferPartition<int>(0, options))
        {
            Enqueue(partition, 8);
            Assert.True(partition.TryPull("existing", 8, out _));
            partition.Commit("existing");
        }

        var staleOffsetPath = Path.Combine(
            options.DataDirectory,
            options.TopicName!,
            "partition-00000",
            "offsets",
            "stale",
            "consumer.offset");
        WriteInt64(staleOffsetPath, 0);

        Assert.Throws<InvalidDataException>(() => new MemoryMappedFileBufferPartition<int>(0, options));
    }

    [Fact]
    public void Recovery_Fails_If_A_Segment_File_Name_Is_Not_Canonical()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = CreateOptions(temporaryDirectory.Path, maxRetainedConsumedSegments: null);
        var partitionDirectory = GetPartitionDirectory(options);
        Directory.CreateDirectory(partitionDirectory);
        using (var stream = new FileStream(Path.Combine(partitionDirectory, "0.log"), FileMode.CreateNew,
                   FileAccess.Write))
        {
            stream.SetLength(options.SegmentSizeInBytes);
        }

        Assert.Throws<InvalidDataException>(() => new MemoryMappedFileBufferPartition<int>(0, options));
        Assert.False(File.Exists(GetSegmentFilePath(options, 0)));
    }

    private static MemoryMappedFileBufferQueueOptions<int> CreateOptions(
        string dataDirectory,
        int? maxRetainedConsumedSegments) =>
        new()
        {
            TopicName = "test",
            DataDirectory = dataDirectory,
            PartitionNumber = 1,
            SegmentSizeInBytes = 18,
            MaxRetainedConsumedSegments = maxRetainedConsumedSegments,
            Serializer = new Int32MemoryMappedFileSerializer()
        };

    private static void Enqueue(MemoryMappedFileBufferPartition<int> partition, int count)
    {
        for (var value = 1; value <= count; value++)
        {
            partition.Enqueue(value);
        }
    }

    private static long[] GetSegmentIndices(MemoryMappedFileBufferQueueOptions<int> options) =>
        Directory.EnumerateFiles(GetPartitionDirectory(options), "*.log")
            .Select(path => long.Parse(Path.GetFileNameWithoutExtension(path)))
            .Order()
            .ToArray();

    private static string GetSegmentFilePath(MemoryMappedFileBufferQueueOptions<int> options, long index) =>
        Path.Combine(GetPartitionDirectory(options), $"{index:D20}.log");

    private static string GetProducerOffsetFilePath(MemoryMappedFileBufferQueueOptions<int> options) =>
        Path.Combine(GetPartitionDirectory(options), "producer.offset");

    private static string GetEarliestOffsetFilePath(MemoryMappedFileBufferQueueOptions<int> options) =>
        Path.Combine(GetPartitionDirectory(options), "earliest.offset");

    private static string GetConsumerOffsetFilePath(
        MemoryMappedFileBufferQueueOptions<int> options,
        int partitionId,
        string groupName) =>
        Path.Combine(
            options.DataDirectory,
            options.TopicName!,
            $"partition-{partitionId:D5}",
            "offsets",
            groupName,
            "consumer.offset");

    private static string GetPartitionDirectory(MemoryMappedFileBufferQueueOptions<int> options) =>
        Path.Combine(options.DataDirectory, options.TopicName!, "partition-00000");

    private static long ReadInt64(string filePath) =>
        BinaryPrimitives.ReadInt64LittleEndian(File.ReadAllBytes(filePath));

    private static void WriteInt64(string filePath, long value)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(filePath)!);
        var bytes = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, value);
        File.WriteAllBytes(filePath, bytes);
    }

    private sealed class Int32MemoryMappedFileSerializer : IMemoryMappedFileSerializer<int>
    {
        public byte[] Serialize(int item)
        {
            var bytes = new byte[sizeof(int)];
            BinaryPrimitives.WriteInt32LittleEndian(bytes, item);
            return bytes;
        }

        public int Deserialize(ReadOnlyMemory<byte> payload) =>
            BinaryPrimitives.ReadInt32LittleEndian(payload.Span);
    }
}
