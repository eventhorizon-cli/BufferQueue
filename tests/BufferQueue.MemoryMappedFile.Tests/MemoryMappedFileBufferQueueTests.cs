// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.Buffers.Binary;
using BufferQueue.MemoryMappedFile;

namespace BufferQueue.MemoryMappedFile.Tests;

public class MemoryMappedFileBufferQueueTests
{
    [Fact]
    public async Task Produce_And_Consume()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        using var queue = new MemoryMappedFileBufferQueue<int>(new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 1,
            SegmentSizeInBytes = 1024
        });
        var producer = queue.GetProducer();
        var consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "TestGroup",
            AutoCommit = false,
            BatchSize = 2
        });

        for (var i = 0; i < 10; i++)
        {
            await producer.ProduceAsync(i);
        }

        var consumedValues = new List<int>();
        await foreach (var items in consumer.ConsumeAsync())
        {
            consumedValues.AddRange(items);
            await consumer.CommitAsync();

            if (consumedValues.Count == 10)
            {
                break;
            }
        }

        Assert.Equal(Enumerable.Range(0, 10), consumedValues);
    }

    [Fact]
    public async Task Offset_Will_Not_Change_If_Consumer_Not_Commit()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        using var queue = new MemoryMappedFileBufferQueue<int>(new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 1,
            SegmentSizeInBytes = 1024
        });
        var producer = queue.GetProducer();
        var consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "TestGroup",
            AutoCommit = false,
            BatchSize = 2
        });

        await producer.ProduceAsync(1);
        await producer.ProduceAsync(2);

        var batches = new List<int[]>();
        await foreach (var items in consumer.ConsumeAsync())
        {
            batches.Add(items.ToArray());
            if (batches.Count == 2)
            {
                break;
            }
        }

        Assert.Equal(new[] { 1, 2 }, batches[0]);
        Assert.Equal(new[] { 1, 2 }, batches[1]);
    }

    [Fact]
    public async Task Committed_Offset_Will_Be_Restored()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 1,
            SegmentSizeInBytes = 1024
        };
        using var queue = new MemoryMappedFileBufferQueue<int>(options);
        var producer = queue.GetProducer();
        var consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "TestGroup",
            AutoCommit = false,
            BatchSize = 2
        });

        for (var i = 0; i < 5; i++)
        {
            await producer.ProduceAsync(i);
        }

        await foreach (var items in consumer.ConsumeAsync())
        {
            Assert.Equal(new[] { 0, 1 }, items);
            await consumer.CommitAsync();
            break;
        }

        using var restoredQueue = new MemoryMappedFileBufferQueue<int>(options);
        var restoredConsumer = restoredQueue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "TestGroup",
            AutoCommit = true,
            BatchSize = 3
        });

        await foreach (var items in restoredConsumer.ConsumeAsync())
        {
            Assert.Equal(new[] { 2, 3, 4 }, items);
            break;
        }
    }

    [Fact]
    public async Task Batch_Flush_Commit_Will_Be_Restored_Before_Threshold()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 1,
            SegmentSizeInBytes = 1024,
            FlushStrategy = MemoryMappedFileFlushStrategy.Batch,
            FlushBatchSize = 100
        };
        using var queue = new MemoryMappedFileBufferQueue<int>(options);
        var producer = queue.GetProducer();
        var consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "TestGroup",
            AutoCommit = false,
            BatchSize = 2
        });

        await producer.ProduceAsync(1);
        await producer.ProduceAsync(2);

        await foreach (var items in consumer.ConsumeAsync())
        {
            Assert.Equal(new[] { 1, 2 }, items);
            await consumer.CommitAsync();
            break;
        }

        using var restoredQueue = new MemoryMappedFileBufferQueue<int>(options);
        await restoredQueue.GetProducer().ProduceAsync(3);
        var restoredConsumer = restoredQueue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "TestGroup",
            AutoCommit = true,
            BatchSize = 3
        });

        await foreach (var items in restoredConsumer.ConsumeAsync())
        {
            Assert.Equal(new[] { 3 }, items);
            break;
        }
    }

    [Fact]
    public async Task Consumer_Offset_Uses_Readable_Group_Directory()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        using var queue = new MemoryMappedFileBufferQueue<int>(new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 1,
            SegmentSizeInBytes = 1024
        });
        var producer = queue.GetProducer();
        var consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "orders/worker 1",
            AutoCommit = false,
            BatchSize = 1
        });

        await producer.ProduceAsync(1);
        await foreach (var items in consumer.ConsumeAsync())
        {
            Assert.Equal(new[] { 1 }, items);
            await consumer.CommitAsync();
            break;
        }

        var offsetFilePath = Path.Combine(
            temporaryDirectory.Path,
            "test",
            "partition-00000",
            "offsets",
            "orders%2Fworker 1",
            "consumer.offset");

        Assert.True(File.Exists(offsetFilePath));
    }

    [Fact]
    public async Task Uncommitted_Offset_Will_Not_Be_Restored()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 1,
            SegmentSizeInBytes = 1024
        };
        using var queue = new MemoryMappedFileBufferQueue<int>(options);
        var producer = queue.GetProducer();
        var consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "TestGroup",
            AutoCommit = false,
            BatchSize = 2
        });

        await producer.ProduceAsync(1);
        await producer.ProduceAsync(2);

        await foreach (var items in consumer.ConsumeAsync())
        {
            Assert.Equal(new[] { 1, 2 }, items);
            break;
        }

        using var restoredQueue = new MemoryMappedFileBufferQueue<int>(options);
        var restoredConsumer = restoredQueue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "TestGroup",
            AutoCommit = true,
            BatchSize = 2
        });

        await foreach (var items in restoredConsumer.ConsumeAsync())
        {
            Assert.Equal(new[] { 1, 2 }, items);
            break;
        }
    }

    [Fact]
    public async Task Producer_Offset_Will_Be_Persisted()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        using var queue = new MemoryMappedFileBufferQueue<int>(new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 1,
            SegmentSizeInBytes = 1024
        });
        var producer = queue.GetProducer();

        await producer.ProduceAsync(1);
        await producer.ProduceAsync(2);

        var producerOffsetFilePath = Path.Combine(
            temporaryDirectory.Path,
            "test",
            "partition-00000",
            "producer.offset");
        var producerOffsetBytes = await File.ReadAllBytesAsync(producerOffsetFilePath);
        var producerOffset = BinaryPrimitives.ReadInt64LittleEndian(producerOffsetBytes);

        Assert.True(producerOffset > 0);
    }

    [Fact]
    public async Task Producer_Offset_Will_Scan_Forward_If_Persisted_Offset_Is_Behind()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 1,
            SegmentSizeInBytes = 1024
        };
        using var queue = new MemoryMappedFileBufferQueue<int>(options);
        var producer = queue.GetProducer();

        await producer.ProduceAsync(1);
        await producer.ProduceAsync(2);
        await producer.ProduceAsync(3);

        var producerOffsetFilePath = Path.Combine(
            options.DataDirectory,
            "test",
            "partition-00000",
            "producer.offset");
        var staleProducerOffsetBytes = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(staleProducerOffsetBytes, 0);
        await File.WriteAllBytesAsync(producerOffsetFilePath, staleProducerOffsetBytes);

        using var restoredQueue = new MemoryMappedFileBufferQueue<int>(options);
        var restoredConsumer = restoredQueue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "TestGroup",
            AutoCommit = true,
            BatchSize = 3
        });

        await foreach (var items in restoredConsumer.ConsumeAsync())
        {
            Assert.Equal(new[] { 1, 2, 3 }, items);
            break;
        }
    }

    [Fact]
    public async Task Producer_Offset_Will_Throw_If_Persisted_Offset_Is_Ahead()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 1,
            SegmentSizeInBytes = 1024
        };
        using var queue = new MemoryMappedFileBufferQueue<int>(options);
        var producer = queue.GetProducer();

        await producer.ProduceAsync(1);
        await producer.ProduceAsync(2);

        var producerOffset = await ReadInt64Async(GetProducerOffsetFilePath(options.DataDirectory, "test"));
        await WriteInt64Async(GetProducerOffsetFilePath(options.DataDirectory, "test"), producerOffset + 100);

        Assert.Throws<InvalidDataException>(() => new MemoryMappedFileBufferQueue<int>(options));
    }

    [Fact]
    public async Task Producer_Offset_Will_Throw_If_Persisted_Offset_Is_Not_Record_Boundary()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 1,
            SegmentSizeInBytes = 1024
        };
        using var queue = new MemoryMappedFileBufferQueue<int>(options);
        var producer = queue.GetProducer();

        await producer.ProduceAsync(1);
        await producer.ProduceAsync(2);

        await WriteInt64Async(GetProducerOffsetFilePath(options.DataDirectory, "test"), 3);

        Assert.Throws<InvalidDataException>(() => new MemoryMappedFileBufferQueue<int>(options));
    }

    [Fact]
    public async Task Producer_Offset_Will_Throw_If_Checkpoint_Is_Malformed()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 1,
            SegmentSizeInBytes = 1024
        };
        using var queue = new MemoryMappedFileBufferQueue<int>(options);

        await queue.GetProducer().ProduceAsync(1);
        await File.WriteAllBytesAsync(GetProducerOffsetFilePath(options.DataDirectory, "test"), [1, 2, 3]);

        Assert.Throws<InvalidDataException>(() => new MemoryMappedFileBufferQueue<int>(options));
    }

    [Fact]
    public async Task Recover_Will_Ignore_Partial_Trailing_Record()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 1,
            SegmentSizeInBytes = 1024
        };
        using var queue = new MemoryMappedFileBufferQueue<int>(options);
        var producer = queue.GetProducer();

        await producer.ProduceAsync(1);
        await producer.ProduceAsync(2);

        var producerOffset = await ReadInt64Async(GetProducerOffsetFilePath(options.DataDirectory, "test"));
        var segmentFilePath = Path.Combine(
            options.DataDirectory,
            "test",
            "partition-00000",
            "00000000000000000000.log");
        await using (var stream = new FileStream(segmentFilePath, FileMode.Open, FileAccess.Write, FileShare.ReadWrite))
        {
            stream.Position = producerOffset;
            var partialLength = new byte[sizeof(int)];
            BinaryPrimitives.WriteInt32LittleEndian(partialLength, 100);
            await stream.WriteAsync(partialLength);
            await stream.WriteAsync(new byte[] { 1, 2, 3 });
        }

        using var restoredQueue = new MemoryMappedFileBufferQueue<int>(options);
        var restoredConsumer = restoredQueue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "TestGroup",
            AutoCommit = true,
            BatchSize = 3
        });

        await foreach (var items in restoredConsumer.ConsumeAsync())
        {
            Assert.Equal(new[] { 1, 2 }, items);
            break;
        }
    }

    [Fact]
    public async Task Consumer_Offset_Will_Throw_If_Persisted_Offset_Is_Ahead_Of_Producer()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 1,
            SegmentSizeInBytes = 1024
        };
        using var queue = new MemoryMappedFileBufferQueue<int>(options);
        var producer = queue.GetProducer();

        await producer.ProduceAsync(1);
        await producer.ProduceAsync(2);
        await WriteInt64Async(GetConsumerOffsetFilePath(options.DataDirectory, "test", "TestGroup"), 999);

        Assert.Throws<InvalidDataException>(() => new MemoryMappedFileBufferQueue<int>(options));
    }

    [Fact]
    public async Task Consumer_Offset_Will_Throw_If_Checkpoint_Is_Malformed()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 1,
            SegmentSizeInBytes = 1024
        };
        using var queue = new MemoryMappedFileBufferQueue<int>(options);
        await queue.GetProducer().ProduceAsync(1);
        var consumerOffsetFilePath = GetConsumerOffsetFilePath(options.DataDirectory, "test", "TestGroup");
        Directory.CreateDirectory(Path.GetDirectoryName(consumerOffsetFilePath)!);
        await File.WriteAllBytesAsync(consumerOffsetFilePath, [1, 2, 3]);
        Assert.Throws<InvalidDataException>(() => new MemoryMappedFileBufferQueue<int>(options));
    }

    [Fact]
    public async Task Recover_Will_Throw_If_Partition_Number_Is_Reduced()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 2,
            SegmentSizeInBytes = 1024
        };
        using var queue = new MemoryMappedFileBufferQueue<int>(options);
        var producer = queue.GetProducer();

        await producer.ProduceAsync(1);
        await producer.ProduceAsync(2);

        var reducedOptions = new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 1,
            SegmentSizeInBytes = 1024
        };

        Assert.Throws<InvalidDataException>(() => new MemoryMappedFileBufferQueue<int>(reducedOptions));
    }

    [Fact]
    public async Task Consumer_Will_Wait_Until_Produce()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        using var queue = new MemoryMappedFileBufferQueue<int>(new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 1,
            SegmentSizeInBytes = 1024
        });
        var producer = queue.GetProducer();
        var consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "TestGroup",
            AutoCommit = true,
            BatchSize = 1
        });

        var consumeTask = Task.Run(async () =>
        {
            await foreach (var items in consumer.ConsumeAsync())
            {
                return items.Single();
            }

            return 0;
        });

        await Task.Delay(100);
        Assert.False(consumeTask.IsCompleted);

        await producer.ProduceAsync(42);

        Assert.Equal(42, await consumeTask.WaitAsync(TimeSpan.FromSeconds(5)));
    }

    [Fact]
    public async Task Produce_And_Consume_With_Multiple_Consumers()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        using var queue = new MemoryMappedFileBufferQueue<int>(new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 2,
            SegmentSizeInBytes = 1024
        });
        var producer = queue.GetProducer();
        var consumers = queue.CreateConsumers(
            new BufferPullConsumerOptions
            {
                TopicName = "test",
                GroupName = "TestGroup",
                AutoCommit = true,
                BatchSize = 6
            },
            2).ToList();

        for (var i = 0; i < 10; i++)
        {
            await producer.ProduceAsync(i);
        }

        await foreach (var items in consumers[0].ConsumeAsync())
        {
            Assert.Equal(new[] { 0, 2, 4, 6, 8 }, items);
            break;
        }

        await foreach (var items in consumers[1].ConsumeAsync())
        {
            Assert.Equal(new[] { 1, 3, 5, 7, 9 }, items);
            break;
        }
    }

    private static string GetProducerOffsetFilePath(string dataDirectory, string topicName) =>
        Path.Combine(dataDirectory, topicName, "partition-00000", "producer.offset");

    private static string GetConsumerOffsetFilePath(string dataDirectory, string topicName, string groupName) =>
        Path.Combine(dataDirectory, topicName, "partition-00000", "offsets", groupName, "consumer.offset");

    private static async Task<long> ReadInt64Async(string filePath)
    {
        var bytes = await File.ReadAllBytesAsync(filePath);
        return BinaryPrimitives.ReadInt64LittleEndian(bytes);
    }

    private static Task WriteInt64Async(string filePath, long value)
    {
        Directory.CreateDirectory(Path.GetDirectoryName(filePath)!);
        var bytes = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, value);
        return File.WriteAllBytesAsync(filePath, bytes);
    }
}
