// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using BufferQueue.MemoryMappedFile;
using Microsoft.Extensions.DependencyInjection;

namespace BufferQueue.MemoryMappedFile.Tests;

public class MemoryMappedFileBufferServiceCollectionExtensionsTests
{
    [Fact]
    public void AddMemoryMappedFileBuffer()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var services = new ServiceCollection();
        services.AddBufferQueue(bufferOptionsBuilder =>
            bufferOptionsBuilder.UseMemoryMappedFile(memoryMappedFileBufferOptionsBuilder =>
                memoryMappedFileBufferOptionsBuilder
                    .AddTopic<int>(options =>
                    {
                        options.TopicName = "topic1";
                        options.DataDirectory = temporaryDirectory.Path;
                        options.PartitionNumber = 1;
                        options.SegmentSizeInBytes = 1024;
                    })
                    .AddTopic<int>(options =>
                    {
                        options.TopicName = "topic2";
                        options.DataDirectory = temporaryDirectory.Path;
                        options.PartitionNumber = 2;
                        options.SegmentSizeInBytes = 1024;
                    })
            ));

        using var provider = services.BuildServiceProvider();
        var bufferQueue = provider.GetRequiredService<IBufferQueue>();

        var topic1Producer = bufferQueue.GetProducer<int>("topic1");
        var topic1Consumer =
            bufferQueue.CreatePullConsumer<int>(new BufferPullConsumerOptions
            {
                TopicName = "topic1",
                GroupName = "test"
            });

        var topic2Producer = bufferQueue.GetProducer<int>("topic2");
        var topic2Consumers =
            bufferQueue.CreatePullConsumers<int>(
                    new BufferPullConsumerOptions { TopicName = "topic2", GroupName = "test" }, 2)
                .ToList();

        Assert.Equal("topic1", topic1Producer.TopicName);
        Assert.Equal("topic1", topic1Consumer.TopicName);
        Assert.Equal("topic2", topic2Producer.TopicName);
        Assert.Equal(2, topic2Consumers.Count);
        foreach (var consumer in topic2Consumers)
        {
            Assert.Equal("topic2", consumer.TopicName);
        }
    }

    [Fact]
    public void AddMemoryMappedFileBuffer_Will_Throw_For_Unsupported_Flush_Strategy()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var services = new ServiceCollection();

        Assert.Throws<ArgumentOutOfRangeException>(() => services.AddBufferQueue(bufferOptionsBuilder =>
            bufferOptionsBuilder.UseMemoryMappedFile(memoryMappedFileBufferOptionsBuilder =>
                memoryMappedFileBufferOptionsBuilder.AddTopic<int>(options =>
                {
                    options.TopicName = "topic";
                    options.DataDirectory = temporaryDirectory.Path;
                    options.FlushStrategy = (MemoryMappedFileFlushStrategy)int.MaxValue;
                }))));
    }

    [Fact]
    public void AddMemoryMappedFileBuffer_Will_Throw_For_Invalid_Flush_Batch_Size()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var services = new ServiceCollection();

        Assert.Throws<ArgumentOutOfRangeException>(() => services.AddBufferQueue(bufferOptionsBuilder =>
            bufferOptionsBuilder.UseMemoryMappedFile(memoryMappedFileBufferOptionsBuilder =>
                memoryMappedFileBufferOptionsBuilder.AddTopic<int>(options =>
                {
                    options.TopicName = "topic";
                    options.DataDirectory = temporaryDirectory.Path;
                    options.FlushStrategy = MemoryMappedFileFlushStrategy.Batch;
                    options.FlushBatchSize = 0;
                }))));
    }

    [Fact]
    public void AddMemoryMappedFileBuffer_Will_Throw_For_Invalid_Segment_Size_In_Bytes()
    {
        var services = new ServiceCollection();

        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => services.AddBufferQueue(
            bufferOptionsBuilder => bufferOptionsBuilder.UseMemoryMappedFile(
                memoryMappedFileBufferOptionsBuilder => memoryMappedFileBufferOptionsBuilder.AddTopic<int>(options =>
                {
                    options.TopicName = "topic";
                    options.SegmentSizeInBytes = 0;
                }))));

        Assert.Equal("SegmentSizeInBytes", exception.ParamName);
    }

    [Fact]
    public void AddMemoryMappedFileBuffer_Will_Throw_For_Invalid_Retained_Segment_Count()
    {
        var services = new ServiceCollection();

        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => services.AddBufferQueue(
            bufferOptionsBuilder => bufferOptionsBuilder.UseMemoryMappedFile(
                memoryMappedFileBufferOptionsBuilder => memoryMappedFileBufferOptionsBuilder.AddTopic<int>(options =>
                {
                    options.TopicName = "topic";
                    options.MaxRetainedConsumedSegments = -1;
                }))));

        Assert.Equal("MaxRetainedConsumedSegments", exception.ParamName);
    }

    [Fact]
    public void AddMemoryMappedFileBuffer_Will_Throw_If_Serializer_Is_Null()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var services = new ServiceCollection();

        Assert.Throws<ArgumentNullException>(() => services.AddBufferQueue(bufferOptionsBuilder =>
            bufferOptionsBuilder.UseMemoryMappedFile(memoryMappedFileBufferOptionsBuilder =>
                memoryMappedFileBufferOptionsBuilder.AddTopic<int>(options =>
                {
                    options.TopicName = "topic";
                    options.DataDirectory = temporaryDirectory.Path;
                    options.Serializer = null!;
                }))));
    }

}
