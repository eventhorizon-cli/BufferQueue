// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using BufferQueue.Memory;
using Microsoft.Extensions.DependencyInjection;

namespace BufferQueue.Tests.Memory;

public class MemoryBufferServiceCollectionExtensionsTests
{
    [Fact]
    public void AddMemoryBuffer()
    {
        var services = new ServiceCollection();
        services.AddBufferQueue(bufferOptionsBuilder =>
            bufferOptionsBuilder.UseMemory(memoryBufferOptionsBuilder =>
                memoryBufferOptionsBuilder
                    .AddTopic<int>(options =>
                    {
                        options.TopicName = "topic1";
                        options.PartitionNumber = 1;
                    })
                    .AddTopic<int>(options =>
                    {
                        options.TopicName = "topic2";
                        options.PartitionNumber = 2;
                    })
            ));
        var provider = services.BuildServiceProvider();
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
        Assert.Equal(2, topic2Consumers.Count());
        foreach (var consumer in topic2Consumers)
        {
            Assert.Equal("topic2", consumer.TopicName);
        }
    }

    [Fact]
    public async Task No_Consumption_If_TopicName_Not_Match()
    {
        var services = new ServiceCollection();
        services.AddBufferQueue(bufferOptionsBuilder =>
            bufferOptionsBuilder.UseMemory(memoryBufferOptionsBuilder =>
                memoryBufferOptionsBuilder
                    .AddTopic<int>(options =>
                    {
                        options.TopicName = "topic1";
                        options.PartitionNumber = 1;
                    })
                    .AddTopic<int>(options =>
                    {
                        options.TopicName = "topic2";
                        options.PartitionNumber = 2;
                    })
            ));

        var provider = services.BuildServiceProvider();
        var bufferQueue = provider.GetRequiredService<IBufferQueue>();

        var topic1Producer = bufferQueue.GetProducer<int>("topic1");
        var topic1Consumer =
            bufferQueue.CreatePullConsumer<int>(new BufferPullConsumerOptions
            {
                TopicName = "topic1",
                GroupName = "test"
            });
        var topic2Consumer =
            bufferQueue.CreatePullConsumer<int>(new BufferPullConsumerOptions
            {
                TopicName = "topic2",
                GroupName = "test"
            });

        await topic1Producer.ProduceAsync(1);

        await foreach (var item in topic1Consumer.ConsumeAsync())
        {
            Assert.Equal(1, item.Single());
            break;
        }

        _ = Task.Run(async () =>
        {
            await foreach (var item in topic2Consumer.ConsumeAsync())
            {
                Assert.True(false, "Should not consume any item.");
            }
        });

        await Task.Delay(100);
    }

    [Fact]
    public void Throw_If_TopicName_Not_Registered()
    {
        var services = new ServiceCollection();
        services.AddBufferQueue(bufferOptionsBuilder =>
            bufferOptionsBuilder.UseMemory(memoryBufferOptionsBuilder =>
                memoryBufferOptionsBuilder
                    .AddTopic<int>(options =>
                    {
                        options.TopicName = "topic1";
                        options.PartitionNumber = 1;
                    })
                    .AddTopic<int>(options =>
                    {
                        options.TopicName = "topic2";
                        options.PartitionNumber = 2;
                    })
            ));

        var provider = services.BuildServiceProvider();
        var bufferQueue = provider.GetRequiredService<IBufferQueue>();

        Assert.Throws<ArgumentException>(() => bufferQueue.GetProducer<int>("topic3"));
        Assert.Throws<ArgumentException>(() =>
            bufferQueue.CreatePullConsumer<int>(new BufferPullConsumerOptions
            {
                TopicName = "topic3",
                GroupName = "test"
            }));
        Assert.Throws<ArgumentException>(() =>
            bufferQueue.CreatePullConsumers<int>(
                new BufferPullConsumerOptions { TopicName = "topic3", GroupName = "test" },
                2));
    }
}
