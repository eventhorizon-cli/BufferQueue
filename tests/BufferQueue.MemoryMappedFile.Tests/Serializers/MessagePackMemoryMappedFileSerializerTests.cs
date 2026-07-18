// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using BufferQueue.MemoryMappedFile;
using MessagePack;
using MessagePack.Resolvers;

namespace BufferQueue.MemoryMappedFile.Tests.Serializers;

public class MessagePackMemoryMappedFileSerializerTests
{
    [Fact]
    public void Serialize_And_Deserialize()
    {
        var serializer = new MessagePackMemoryMappedFileSerializer<MessagePackTestItem>();
        var item = new MessagePackTestItem { Id = 42, Name = "test" };

        var payload = serializer.Serialize(item);
        var restoredItem = serializer.Deserialize(payload);

        Assert.NotEmpty(payload);
        Assert.Equal(item.Id, restoredItem.Id);
        Assert.Equal(item.Name, restoredItem.Name);
    }

    [Fact]
    public void Source_Generator_Creates_Formatter()
    {
        var formatter = global::MessagePack.GeneratedMessagePackResolver.Instance
            .GetFormatter<MessagePackTestItem>();

        Assert.NotNull(formatter);
    }

    [Fact]
    public void Serializer_Uses_Configured_MessagePack_Options()
    {
        var options = MessagePackSerializerOptions.Standard.WithResolver(ContractlessStandardResolver.Instance);
        var serializer = new MessagePackMemoryMappedFileSerializer<ContractlessMessagePackTestItem>(options);
        var item = new ContractlessMessagePackTestItem { Id = 42, Name = "test" };

        var restoredItem = serializer.Deserialize(serializer.Serialize(item));

        Assert.Equal(item.Id, restoredItem.Id);
        Assert.Equal(item.Name, restoredItem.Name);
    }

    [Fact]
    public void Serializer_Can_Be_Used_Concurrently()
    {
        var serializer = new MessagePackMemoryMappedFileSerializer<MessagePackTestItem>();

        Parallel.For(0, 100, id =>
        {
            var item = new MessagePackTestItem { Id = id, Name = $"item-{id}" };
            var restoredItem = serializer.Deserialize(serializer.Serialize(item));

            Assert.Equal(item.Id, restoredItem.Id);
            Assert.Equal(item.Name, restoredItem.Name);
        });
    }

    [Fact]
    public void Constructor_Will_Throw_If_Options_Are_Null()
    {
        Assert.Throws<ArgumentNullException>(() =>
            new MessagePackMemoryMappedFileSerializer<MessagePackTestItem>(null!));
    }

    [Fact]
    public void Deserialize_Will_Throw_If_Payload_Contains_Null()
    {
        var serializer = new MessagePackMemoryMappedFileSerializer<MessagePackTestItem>();

        Assert.Throws<InvalidDataException>(() => serializer.Deserialize(new byte[] { 0xC0 }));
    }

    [Fact]
    public async Task Produce_And_Consume_With_MessagePack_Serializer()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = new MemoryMappedFileBufferQueueOptions<MessagePackTestItem>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            SegmentSizeInBytes = 1024,
            Serializer = new MessagePackMemoryMappedFileSerializer<MessagePackTestItem>()
        };
        using var queue = new MemoryMappedFileBufferQueue<MessagePackTestItem>(options);
        var consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "TestGroup",
            AutoCommit = true,
            BatchSize = 1
        });

        await queue.GetProducer().ProduceAsync(new MessagePackTestItem { Id = 42, Name = "test" });

        await foreach (var items in consumer.ConsumeAsync())
        {
            var item = Assert.Single(items);
            Assert.Equal(42, item.Id);
            Assert.Equal("test", item.Name);
            break;
        }
    }
}

[MessagePackObject]
public sealed class MessagePackTestItem
{
    [Key(0)]
    public int Id { get; set; }

    [Key(1)]
    public string Name { get; set; } = string.Empty;
}

public sealed class ContractlessMessagePackTestItem
{
    public int Id { get; set; }

    public string Name { get; set; } = string.Empty;
}
