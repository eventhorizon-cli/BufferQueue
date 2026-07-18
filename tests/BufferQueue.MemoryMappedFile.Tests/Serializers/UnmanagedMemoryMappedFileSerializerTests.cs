// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;
using BufferQueue.MemoryMappedFile;

namespace BufferQueue.MemoryMappedFile.Tests.Serializers;

public class UnmanagedMemoryMappedFileSerializerTests
{
    [Fact]
    public void Serialize_And_Deserialize_Raw_Memory()
    {
        var serializer = UnmanagedMemoryMappedFileSerializer<UnmanagedTestItem>.Instance;
        var item = new UnmanagedTestItem(42, 638882496000000000, 7);
        var expectedPayload = MemoryMarshal.AsBytes(new[] { item }.AsSpan()).ToArray();

        var payload = serializer.Serialize(item);
        var restoredItem = serializer.Deserialize(payload);

        Assert.Equal(Unsafe.SizeOf<UnmanagedTestItem>(), payload.Length);
        Assert.Equal(expectedPayload, payload);
        Assert.Equal(item.Id, restoredItem.Id);
        Assert.Equal(item.Timestamp, restoredItem.Timestamp);
        Assert.Equal(item.Code, restoredItem.Code);
    }

    [Fact]
    public void Deserialize_Will_Throw_If_Payload_Length_Does_Not_Match()
    {
        var serializer = UnmanagedMemoryMappedFileSerializer<UnmanagedTestItem>.Instance;
        var payloadSize = Unsafe.SizeOf<UnmanagedTestItem>();

        var shortException = Assert.Throws<InvalidDataException>(
            () => serializer.Deserialize(new byte[payloadSize - 1]));
        var longException = Assert.Throws<InvalidDataException>(
            () => serializer.Deserialize(new byte[payloadSize + 1]));

        Assert.Contains($"exactly {payloadSize} bytes", shortException.Message);
        Assert.Contains($"exactly {payloadSize} bytes", longException.Message);
    }

    [Fact]
    public async Task Produce_And_Consume_With_Unmanaged_Serializer()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = new MemoryMappedFileBufferQueueOptions<UnmanagedTestItem>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            SegmentSizeInBytes = 1024,
            Serializer = UnmanagedMemoryMappedFileSerializer<UnmanagedTestItem>.Instance
        };
        using var queue = new MemoryMappedFileBufferQueue<UnmanagedTestItem>(options);
        var consumer = queue.CreateConsumer(new BufferPullConsumerOptions
        {
            TopicName = "test",
            GroupName = "TestGroup",
            AutoCommit = true,
            BatchSize = 1
        });
        var expectedItem = new UnmanagedTestItem(42, 638882496000000000, 7);

        await queue.GetProducer().ProduceAsync(expectedItem);

        await foreach (var items in consumer.ConsumeAsync())
        {
            var item = Assert.Single(items);
            Assert.Equal(expectedItem.Id, item.Id);
            Assert.Equal(expectedItem.Timestamp, item.Timestamp);
            Assert.Equal(expectedItem.Code, item.Code);
            break;
        }
    }

    [StructLayout(LayoutKind.Sequential, Pack = 1)]
    private readonly struct UnmanagedTestItem
    {
        public UnmanagedTestItem(int id, long timestamp, short code)
        {
            Id = id;
            Timestamp = timestamp;
            Code = code;
        }

        public int Id { get; }

        public long Timestamp { get; }

        public short Code { get; }
    }
}
