// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using BenchmarkDotNet.Attributes;
using BufferQueue.MemoryMappedFile;

namespace BufferQueue.Benchmarks;

[ShortRunJob]
public class MemoryMappedFileSerializerSerializeBenchmark
{
    private readonly MemoryMappedFileBenchmarkMessage _item =
        MemoryMappedFileBenchmarkMessage.Create(123456789);

    private readonly IMemoryMappedFileSerializer<MemoryMappedFileBenchmarkMessage> _systemTextJsonSerializer =
        new MemoryMappedFileBufferQueueOptions<MemoryMappedFileBenchmarkMessage>().Serializer;

    private readonly IMemoryMappedFileSerializer<MemoryMappedFileBenchmarkMessage> _messagePackSerializer =
        new MessagePackMemoryMappedFileSerializer<MemoryMappedFileBenchmarkMessage>();

    private readonly IMemoryMappedFileSerializer<MemoryMappedFileBenchmarkMessage> _unmanagedSerializer =
        UnmanagedMemoryMappedFileSerializer<MemoryMappedFileBenchmarkMessage>.Instance;

    [GlobalSetup]
    public void Setup()
    {
        if (global::MessagePack.GeneratedMessagePackResolver.Instance
                .GetFormatter<MemoryMappedFileBenchmarkMessage>() is null)
        {
            throw new InvalidOperationException("The MessagePack source-generated formatter was not found.");
        }

        MemoryMappedFileSerializerBenchmarkValidation.ValidateRoundTrip(_systemTextJsonSerializer, _item);
        MemoryMappedFileSerializerBenchmarkValidation.ValidateRoundTrip(_messagePackSerializer, _item);
        MemoryMappedFileSerializerBenchmarkValidation.ValidateRoundTrip(_unmanagedSerializer, _item);
    }

    [Benchmark(Baseline = true)]
    public byte[] SystemTextJson_Serialize() => _systemTextJsonSerializer.Serialize(_item);

    [Benchmark]
    public byte[] MessagePack_Serialize() => _messagePackSerializer.Serialize(_item);

    [Benchmark]
    public byte[] Unmanaged_Serialize() => _unmanagedSerializer.Serialize(_item);
}

[ShortRunJob]
public class MemoryMappedFileSerializerDeserializeBenchmark
{
    private readonly IMemoryMappedFileSerializer<MemoryMappedFileBenchmarkMessage> _systemTextJsonSerializer =
        new MemoryMappedFileBufferQueueOptions<MemoryMappedFileBenchmarkMessage>().Serializer;

    private readonly IMemoryMappedFileSerializer<MemoryMappedFileBenchmarkMessage> _messagePackSerializer =
        new MessagePackMemoryMappedFileSerializer<MemoryMappedFileBenchmarkMessage>();

    private readonly IMemoryMappedFileSerializer<MemoryMappedFileBenchmarkMessage> _unmanagedSerializer =
        UnmanagedMemoryMappedFileSerializer<MemoryMappedFileBenchmarkMessage>.Instance;

    private byte[] _systemTextJsonPayload = null!;
    private byte[] _messagePackPayload = null!;
    private byte[] _unmanagedPayload = null!;

    [GlobalSetup]
    public void Setup()
    {
        if (global::MessagePack.GeneratedMessagePackResolver.Instance
                .GetFormatter<MemoryMappedFileBenchmarkMessage>() is null)
        {
            throw new InvalidOperationException("The MessagePack source-generated formatter was not found.");
        }

        var item = MemoryMappedFileBenchmarkMessage.Create(123456789);

        _systemTextJsonPayload = _systemTextJsonSerializer.Serialize(item);
        _messagePackPayload = _messagePackSerializer.Serialize(item);
        _unmanagedPayload = _unmanagedSerializer.Serialize(item);

        MemoryMappedFileSerializerBenchmarkValidation.Validate(
            item,
            _systemTextJsonSerializer.Deserialize(_systemTextJsonPayload));
        MemoryMappedFileSerializerBenchmarkValidation.Validate(
            item,
            _messagePackSerializer.Deserialize(_messagePackPayload));
        MemoryMappedFileSerializerBenchmarkValidation.Validate(
            item,
            _unmanagedSerializer.Deserialize(_unmanagedPayload));
    }

    [Benchmark(Baseline = true)]
    public MemoryMappedFileBenchmarkMessage SystemTextJson_Deserialize() =>
        _systemTextJsonSerializer.Deserialize(_systemTextJsonPayload);

    [Benchmark]
    public MemoryMappedFileBenchmarkMessage MessagePack_Deserialize() =>
        _messagePackSerializer.Deserialize(_messagePackPayload);

    [Benchmark]
    public MemoryMappedFileBenchmarkMessage Unmanaged_Deserialize() =>
        _unmanagedSerializer.Deserialize(_unmanagedPayload);
}

internal static class MemoryMappedFileSerializerBenchmarkValidation
{
    public static void ValidateRoundTrip(
        IMemoryMappedFileSerializer<MemoryMappedFileBenchmarkMessage> serializer,
        MemoryMappedFileBenchmarkMessage item) =>
        Validate(item, serializer.Deserialize(serializer.Serialize(item)));

    public static void Validate(
        MemoryMappedFileBenchmarkMessage expected,
        MemoryMappedFileBenchmarkMessage actual)
    {
        if (actual.Sequence != expected.Sequence
            || actual.Timestamp != expected.Timestamp
            || actual.Price != expected.Price
            || actual.Quantity != expected.Quantity)
        {
            throw new InvalidOperationException(
                "A serializer failed the benchmark round-trip validation.");
        }
    }
}
