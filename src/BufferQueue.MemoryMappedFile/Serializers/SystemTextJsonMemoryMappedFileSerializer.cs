using System;
using System.Text.Json;

namespace BufferQueue.MemoryMappedFile;

internal sealed class SystemTextJsonMemoryMappedFileSerializer<T> : IMemoryMappedFileSerializer<T>
    where T : notnull
{
    public static SystemTextJsonMemoryMappedFileSerializer<T> Instance { get; } = new();

    private SystemTextJsonMemoryMappedFileSerializer()
    {
    }

    /// <inheritdoc />
    public byte[] Serialize(T item) => JsonSerializer.SerializeToUtf8Bytes(item);

    /// <inheritdoc />
    public T Deserialize(ReadOnlyMemory<byte> payload)
    {
        var item = JsonSerializer.Deserialize<T>(payload.Span);
        return item is not null
            ? item
            : throw new JsonException("The JSON payload deserialized to null.");
    }
}
