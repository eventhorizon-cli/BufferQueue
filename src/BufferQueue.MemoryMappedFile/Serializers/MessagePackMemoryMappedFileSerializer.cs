// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;
using System.IO;
using MessagePack;

namespace BufferQueue.MemoryMappedFile;

/// <summary>
/// Serializes memory-mapped-file buffer queue items with MessagePack for C#.
/// </summary>
/// <typeparam name="T">The item type.</typeparam>
/// <remarks>
/// Configured resolvers and formatters can be invoked concurrently and must be thread-safe.
/// Serializer options are part of the persisted representation and must remain compatible with existing records.
/// </remarks>
public sealed class MessagePackMemoryMappedFileSerializer<T> : IMemoryMappedFileSerializer<T>
    where T : notnull
{
    private readonly MessagePackSerializerOptions _options;

    /// <summary>
    /// Initializes a serializer with <see cref="MessagePackSerializerOptions.Standard"/>.
    /// </summary>
    public MessagePackMemoryMappedFileSerializer()
        : this(MessagePackSerializerOptions.Standard)
    {
    }

    /// <summary>
    /// Initializes a serializer with the specified MessagePack options.
    /// </summary>
    /// <param name="options">
    /// The MessagePack serialization options. Its resolver and formatters must be thread-safe.
    /// </param>
    public MessagePackMemoryMappedFileSerializer(MessagePackSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
    }

    /// <inheritdoc />
    public byte[] Serialize(T item) => MessagePackSerializer.Serialize(item, _options);

    /// <inheritdoc />
    public T Deserialize(ReadOnlyMemory<byte> payload)
    {
        var item = MessagePackSerializer.Deserialize<T>(payload, _options);
        return item is not null
            ? item
            : throw new InvalidDataException("The MessagePack payload deserialized to null.");
    }
}
