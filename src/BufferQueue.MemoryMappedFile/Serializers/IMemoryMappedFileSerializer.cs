// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;

namespace BufferQueue.MemoryMappedFile;

/// <summary>
/// Serializes and deserializes items stored in a memory-mapped-file buffer queue.
/// </summary>
/// <typeparam name="T">The item type.</typeparam>
/// <remarks>
/// A serializer instance can be invoked concurrently by producers and consumers across multiple partitions.
/// Implementations must be thread-safe.
/// The persisted representation must remain compatible with records already stored for the topic.
/// </remarks>
public interface IMemoryMappedFileSerializer<T>
    where T : notnull
{
    /// <summary>
    /// Serializes an item to its persisted representation.
    /// </summary>
    byte[] Serialize(T item);

    /// <summary>
    /// Deserializes an item from its persisted representation.
    /// </summary>
    T Deserialize(ReadOnlyMemory<byte> payload);
}
