// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;
using System.IO;
using System.Runtime.CompilerServices;
using System.Runtime.InteropServices;

namespace BufferQueue.MemoryMappedFile;

/// <summary>
/// Serializes an unmanaged value by copying its in-memory representation.
/// </summary>
/// <typeparam name="T">The unmanaged value type.</typeparam>
/// <remarks>
/// The persisted representation depends on the type layout, packing, native endianness, runtime, and process
/// architecture. Those characteristics must remain compatible with existing records.
/// </remarks>
public sealed class UnmanagedMemoryMappedFileSerializer<T> : IMemoryMappedFileSerializer<T>
    where T : unmanaged
{
    private static readonly int _payloadSize = Unsafe.SizeOf<T>();

    private UnmanagedMemoryMappedFileSerializer()
    {
    }

    /// <summary>
    /// Gets the shared serializer instance.
    /// </summary>
    public static UnmanagedMemoryMappedFileSerializer<T> Instance { get; } = new();

    /// <inheritdoc />
    public byte[] Serialize(T item)
    {
        var payload = new byte[_payloadSize];
        MemoryMarshal.Write(payload.AsSpan(), in item);
        return payload;
    }

    /// <inheritdoc />
    public T Deserialize(ReadOnlyMemory<byte> payload)
    {
        if (payload.Length != _payloadSize)
        {
            throw new InvalidDataException(
                $"The unmanaged payload for '{typeof(T)}' must be exactly {_payloadSize} bytes, " +
                $"but was {payload.Length} bytes.");
        }

        return MemoryMarshal.Read<T>(payload.Span);
    }
}
