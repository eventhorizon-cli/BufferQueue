// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;
using System.Buffers.Binary;
using System.IO;

namespace BufferQueue.MemoryMappedFile;

internal sealed class OffsetCheckpoint(string filePath)
{
    public long ReadOrDefault(long defaultValue = 0) =>
        TryRead(out var offset) ? offset : defaultValue;

    public bool TryRead(out long offset)
    {
        offset = 0;

        if (!File.Exists(filePath))
        {
            return false;
        }

        var bytes = File.ReadAllBytes(filePath);
        if (bytes.Length != sizeof(long))
        {
            throw new InvalidDataException(
                $"The offset checkpoint '{filePath}' must contain exactly {sizeof(long)} bytes, but contains {bytes.Length} bytes.");
        }

        offset = BinaryPrimitives.ReadInt64LittleEndian(bytes);
        if (offset < 0)
        {
            throw new InvalidDataException(
                $"The offset checkpoint '{filePath}' contains the invalid negative offset {offset}.");
        }

        return true;
    }

    public void Write(long offset)
    {
        if (offset < 0)
        {
            throw new ArgumentOutOfRangeException(nameof(offset), "Offset must be greater than or equal to 0.");
        }

        var tempFilePath = $"{filePath}.{Guid.NewGuid():N}.tmp";
        Directory.CreateDirectory(Path.GetDirectoryName(filePath)!);
        Span<byte> bytes = stackalloc byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, offset);
        File.WriteAllBytes(tempFilePath, bytes.ToArray());

        if (File.Exists(filePath))
        {
            File.Replace(tempFilePath, filePath, null);
        }
        else
        {
            File.Move(tempFilePath, filePath);
        }
    }
}
