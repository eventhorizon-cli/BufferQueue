// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.Buffers.Binary;
using BufferQueue.MemoryMappedFile;

namespace BufferQueue.MemoryMappedFile.Tests;

public class OffsetCheckpointTests
{
    [Fact]
    public void ReadOrDefault_Returns_Default_If_File_Not_Exists()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var filePath = Path.Combine(temporaryDirectory.Path, "offset");
        var checkpoint = new OffsetCheckpoint(filePath);

        Assert.Equal(123, checkpoint.ReadOrDefault(123));
        Assert.False(checkpoint.TryRead(out _));
    }

    [Fact]
    public async Task Read_Will_Throw_If_File_Length_Is_Invalid()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var filePath = Path.Combine(temporaryDirectory.Path, "offset");
        await File.WriteAllBytesAsync(filePath, [1, 2, 3]);
        var checkpoint = new OffsetCheckpoint(filePath);

        Assert.Throws<InvalidDataException>(() => checkpoint.ReadOrDefault(123));
        Assert.Throws<InvalidDataException>(() => checkpoint.TryRead(out _));
    }

    [Fact]
    public async Task Read_Will_Throw_If_Offset_Is_Negative()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var filePath = Path.Combine(temporaryDirectory.Path, "offset");
        var bytes = new byte[sizeof(long)];
        BinaryPrimitives.WriteInt64LittleEndian(bytes, -1);
        await File.WriteAllBytesAsync(filePath, bytes);
        var checkpoint = new OffsetCheckpoint(filePath);

        Assert.Throws<InvalidDataException>(() => checkpoint.ReadOrDefault(123));
        Assert.Throws<InvalidDataException>(() => checkpoint.TryRead(out _));
    }

    [Fact]
    public void Write_And_Read()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var filePath = Path.Combine(temporaryDirectory.Path, "offset");
        var checkpoint = new OffsetCheckpoint(filePath);

        checkpoint.Write(42);

        Assert.True(checkpoint.TryRead(out var offset));
        Assert.Equal(42, offset);
        Assert.Equal(42, checkpoint.ReadOrDefault());
    }

    [Fact]
    public void Write_Throws_If_Offset_Is_Negative()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var filePath = Path.Combine(temporaryDirectory.Path, "offset");
        var checkpoint = new OffsetCheckpoint(filePath);

        Assert.Throws<ArgumentOutOfRangeException>(() => checkpoint.Write(-1));
    }
}
