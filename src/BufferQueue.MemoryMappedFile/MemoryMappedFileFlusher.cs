// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.IO.MemoryMappedFiles;

namespace BufferQueue.MemoryMappedFile;

internal interface IMemoryMappedFileFlusher
{
    void Flush(MemoryMappedViewAccessor accessor);
}

internal sealed class MemoryMappedFileFlusher : IMemoryMappedFileFlusher
{
    public static MemoryMappedFileFlusher Instance { get; } = new();

    private MemoryMappedFileFlusher()
    {
    }

    public void Flush(MemoryMappedViewAccessor accessor) => accessor.Flush();
}
