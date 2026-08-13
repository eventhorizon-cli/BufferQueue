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
