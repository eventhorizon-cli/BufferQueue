using System;
using BufferQueue.MemoryMappedFile;

namespace BufferQueue;

public static class MemoryMappedFileBufferOptionsBuilderExtensions
{
    public static BufferOptionsBuilder UseMemoryMappedFile(
        this BufferOptionsBuilder builder,
        Action<MemoryMappedFileBufferOptionsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        configure(new MemoryMappedFileBufferOptionsBuilder(builder.Services));

        return builder;
    }
}
