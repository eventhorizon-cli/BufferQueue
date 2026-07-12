// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

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

        var options = new MemoryMappedFileBufferOptionsBuilder(builder.Services);
        configure(options);

        return builder;
    }
}
