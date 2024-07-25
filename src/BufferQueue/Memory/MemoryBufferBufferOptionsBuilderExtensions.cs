// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;
using BufferQueue.Memory;

namespace BufferQueue;

public static class MemoryBufferBufferOptionsBuilderExtensions
{
    public static BufferOptionsBuilder UseMemory(
        this BufferOptionsBuilder builder,
        Action<MemoryBufferOptions> configure)
    {
        if (builder is null)
        {
            throw new ArgumentNullException(nameof(builder));
        }

        if (configure is null)
        {
            throw new ArgumentNullException(nameof(configure));
        }

        var options = new MemoryBufferOptions(builder.Services);
        configure(options);

        return builder;
    }
}
