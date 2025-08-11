// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;
using BufferQueue.Memory;

namespace BufferQueue;

public static class MemoryBufferOptionsBuilderExtensions
{
    public static BufferOptionsBuilder UseMemory(
        this BufferOptionsBuilder builder,
        Action<MemoryBufferOptionsBuilder> configure)
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(configure);

        var options = new MemoryBufferOptionsBuilder(builder.Services);
        configure(options);

        return builder;
    }
}
