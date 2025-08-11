// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

namespace BufferQueue;

public class BufferPullConsumerOptions
{
    public required string TopicName { get; init; }

    public required string GroupName { get; init; }

    public bool AutoCommit { get; init; }

    public int BatchSize { get; init; } = 100;
}
