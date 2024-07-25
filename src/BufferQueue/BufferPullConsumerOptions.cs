// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.Diagnostics.CodeAnalysis;

namespace BufferQueue;

public class BufferPullConsumerOptions
{
    public string TopicName { get; init; } = string.Empty;

    public string GroupName { get; init; } = string.Empty;

    public bool AutoCommit { get; init; } = false;

    public int BatchSize { get; init; } = 100;
}
