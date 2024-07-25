// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BufferQueue;

public interface IBufferPullConsumer<out T> : IBufferConsumerCommitter
{
    string TopicName { get; }

    string GroupName { get; }

    IAsyncEnumerable<IEnumerable<T>> ConsumeAsync(CancellationToken cancellationToken = default);
}
