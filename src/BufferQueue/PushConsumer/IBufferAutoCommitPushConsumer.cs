// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BufferQueue.PushConsumer;

public interface IBufferAutoCommitPushConsumer<in T> : IBufferPushConsumer
{
    Task ConsumeAsync(IEnumerable<T> buffer, CancellationToken cancellationToken);
}
