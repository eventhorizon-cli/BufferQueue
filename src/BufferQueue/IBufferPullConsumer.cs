using System.Collections.Generic;
using System.Threading;

namespace BufferQueue;

public interface IBufferPullConsumer<out T> : IBufferConsumerCommitter
{
    string TopicName { get; }

    string GroupName { get; }

    /// <summary>Reads materialized batches with a count and indexed access.</summary>
    /// <remarks>The queue does not invalidate returned batches on later pulls or commits.</remarks>
    IAsyncEnumerable<IReadOnlyList<T>> ConsumeAsync(CancellationToken cancellationToken = default);
}
