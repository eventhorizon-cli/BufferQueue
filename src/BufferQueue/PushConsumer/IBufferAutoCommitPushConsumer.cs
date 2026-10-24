using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BufferQueue.PushConsumer;

public interface IBufferAutoCommitPushConsumer<in T> : IBufferPushConsumer
{
    /// <summary>Processes a materialized batch with a count and indexed access.</summary>
    /// <remarks>The queue does not invalidate the batch after this handler completes.</remarks>
    Task ConsumeAsync(IReadOnlyList<T> buffer, CancellationToken cancellationToken);
}
