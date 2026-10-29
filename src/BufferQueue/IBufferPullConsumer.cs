using System.Collections.Generic;
using System.Threading;

namespace BufferQueue;

public interface IBufferPullConsumer<out T> : IBufferConsumerCommitter
{
    string TopicName { get; }

    string GroupName { get; }

    /// <summary>Tries to observe the aggregate offset lag of this consumer's assigned partitions.</summary>
    /// <param name="snapshot">The approximate lag and its unit, or default when unavailable.</param>
    /// <returns>True for an available observation, including zero; otherwise false.</returns>
    /// <remarks>
    /// Positions are sampled independently without snapshot locks or consistency retries.
    /// Manual commit includes pulled but uncommitted items; auto commit excludes them before application processing.
    /// Memory reports items; MemoryMappedFile reports a byte span, including framing and segment padding.
    /// Negative partition differences are clamped to zero; aggregate overflow saturates at ulong.MaxValue.
    /// Use for monitoring, not flow control or an exact empty check. Unsupported implementations return false.
    /// </remarks>
    bool TryGetLagSnapshot(out BufferConsumerLagSnapshot snapshot)
    {
        snapshot = default;
        return false;
    }

    /// <summary>Reads materialized batches with a count and indexed access.</summary>
    /// <remarks>The queue does not invalidate returned batches on later pulls or commits.</remarks>
    IAsyncEnumerable<IReadOnlyList<T>> ConsumeAsync(CancellationToken cancellationToken = default);
}
