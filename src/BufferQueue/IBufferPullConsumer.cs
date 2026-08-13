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
