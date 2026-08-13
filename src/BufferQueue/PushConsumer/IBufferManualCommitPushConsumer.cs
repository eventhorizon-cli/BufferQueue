using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BufferQueue.PushConsumer;

public interface IBufferManualCommitPushConsumer<in T> : IBufferPushConsumer
{
    Task ConsumeAsync(IEnumerable<T> buffer, IBufferConsumerCommitter committer, CancellationToken cancellationToken);
}
