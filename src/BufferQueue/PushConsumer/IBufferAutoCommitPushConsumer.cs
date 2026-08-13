using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BufferQueue.PushConsumer;

public interface IBufferAutoCommitPushConsumer<in T> : IBufferPushConsumer
{
    Task ConsumeAsync(IEnumerable<T> buffer, CancellationToken cancellationToken);
}
