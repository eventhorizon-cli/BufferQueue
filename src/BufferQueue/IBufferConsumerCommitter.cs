using System.Threading.Tasks;

namespace BufferQueue;

public interface IBufferConsumerCommitter
{
    ValueTask CommitAsync();
}

