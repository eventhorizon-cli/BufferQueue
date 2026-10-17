using System.Threading;

namespace BufferQueue.Memory;

/// <summary>
/// Publishes a consumer's committed partition offset without allocating per commit.
/// </summary>
internal sealed class MemoryBufferConsumerProgress
{
    private long _version;
    private ulong _generation;
    private ulong _index;

    public MemoryBufferConsumerProgress(MemoryBufferPartitionOffset initialPosition)
    {
        _generation = initialPosition.Generation;
        _index = initialPosition.Index;
    }

    public MemoryBufferPartitionOffset Read()
    {
        while (true)
        {
            var version = Volatile.Read(ref _version);
            if ((version & 1) != 0)
            {
                Thread.SpinWait(1);
                continue;
            }

            var generation = Volatile.Read(ref _generation);
            var index = Volatile.Read(ref _index);
            if (version == Volatile.Read(ref _version))
            {
                return new(generation, index);
            }
        }
    }

    public void Publish(MemoryBufferPartitionOffset position)
    {
        var version = Interlocked.Increment(ref _version);
        Volatile.Write(ref _generation, position.Generation);
        Volatile.Write(ref _index, position.Index);
        Volatile.Write(ref _version, version + 1);
    }
}
