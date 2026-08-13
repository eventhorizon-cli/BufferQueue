using System.Threading.Tasks;

namespace BufferQueue;

public interface IBufferProducer<in T>
{
    string TopicName { get; }

    ValueTask ProduceAsync(T item);

    ValueTask<bool> TryProduceAsync(T item);
}
