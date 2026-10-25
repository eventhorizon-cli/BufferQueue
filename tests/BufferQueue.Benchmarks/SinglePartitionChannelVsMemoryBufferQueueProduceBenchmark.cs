namespace BufferQueue.Benchmarks;

// Keep producer concurrency and the timed APIs identical; change only the queue topology.
public class SinglePartitionChannelVsMemoryBufferQueueProduceBenchmark
    : ChannelVsMemoryBufferQueueProduceBenchmark
{
    protected override int PartitionCount => 1;
}
