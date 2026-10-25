namespace BufferQueue.Benchmarks;

// One partition supports one consumer per group, so no multi-consumer cases are generated.
public class SinglePartitionBoundedChannelVsMemoryBufferQueueConsumeBenchmark
    : BoundedChannelVsMemoryBufferQueueConsumeBenchmark
{
    public override IEnumerable<int> ConsumerCounts => [1];

    protected override int PartitionCount => 1;
}
