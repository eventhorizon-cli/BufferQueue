namespace BufferQueue.Benchmarks;

// Reuse the full queue API and timed workload; only the topology differs.
public class SinglePartitionUnboundedChannelVsMemoryBufferQueueConsumeBenchmark
    : UnboundedChannelVsMemoryBufferQueueConsumeBenchmark
{
    public override IEnumerable<int> ConsumerCounts => [1];

    protected override int PartitionCount => 1;
}
