using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Running;
using BufferQueue.Benchmarks;

if (args is ["--batch-enumeration-probe"])
{
    BatchEnumerationProbe.Run();
    return;
}

var config = ManualConfig
    .Create(DefaultConfig.Instance)
    .AddDiagnoser(MemoryDiagnoser.Default);

var allBenchmarks = new[]
{
    typeof(BlockingCollectionVsMemoryBufferQueueProduceBenchmark),
    typeof(ChannelVsMemoryBufferQueueProduceBenchmark),
    typeof(SinglePartitionChannelVsMemoryBufferQueueProduceBenchmark),
    typeof(MemoryVsMemoryMappedFileBufferQueueProduceBenchmark),
    typeof(MemoryBufferPartitionerBenchmark),
    typeof(StringPartitionRoutingBenchmark),
    typeof(MemoryMappedFileSerializerSerializeBenchmark),
    typeof(BlockingCollectionVsMemoryBufferQueueConsumeBenchmark),
    typeof(UnboundedChannelVsMemoryBufferQueueConsumeBenchmark),
    typeof(SingleReaderChannelVsMemoryBufferQueueConsumeBenchmark),
    typeof(BoundedChannelVsMemoryBufferQueueConsumeBenchmark),
    typeof(SinglePartitionUnboundedChannelVsMemoryBufferQueueConsumeBenchmark),
    typeof(SinglePartitionBoundedChannelVsMemoryBufferQueueConsumeBenchmark),
    typeof(MemoryBufferOffsetBenchmark),
    typeof(MemoryCommitModeBenchmark),
    typeof(MemoryAutoCommitOptimizationBenchmark),
    typeof(MemoryNotificationOptimizationBenchmark),
    typeof(MemoryGroupProgressOptimizationBenchmark),
    typeof(MemorySparseBatchBenchmark),
    typeof(MemoryPartialBatchAllocationBenchmark),
    typeof(ChannelReadPathBenchmark),
    typeof(PullConsumerSchedulingBenchmark),
    typeof(MemoryVsMemoryMappedFileBufferQueueConsumeBenchmark),
    typeof(MemoryMappedFileSerializerDeserializeBenchmark),
    typeof(MemoryConsumerLagSnapshotQueryBenchmark),
    typeof(MemoryConsumerLagSnapshotSamplingBenchmark),
};

new BenchmarkSwitcher(allBenchmarks).Run(args, config);
