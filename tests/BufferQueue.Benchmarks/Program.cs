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
    typeof(MemoryVsMemoryMappedFileBufferQueueProduceBenchmark),
    typeof(MemoryBufferPartitionerBenchmark),
    typeof(StringPartitionRoutingBenchmark),
    typeof(MemoryMappedFileSerializerSerializeBenchmark),
    typeof(BlockingCollectionVsMemoryBufferQueueConsumeBenchmark),
    typeof(UnboundedChannelVsMemoryBufferQueueConsumeBenchmark),
    typeof(SingleReaderChannelVsMemoryBufferQueueConsumeBenchmark),
    typeof(BoundedChannelVsMemoryBufferQueueConsumeBenchmark),
    typeof(MemoryBufferOffsetBenchmark),
    typeof(MemoryCommitModeBenchmark),
    typeof(MemoryAutoCommitOptimizationBenchmark),
    typeof(MemoryNotificationOptimizationBenchmark),
    typeof(MemoryGroupProgressOptimizationBenchmark),
    typeof(MemorySparseBatchBenchmark),
    typeof(ChannelReadPathBenchmark),
    typeof(PullConsumerSchedulingBenchmark),
    typeof(MemoryVsMemoryMappedFileBufferQueueConsumeBenchmark),
    typeof(MemoryMappedFileSerializerDeserializeBenchmark),
};

new BenchmarkSwitcher(allBenchmarks).Run(args, config);
