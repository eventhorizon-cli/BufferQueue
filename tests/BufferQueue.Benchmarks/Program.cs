using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Diagnosers;
using BenchmarkDotNet.Running;
using BufferQueue.Benchmarks;

var config = ManualConfig
    .Create(DefaultConfig.Instance)
    .AddDiagnoser(MemoryDiagnoser.Default);

var allBenchmarks = new[]
{
    typeof(BlockingCollectionVsMemoryBufferQueueProduceBenchmark),
    typeof(BlockingCollectionVsMemoryBufferQueueConsumeBenchmark),
    typeof(ChannelVsMemoryBufferQueueProduceBenchmark),
    typeof(UnboundedChannelVsMemoryBufferQueueConsumeBenchmark),
    typeof(SingleReaderChannelVsMemoryBufferQueueConsumeBenchmark),
    typeof(BoundedChannelVsMemoryBufferQueueConsumeBenchmark),
    typeof(MemoryBufferPartitionerBenchmark),
    typeof(MemoryBufferOffsetBenchmark),
    typeof(MemoryCommitModeBenchmark),
    typeof(MemoryAutoCommitOptimizationBenchmark),
    typeof(MemoryNotificationOptimizationBenchmark),
    typeof(MemoryGroupProgressOptimizationBenchmark),
    typeof(MemorySparseBatchBenchmark),
    typeof(ChannelReadPathBenchmark),
    typeof(StringPartitionRoutingBenchmark),
    typeof(PullConsumerSchedulingBenchmark),
    typeof(MemoryVsMemoryMappedFileBufferQueueProduceBenchmark),
    typeof(MemoryVsMemoryMappedFileBufferQueueConsumeBenchmark),
    typeof(MemoryMappedFileSerializerSerializeBenchmark),
    typeof(MemoryMappedFileSerializerDeserializeBenchmark),
};

new BenchmarkSwitcher(allBenchmarks).Run(args, config);
