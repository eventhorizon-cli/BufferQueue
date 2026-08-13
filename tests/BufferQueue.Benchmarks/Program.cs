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
    typeof(BoundedChannelVsMemoryBufferQueueConsumeBenchmark),
    typeof(MemoryBufferPartitionerBenchmark),
    typeof(MemoryVsMemoryMappedFileBufferQueueProduceBenchmark),
    typeof(MemoryVsMemoryMappedFileBufferQueueConsumeBenchmark),
    typeof(MemoryMappedFileSerializerSerializeBenchmark),
    typeof(MemoryMappedFileSerializerDeserializeBenchmark),
};

new BenchmarkSwitcher(allBenchmarks).Run(args, config);
