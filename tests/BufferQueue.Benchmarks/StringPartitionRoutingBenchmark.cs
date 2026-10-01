using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;

namespace BufferQueue.Benchmarks;

[SimpleJob(RuntimeMoniker.Net10_0, launchCount: 1, warmupCount: 6, iterationCount: 10)]
[IterationTime(100)]
[MemoryDiagnoser]
public class StringPartitionRoutingBenchmark
{
    private string[] _keys = null!;

    [Params(4, 9, 64, 256)]
    public int KeyLength { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        _keys = Enumerable.Range(0, 1000)
            .Select(index => new string('x', KeyLength - 4) + index.ToString("D4"))
            .ToArray();
    }

    [Benchmark(OperationsPerInvoke = 1000)]
    public int Route()
    {
        var sum = 0;
        foreach (var key in _keys)
        {
            sum += PartitionKeyRouting.SelectStringPartition(key, 8);
        }

        return sum;
    }
}
