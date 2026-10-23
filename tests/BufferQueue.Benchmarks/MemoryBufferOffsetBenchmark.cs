using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BufferQueue.Memory;

namespace BufferQueue.Benchmarks;

[SimpleJob(RuntimeMoniker.Net10_0, launchCount: 1, warmupCount: 3, iterationCount: 10)]
public class MemoryBufferOffsetBenchmark
{
    private const int SampleCount = 1024;
    private readonly MemoryBufferPartitionOffset[] _positions = new MemoryBufferPartitionOffset[SampleCount];
    private readonly MemoryBufferPartitionOffset[] _starts = new MemoryBufferPartitionOffset[SampleCount];
    private readonly MemoryBufferPartitionOffset[] _ends = new MemoryBufferPartitionOffset[SampleCount];

    [Params("Contiguous", "Wrap")]
    public string Scenario { get; set; } = "Contiguous";

    [GlobalSetup]
    public void Setup()
    {
        for (var i = 0; i < SampleCount; i++)
        {
            var start = Scenario switch
            {
                "Wrap" => new MemoryBufferPartitionOffset(ulong.MaxValue - 31),
                _ => new MemoryBufferPartitionOffset(0)
            };
            _starts[i] = start;
            _positions[i] = start + (ulong)(i + 128);
            _ends[i] = start + 4095;
        }
    }

    [Benchmark(OperationsPerInvoke = SampleCount)]
    public int Compare()
    {
        var count = 0;
        for (var i = 0; i < SampleCount; i++)
        {
            if (_positions[i] > _starts[i])
            {
                count++;
            }

            if (_positions[i] < _ends[i])
            {
                count++;
            }
        }

        return count;
    }

    [Benchmark(OperationsPerInvoke = SampleCount)]
    public int Distance()
    {
        var total = 0;
        for (var i = 0; i < SampleCount; i++)
        {
            total += checked((int)(_positions[i] - _starts[i]));
        }

        return total;
    }

    [Benchmark(OperationsPerInvoke = SampleCount)]
    public int ReadAndCommitOffsets()
    {
        var total = 0;
        for (var i = 0; i < SampleCount; i++)
        {
            var position = _positions[i];
            var start = _starts[i];
            var end = _ends[i];
            if (position < start || position > end)
            {
                continue;
            }

            total += checked((int)(position - start));
            var committed = position + 1;
            if (end < committed)
            {
                total++;
            }
        }

        return total;
    }
}
