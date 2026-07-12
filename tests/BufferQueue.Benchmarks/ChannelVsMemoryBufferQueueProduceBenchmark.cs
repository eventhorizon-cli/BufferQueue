// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.Threading.Channels;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BufferQueue.Memory;

namespace BufferQueue.Benchmarks;

[SimpleJob(
    RuntimeMoniker.Net10_0,
    launchCount: 1,
    warmupCount: 6,
    iterationCount: 15,
    id: "Fixed")]
public class ChannelVsMemoryBufferQueueProduceBenchmark
{
    public enum CapacityMode
    {
        Unbounded,
        Bounded,
    }

    private int[][] _chunks = null!;

    [Params(CapacityMode.Unbounded, CapacityMode.Bounded)]
    public CapacityMode Mode { get; set; }

    [Params(8192)] public int MessageSize { get; set; }

    [GlobalSetup]
    public void Setup()
    {
        var chunkSize = (int)Math.Ceiling(MessageSize * 1.0d / Environment.ProcessorCount);
        _chunks = Enumerable.Range(0, MessageSize).Chunk(chunkSize).ToArray();
    }

    [Benchmark(Baseline = true)]
    public async Task Channel_Produce_Concurrent()
    {
        var channel = Mode == CapacityMode.Bounded
            ? Channel.CreateBounded<int>(new BoundedChannelOptions(MessageSize)
            {
                SingleReader = false,
                SingleWriter = false,
            })
            : Channel.CreateUnbounded<int>();
        var writer = channel.Writer;
        var tasks = _chunks.Select(chunk => Task.Run(() =>
        {
            foreach (var item in chunk)
            {
                writer.TryWrite(item);
            }
        })).ToArray();

        await Task.WhenAll(tasks);
    }

    [Benchmark]
    public async Task MemoryBufferQueue_Produce_Concurrent()
    {
        var queue = new MemoryBufferQueue<int>(new MemoryBufferQueueOptions
        {
            TopicName = "test",
            PartitionNumber = Environment.ProcessorCount,
            BoundedCapacity = Mode == CapacityMode.Bounded ? (ulong)MessageSize : null,
        });
        var producer = queue.GetProducer();
        var tasks = _chunks.Select(chunk => Task.Run(async () =>
        {
            foreach (var item in chunk)
            {
                var valueTask = producer.ProduceAsync(item);
                if (!valueTask.IsCompletedSuccessfully)
                {
                    await valueTask.AsTask();
                }
            }
        })).ToArray();

        await Task.WhenAll(tasks);
    }
}
