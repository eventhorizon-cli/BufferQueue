using System.Numerics;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Jobs;
using BufferQueue.Memory;

namespace BufferQueue.Benchmarks;

[SimpleJob(
    RuntimeMoniker.Net10_0,
    launchCount: 1,
    warmupCount: 6,
    iterationCount: 15,
    invocationCount: 1,
    id: "Fixed")]
public class MemoryBufferPartitionerBenchmark
{
    private const int MessageCount = 8192;
    private const int BatchCount = 832;
    private const int OperationsPerInvoke = MessageCount * BatchCount;
    private const int PartitionCount = 8;
    private PartitionBenchmarkMessage[] _messages = null!;
    private IBufferProducer<PartitionBenchmarkMessage> _producer = null!;

    [GlobalSetup]
    public void GlobalSetup()
    {
        _messages = Enumerable.Range(0, MessageCount)
            .Select(index =>
            {
                var key = index % 1024;
                return new PartitionBenchmarkMessage(
                    key + 1,
                    $"{(char)('A' + key % 26)}{(char)('A' + key / 26 % 26)}-{key:D4}",
                    new CustomerPartitionKey(index % 32, key + 1));
            })
            .ToArray();
    }

    [IterationSetup(Target = nameof(RoundRobin))]
    public void SetupRoundRobin() =>
        _producer = CreateProducer();

    [IterationSetup(Target = nameof(PartitionKey_Int32))]
    public void SetupInt32PartitionKey() =>
        _producer = CreateNumericProducer(static message => message.Int32Key);

    [IterationSetup(Target = nameof(PartitionKey_String))]
    public void SetupStringPartitionKey() =>
        _producer = CreateStringProducer(static message => message.StringKey);

    [IterationSetup(Target = nameof(PartitionKey_CustomNumeric))]
    public void SetupCustomPartitionKey() =>
        _producer = CreateNumericProducer(static message => message.CustomKey.CustomerId);

    [Benchmark(Baseline = true, OperationsPerInvoke = OperationsPerInvoke)]
    public void RoundRobin() => ProduceMessages();

    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public void PartitionKey_Int32() => ProduceMessages();

    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public void PartitionKey_String() => ProduceMessages();

    [Benchmark(OperationsPerInvoke = OperationsPerInvoke)]
    public void PartitionKey_CustomNumeric() => ProduceMessages();

    private static IBufferProducer<PartitionBenchmarkMessage> CreateProducer()
    {
        var queue = new MemoryBufferQueue<PartitionBenchmarkMessage>(new MemoryBufferQueueOptions
        {
            TopicName = "partitioner-benchmark",
            PartitionNumber = PartitionCount,
            SegmentSize = OperationsPerInvoke / PartitionCount,
        });
        return queue.GetProducer();
    }

    private IBufferProducer<PartitionBenchmarkMessage> CreateNumericProducer<TNumber>(
        Func<PartitionBenchmarkMessage, TNumber> partitionKeySelector)
        where TNumber : INumber<TNumber>
    {
        var options = new MemoryBufferQueueOptions<PartitionBenchmarkMessage>
        {
            TopicName = "partitioner-benchmark",
            PartitionNumber = PartitionCount,
            SegmentSize = GetNumericPartitionCapacity(partitionKeySelector),
        };
        options.UsePartitionKey(partitionKeySelector);

        var queue = new MemoryBufferQueue<PartitionBenchmarkMessage>(options);
        return queue.GetProducer();
    }

    private IBufferProducer<PartitionBenchmarkMessage> CreateStringProducer(
        Func<PartitionBenchmarkMessage, string> partitionKeySelector)
    {
        var options = new MemoryBufferQueueOptions<PartitionBenchmarkMessage>
        {
            TopicName = "partitioner-benchmark",
            PartitionNumber = PartitionCount,
            SegmentSize = GetStringPartitionCapacity(partitionKeySelector),
        };
        options.UsePartitionKey(partitionKeySelector);

        var queue = new MemoryBufferQueue<PartitionBenchmarkMessage>(options);
        return queue.GetProducer();
    }

    private int GetNumericPartitionCapacity<TNumber>(
        Func<PartitionBenchmarkMessage, TNumber> partitionKeySelector)
        where TNumber : INumber<TNumber>
    {
        var itemsPerPartition = new int[PartitionCount];
        foreach (var message in _messages)
        {
            var partitionIndex = PartitionKeyRouting.SelectNumericPartition(
                partitionKeySelector(message), PartitionCount);
            itemsPerPartition[partitionIndex]++;
        }

        return itemsPerPartition.Max() * BatchCount;
    }

    private int GetStringPartitionCapacity(Func<PartitionBenchmarkMessage, string> partitionKeySelector)
    {
        var itemsPerPartition = new int[PartitionCount];
        foreach (var message in _messages)
        {
            var partitionIndex = PartitionKeyRouting.SelectStringPartition(
                partitionKeySelector(message), PartitionCount);
            itemsPerPartition[partitionIndex]++;
        }

        return itemsPerPartition.Max() * BatchCount;
    }

    private void ProduceMessages()
    {
        for (var batch = 0; batch < BatchCount; batch++)
        {
            foreach (var message in _messages)
            {
                var produceTask = _producer.ProduceAsync(message);
                if (!produceTask.IsCompletedSuccessfully)
                {
                    produceTask.AsTask().GetAwaiter().GetResult();
                }
            }
        }
    }

    private sealed record PartitionBenchmarkMessage(
        int Int32Key,
        string StringKey,
        CustomerPartitionKey CustomKey);

    private readonly record struct CustomerPartitionKey(int TenantId, int CustomerId);
}
