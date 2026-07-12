// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using BenchmarkDotNet.Attributes;
using BufferQueue.Memory;
using BufferQueue.MemoryMappedFile;

namespace BufferQueue.Benchmarks;

[ShortRunJob]
public class MemoryVsMemoryMappedFileBufferQueueConsumeBenchmark
{
    private const string TopicName = "storage-consume";
    private string? _dataDirectory;
    private MemoryMappedFileBenchmarkMessage[] _messages = null!;
    private IEnumerable<IBufferPullConsumer<MemoryMappedFileBenchmarkMessage>> _memoryConsumers = null!;
    private MemoryMappedFileBufferQueue<MemoryMappedFileBenchmarkMessage>? _memoryMappedFileQueue;
    private IEnumerable<IBufferPullConsumer<MemoryMappedFileBenchmarkMessage>> _memoryMappedFileConsumers = null!;

    [Params(1024)] public int MessageSize { get; set; }

    [Params(100)] public int BatchSize { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        _messages = Enumerable.Range(0, MessageSize)
            .Select(MemoryMappedFileBenchmarkMessage.Create)
            .ToArray();
    }

    [IterationSetup(Target = nameof(MemoryBufferQueue_Consume_Concurrent))]
    public void SetupMemory()
    {
        var queue = new MemoryBufferQueue<MemoryMappedFileBenchmarkMessage>(new MemoryBufferQueueOptions
        {
            TopicName = TopicName,
            PartitionNumber = Environment.ProcessorCount
        });
        var producer = queue.GetProducer();

        ProduceMessages(producer);
        _memoryConsumers = CreateConsumers(queue);
    }

    [IterationSetup(Target = nameof(MemoryMappedFileBufferQueue_Consume_SystemTextJson_Concurrent))]
    public void SetupMemoryMappedFileSystemTextJson() =>
        SetupMemoryMappedFile(new MemoryMappedFileBufferQueueOptions<MemoryMappedFileBenchmarkMessage>().Serializer);

    [IterationSetup(Target = nameof(MemoryMappedFileBufferQueue_Consume_MessagePack_Concurrent))]
    public void SetupMemoryMappedFileMessagePack() =>
        SetupMemoryMappedFile(new MessagePackMemoryMappedFileSerializer<MemoryMappedFileBenchmarkMessage>());

    [IterationSetup(Target = nameof(MemoryMappedFileBufferQueue_Consume_Unmanaged_Concurrent))]
    public void SetupMemoryMappedFileUnmanaged() =>
        SetupMemoryMappedFile(UnmanagedMemoryMappedFileSerializer<MemoryMappedFileBenchmarkMessage>.Instance);

    [IterationCleanup(Targets = new[]
    {
        nameof(MemoryMappedFileBufferQueue_Consume_SystemTextJson_Concurrent),
        nameof(MemoryMappedFileBufferQueue_Consume_MessagePack_Concurrent),
        nameof(MemoryMappedFileBufferQueue_Consume_Unmanaged_Concurrent)
    })]
    public void CleanupMemoryMappedFile()
    {
        _memoryMappedFileQueue?.Dispose();
        _memoryMappedFileQueue = null;
        DeleteDataDirectory();
    }

    [Benchmark(Baseline = true)]
    public void MemoryBufferQueue_Consume_Concurrent()
    {
        Consume(_memoryConsumers);
    }

    [Benchmark]
    public void MemoryMappedFileBufferQueue_Consume_SystemTextJson_Concurrent()
    {
        Consume(_memoryMappedFileConsumers);
    }

    [Benchmark]
    public void MemoryMappedFileBufferQueue_Consume_MessagePack_Concurrent()
    {
        Consume(_memoryMappedFileConsumers);
    }

    [Benchmark]
    public void MemoryMappedFileBufferQueue_Consume_Unmanaged_Concurrent()
    {
        Consume(_memoryMappedFileConsumers);
    }

    private void SetupMemoryMappedFile(IMemoryMappedFileSerializer<MemoryMappedFileBenchmarkMessage> serializer)
    {
        _dataDirectory = CreateDataDirectory();
        _memoryMappedFileQueue = new MemoryMappedFileBufferQueue<MemoryMappedFileBenchmarkMessage>(
            new MemoryMappedFileBufferQueueOptions<MemoryMappedFileBenchmarkMessage>
            {
                TopicName = TopicName,
                DataDirectory = _dataDirectory,
                PartitionNumber = Environment.ProcessorCount,
                SegmentSize = 64L * 1024 * 1024,
                Serializer = serializer
            });
        var producer = _memoryMappedFileQueue.GetProducer();

        ProduceMessages(producer);
        _memoryMappedFileConsumers = CreateConsumers(_memoryMappedFileQueue);
    }

    private void ProduceMessages(IBufferProducer<MemoryMappedFileBenchmarkMessage> producer)
    {
        foreach (var message in _messages)
        {
            producer.ProduceAsync(message);
        }
    }

    private IEnumerable<IBufferPullConsumer<MemoryMappedFileBenchmarkMessage>> CreateConsumers(
        IBufferQueue<MemoryMappedFileBenchmarkMessage> queue) =>
        queue.CreateConsumers(
            new BufferPullConsumerOptions
            {
                GroupName = "TestGroup",
                TopicName = TopicName,
                AutoCommit = true,
                BatchSize = BatchSize,
            },
            Environment.ProcessorCount);

    private void Consume(IEnumerable<IBufferPullConsumer<MemoryMappedFileBenchmarkMessage>> consumers)
    {
        var consumerList = consumers.ToList();
        var partitionCount = consumerList.Count;
        var baseMessageCount = MessageSize / partitionCount;
        var remainder = MessageSize % partitionCount;
        var tasks = consumerList.Select((consumer, index) => Task.Run(async () =>
        {
            var expectedCount = baseMessageCount + (index < remainder ? 1 : 0);
            var consumedCount = 0;
            await foreach (var items in consumer.ConsumeAsync())
            {
                consumedCount += items.Count();
                if (consumedCount >= expectedCount)
                {
                    break;
                }
            }
        })).ToArray();

        Task.WaitAll(tasks);
    }

    private static string CreateDataDirectory() =>
        Path.Combine(
            AppContext.BaseDirectory,
            "MemoryMappedFileBenchmarkData",
            Guid.NewGuid().ToString("N"));

    private void DeleteDataDirectory()
    {
        if (_dataDirectory is not null && Directory.Exists(_dataDirectory))
        {
            Directory.Delete(_dataDirectory, recursive: true);
        }

        _dataDirectory = null;
    }
}
