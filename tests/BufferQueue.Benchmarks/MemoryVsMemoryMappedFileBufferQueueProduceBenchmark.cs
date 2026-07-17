// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using BenchmarkDotNet.Attributes;
using BufferQueue.Memory;
using BufferQueue.MemoryMappedFile;

namespace BufferQueue.Benchmarks;

[ShortRunJob]
public class MemoryVsMemoryMappedFileBufferQueueProduceBenchmark
{
    private const string TopicName = "storage-produce";
    private MemoryMappedFileBenchmarkMessage[][] _chunks = null!;
    private string? _dataDirectory;
    private IBufferProducer<MemoryMappedFileBenchmarkMessage> _memoryProducer = null!;
    private MemoryMappedFileBufferQueue<MemoryMappedFileBenchmarkMessage>? _memoryMappedFileQueue;
    private IBufferProducer<MemoryMappedFileBenchmarkMessage> _memoryMappedFileProducer = null!;

    [Params(1024)] public int MessageSize { get; set; }

    [GlobalSetup]
    public void GlobalSetup()
    {
        var chunkSize = (int)Math.Ceiling(MessageSize * 1.0d / Environment.ProcessorCount);
        _chunks = Enumerable.Range(0, MessageSize)
            .Select(MemoryMappedFileBenchmarkMessage.Create)
            .Chunk(chunkSize)
            .ToArray();
    }

    [IterationSetup(Target = nameof(MemoryBufferQueue_Produce_Concurrent))]
    public void SetupMemory()
    {
        var queue = new MemoryBufferQueue<MemoryMappedFileBenchmarkMessage>(new MemoryBufferQueueOptions
        {
            TopicName = TopicName,
            PartitionNumber = Environment.ProcessorCount
        });
        _memoryProducer = queue.GetProducer();
    }

    [IterationSetup(Target = nameof(MemoryMappedFileBufferQueue_Produce_SystemTextJson_Concurrent))]
    public void SetupMemoryMappedFileSystemTextJson() =>
        SetupMemoryMappedFile(new MemoryMappedFileBufferQueueOptions<MemoryMappedFileBenchmarkMessage>().Serializer);

    [IterationSetup(Target = nameof(MemoryMappedFileBufferQueue_Produce_MessagePack_Concurrent))]
    public void SetupMemoryMappedFileMessagePack() =>
        SetupMemoryMappedFile(new MessagePackMemoryMappedFileSerializer<MemoryMappedFileBenchmarkMessage>());

    [IterationSetup(Target = nameof(MemoryMappedFileBufferQueue_Produce_Unmanaged_Concurrent))]
    public void SetupMemoryMappedFileUnmanaged() =>
        SetupMemoryMappedFile(UnmanagedMemoryMappedFileSerializer<MemoryMappedFileBenchmarkMessage>.Instance);

    [IterationCleanup(Targets = new[]
    {
        nameof(MemoryMappedFileBufferQueue_Produce_SystemTextJson_Concurrent),
        nameof(MemoryMappedFileBufferQueue_Produce_MessagePack_Concurrent),
        nameof(MemoryMappedFileBufferQueue_Produce_Unmanaged_Concurrent)
    })]
    public void CleanupMemoryMappedFile()
    {
        _memoryMappedFileQueue?.Dispose();
        _memoryMappedFileQueue = null;
        DeleteDataDirectory();
    }

    [Benchmark(Baseline = true)]
    public Task MemoryBufferQueue_Produce_Concurrent() => ProduceAsync(_memoryProducer);

    [Benchmark]
    public Task MemoryMappedFileBufferQueue_Produce_SystemTextJson_Concurrent() =>
        ProduceAsync(_memoryMappedFileProducer);

    [Benchmark]
    public Task MemoryMappedFileBufferQueue_Produce_MessagePack_Concurrent() =>
        ProduceAsync(_memoryMappedFileProducer);

    [Benchmark]
    public Task MemoryMappedFileBufferQueue_Produce_Unmanaged_Concurrent() =>
        ProduceAsync(_memoryMappedFileProducer);

    private void SetupMemoryMappedFile(IMemoryMappedFileSerializer<MemoryMappedFileBenchmarkMessage> serializer)
    {
        _dataDirectory = CreateDataDirectory();
        _memoryMappedFileQueue = new MemoryMappedFileBufferQueue<MemoryMappedFileBenchmarkMessage>(
            new MemoryMappedFileBufferQueueOptions<MemoryMappedFileBenchmarkMessage>
            {
                TopicName = TopicName,
                DataDirectory = _dataDirectory,
                PartitionNumber = Environment.ProcessorCount,
                SegmentSizeInBytes = 64L * 1024 * 1024,
                Serializer = serializer
            });
        _memoryMappedFileProducer = _memoryMappedFileQueue.GetProducer();
    }

    private async Task ProduceAsync(IBufferProducer<MemoryMappedFileBenchmarkMessage> producer)
    {
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
