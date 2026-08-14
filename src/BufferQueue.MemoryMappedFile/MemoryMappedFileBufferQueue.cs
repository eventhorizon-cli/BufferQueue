using System;
using System.IO;

namespace BufferQueue.MemoryMappedFile;

internal sealed class MemoryMappedFileBufferQueue<T> : BufferQueue<T>, IDisposable
    where T : notnull
{
    private readonly MemoryMappedFileBufferPartition<T>[] _partitions;
    private bool _disposed;

    public MemoryMappedFileBufferQueue(MemoryMappedFileBufferQueueOptions<T> options)
        : this(
            options,
            options.PartitionIndexSelector is { } selector
                ? new KeyPartitioner<T>(selector)
                : new ConcurrentRoundRobinPartitioner<T>())
    {
    }

    internal MemoryMappedFileBufferQueue(
        MemoryMappedFileBufferQueueOptions<T> options,
        IPartitioner<T> partitioner)
        : this(options.Validate(), partitioner, CreatePartitions(options))
    {
    }

    private MemoryMappedFileBufferQueue(
        MemoryMappedFileBufferQueueOptions<T> options,
        IPartitioner<T> partitioner,
        MemoryMappedFileBufferPartition<T>[] partitions)
        : base(options.TopicName!, partitions, new MemoryMappedFileBufferProducer<T>(options, partitions, partitioner))
    {
        _partitions = partitions;
    }

    public void Dispose()
    {
        if (_disposed)
        {
            return;
        }

        foreach (var partition in _partitions)
        {
            partition.Dispose();
        }

        _disposed = true;
    }

    private static MemoryMappedFileBufferPartition<T>[] CreatePartitions(MemoryMappedFileBufferQueueOptions<T> options)
    {
        ValidateExistingPartitions(options);

        var partitions = new MemoryMappedFileBufferPartition<T>[options.PartitionNumber];
        var createdPartitionCount = 0;
        try
        {
            for (var i = 0; i < partitions.Length; i++)
            {
                partitions[i] = new MemoryMappedFileBufferPartition<T>(i, options);
                createdPartitionCount++;
            }
        }
        catch
        {
            for (var i = 0; i < createdPartitionCount; i++)
            {
                partitions[i].Dispose();
            }

            throw;
        }

        return partitions;
    }

    private static void ValidateExistingPartitions(MemoryMappedFileBufferQueueOptions<T> options)
    {
        var topicDirectory = Path.Combine(options.DataDirectory, options.TopicName!);
        if (!Directory.Exists(topicDirectory))
        {
            return;
        }

        foreach (var partitionDirectory in Directory.EnumerateDirectories(topicDirectory, "partition-*"))
        {
            var partitionDirectoryName = Path.GetFileName(partitionDirectory);
            if (!TryParsePartitionId(partitionDirectoryName, out var partitionId))
            {
                continue;
            }

            if (partitionId >= options.PartitionNumber)
            {
                throw new InvalidDataException(
                    $"The configured partition number {options.PartitionNumber} is smaller than existing MemoryMappedFile topic '{options.TopicName}' partition directories. Existing partition '{partitionDirectoryName}' would be ignored during recovery.");
            }
        }
    }

    private static bool TryParsePartitionId(string partitionDirectoryName, out int partitionId)
    {
        const string prefix = "partition-";

        partitionId = 0;
        return partitionDirectoryName.Length == prefix.Length + 5
               && partitionDirectoryName.StartsWith(prefix, StringComparison.Ordinal)
               && int.TryParse(partitionDirectoryName[prefix.Length..], out partitionId);
    }
}
