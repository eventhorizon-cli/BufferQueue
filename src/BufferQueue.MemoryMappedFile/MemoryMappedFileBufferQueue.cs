// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;
using System.IO;

namespace BufferQueue.MemoryMappedFile;

internal sealed class MemoryMappedFileBufferQueue<T> : BufferQueue<T>, IDisposable
    where T : notnull
{
    private readonly MemoryMappedFileBufferPartition<T>[] _partitions;
    private bool _disposed;

    public MemoryMappedFileBufferQueue(MemoryMappedFileBufferQueueOptions<T> options)
        : this(options, CreatePartitions(options))
    {
    }

    private MemoryMappedFileBufferQueue(
        MemoryMappedFileBufferQueueOptions<T> options,
        MemoryMappedFileBufferPartition<T>[] partitions)
        : base(options.TopicName!, partitions, new MemoryMappedFileBufferProducer<T>(options, partitions))
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
        ValidateOptions(options);
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

    private static void ValidateOptions(MemoryMappedFileBufferQueueOptions<T> options)
    {
        _ = options.GetSegmentSizeInBytes();
        _ = options.GetMaxRetainedConsumedSegments();
        ArgumentNullException.ThrowIfNull(options.Serializer, nameof(options.Serializer));

        if (!Enum.IsDefined(options.FlushStrategy))
        {
            throw new ArgumentOutOfRangeException(nameof(options.FlushStrategy),
                "The flush strategy is not supported.");
        }

        if (options.FlushStrategy == MemoryMappedFileFlushStrategy.Batch && options.FlushBatchSize <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(options.FlushBatchSize),
                "Flush batch size must be greater than zero when using the batch flush strategy.");
        }
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
        const string Prefix = "partition-";

        partitionId = 0;
        return partitionDirectoryName.Length == Prefix.Length + 5
               && partitionDirectoryName.StartsWith(Prefix, StringComparison.Ordinal)
               && int.TryParse(partitionDirectoryName[Prefix.Length..], out partitionId);
    }
}
