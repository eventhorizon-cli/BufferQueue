using System;
using System.Collections.Generic;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace BufferQueue.Memory;

internal sealed class MemoryBufferProducer<T>(
    MemoryBufferQueueOptions options,
    MemoryBufferPartition<T>[] partitions,
    IPartitioner<T> partitioner)
    : IBufferProducer<T>
{
    public MemoryBufferProducer(
        MemoryBufferQueueOptions options,
        MemoryBufferPartition<T>[] partitions)
        : this(
            options,
            partitions,
            options is MemoryBufferQueueOptions<T> { PartitionIndexSelector: { } selector }
                ? new KeyPartitioner<T>(selector)
                : new RoundRobinPartitioner<T>())
    {
    }

    private readonly MemoryBufferCapacityGate? _capacityGate = CreateCapacityGate(options, partitions);
    private readonly BufferQueueFullMode _fullMode = options.FullMode;
    private readonly object? _serializedAppendLock = partitioner.SupportsConcurrentSelection
        ? null
        : GetSharedAppendLock(partitions);

    public string TopicName { get; } = options.TopicName!;

    public ValueTask<bool> TryProduceAsync(
        T item,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new(TryEnqueue(item));
    }

    public ValueTask<bool> TryProduceAsync(
        ReadOnlyMemory<T> items,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return new(TryEnqueueBatch(items.Span));
    }

    [MethodImpl(MethodImplOptions.AggressiveInlining)]
    public ValueTask ProduceAsync(T item, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (TryEnqueue(item))
        {
            return ValueTask.CompletedTask;
        }

        return HandleFullItem(item, cancellationToken);
    }

    private ValueTask HandleFullItem(T item, CancellationToken cancellationToken)
    {
        if (_fullMode == BufferQueueFullMode.Fail)
        {
            throw CreateQueueFullException("item");
        }

        return EnqueueWhenCapacityAvailableAsync(item, cancellationToken);
    }

    public ValueTask ProduceAsync(
        ReadOnlyMemory<T> items,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (items.IsEmpty)
        {
            return ValueTask.CompletedTask;
        }

        if (items.Length == 1)
        {
            return ProduceAsync(items.Span[0], cancellationToken);
        }

        var capacityGate = _capacityGate;
        if (capacityGate != null && (ulong)items.Length > capacityGate.Capacity)
        {
            if (_fullMode == BufferQueueFullMode.Fail)
            {
                throw CreateQueueFullException("batch");
            }

            throw new ArgumentOutOfRangeException(nameof(items),
                "The batch size cannot exceed the bounded queue capacity.");
        }

        if (_serializedAppendLock != null)
        {
            if (TryEnqueueBatchSerialized(items.Span))
            {
                return ValueTask.CompletedTask;
            }

            return HandleFullBatch(items, null, cancellationToken);
        }

        var itemsByPartition = GroupItemsByPartition(items.Span);
        if (TryEnqueueBatchConcurrent(itemsByPartition, items.Length))
        {
            return ValueTask.CompletedTask;
        }

        return HandleFullBatch(items, itemsByPartition, cancellationToken);
    }

    private ValueTask HandleFullBatch(
        ReadOnlyMemory<T> items,
        List<T>?[]? itemsByPartition,
        CancellationToken cancellationToken)
    {
        if (_fullMode == BufferQueueFullMode.Fail)
        {
            throw CreateQueueFullException("batch");
        }

        return EnqueueBatchWhenCapacityAvailableAsync(items, itemsByPartition, cancellationToken);
    }

    private bool TryEnqueue(T item)
    {
        var capacityGate = _capacityGate;
        if (capacityGate == null)
        {
            var partition = Append(item);
            partition.NotifyConsumers();
            return true;
        }

        if (_serializedAppendLock is { } serializedAppendLock)
        {
            return TryEnqueueSerialized(item, capacityGate, serializedAppendLock);
        }

        return TryEnqueueConcurrent(item, capacityGate);
    }

    private bool TryEnqueueSerialized(
        T item,
        MemoryBufferCapacityGate capacityGate,
        object appendLock)
    {
        MemoryBufferPartition<T> partition;
        lock (appendLock)
        {
            if (!capacityGate.TryAcquire())
            {
                return false;
            }

            try
            {
                partition = AppendSelectedPartition(item);
            }
            catch
            {
                capacityGate.Release();
                throw;
            }
        }

        partition.NotifyConsumers();
        return true;
    }

    private bool TryEnqueueConcurrent(T item, MemoryBufferCapacityGate capacityGate)
    {
        if (!capacityGate.TryAcquire())
        {
            return false;
        }

        MemoryBufferPartition<T> partition;
        try
        {
            partition = Append(item);
        }
        catch
        {
            capacityGate?.Release();
            throw;
        }

        partition.NotifyConsumers();
        return true;
    }

    private bool TryEnqueueBatch(ReadOnlySpan<T> items)
    {
        if (items.IsEmpty)
        {
            return true;
        }

        if (items.Length == 1)
        {
            return TryEnqueue(items[0]);
        }

        var capacityGate = _capacityGate;
        if (capacityGate != null && (ulong)items.Length > capacityGate.Capacity)
        {
            return false;
        }

        if (_serializedAppendLock != null)
        {
            return TryEnqueueBatchSerialized(items);
        }

        var itemsByPartition = GroupItemsByPartition(items);
        return TryEnqueueBatchConcurrent(itemsByPartition, items.Length);
    }

    private bool TryEnqueueBatchSerialized(ReadOnlySpan<T> items)
    {
        var capacityGate = _capacityGate;
        if (capacityGate != null && !capacityGate.TryAcquire((ulong)items.Length))
        {
            return false;
        }

        AppendBatchSerialized(items, capacityGate);
        return true;
    }

    private bool TryEnqueueBatchConcurrent(List<T>?[] itemsByPartition, int itemCount)
    {
        var capacityGate = _capacityGate;
        if (capacityGate != null && !capacityGate.TryAcquire((ulong)itemCount))
        {
            return false;
        }

        AppendBatchConcurrent(itemsByPartition, itemCount, capacityGate);
        return true;
    }

    private async ValueTask EnqueueWhenCapacityAvailableAsync(
        T item,
        CancellationToken cancellationToken)
    {
        var capacityGate = _capacityGate!;
        await capacityGate.AcquireAsync(1, cancellationToken).ConfigureAwait(false);

        MemoryBufferPartition<T> partition;
        try
        {
            partition = Append(item);
        }
        catch
        {
            capacityGate.Release();
            throw;
        }

        partition.NotifyConsumers();
    }

    private async ValueTask EnqueueBatchWhenCapacityAvailableAsync(
        ReadOnlyMemory<T> items,
        List<T>?[]? itemsByPartition,
        CancellationToken cancellationToken)
    {
        var capacityGate = _capacityGate!;
        await capacityGate.AcquireAsync((ulong)items.Length, cancellationToken).ConfigureAwait(false);

        if (itemsByPartition == null)
        {
            AppendBatchSerialized(items.Span, capacityGate);
        }
        else
        {
            AppendBatchConcurrent(itemsByPartition, items.Length, capacityGate);
        }
    }

    private void AppendBatchSerialized(
        ReadOnlySpan<T> items,
        MemoryBufferCapacityGate? capacityGate)
    {
        var modifiedPartitions = partitions.Length <= 128
            ? stackalloc bool[partitions.Length]
            : new bool[partitions.Length];
        var appendedCount = 0;

        try
        {
            lock (_serializedAppendLock!)
            {
                foreach (var item in items)
                {
                    var partitionIndex = SelectPartitionIndex(item);
                    partitions[partitionIndex].AppendFromSerializedProducer(item);
                    modifiedPartitions[partitionIndex] = true;
                    appendedCount++;
                }
            }
        }
        catch
        {
            capacityGate?.Release((ulong)(items.Length - appendedCount));
            throw;
        }
        finally
        {
            NotifyConsumers(modifiedPartitions);
        }
    }

    private void AppendBatchConcurrent(
        List<T>?[] itemsByPartition,
        int itemCount,
        MemoryBufferCapacityGate? capacityGate)
    {
        var modifiedPartitions = partitions.Length <= 128
            ? stackalloc bool[partitions.Length]
            : new bool[partitions.Length];
        var appendedCount = 0;

        try
        {
            for (var partitionIndex = 0; partitionIndex < partitions.Length; partitionIndex++)
            {
                var partitionItems = itemsByPartition[partitionIndex];
                if (partitionItems == null)
                {
                    continue;
                }

                var partition = partitions[partitionIndex];
                modifiedPartitions[partitionIndex] = true;
                lock (partition.AppendLock)
                {
                    foreach (var item in partitionItems)
                    {
                        partition.AppendFromSerializedProducer(item);
                        appendedCount++;
                    }
                }
            }
        }
        catch
        {
            capacityGate?.Release((ulong)(itemCount - appendedCount));
            throw;
        }
        finally
        {
            NotifyConsumers(modifiedPartitions);
        }
    }

    private List<T>?[] GroupItemsByPartition(ReadOnlySpan<T> items)
    {
        var itemsByPartition = new List<T>?[partitions.Length];
        foreach (var item in items)
        {
            var partitionIndex = SelectPartitionIndex(item);
            var partitionItems = itemsByPartition[partitionIndex];
            if (partitionItems == null)
            {
                partitionItems = [];
                itemsByPartition[partitionIndex] = partitionItems;
            }

            partitionItems.Add(item);
        }

        return itemsByPartition;
    }

    private MemoryBufferPartition<T> Append(T item)
    {
        if (_serializedAppendLock is { } serializedAppendLock)
        {
            lock (serializedAppendLock)
            {
                return AppendSelectedPartition(item);
            }
        }

        var partition = SelectPartition(item);
        lock (partition.AppendLock)
        {
            partition.AppendFromSerializedProducer(item);
        }

        return partition;
    }

    private MemoryBufferPartition<T> AppendSelectedPartition(T item)
    {
        var partition = SelectPartition(item);
        partition.AppendFromSerializedProducer(item);
        return partition;
    }

    private int SelectPartitionIndex(T item) => partitioner.SelectPartition(item, partitions.Length);

    private MemoryBufferPartition<T> SelectPartition(T item) => partitions[SelectPartitionIndex(item)];

    private void NotifyConsumers(ReadOnlySpan<bool> modifiedPartitions)
    {
        for (var partitionIndex = 0; partitionIndex < modifiedPartitions.Length; partitionIndex++)
        {
            if (modifiedPartitions[partitionIndex])
            {
                partitions[partitionIndex].NotifyConsumers();
            }
        }
    }

    private BufferQueueFullException CreateQueueFullException(string subject) =>
        new($"The queue '{TopicName}' is full, and the {subject} cannot be produced.");

    private static MemoryBufferCapacityGate? CreateCapacityGate(
        MemoryBufferQueueOptions options,
        MemoryBufferPartition<T>[] bufferPartitions)
    {
        if (options.BoundedCapacity is not { } capacity)
        {
            return null;
        }

        var capacityGate = new MemoryBufferCapacityGate(capacity);
        Action<ulong> releaseCapacity = capacityGate.Release;
        foreach (var partition in bufferPartitions)
        {
            partition.SetCapacityReleaseHandler(releaseCapacity);
        }

        return capacityGate;
    }

    private static object GetSharedAppendLock(MemoryBufferPartition<T>[] bufferPartitions)
    {
        if (bufferPartitions.Length == 0)
        {
            throw new ArgumentException("At least one partition is required.", nameof(bufferPartitions));
        }

        var appendLock = bufferPartitions[0].AppendLock;
        for (var i = 1; i < bufferPartitions.Length; i++)
        {
            if (!ReferenceEquals(appendLock, bufferPartitions[i].AppendLock))
            {
                throw new ArgumentException("All partitions must share the same append lock.", nameof(bufferPartitions));
            }
        }

        return appendLock;
    }
}
