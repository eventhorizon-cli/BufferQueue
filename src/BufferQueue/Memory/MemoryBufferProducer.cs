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
    private readonly IRoundRobinBatchPartitioner? _roundRobinBatchPartitioner =
        partitioner as IRoundRobinBatchPartitioner;
    private readonly object? _serializedAppendLock = partitioner.SupportsConcurrentSelection
        ? null
        : GetAppendSynchronization(partitions).AppendCoordinator;
    private readonly bool _canAppendRoundRobinSingleWithoutPartitionLock =
        partitioner is IRoundRobinBatchPartitioner && GetAppendSynchronization(partitions).IsShared;
    private int _activeRoundRobinBatchAppendCount;

    public string TopicName { get; } = options.TopicName!;

    public ValueTask<bool> TryProduceAsync(
        T item,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_capacityGate != null && _fullMode == BufferQueueFullMode.Wait)
        {
            return ToTryProduceResult(ProduceAsync(item, cancellationToken));
        }

        return new(TryEnqueue(item));
    }

    public ValueTask<bool> TryProduceAsync(
        ReadOnlyMemory<T> items,
        CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        if (_capacityGate != null && _fullMode == BufferQueueFullMode.Wait)
        {
            return ToTryProduceResult(ProduceAsync(items, cancellationToken));
        }

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

    private static ValueTask<bool> ToTryProduceResult(ValueTask produceTask)
    {
        if (produceTask.IsCompletedSuccessfully)
        {
            produceTask.GetAwaiter().GetResult();
            return new(true);
        }

        return ToTryProduceResultAsync(produceTask);
    }

    private static async ValueTask<bool> ToTryProduceResultAsync(ValueTask produceTask)
    {
        await produceTask.ConfigureAwait(false);
        return true;
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

            return EnqueueOversizedBatchWhenCapacityAvailableAsync(items, capacityGate, cancellationToken);
        }

        if (_roundRobinBatchPartitioner is { } roundRobinBatchPartitioner)
        {
            if (TryEnqueueRoundRobinBatch(items.Span, roundRobinBatchPartitioner))
            {
                return ValueTask.CompletedTask;
            }

            return HandleFullRoundRobinBatch(items, roundRobinBatchPartitioner, cancellationToken);
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

    private ValueTask HandleFullRoundRobinBatch(
        ReadOnlyMemory<T> items,
        IRoundRobinBatchPartitioner roundRobinBatchPartitioner,
        CancellationToken cancellationToken)
    {
        if (_fullMode == BufferQueueFullMode.Fail)
        {
            throw CreateQueueFullException("batch");
        }

        return EnqueueRoundRobinBatchWhenCapacityAvailableAsync(
            items,
            roundRobinBatchPartitioner,
            cancellationToken);
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
        var appendedWithoutPartitionLock = false;
        lock (appendLock)
        {
            if (!capacityGate.TryAcquire())
            {
                return false;
            }

            try
            {
                partition = SelectPartition(item);
                if (CanAppendRoundRobinSingleWithoutPartitionLock())
                {
                    partition.AppendFromSerializedProducer(item);
                    appendedWithoutPartitionLock = true;
                }
            }
            catch
            {
                capacityGate.Release();
                throw;
            }
        }

        if (!appendedWithoutPartitionLock)
        {
            try
            {
                AppendToPartition(partition, item);
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

        if (_roundRobinBatchPartitioner is { } roundRobinBatchPartitioner)
        {
            return TryEnqueueRoundRobinBatch(items, roundRobinBatchPartitioner);
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

    private bool TryEnqueueRoundRobinBatch(
        ReadOnlySpan<T> items,
        IRoundRobinBatchPartitioner roundRobinBatchPartitioner)
    {
        var capacityGate = _capacityGate;
        if (capacityGate != null && !capacityGate.TryAcquire((ulong)items.Length))
        {
            return false;
        }

        AppendRoundRobinBatch(items, roundRobinBatchPartitioner, capacityGate);
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

    private async ValueTask EnqueueOversizedBatchWhenCapacityAvailableAsync(
        ReadOnlyMemory<T> items,
        MemoryBufferCapacityGate capacityGate,
        CancellationToken cancellationToken)
    {
        var offset = 0;
        while (offset < items.Length)
        {
            var chunkSize = GetChunkSize(items.Length - offset, capacityGate.Capacity);
            await ProduceAsync(items.Slice(offset, chunkSize), cancellationToken).ConfigureAwait(false);
            offset += chunkSize;
        }
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

    private async ValueTask EnqueueRoundRobinBatchWhenCapacityAvailableAsync(
        ReadOnlyMemory<T> items,
        IRoundRobinBatchPartitioner roundRobinBatchPartitioner,
        CancellationToken cancellationToken)
    {
        var capacityGate = _capacityGate!;
        await capacityGate.AcquireAsync((ulong)items.Length, cancellationToken).ConfigureAwait(false);

        AppendRoundRobinBatch(items.Span, roundRobinBatchPartitioner, capacityGate);
    }

    private void AppendRoundRobinBatch(
        ReadOnlySpan<T> items,
        IRoundRobinBatchPartitioner roundRobinBatchPartitioner,
        MemoryBufferCapacityGate? capacityGate)
    {
        var batchAppendStarted = false;
        try
        {
            var firstPartitionIndex = BeginRoundRobinBatchAppend(
                roundRobinBatchPartitioner,
                items.Length);
            batchAppendStarted = true;
            AppendBatchRoundRobin(items, firstPartitionIndex, capacityGate);
        }
        catch
        {
            if (!batchAppendStarted)
            {
                capacityGate?.Release((ulong)items.Length);
            }

            throw;
        }
        finally
        {
            if (batchAppendStarted)
            {
                EndRoundRobinBatchAppend();
            }
        }
    }

    private void AppendBatchRoundRobin(
        ReadOnlySpan<T> items,
        int firstPartitionIndex,
        MemoryBufferCapacityGate? capacityGate)
    {
        var modifiedPartitionCount = 0;
        var appendedCount = 0;
        var partitionCount = partitions.Length;
        var routedPartitionCount = Math.Min(items.Length, partitionCount);

        try
        {
            for (var itemOffset = 0; itemOffset < routedPartitionCount; itemOffset++)
            {
                var partitionIndex = firstPartitionIndex + itemOffset;
                if (partitionIndex >= partitionCount)
                {
                    partitionIndex -= partitionCount;
                }

                modifiedPartitionCount++;
                var partition = partitions[partitionIndex];
                lock (partition.AppendLock)
                {
                    for (var itemIndex = itemOffset; itemIndex < items.Length; itemIndex += partitionCount)
                    {
                        partition.AppendFromSerializedProducer(items[itemIndex]);
                        appendedCount++;
                    }
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
            NotifyRoundRobinConsumers(firstPartitionIndex, modifiedPartitionCount);
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
                    var partition = partitions[partitionIndex];
                    lock (partition.AppendLock)
                    {
                        partition.AppendFromSerializedProducer(item);
                    }

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
        MemoryBufferPartition<T> partition;
        if (_serializedAppendLock is { } serializedAppendLock)
        {
            lock (serializedAppendLock)
            {
                partition = SelectPartition(item);
                if (CanAppendRoundRobinSingleWithoutPartitionLock())
                {
                    partition.AppendFromSerializedProducer(item);
                    return partition;
                }
            }
        }

        else
        {
            partition = SelectPartition(item);
        }

        AppendToPartition(partition, item);

        return partition;
    }

    private static void AppendToPartition(MemoryBufferPartition<T> partition, T item)
    {
        lock (partition.AppendLock)
        {
            partition.AppendFromSerializedProducer(item);
        }
    }

    private int BeginRoundRobinBatchAppend(
        IRoundRobinBatchPartitioner roundRobinBatchPartitioner,
        int itemCount)
    {
        if (_serializedAppendLock is not { } serializedAppendLock)
        {
            return roundRobinBatchPartitioner.ReserveBatch(itemCount, partitions.Length);
        }

        lock (serializedAppendLock)
        {
            _activeRoundRobinBatchAppendCount++;
            try
            {
                return roundRobinBatchPartitioner.ReserveBatch(itemCount, partitions.Length);
            }
            catch
            {
                _activeRoundRobinBatchAppendCount--;
                throw;
            }
        }
    }

    private void EndRoundRobinBatchAppend()
    {
        if (_serializedAppendLock != null)
        {
            Interlocked.Decrement(ref _activeRoundRobinBatchAppendCount);
        }
    }

    private bool CanAppendRoundRobinSingleWithoutPartitionLock() =>
        _canAppendRoundRobinSingleWithoutPartitionLock &&
        Volatile.Read(ref _activeRoundRobinBatchAppendCount) == 0;

    private static (object AppendCoordinator, bool IsShared) GetAppendSynchronization(
        MemoryBufferPartition<T>[] bufferPartitions)
    {
        if (bufferPartitions.Length == 0)
        {
            throw new ArgumentException("At least one partition is required.", nameof(bufferPartitions));
        }

        var appendCoordinator = bufferPartitions[0].AppendCoordinator;
        if (appendCoordinator != null)
        {
            var hasSharedCoordinator = true;
            for (var i = 1; i < bufferPartitions.Length; i++)
            {
                if (!ReferenceEquals(appendCoordinator, bufferPartitions[i].AppendCoordinator))
                {
                    hasSharedCoordinator = false;
                    break;
                }
            }

            if (hasSharedCoordinator)
            {
                return (appendCoordinator, true);
            }
        }

        var appendLock = bufferPartitions[0].AppendLock;
        var hasSharedAppendLock = true;
        for (var i = 1; i < bufferPartitions.Length; i++)
        {
            if (!ReferenceEquals(appendLock, bufferPartitions[i].AppendLock))
            {
                hasSharedAppendLock = false;
                break;
            }
        }

        if (hasSharedAppendLock)
        {
            return (appendLock, true);
        }

        return (new object(), false);
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

    private void NotifyRoundRobinConsumers(int firstPartitionIndex, int modifiedPartitionCount)
    {
        for (var itemOffset = 0; itemOffset < modifiedPartitionCount; itemOffset++)
        {
            var partitionIndex = firstPartitionIndex + itemOffset;
            if (partitionIndex >= partitions.Length)
            {
                partitionIndex -= partitions.Length;
            }

            partitions[partitionIndex].NotifyConsumers();
        }
    }

    private BufferQueueFullException CreateQueueFullException(string subject) =>
        new($"The queue '{TopicName}' is full, and the {subject} cannot be produced.");

    private static int GetChunkSize(int remainingCount, ulong capacity)
    {
        if (capacity == 0)
        {
            throw new InvalidOperationException("Bounded queue capacity must be greater than zero.");
        }

        return (int)Math.Min((ulong)remainingCount, capacity);
    }

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

}
