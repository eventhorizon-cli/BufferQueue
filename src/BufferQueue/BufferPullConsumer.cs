// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

namespace BufferQueue;

internal sealed class BufferPullConsumer<TItem>(BufferPullConsumerOptions options)
    : IBufferPullConsumer<TItem>, IBufferPartitionConsumer<TItem>
{
    private volatile IBufferPartition<TItem>[] _assignedPartitions = [];
    private int _partitionIndex;
    private IBufferPartition<TItem>? _partitionBeingConsumed;
    private volatile int _pendingDataVersion;
    private readonly PendingDataValueTaskSource<IBufferPartition<TItem>> _pendingDataValueTaskSource = new();
    private readonly ReaderWriterLockSlim _pendingDataLock = new();

    public string TopicName => options.TopicName;

    public string GroupName => options.GroupName;

    public void AssignPartitions(params IBufferPartition<TItem>[] partitions)
    {
        _assignedPartitions = partitions;
        foreach (var partition in partitions)
        {
            partition.RegisterConsumer(this);
        }
    }

    public async IAsyncEnumerable<IEnumerable<TItem>> ConsumeAsync(
        [EnumeratorCancellation] CancellationToken cancellationToken = default)
    {
        if (_assignedPartitions.Length == 0)
        {
            throw new InvalidOperationException("No partition is assigned.");
        }

        while (!cancellationToken.IsCancellationRequested)
        {
            var pendingDataVersion = _pendingDataVersion;
            var partition = SelectPartition();
            var batchSize = options.BatchSize;

            if (TryPull(partition, batchSize, out var items))
            {
                yield return items;
                continue;
            }

            IEnumerable<TItem> itemsFromOtherPartition = null!;
            var hasItemFromOtherPartition = false;

            foreach (var t in _assignedPartitions)
            {
                partition = t;

                if (partition == _partitionBeingConsumed)
                {
                    continue;
                }

                if (TryPull(partition, batchSize, out items))
                {
                    itemsFromOtherPartition = items;
                    hasItemFromOtherPartition = true;
                    break;
                }
            }

            if (hasItemFromOtherPartition)
            {
                yield return itemsFromOtherPartition;
                continue;
            }

            try
            {
                _pendingDataLock.EnterWriteLock();

                if (_pendingDataVersion != pendingDataVersion)
                {
                    continue;
                }

                _pendingDataValueTaskSource.Reset();
            }
            finally
            {
                _pendingDataLock.ExitWriteLock();
            }

            var pendingDataTask = _pendingDataValueTaskSource.ValueTask;
            var partitionWithNewData = pendingDataTask.IsCompletedSuccessfully
                ? pendingDataTask.Result
                : await pendingDataTask.AsTask().WaitAsync(cancellationToken);

            if (TryPull(partitionWithNewData, batchSize, out items))
            {
                yield return items;
            }
        }
    }

    public ValueTask CommitAsync()
    {
        if (options.AutoCommit)
        {
            throw new InvalidOperationException("Auto commit is enabled.");
        }

        var partition = _partitionBeingConsumed ??
                        throw new InvalidOperationException("No partition is in consumption.");

        partition.Commit(options.GroupName);
        _partitionBeingConsumed = null;

        return ValueTask.CompletedTask;
    }

    public void NotifyNewDataAvailable(IBufferPartition<TItem> partition)
    {
        Interlocked.Increment(ref _pendingDataVersion);

        _pendingDataLock.EnterUpgradeableReadLock();
        try
        {
            if (!_pendingDataValueTaskSource.IsWaiting)
            {
                return;
            }

            _pendingDataLock.EnterWriteLock();
            try
            {
                if (!_pendingDataValueTaskSource.IsWaiting)
                {
                    return;
                }

                _pendingDataValueTaskSource.SetResult(partition);
            }
            finally
            {
                _pendingDataLock.ExitWriteLock();
            }
        }
        finally
        {
            _pendingDataLock.ExitUpgradeableReadLock();
        }
    }

    private bool TryPull(IBufferPartition<TItem> partition, int batchSize,
        [NotNullWhen(true)] out IEnumerable<TItem>? items)
    {
        _partitionBeingConsumed = partition;
        var dataAvailable = partition.TryPull(options.GroupName, batchSize, out items);

        if (dataAvailable && options.AutoCommit)
        {
            partition.Commit(options.GroupName);
        }

        return dataAvailable;
    }

    private IBufferPartition<TItem> SelectPartition()
    {
        var partitions = _assignedPartitions;

        if (partitions.Length == 0)
        {
            throw new InvalidOperationException("No partition is assigned.");
        }

        var index = _partitionIndex++ % partitions.Length;
        return partitions[index];
    }
}
