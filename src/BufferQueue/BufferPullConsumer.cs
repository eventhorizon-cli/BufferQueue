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
        _partitionIndex = 0;
        _assignedPartitions = partitions;
        var registeredPartitionCount = 0;
        try
        {
            foreach (var partition in partitions)
            {
                partition.RegisterConsumer(this);
                registeredPartitionCount++;
            }
        }
        catch
        {
            _assignedPartitions = [];
            for (var i = 0; i < registeredPartitionCount; i++)
            {
                partitions[i].UnregisterConsumer(this);
            }

            throw;
        }
    }

    public void UnassignPartitions()
    {
        var partitions = _assignedPartitions;
        _assignedPartitions = [];
        foreach (var partition in partitions)
        {
            partition.UnregisterConsumer(this);
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
            if (TryPullNext(out var items))
            {
                yield return items;
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
            // Notifications only wake the reader; resume the ring without favoring the sender.
            if (pendingDataTask.IsCompletedSuccessfully)
            {
                _ = pendingDataTask.Result;
            }
            else
            {
                await pendingDataTask.AsTask().WaitAsync(cancellationToken);
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

    private bool TryPullNext([NotNullWhen(true)] out IEnumerable<TItem>? items)
    {
        var partitions = _assignedPartitions;
        if (partitions.Length == 0)
        {
            throw new InvalidOperationException("No partition is assigned.");
        }

        var index = _partitionIndex;
        for (var remaining = partitions.Length; remaining > 0; remaining--)
        {
            var partition = partitions[index];
            // Keep the cursor bounded instead of allowing a monotonically increasing int to wrap.
            index = index + 1 == partitions.Length ? 0 : index + 1;
            if (TryPull(partition, options.BatchSize, out items))
            {
                _partitionIndex = index;
                return true;
            }
        }

        items = null;
        return false;
    }
}
