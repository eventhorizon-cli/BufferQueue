using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;

namespace BufferQueue.Memory;

[DebuggerDisplay("PartitionId = {PartitionId}, Capacity = {Capacity}, Count = {Count}")]
[DebuggerTypeProxy(typeof(MemoryBufferPartition<>.DebugView))]
internal sealed class MemoryBufferPartition<T>
    : IBufferPartition<T>
{
    // internal for test
    internal readonly int _segmentSize;

    private volatile MemoryBufferSegment<T> _head;
    private volatile MemoryBufferSegment<T> _tail;

    // At most one consumer per group can consume the same partition at the same time,
    private readonly ConcurrentDictionary<string /* group name */, Reader> _consumerReaders;
    private readonly HashSet<IBufferPartitionConsumer<T>> _consumers;

    private readonly object _appendLock;
    private MemoryBufferPartitionOffset _releasedCapacityPosition;
    private Action<ulong>? _releaseCapacity;

    public MemoryBufferPartition(int id, int segmentSize)
        : this(id, segmentSize, new())
    {
    }

    internal MemoryBufferPartition(int id, int segmentSize, object appendLock)
    {
        ArgumentNullException.ThrowIfNull(appendLock);

        _segmentSize = segmentSize;
        PartitionId = id;
        _head = _tail = new(_segmentSize, default);
        _consumerReaders = new();
        _consumers = [];

        _appendLock = appendLock;
        _releasedCapacityPosition = _head.StartOffset;
    }

    public int PartitionId { get; }

    internal object AppendLock => _appendLock;

    public ulong Capacity => (ulong)(_tail.EndOffset - _head.StartOffset + 1);

    public ulong Count
    {
        get
        {
            var freeCount = (ulong)(_tail.Capacity - _tail.Count);
            return Capacity - freeCount;
        }
    }

    public void RegisterConsumer(IBufferPartitionConsumer<T> consumer)
    {
        lock (_appendLock)
        {
            _consumers.Add(consumer);
            _consumerReaders.TryAdd(consumer.GroupName, CreateReader());
        }
    }

    public void UnregisterConsumer(IBufferPartitionConsumer<T> consumer)
    {
        ulong releasedCount;
        lock (_appendLock)
        {
            _consumers.Remove(consumer);
            _consumerReaders.TryRemove(consumer.GroupName, out _);
            releasedCount = ReleaseCommittedCapacity();
        }

        _releaseCapacity?.Invoke(releasedCount);
    }

    public void Enqueue(T item)
    {
        lock (_appendLock)
        {
            AppendSingleWriter(item);
        }

        NotifyConsumers();
    }

    internal void AppendFromSerializedProducer(T item) => AppendSingleWriter(item);

    internal void SetCapacityReleaseHandler(Action<ulong> releaseCapacity)
    {
        ArgumentNullException.ThrowIfNull(releaseCapacity);

        lock (_appendLock)
        {
            if (_releaseCapacity != null)
            {
                throw new InvalidOperationException("A capacity release handler is already registered.");
            }

            _releaseCapacity = releaseCapacity;
        }
    }

    internal void NotifyConsumers()
    {
        foreach (var consumer in _consumers)
        {
            consumer.NotifyNewDataAvailable(this);
        }
    }

    private void AppendSingleWriter(T item)
    {
        var tail = _tail;
        if (tail.TryEnqueueSingleWriter(item))
        {
            return;
        }

        var newSegmentStartOffset = tail.EndOffset + 1;
        var newSegment = TryRecycleSegment(
            newSegmentStartOffset,
            out var recycledSegment,
            out var recycledHead)
            ? recycledSegment
            : new(_segmentSize, newSegmentStartOffset);
        if (!newSegment.TryEnqueueSingleWriter(item))
        {
            throw new InvalidOperationException("A new memory segment must have space for its first item.");
        }

        tail.NextSegment = newSegment;
        _tail = newSegment;
        if (recycledHead != null)
        {
            _head = recycledHead;
        }
    }

    public bool TryPull(string groupName, int batchSize, [NotNullWhen(true)] out IEnumerable<T>? items)
    {
        if (!_consumerReaders.TryGetValue(groupName, out var reader))
        {
            lock (_appendLock)
            {
                reader = _consumerReaders.GetOrAdd(groupName, _ => CreateReader());
            }
        }

        return reader.TryRead(batchSize, out items);
    }

    public void Commit(string groupName)
    {
        ulong releasedCount;
        lock (_appendLock)
        {
            if (!_consumerReaders.TryGetValue(groupName, out var reader))
            {
                throw new InvalidOperationException("Specified group name not found.");
            }

            reader.MoveNext();
            releasedCount = ReleaseCommittedCapacity();
        }

        _releaseCapacity?.Invoke(releasedCount);
    }

    private bool TryRecycleSegment(
        MemoryBufferPartitionOffset newSegmentStartOffset,
        [NotNullWhen(true)] out MemoryBufferSegment<T>? recycledSegment,
        out MemoryBufferSegment<T>? recycledHead)
    {
        recycledSegment = null;
        recycledHead = null;

        if (_head == _tail)
        {
            return false;
        }

        var minConsumerReadPosition = MinConsumerReadPosition();

        MemoryBufferSegment<T>? recyclableSegment = null;
        for (var segment = _head; segment != _tail; segment = segment.NextSegment!)
        {
            var wholeSegmentConsumed = segment.EndOffset < minConsumerReadPosition;
            if (wholeSegmentConsumed)
            {
                recyclableSegment = segment;
            }
        }

        if (recyclableSegment == null)
        {
            return false;
        }

        recycledSegment = recyclableSegment.RecycleSlots(newSegmentStartOffset);
        recycledHead = recyclableSegment.NextSegment!;

        return true;
    }

    private Reader CreateReader()
    {
        var head = _head;
        var startPosition = _releasedCapacityPosition > head.StartOffset
            ? _releasedCapacityPosition
            : head.StartOffset;
        return new(head, startPosition);
    }

    private ulong ReleaseCommittedCapacity()
    {
        if (_releaseCapacity == null || _consumerReaders.IsEmpty)
        {
            return 0;
        }

        var minConsumerReadPosition = MinConsumerReadPosition();
        if (!(minConsumerReadPosition > _releasedCapacityPosition))
        {
            return 0;
        }

        var releasedCount = (minConsumerReadPosition - _releasedCapacityPosition).ToUInt64();
        _releasedCapacityPosition = minConsumerReadPosition;
        return releasedCount;
    }

    private MemoryBufferPartitionOffset MinConsumerReadPosition()
    {
        MemoryBufferPartitionOffset? minReadPosition = null;
        foreach (var reader in _consumerReaders.Values)
        {
            var readPosition = reader.ReadPosition;

            if (minReadPosition == null)
            {
                minReadPosition = readPosition;
                continue;
            }

            if (readPosition < minReadPosition)
            {
                minReadPosition = readPosition;
            }
        }

        return minReadPosition ?? _head.StartOffset;
    }

    // One reader can only be used by one consumer at the same time.
    private sealed class Reader(MemoryBufferSegment<T> currentSegment, MemoryBufferPartitionOffset currentOffset)
    {
        private MemoryBufferSegment<T> _currentSegment = currentSegment;
        private MemoryBufferPartitionOffset _readPosition = currentOffset;
        private int _lastReadCount;

        public MemoryBufferPartitionOffset ReadPosition => _readPosition;

        public bool TryRead(int batchSize, [NotNullWhen(true)] out IEnumerable<T>? items)
        {
            var remainingCount = batchSize;
            var readPosition = _readPosition;
            var currentSegment = _currentSegment;
            T[]? result = null;
            T singleItem = default!;
            var copiedCount = 0;

            while (true)
            {
                while (currentSegment.EndOffset < readPosition && currentSegment.NextSegment != null)
                {
                    currentSegment = currentSegment.NextSegment;
                }

                var retrievalSuccess = currentSegment.TryGet(readPosition, remainingCount, out var segmentItems);
                if (retrievalSuccess)
                {
                    var length = segmentItems.Count;
                    readPosition += (ulong)length;
                    remainingCount -= length;

                    if (batchSize == 1)
                    {
                        singleItem = segmentItems.Array![segmentItems.Offset];
                        copiedCount = 1;
                    }
                    else
                    {
                        result ??= new T[batchSize];
                        Array.Copy(segmentItems.Array!, segmentItems.Offset, result, copiedCount, length);
                        copiedCount += length;
                    }
                }
                else
                {
                    break;
                }

                if (remainingCount == 0)
                {
                    break;
                }

                var nextSegment = currentSegment.NextSegment;
                var continueReading = nextSegment != null;
                if (continueReading)
                {
                    currentSegment = nextSegment!;
                }
                else
                {
                    break;
                }
            }

            if (remainingCount == batchSize)
            {
                items = null;
                return false;
            }

            _lastReadCount = batchSize - remainingCount;
            if (batchSize == 1)
            {
                items = new SingleItemBatch<T>(singleItem);
                return true;
            }

            items = _lastReadCount == result!.Length
                ? result
                : new SnapshotBatch<T>(result, _lastReadCount);
            return true;
        }

        public void MoveNext()
        {
            _readPosition += (ulong)_lastReadCount;
            while (_currentSegment.EndOffset < _readPosition && _currentSegment.NextSegment != null)
            {
                _currentSegment = _currentSegment.NextSegment!;
            }
        }
    }

    private sealed class SingleItemBatch<TItem>(TItem item) : IReadOnlyList<TItem>, ICollection<TItem>
    {
        public int Count => 1;

        public bool IsReadOnly => true;

        public TItem this[int index] => index == 0
            ? item
            : throw new ArgumentOutOfRangeException(nameof(index));

        public IEnumerator<TItem> GetEnumerator()
        {
            yield return item;
        }

        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        public bool Contains(TItem itemToFind) => EqualityComparer<TItem>.Default.Equals(item, itemToFind);

        public void CopyTo(TItem[] array, int arrayIndex) => array[arrayIndex] = item;

        public void Add(TItem itemToAdd) => throw new NotSupportedException();

        public void Clear() => throw new NotSupportedException();

        public bool Remove(TItem itemToRemove) => throw new NotSupportedException();
    }

    private sealed class SnapshotBatch<TItem>(TItem[] items, int count) : IReadOnlyList<TItem>, ICollection<TItem>
    {
        public int Count => count;

        public bool IsReadOnly => true;

        public TItem this[int index]
        {
            get
            {
                if ((uint)index >= (uint)count)
                {
                    throw new ArgumentOutOfRangeException(nameof(index));
                }

                return items[index];
            }
        }

        public IEnumerator<TItem> GetEnumerator()
        {
            for (var i = 0; i < count; i++)
            {
                yield return items[i];
            }
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() => GetEnumerator();

        public bool Contains(TItem item) => Array.IndexOf(items, item, 0, count) >= 0;

        public void CopyTo(TItem[] array, int arrayIndex) => Array.Copy(items, 0, array, arrayIndex, count);

        public void Add(TItem item) => throw new NotSupportedException();

        public void Clear() => throw new NotSupportedException();

        public bool Remove(TItem item) => throw new NotSupportedException();
    }

    private class DebugView(MemoryBufferPartition<T> partition)
    {
        public int PartitionId => partition.PartitionId;

        public ulong Capacity => partition.Capacity;

        public ulong Count => partition.Count;

        public HashSet<IBufferPartitionConsumer<T>> Consumers => partition._consumers;

        public ConcurrentDictionary<string, Reader> ConsumerReaders => partition._consumerReaders;

        public MemoryBufferSegment<T>[] Segments
        {
            get
            {
                List<MemoryBufferSegment<T>> segments = [];
                for (var segment = partition._head; segment != null; segment = segment.NextSegment)
                {
                    segments.Add(segment);
                }

                return segments.ToArray();
            }
        }
    }
}
