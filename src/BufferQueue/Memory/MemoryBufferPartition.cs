using System;
using System.Collections;
using System.Collections.Frozen;
using System.Collections.Generic;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Threading;

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

    private readonly ReaderRegistry _readers = new();
    // The append lock serializes replacements. Published sets are never mutated, so notifications
    // can enumerate an older set without locking or copying it.
    private HashSet<IBufferPartitionConsumer<T>> _consumers;

    private readonly object _appendLock;
    private readonly object? _appendCoordinator;
    private MemoryBufferPartitionOffset _releasedCapacityPosition;
    private Action<ulong>? _releaseCapacity;

    public MemoryBufferPartition(int id, int segmentSize)
        : this(id, segmentSize, new(), null)
    {
    }

    internal MemoryBufferPartition(int id, int segmentSize, object appendLock)
        : this(id, segmentSize, appendLock, null)
    {
    }

    internal MemoryBufferPartition(
        int id,
        int segmentSize,
        object appendLock,
        object? appendCoordinator)
    {
        ArgumentNullException.ThrowIfNull(appendLock);

        _segmentSize = segmentSize;
        PartitionId = id;
        _head = _tail = new(_segmentSize, default);
        _consumers = [];

        _appendLock = appendLock;
        _appendCoordinator = appendCoordinator;
        _releasedCapacityPosition = _head.StartOffset;
    }

    public int PartitionId { get; }

    internal object AppendLock => _appendLock;

    internal object? AppendCoordinator => _appendCoordinator;

    public ulong Capacity => _tail.EndOffset - _head.StartOffset + 1;

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
        EnterAppendLock();
        try
        {
            var consumers = _consumers;
            if (!consumers.Contains(consumer))
            {
                consumers = new(consumers) { consumer };
            }

            // Initialize the reader before making the consumer visible to notifications.
            GetOrCreateReader(consumer.GroupName);
            Volatile.Write(ref _consumers, consumers);
        }
        finally
        {
            ExitAppendLock();
        }
    }

    public void UnregisterConsumer(IBufferPartitionConsumer<T> consumer)
    {
        ulong releasedCount;
        EnterAppendLock();
        try
        {
            var consumers = _consumers;
            if (consumers.Contains(consumer))
            {
                consumers = new(consumers);
                consumers.Remove(consumer);
            }

            _readers.Remove(consumer.GroupName);
            // Notifications already using the old set may still call this consumer.
            Volatile.Write(ref _consumers, consumers);
            releasedCount = ReleaseCommittedCapacity();
        }
        finally
        {
            ExitAppendLock();
        }

        _releaseCapacity?.Invoke(releasedCount);
    }

    public void Enqueue(T item)
    {
        EnterAppendLock();
        try
        {
            AppendSingleWriter(item);
        }
        finally
        {
            ExitAppendLock();
        }

        NotifyConsumers();
    }

    internal void AppendFromSerializedProducer(T item) => AppendSingleWriter(item);

    internal void SetCapacityReleaseHandler(Action<ulong> releaseCapacity)
    {
        ArgumentNullException.ThrowIfNull(releaseCapacity);

        EnterAppendLock();
        try
        {
            if (_releaseCapacity != null)
            {
                throw new InvalidOperationException("A capacity release handler is already registered.");
            }

            _releaseCapacity = releaseCapacity;
        }
        finally
        {
            ExitAppendLock();
        }
    }

    internal void NotifyConsumers()
    {
        // A consumer missing from this snapshot checks existing data before it starts waiting.
        foreach (var consumer in Volatile.Read(ref _consumers))
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
        if (!_readers.TryGet(groupName, out var reader))
        {
            EnterAppendLock();
            try
            {
                reader = GetOrCreateReader(groupName);
            }
            finally
            {
                ExitAppendLock();
            }
        }

        return reader.TryRead(batchSize, out items);
    }

    public void Commit(string groupName)
    {
        ulong releasedCount;
        EnterAppendLock();
        try
        {
            if (!_readers.TryGet(groupName, out var reader))
            {
                throw new InvalidOperationException("Specified group name not found.");
            }

            reader.MoveNext();
            releasedCount = ReleaseCommittedCapacity();
        }
        finally
        {
            ExitAppendLock();
        }

        _releaseCapacity?.Invoke(releasedCount);
    }

    private void EnterAppendLock()
    {
        if (_appendCoordinator is { } appendCoordinator)
        {
            System.Threading.Monitor.Enter(appendCoordinator);
        }

        System.Threading.Monitor.Enter(_appendLock);
    }

    private void ExitAppendLock()
    {
        System.Threading.Monitor.Exit(_appendLock);
        if (_appendCoordinator is { } appendCoordinator)
        {
            System.Threading.Monitor.Exit(appendCoordinator);
        }
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

    private Reader GetOrCreateReader(string groupName)
    {
        Debug.Assert(Monitor.IsEntered(_appendLock));
        if (_readers.TryGet(groupName, out var reader))
        {
            return reader;
        }

        reader = CreateReader();
        _readers.Add(groupName, reader);
        return reader;
    }

    private Reader CreateReader()
    {
        var head = _head;
        // An unbounded partition never advances the release cursor; it may be outside the current live window.
        var startPosition = _releaseCapacity != null && _releasedCapacityPosition > head.StartOffset
            ? _releasedCapacityPosition
            : head.StartOffset;
        return new(head, startPosition);
    }

    private ulong ReleaseCommittedCapacity()
    {
        if (_releaseCapacity == null || _readers.Count == 0)
        {
            return 0;
        }

        var minConsumerReadPosition = MinConsumerReadPosition();
        if (!(minConsumerReadPosition > _releasedCapacityPosition))
        {
            return 0;
        }

        var releasedCount = minConsumerReadPosition - _releasedCapacityPosition;
        _releasedCapacityPosition = minConsumerReadPosition;
        return releasedCount;
    }

    private MemoryBufferPartitionOffset MinConsumerReadPosition()
    {
        Debug.Assert(Monitor.IsEntered(_appendLock) ||
                     _appendCoordinator != null && Monitor.IsEntered(_appendCoordinator));
        return _readers.Count == 0 ? _head.StartOffset : _readers.GetMinimumReadPosition();
    }

    // Mutations and progress scans require the partition's append synchronization; lookups may run concurrently.
    private sealed class ReaderRegistry
    {
        // Only registration changes replace this snapshot. Published lookup tables are never mutated.
        private FrozenDictionary<string, Reader> _byGroup = FrozenDictionary<string, Reader>.Empty;

        public int Count => _byGroup.Count;

        public FrozenDictionary<string, Reader> ReadersForDebugger => Volatile.Read(ref _byGroup);

        public bool TryGet(string groupName, [NotNullWhen(true)] out Reader? reader)
            => Volatile.Read(ref _byGroup).TryGetValue(groupName, out reader);

        public void Add(string groupName, Reader reader)
        {
            var updated = new Dictionary<string, Reader>(_byGroup, StringComparer.Ordinal)
            {
                [groupName] = reader
            };
            Volatile.Write(ref _byGroup, updated.ToFrozenDictionary(StringComparer.Ordinal));
        }

        public void Remove(string groupName)
        {
            var updated = new Dictionary<string, Reader>(_byGroup, StringComparer.Ordinal);
            if (updated.Remove(groupName))
            {
                Volatile.Write(ref _byGroup, updated.ToFrozenDictionary(StringComparer.Ordinal));
            }
        }

        public MemoryBufferPartitionOffset GetMinimumReadPosition()
        {
            var readers = _byGroup.Values;
            Debug.Assert(readers.Length > 0);
            var minimum = readers[0].ReadPosition;
            for (var i = 1; i < readers.Length; i++)
            {
                var position = readers[i].ReadPosition;
                if (position < minimum)
                {
                    minimum = position;
                }
            }

            return minimum;
        }
    }

    // One reader can only be used by one consumer at the same time.
    private sealed class Reader(MemoryBufferSegment<T> currentSegment, MemoryBufferPartitionOffset currentOffset)
    {
        private MemoryBufferSegment<T> _currentSegment = currentSegment;
        private MemoryBufferPartitionOffset _readPosition = currentOffset;
        private int _lastReadCount;

        public MemoryBufferPartitionOffset ReadPosition => _readPosition;

        public bool TryRead(int batchSize, [NotNullWhen(true)] out IEnumerable<T>? items)
            => batchSize == 1 ? TryReadSingle(out items) : TryReadBatch(batchSize, out items);

        private bool TryReadSingle([NotNullWhen(true)] out IEnumerable<T>? items)
        {
            var readPosition = _readPosition;
            var currentSegment = _currentSegment;
            while (currentSegment.EndOffset < readPosition && currentSegment.NextSegment is { } nextSegment)
            {
                currentSegment = nextSegment;
            }

            if (!currentSegment.TryGet(readPosition, 1, out var segmentItems))
            {
                items = null;
                return false;
            }

            // Copy before committing: the producer may recycle this slot as soon as progress advances.
            items = new SingleItemBatch<T>(segmentItems.Array![segmentItems.Offset]);
            _lastReadCount = 1;
            return true;
        }

        private bool TryReadBatch(int batchSize, [NotNullWhen(true)] out IEnumerable<T>? items)
        {
            var remainingCount = batchSize;
            var readPosition = _readPosition;
            var currentSegment = _currentSegment;
            T[]? result = null;
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

                    result ??= new T[batchSize];
                    Array.Copy(segmentItems.Array!, segmentItems.Offset, result, copiedCount, length);
                    copiedCount += length;
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

        public HashSet<IBufferPartitionConsumer<T>> Consumers => Volatile.Read(ref partition._consumers);

        public FrozenDictionary<string, Reader> ConsumerReaders => partition._readers.ReadersForDebugger;

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
