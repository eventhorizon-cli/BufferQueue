// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;
using System.Diagnostics;
using System.Linq;
using System.Threading;

namespace BufferQueue.Memory;

[DebuggerDisplay("StartOffset = {StartOffset}, EndOffset = {EndOffset}")]
[DebuggerTypeProxy(typeof(MemoryBufferSegment<>.DebugView))]
internal sealed class MemoryBufferSegment<T>
{
    private readonly MemoryBufferPartitionOffset _startOffset;
    private readonly MemoryBufferPartitionOffset _endOffset;
    private readonly T[] _slots;
    private int _lastPublishedPosition;

    public MemoryBufferSegment(int length, MemoryBufferPartitionOffset startOffset)
    {
        _startOffset = startOffset;
        _endOffset = startOffset + (ulong)(length - 1);
        _slots = new T[length];
        _lastPublishedPosition = -1;
    }

    private MemoryBufferSegment(T[] slots, MemoryBufferPartitionOffset startOffset)
    {
        _startOffset = startOffset;
        _endOffset = startOffset + (ulong)(slots.Length - 1);
        _slots = slots;
        _lastPublishedPosition = -1;
    }

    public MemoryBufferSegment<T>? NextSegment
    {
        get => Volatile.Read(ref field);
        set => Volatile.Write(ref field, value);
    }

    public MemoryBufferPartitionOffset StartOffset => _startOffset;

    public MemoryBufferPartitionOffset EndOffset => _endOffset;

    public int Capacity => _slots.Length;

    public int Count => Math.Min(Capacity, Volatile.Read(ref _lastPublishedPosition) + 1);

    public bool TryEnqueueSingleWriter(T item)
    {
        var nextPosition = _lastPublishedPosition + 1;
        if (nextPosition >= _slots.Length)
        {
            return false;
        }

        // Partition append serialization guarantees a single writer. Publish only after storing the item.
        _slots[nextPosition] = item;
        Volatile.Write(ref _lastPublishedPosition, nextPosition);
        return true;
    }

    public bool TryGet(MemoryBufferPartitionOffset offset, int count, out ArraySegment<T> items)
    {
        if (offset < _startOffset || offset > _endOffset)
        {
            items = default;
            return false;
        }

        var readPosition = (offset - _startOffset).ToInt32();

        var lastPublishedPosition = Volatile.Read(ref _lastPublishedPosition);
        if (lastPublishedPosition < 0 || readPosition > lastPublishedPosition)
        {
            items = default;
            return false;
        }

        var lastReadablePosition = Math.Min(lastPublishedPosition, _slots.Length - 1);
        // Number of items actually available to return (bounded by requested count and written items).
        var availableCount = Math.Min(count, lastReadablePosition - readPosition + 1);
        items = new(_slots, readPosition, availableCount);
        return true;
    }

    public MemoryBufferSegment<T> RecycleSlots(MemoryBufferPartitionOffset startOffset)
    {
        Array.Clear(_slots, 0, _slots.Length);
        return new(_slots, startOffset);
    }

    private class DebugView(MemoryBufferSegment<T> segment)
    {
        public MemoryBufferPartitionOffset StartOffset => segment._startOffset;

        public MemoryBufferPartitionOffset EndOffset => segment._endOffset;

        public int Capacity => segment.Capacity;

        public int Count => segment.Count;

        public T[] Items => segment._slots
            .Take(Volatile.Read(ref segment._lastPublishedPosition) + 1)
            .ToArray();
    }
}
