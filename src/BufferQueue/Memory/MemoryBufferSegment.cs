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
    private readonly bool[] _slotWritten;
    private int _lastReservedPosition;
    private int _lastPublishedPosition;

    public MemoryBufferSegment(int length, MemoryBufferPartitionOffset startOffset)
    {
        _startOffset = startOffset;
        _endOffset = startOffset + (ulong)(length - 1);
        _slots = new T[length];
        _slotWritten = new bool[length];
        _lastReservedPosition = -1;
        _lastPublishedPosition = -1;
    }

    private MemoryBufferSegment(
        T[] slots,
        bool[] slotWritten,
        MemoryBufferPartitionOffset startOffset)
    {
        _startOffset = startOffset;
        _endOffset = startOffset + (ulong)(slots.Length - 1);
        _slots = slots;
        _slotWritten = slotWritten;
        _lastReservedPosition = -1;
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

    public bool TryEnqueue(T item)
    {
        while (true)
        {
            var lastReservedPosition = Volatile.Read(ref _lastReservedPosition);
            var nextPosition = lastReservedPosition + 1;
            if (nextPosition >= _slots.Length)
            {
                // No more space to write in this segment.
                return false;
            }

            if (Interlocked.CompareExchange(
                    ref _lastReservedPosition,
                    nextPosition,
                    lastReservedPosition)
                != lastReservedPosition)
            {
                // Another thread has already written to the next position, retry.
                continue;
            }

            // Write the item to the slot.
            // It's safe to write directly without locks because each position is written by at most one thread.
            _slots[nextPosition] = item;

            Volatile.Write(ref _slotWritten[nextPosition], true);
            WaitUntilPositionPublished(nextPosition);

            return true;
        }
    }

    internal void WaitUntilAllSlotsPublished() => WaitUntilPositionPublished(_slots.Length - 1);

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
        Array.Clear(_slotWritten, 0, _slotWritten.Length);
        return new(_slots, _slotWritten, startOffset);
    }

    private void WaitUntilPositionPublished(int targetPosition)
    {
        var spinWait = new SpinWait();
        while (Volatile.Read(ref _lastPublishedPosition) < targetPosition)
        {
            if (TryAdvancePublishedPosition())
            {
                spinWait.Reset();
                continue;
            }

            spinWait.SpinOnce();
        }
    }

    private bool TryAdvancePublishedPosition()
    {
        // Any writer can publish the longest gap-free sequence of written slots.
        while (true)
        {
            var lastPublishedPosition = Volatile.Read(ref _lastPublishedPosition);
            var nextPosition = lastPublishedPosition + 1;
            var candidatePublishedPosition = lastPublishedPosition;
            while (nextPosition < _slotWritten.Length &&
                   Volatile.Read(ref _slotWritten[nextPosition]))
            {
                candidatePublishedPosition = nextPosition;
                nextPosition++;
            }

            if (candidatePublishedPosition == lastPublishedPosition)
            {
                return false;
            }

            if (Interlocked.CompareExchange(
                    ref _lastPublishedPosition,
                    candidatePublishedPosition,
                    lastPublishedPosition)
                == lastPublishedPosition)
            {
                return true;
            }
        }
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
