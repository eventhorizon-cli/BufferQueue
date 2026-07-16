// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;
using System.Buffers.Binary;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.IO.MemoryMappedFiles;
using System.Text;
using System.Threading;

namespace BufferQueue.MemoryMappedFile;

internal sealed class MemoryMappedFileBufferPartition<T>
    : IBufferPartition<T>, IDisposable
    where T : notnull
{
    internal const int MaxRecordOverhead = sizeof(int) + sizeof(byte);
    private const byte RecordEndMarker = 0xBB;
    private const int SegmentEndMarker = -1;

    private readonly MemoryMappedFileBufferQueueOptions<T> _options;
    private readonly IMemoryMappedFileSerializer<T> _serializer;
    private readonly IMemoryMappedFileFlusher _flusher;
    private readonly MemoryMappedFileFlushStrategy _flushStrategy;
    private readonly int _flushBatchSize;
    private readonly string _partitionDirectory;
    private readonly string _offsetDirectory;
    private readonly OffsetCheckpoint _writerOffsetCheckpoint;
    private readonly object _appendLock;
    private readonly object _segmentsLock;
    private readonly object _consumersLock;
    private readonly ConcurrentDictionary<string, Reader> _consumerReaders;
    private readonly Dictionary<long, Segment> _segments;
    private readonly HashSet<Segment> _dirtySegments;
    private readonly HashSet<IBufferPartitionConsumer<T>> _consumers;
    private long _checkpointedWriteOffset;
    private long _writeOffset;
    private int _pendingRecordCount;
    private bool _disposed;

    public MemoryMappedFileBufferPartition(int id, MemoryMappedFileBufferQueueOptions<T> options)
        : this(id, options, MemoryMappedFileFlusher.Instance)
    {
    }

    internal MemoryMappedFileBufferPartition(
        int id,
        MemoryMappedFileBufferQueueOptions<T> options,
        IMemoryMappedFileFlusher flusher)
    {
        PartitionId = id;
        _options = options;
        _serializer = options.Serializer;
        _flusher = flusher;
        _flushStrategy = options.FlushStrategy;
        _flushBatchSize = options.FlushBatchSize;
        _partitionDirectory = Path.Combine(options.DataDirectory, options.TopicName!, $"partition-{id:D5}");
        _offsetDirectory = Path.Combine(_partitionDirectory, "offsets");
        _writerOffsetCheckpoint = new OffsetCheckpoint(Path.Combine(_partitionDirectory, "writer.offset"));
        Directory.CreateDirectory(_partitionDirectory);
        Directory.CreateDirectory(_offsetDirectory);
        _appendLock = new();
        _segmentsLock = new();
        _consumersLock = new();
        _consumerReaders = new();
        _segments = [];
        _dirtySegments = [];
        _consumers = [];
        try
        {
            _writeOffset = FindWriteOffset();
            MarkRecoveredTailDirty();
        }
        catch
        {
            DisposeSegments();
            throw;
        }
    }

    public int PartitionId { get; }

    public void Dispose()
    {
        lock (_appendLock)
        {
            if (_disposed)
            {
                return;
            }

            DisposeSegments();
            _consumerReaders.Clear();
            lock (_consumersLock)
            {
                _consumers.Clear();
            }

            _disposed = true;
        }
    }

    public void RegisterConsumer(IBufferPartitionConsumer<T> consumer)
    {
        ThrowIfDisposed();

        lock (_consumersLock)
        {
            _consumers.Add(consumer);
        }
    }

    public void Enqueue(T item)
    {
        ThrowIfDisposed();

        var payload = _serializer.Serialize(item);
        if (payload is null)
        {
            throw new InvalidOperationException(
                $"The memory-mapped-file serializer '{_serializer.GetType().FullName}' returned a null payload.");
        }

        var recordSize = MaxRecordOverhead + payload.Length;
        if (recordSize > _options.SegmentSize)
        {
            throw new InvalidOperationException("The serialized item is larger than the memory-mapped file segment size.");
        }

        lock (_appendLock)
        {
            var offset = _writeOffset;
            var position = PositionInSegment(offset);
            if (position + recordSize > _options.SegmentSize)
            {
                WriteSegmentEnd(offset);
                offset = NextSegmentOffset(offset);
                FlushPendingWrites(offset);
                Volatile.Write(ref _writeOffset, offset);
            }

            var segment = GetSegment(SegmentIndex(offset));
            position = PositionInSegment(offset);

            Span<byte> lengthBytes = stackalloc byte[sizeof(int)];
            BinaryPrimitives.WriteInt32LittleEndian(lengthBytes, payload.Length);
            segment.Accessor.WriteArray(position, lengthBytes.ToArray(), 0, lengthBytes.Length);
            segment.Accessor.WriteArray(position + sizeof(int), payload, 0, payload.Length);
            segment.Accessor.Write(position + sizeof(int) + payload.Length, RecordEndMarker);
            _dirtySegments.Add(segment);

            var nextWriteOffset = offset + recordSize;
            _pendingRecordCount++;

            if (ShouldFlushAfterAppend() || PositionInSegment(nextWriteOffset) == 0)
            {
                FlushPendingWrites(nextWriteOffset);
            }

            Volatile.Write(ref _writeOffset, nextWriteOffset);
        }

        IBufferPartitionConsumer<T>[] consumers;
        lock (_consumersLock)
        {
            consumers = [.. _consumers];
        }

        foreach (var consumer in consumers)
        {
            consumer.NotifyNewDataAvailable(this);
        }
    }

    public bool TryPull(string groupName, int batchSize, [NotNullWhen(true)] out IEnumerable<T>? items)
    {
        ThrowIfDisposed();

        var reader = _consumerReaders.GetOrAdd(groupName, _ => new Reader(this, groupName));
        return reader.TryRead(batchSize, out items);
    }

    public void Commit(string groupName)
    {
        ThrowIfDisposed();

        if (!_consumerReaders.TryGetValue(groupName, out var reader))
        {
            throw new InvalidOperationException("Specified group name not found.");
        }

        lock (_appendLock)
        {
            FlushPendingWrites(Volatile.Read(ref _writeOffset));
            WriteCommittedOffset(groupName, reader.NextReadOffset);
            reader.MoveNext();
        }
    }

    private bool TryRead(long offset, [NotNullWhen(true)] out T? item, out long nextOffset)
    {
        item = default;
        nextOffset = offset;

        var writeOffset = Volatile.Read(ref _writeOffset);
        if (offset >= writeOffset)
        {
            return false;
        }

        var segment = GetSegment(SegmentIndex(offset));
        var position = PositionInSegment(offset);
        if (position + MaxRecordOverhead > _options.SegmentSize)
        {
            nextOffset = NextSegmentOffset(offset);
            return TryRead(nextOffset, out item, out nextOffset);
        }

        var length = segment.Accessor.ReadInt32(position);
        if (length == SegmentEndMarker)
        {
            nextOffset = NextSegmentOffset(offset);
            return TryRead(nextOffset, out item, out nextOffset);
        }

        if (length <= 0)
        {
            return false;
        }

        var recordSize = MaxRecordOverhead + length;
        if (position + recordSize > _options.SegmentSize || offset + recordSize > writeOffset)
        {
            return false;
        }

        var recordEndMarker = segment.Accessor.ReadByte(position + sizeof(int) + length);
        if (recordEndMarker != RecordEndMarker)
        {
            return false;
        }

        var payload = new byte[length];
        segment.Accessor.ReadArray(position + sizeof(int), payload, 0, payload.Length);
        var deserializedItem = _serializer.Deserialize(payload);
        if (deserializedItem is null)
        {
            throw new InvalidDataException(
                $"The memory-mapped-file serializer '{_serializer.GetType().FullName}' deserialized a record to null.");
        }

        item = deserializedItem;
        nextOffset = offset + recordSize;
        return true;
    }

    private long FindWriteOffset()
    {
        var storedWriterOffset = ReadWriterOffset();
        if (!storedWriterOffset.HasValue)
        {
            _checkpointedWriteOffset = 0;
            return FindWriteOffset(0);
        }

        _checkpointedWriteOffset = storedWriterOffset.Value;

        return IsRecordBoundary(storedWriterOffset.Value)
            ? FindWriteOffset(storedWriterOffset.Value)
            : throw new InvalidDataException(
                $"The stored writer offset {storedWriterOffset.Value} is not aligned to a record boundary.");
    }

    private long FindWriteOffset(long startOffset)
    {
        var offset = startOffset;

        while (true)
        {
            var segment = GetSegment(SegmentIndex(offset));
            var position = PositionInSegment(offset);
            if (position >= _options.SegmentSize)
            {
                offset = NextSegmentOffset(offset);
                continue;
            }

            if (position + MaxRecordOverhead > _options.SegmentSize)
            {
                return offset;
            }

            var length = segment.Accessor.ReadInt32(position);
            if (length == SegmentEndMarker)
            {
                offset = NextSegmentOffset(offset);
                continue;
            }

            if (length <= 0)
            {
                return offset;
            }

            var recordSize = MaxRecordOverhead + length;
            if (position + recordSize > _options.SegmentSize)
            {
                return offset;
            }

            var recordEndMarker = segment.Accessor.ReadByte(position + sizeof(int) + length);
            if (recordEndMarker != RecordEndMarker)
            {
                return offset;
            }

            offset += recordSize;
        }
    }

    private bool IsRecordBoundary(long candidateOffset)
    {
        if (candidateOffset == 0)
        {
            return true;
        }

        var offset = 0L;
        while (offset < candidateOffset)
        {
            var segment = GetSegment(SegmentIndex(offset));
            var position = PositionInSegment(offset);
            if (position + MaxRecordOverhead > _options.SegmentSize)
            {
                offset = NextSegmentOffset(offset);
                continue;
            }

            var length = segment.Accessor.ReadInt32(position);
            if (length == SegmentEndMarker)
            {
                offset = NextSegmentOffset(offset);
                continue;
            }

            if (length <= 0)
            {
                return false;
            }

            var recordSize = MaxRecordOverhead + length;
            if (position + recordSize > _options.SegmentSize)
            {
                return false;
            }

            var recordEndMarker = segment.Accessor.ReadByte(position + sizeof(int) + length);
            if (recordEndMarker != RecordEndMarker)
            {
                return false;
            }

            offset += recordSize;
        }

        return offset == candidateOffset;
    }

    private void WriteSegmentEnd(long offset)
    {
        var segment = GetSegment(SegmentIndex(offset));
        var position = PositionInSegment(offset);
        if (position + sizeof(int) > _options.SegmentSize)
        {
            return;
        }

        segment.Accessor.Write(position, SegmentEndMarker);
        _dirtySegments.Add(segment);
    }

    private bool ShouldFlushAfterAppend() =>
        _flushStrategy switch
        {
            MemoryMappedFileFlushStrategy.Immediate => true,
            MemoryMappedFileFlushStrategy.Batch => _pendingRecordCount >= _flushBatchSize,
            _ => throw new InvalidOperationException($"Unsupported flush strategy '{_flushStrategy}'.")
        };

    private void FlushPendingWrites(long writeOffset)
    {
        foreach (var segment in _dirtySegments)
        {
            _flusher.Flush(segment.Accessor);
        }

        if (_checkpointedWriteOffset != writeOffset)
        {
            _writerOffsetCheckpoint.Write(writeOffset);
        }

        _dirtySegments.Clear();
        _pendingRecordCount = 0;
        _checkpointedWriteOffset = writeOffset;
    }

    private void MarkRecoveredTailDirty()
    {
        if (_writeOffset <= _checkpointedWriteOffset)
        {
            return;
        }

        var firstSegmentIndex = SegmentIndex(_checkpointedWriteOffset);
        var lastSegmentIndex = SegmentIndex(_writeOffset - 1);
        for (var segmentIndex = firstSegmentIndex; segmentIndex <= lastSegmentIndex; segmentIndex++)
        {
            _dirtySegments.Add(GetSegment(segmentIndex));
        }
    }

    private long? ReadWriterOffset()
    {
        if (!_writerOffsetCheckpoint.TryRead(out var offset))
        {
            return null;
        }

        var lastExistingSegmentIndex = LastExistingSegmentIndex();
        if (lastExistingSegmentIndex < 0)
        {
            return offset == 0
                ? 0
                : throw new InvalidDataException(
                    $"The stored writer offset {offset} points to a log that does not exist.");
        }

        var maxExistingOffset = (lastExistingSegmentIndex + 1) * _options.SegmentSize;
        return offset <= maxExistingOffset
            ? offset
            : throw new InvalidDataException(
                $"The stored writer offset {offset} is greater than the memory-mapped-file log size {maxExistingOffset}.");
    }

    private long ReadCommittedOffset(string groupName)
    {
        var offset = GetConsumerOffsetCheckpoint(groupName).ReadOrDefault();
        var writeOffset = Volatile.Read(ref _writeOffset);
        return offset <= writeOffset
            ? offset
            : throw new InvalidDataException(
                $"The committed offset {offset} for consumer group '{groupName}' is greater than the writer offset {writeOffset}.");
    }

    private void WriteCommittedOffset(string groupName, long offset)
    {
        GetConsumerOffsetCheckpoint(groupName).Write(offset);
    }

    private OffsetCheckpoint GetConsumerOffsetCheckpoint(string groupName) =>
        new(Path.Combine(_offsetDirectory, EscapePathComponent(groupName), "offset"));

    private long LastExistingSegmentIndex()
    {
        var maxIndex = -1L;
        foreach (var filePath in Directory.EnumerateFiles(_partitionDirectory, "*.log"))
        {
            var fileName = Path.GetFileNameWithoutExtension(filePath);
            if (!long.TryParse(fileName, out var index))
            {
                continue;
            }

            if (index > maxIndex)
            {
                maxIndex = index;
            }
        }

        return maxIndex;
    }

    private static string EscapePathComponent(string value)
    {
        var escaped = new StringBuilder(value.Length);
        var invalidFileNameChars = Path.GetInvalidFileNameChars();
        foreach (var c in value)
        {
            if (c == '%' || Array.IndexOf(invalidFileNameChars, c) >= 0)
            {
                AppendEscaped(escaped, c);
                continue;
            }

            escaped.Append(c);
        }

        return escaped.ToString() switch
        {
            "." => "%2E",
            ".." => "%2E%2E",
            var pathComponent => pathComponent
        };
    }

    private static void AppendEscaped(StringBuilder builder, char value)
    {
        Span<byte> bytes = stackalloc byte[4];
        var length = Encoding.UTF8.GetBytes([value], bytes);
        for (var i = 0; i < length; i++)
        {
            builder.Append('%');
            builder.Append(bytes[i].ToString("X2"));
        }
    }

    private Segment GetSegment(long index)
    {
        lock (_segmentsLock)
        {
            if (_segments.TryGetValue(index, out var segment))
            {
                return segment;
            }

            var filePath = Path.Combine(_partitionDirectory, $"{index:D20}.log");
            var fileInfo = new FileInfo(filePath);
            if (!fileInfo.Exists || fileInfo.Length < _options.SegmentSize)
            {
                using var stream = new FileStream(filePath, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite);
                stream.SetLength(_options.SegmentSize);
            }

            var memoryMappedFile = System.IO.MemoryMappedFiles.MemoryMappedFile.CreateFromFile(
                filePath,
                FileMode.Open,
                mapName: null,
                _options.SegmentSize,
                MemoryMappedFileAccess.ReadWrite);
            try
            {
                var accessor = memoryMappedFile.CreateViewAccessor(
                    0,
                    _options.SegmentSize,
                    MemoryMappedFileAccess.ReadWrite);
                segment = new(memoryMappedFile, accessor);
            }
            catch
            {
                memoryMappedFile.Dispose();
                throw;
            }

            _segments.Add(index, segment);
            return segment;
        }
    }

    private void DisposeSegments()
    {
        lock (_segmentsLock)
        {
            foreach (var segment in _segments.Values)
            {
                segment.Dispose();
            }

            _segments.Clear();
            _dirtySegments.Clear();
        }
    }

    private void ThrowIfDisposed()
    {
        if (_disposed)
        {
            throw new ObjectDisposedException(GetType().FullName);
        }
    }

    private long SegmentIndex(long offset) => offset / _options.SegmentSize;

    private long PositionInSegment(long offset) => offset % _options.SegmentSize;

    private long NextSegmentOffset(long offset) => (SegmentIndex(offset) + 1) * _options.SegmentSize;

    private sealed class Reader(MemoryMappedFileBufferPartition<T> partition, string groupName)
    {
        private long _readOffset = partition.ReadCommittedOffset(groupName);
        private long _nextReadOffset;

        public long ReadOffset => _readOffset;

        public long NextReadOffset => _nextReadOffset;

        public bool TryRead(int batchSize, [NotNullWhen(true)] out IEnumerable<T>? items)
        {
            var remainingCount = batchSize;
            var readOffset = _readOffset;
            var result = new List<T>(batchSize);

            while (remainingCount > 0 && partition.TryRead(readOffset, out var item, out var nextOffset))
            {
                result.Add(item);
                remainingCount--;
                readOffset = nextOffset;
            }

            if (result.Count == 0)
            {
                items = null;
                return false;
            }

            _nextReadOffset = readOffset;
            items = result;
            return true;
        }

        public void MoveNext() => _readOffset = _nextReadOffset;
    }

    private sealed class Segment(
        System.IO.MemoryMappedFiles.MemoryMappedFile memoryMappedFile,
        MemoryMappedViewAccessor accessor) : IDisposable
    {
        public MemoryMappedViewAccessor Accessor { get; } = accessor;

        public void Dispose()
        {
            try
            {
                Accessor.Dispose();
            }
            finally
            {
                memoryMappedFile.Dispose();
            }
        }
    }
}
