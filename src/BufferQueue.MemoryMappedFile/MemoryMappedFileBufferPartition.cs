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
    private const string ConsumerOffsetFileName = "consumer.offset";

    private readonly long _segmentSize;
    private readonly int? _maxRetainedConsumedSegments;
    private readonly IMemoryMappedFileSerializer<T> _serializer;
    private readonly IMemoryMappedFileFlusher _flusher;
    private readonly MemoryMappedFileFlushStrategy _flushStrategy;
    private readonly int _flushBatchSize;
    private readonly string _partitionDirectory;
    private readonly string _offsetDirectory;
    private readonly OffsetCheckpoint _producerOffsetCheckpoint;
    private readonly OffsetCheckpoint _earliestOffsetCheckpoint;
    private readonly object _appendLock;
    private readonly object _segmentsLock;
    private readonly object _consumersLock;
    private readonly ConcurrentDictionary<string, Reader> _consumerReaders;
    private readonly Dictionary<long, Segment> _segments;
    private readonly HashSet<Segment> _dirtySegments;
    private readonly HashSet<IBufferPartitionConsumer<T>> _consumers;
    private long _earliestOffset;
    private long _checkpointedProducerOffset;
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
        _segmentSize = options.GetSegmentSizeInBytes();
        _maxRetainedConsumedSegments = options.GetMaxRetainedConsumedSegments();
        _serializer = options.Serializer;
        _flusher = flusher;
        _flushStrategy = options.FlushStrategy;
        _flushBatchSize = options.FlushBatchSize;
        _partitionDirectory = Path.Combine(options.DataDirectory, options.TopicName!, $"partition-{id:D5}");
        _offsetDirectory = Path.Combine(_partitionDirectory, "offsets");
        _producerOffsetCheckpoint = new OffsetCheckpoint(Path.Combine(_partitionDirectory, "producer.offset"));
        _earliestOffsetCheckpoint = new OffsetCheckpoint(Path.Combine(_partitionDirectory, "earliest.offset"));
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
            _earliestOffset = ReadEarliestOffset();
            _writeOffset = FindWriteOffset();
            ValidatePersistedConsumerOffsets();
            MarkRecoveredTailDirty();
            DeleteSegmentFilesBefore(_earliestOffset);
            DeleteConsumedSegments();
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

        lock (_segmentsLock)
        {
            GetOrCreateReader(consumer.GroupName);
        }

        lock (_consumersLock)
        {
            _consumers.Add(consumer);
        }
    }

    public void UnregisterConsumer(IBufferPartitionConsumer<T> consumer)
    {
        lock (_segmentsLock)
        {
            var groupStillRegistered = false;
            lock (_consumersLock)
            {
                _consumers.Remove(consumer);
                foreach (var registeredConsumer in _consumers)
                {
                    if (registeredConsumer.GroupName == consumer.GroupName)
                    {
                        groupStillRegistered = true;
                        break;
                    }
                }
            }

            if (groupStillRegistered ||
                !_consumerReaders.TryRemove(consumer.GroupName, out var reader) ||
                !reader.CreatedCheckpoint)
            {
                return;
            }

            var groupDirectory = GetConsumerOffsetDirectory(consumer.GroupName);
            if (Directory.Exists(groupDirectory))
            {
                Directory.Delete(groupDirectory, recursive: true);
            }
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
        if (recordSize > _segmentSize)
        {
            throw new InvalidOperationException("The serialized item is larger than the memory-mapped file segment size.");
        }

        lock (_appendLock)
        {
            var offset = _writeOffset;
            var position = PositionInSegment(offset);
            if (position + recordSize > _segmentSize)
            {
                WriteSegmentEnd(offset);
                offset = NextSegmentOffset(offset);
                FlushPendingWrites(offset);
                Volatile.Write(ref _writeOffset, offset);
            }

            var segment = GetOrCreateSegment(SegmentIndex(offset));
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

        lock (_segmentsLock)
        {
            return GetOrCreateReader(groupName).TryRead(batchSize, out items);
        }
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
            DeleteConsumedSegments();
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

        var segment = GetExistingSegment(SegmentIndex(offset));
        var position = PositionInSegment(offset);
        if (position + MaxRecordOverhead > _segmentSize)
        {
            if (position + sizeof(int) <= _segmentSize &&
                segment.Accessor.ReadInt32(position) != SegmentEndMarker)
            {
                return false;
            }

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
        if (position + recordSize > _segmentSize || offset + recordSize > writeOffset)
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
        var lastExistingSegmentIndex = ValidateExistingSegmentFiles();
        var storedProducerOffset = ReadProducerOffset(lastExistingSegmentIndex);
        long writeOffset;
        if (!storedProducerOffset.HasValue)
        {
            _checkpointedProducerOffset = _earliestOffset;
            writeOffset = FindWriteOffset(_earliestOffset);
        }
        else
        {
            _checkpointedProducerOffset = storedProducerOffset.Value;
            writeOffset = IsRecordBoundary(storedProducerOffset.Value)
                ? FindWriteOffset(storedProducerOffset.Value)
                : throw new InvalidDataException(
                    $"The stored producer offset {storedProducerOffset.Value} is not aligned to a record boundary.");
        }

        if (lastExistingSegmentIndex is { } lastIndex && lastIndex > SegmentIndex(writeOffset))
        {
            throw new InvalidDataException(
                $"The memory-mapped-file log contains segment {lastExistingSegmentIndex} after the recovered write offset {writeOffset}.");
        }

        return writeOffset;
    }

    private long FindWriteOffset(long startOffset)
    {
        var offset = startOffset;

        while (true)
        {
            var position = PositionInSegment(offset);
            if (!TryGetExistingSegment(SegmentIndex(offset), out var segment))
            {
                if (position != 0)
                {
                    throw new InvalidDataException(
                        $"The memory-mapped-file log segment containing offset {offset} does not exist.");
                }

                return offset;
            }
            if (position >= _segmentSize)
            {
                offset = NextSegmentOffset(offset);
                continue;
            }

            if (position + MaxRecordOverhead > _segmentSize)
            {
                var nextSegmentOffset = NextSegmentOffset(offset);
                if (position + sizeof(int) <= _segmentSize)
                {
                    if (segment.Accessor.ReadInt32(position) != SegmentEndMarker)
                    {
                        return offset;
                    }

                    offset = nextSegmentOffset;
                    continue;
                }

                if (!TryGetExistingSegment(SegmentIndex(nextSegmentOffset), out _))
                {
                    return offset;
                }

                offset = nextSegmentOffset;
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
                return offset;
            }

            var recordSize = MaxRecordOverhead + length;
            if (position + recordSize > _segmentSize)
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
        if (candidateOffset == _earliestOffset)
        {
            return true;
        }

        if (candidateOffset < _earliestOffset)
        {
            return false;
        }

        var offset = _earliestOffset;
        while (offset < candidateOffset)
        {
            var segment = GetExistingSegment(SegmentIndex(offset));
            var position = PositionInSegment(offset);
            if (position + MaxRecordOverhead > _segmentSize)
            {
                if (position + sizeof(int) <= _segmentSize &&
                    segment.Accessor.ReadInt32(position) != SegmentEndMarker)
                {
                    return false;
                }

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
            if (position + recordSize > _segmentSize)
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
        var segment = GetExistingSegment(SegmentIndex(offset));
        var position = PositionInSegment(offset);
        if (position + sizeof(int) > _segmentSize)
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

        if (_checkpointedProducerOffset != writeOffset)
        {
            _producerOffsetCheckpoint.Write(writeOffset);
        }

        _dirtySegments.Clear();
        _pendingRecordCount = 0;
        _checkpointedProducerOffset = writeOffset;
    }

    private void MarkRecoveredTailDirty()
    {
        if (_writeOffset <= _checkpointedProducerOffset)
        {
            return;
        }

        var firstSegmentIndex = SegmentIndex(_checkpointedProducerOffset);
        var lastSegmentIndex = SegmentIndex(_writeOffset - 1);
        for (var segmentIndex = firstSegmentIndex; segmentIndex <= lastSegmentIndex; segmentIndex++)
        {
            _dirtySegments.Add(GetExistingSegment(segmentIndex));
        }
    }

    private long ReadEarliestOffset()
    {
        var offset = _earliestOffsetCheckpoint.ReadOrDefault();
        if (PositionInSegment(offset) != 0)
        {
            throw new InvalidDataException(
                $"The earliest offset {offset} is not aligned to a memory-mapped-file segment boundary.");
        }

        return offset;
    }

    private long? ReadProducerOffset(long? lastExistingSegmentIndex)
    {
        if (!_producerOffsetCheckpoint.TryRead(out var offset))
        {
            return null;
        }

        if (offset < _earliestOffset)
        {
            throw new InvalidDataException(
                $"The stored producer offset {offset} is less than the earliest offset {_earliestOffset}.");
        }

        if (!lastExistingSegmentIndex.HasValue)
        {
            return offset == _earliestOffset
                ? offset
                : throw new InvalidDataException(
                    $"The stored producer offset {offset} points to a log that does not exist.");
        }

        long maxExistingOffset;
        try
        {
            maxExistingOffset = checked((lastExistingSegmentIndex.Value + 1) * _segmentSize);
        }
        catch (OverflowException exception)
        {
            throw new InvalidDataException("The memory-mapped-file log size exceeds the supported offset range.",
                exception);
        }

        return offset <= maxExistingOffset
            ? offset
            : throw new InvalidDataException(
                $"The stored producer offset {offset} is greater than the memory-mapped-file log size {maxExistingOffset}.");
    }

    private long ReadCommittedOffset(string groupName, out bool createdCheckpoint)
    {
        createdCheckpoint = false;
        var groupDirectory = GetConsumerOffsetDirectory(groupName);
        var checkpoint = new OffsetCheckpoint(Path.Combine(groupDirectory, ConsumerOffsetFileName));
        if (!checkpoint.TryRead(out var offset))
        {
            if (Directory.Exists(groupDirectory))
            {
                throw new InvalidDataException(
                    $"The consumer group offset directory '{groupDirectory}' exists without an offset checkpoint.");
            }

            checkpoint.Write(_earliestOffset);
            createdCheckpoint = true;
            return _earliestOffset;
        }

        ValidateCommittedOffset(offset, $"consumer group '{groupName}'");
        return offset;
    }

    private void WriteCommittedOffset(string groupName, long offset)
    {
        GetConsumerOffsetCheckpoint(groupName).Write(offset);
    }

    private OffsetCheckpoint GetConsumerOffsetCheckpoint(string groupName) =>
        new(Path.Combine(GetConsumerOffsetDirectory(groupName), ConsumerOffsetFileName));

    private string GetConsumerOffsetDirectory(string groupName) =>
        Path.Combine(_offsetDirectory, EscapePathComponent(groupName));

    private Reader GetOrCreateReader(string groupName)
    {
        if (_consumerReaders.TryGetValue(groupName, out var reader))
        {
            return reader;
        }

        var readOffset = ReadCommittedOffset(groupName, out var createdCheckpoint);
        reader = new(this, readOffset, createdCheckpoint);
        _consumerReaders[groupName] = reader;
        return reader;
    }

    private void ValidatePersistedConsumerOffsets()
    {
        foreach (var groupDirectory in Directory.EnumerateDirectories(_offsetDirectory))
        {
            var checkpointPath = Path.Combine(groupDirectory, ConsumerOffsetFileName);
            var checkpoint = new OffsetCheckpoint(checkpointPath);
            if (!checkpoint.TryRead(out var offset))
            {
                throw new InvalidDataException(
                    $"The consumer group offset directory '{groupDirectory}' exists without an offset checkpoint.");
            }

            ValidateCommittedOffset(offset, $"checkpoint '{checkpointPath}'");
        }
    }

    private void ValidateCommittedOffset(long offset, string source)
    {
        ValidateCommittedOffsetRange(offset, source);

        if (!IsRecordBoundary(offset))
        {
            throw new InvalidDataException(
                $"The committed offset {offset} for {source} is not aligned to a record boundary.");
        }
    }

    private void ValidateCommittedOffsetRange(long offset, string source)
    {
        if (offset < _earliestOffset)
        {
            throw new InvalidDataException(
                $"The committed offset {offset} for {source} is less than the earliest offset {_earliestOffset}.");
        }

        var writeOffset = Volatile.Read(ref _writeOffset);
        if (offset > writeOffset)
        {
            throw new InvalidDataException(
                $"The committed offset {offset} for {source} is greater than the producer offset {writeOffset}.");
        }
    }

    private long? ValidateExistingSegmentFiles()
    {
        var earliestSegmentIndex = SegmentIndex(_earliestOffset);
        var segmentIndices = new SortedSet<long>();
        foreach (var filePath in Directory.EnumerateFiles(_partitionDirectory, "*.log"))
        {
            var index = ParseSegmentIndex(filePath);
            if (index < earliestSegmentIndex)
            {
                continue;
            }

            if (!segmentIndices.Add(index))
            {
                throw new InvalidDataException(
                    $"Multiple memory-mapped-file log files resolve to segment index {index}.");
            }
        }

        if (segmentIndices.Count == 0)
        {
            return null;
        }

        var expectedIndex = earliestSegmentIndex;
        foreach (var index in segmentIndices)
        {
            if (index != expectedIndex)
            {
                throw new InvalidDataException(
                    $"Memory-mapped-file log segment {expectedIndex} is missing before segment {index}.");
            }

            if (expectedIndex == long.MaxValue)
            {
                throw new InvalidDataException("The memory-mapped-file segment index exceeds the supported range.");
            }

            expectedIndex++;
        }

        return segmentIndices.Max;
    }

    private void DeleteConsumedSegments()
    {
        if (!_maxRetainedConsumedSegments.HasValue)
        {
            return;
        }

        lock (_segmentsLock)
        {
            DeleteSegmentFilesBefore(_earliestOffset);

            var minimumCommittedOffset = ReadMinimumCommittedOffset();
            if (!minimumCommittedOffset.HasValue)
            {
                return;
            }

            var reclaimableOffset = Math.Min(minimumCommittedOffset.Value, _checkpointedProducerOffset);
            var firstNotFullyConsumedSegment = SegmentIndex(reclaimableOffset);
            var earliestRetainedSegment = Math.Max(
                SegmentIndex(_earliestOffset),
                firstNotFullyConsumedSegment - _maxRetainedConsumedSegments.Value);
            var newEarliestOffset = checked(earliestRetainedSegment * _segmentSize);
            if (newEarliestOffset <= _earliestOffset)
            {
                return;
            }

            _earliestOffsetCheckpoint.Write(newEarliestOffset);
            _earliestOffset = newEarliestOffset;
            DeleteSegmentFilesBefore(newEarliestOffset);
        }
    }

    private long? ReadMinimumCommittedOffset()
    {
        foreach (var (groupName, reader) in _consumerReaders)
        {
            var normalizedOffset = NormalizeCommittedOffsetWithinProducerCheckpoint(reader.ReadOffset);
            if (normalizedOffset == reader.ReadOffset)
            {
                continue;
            }

            GetConsumerOffsetCheckpoint(groupName).Write(normalizedOffset);
            reader.MoveReadOffsetTo(normalizedOffset);
        }

        long? minimumOffset = null;
        foreach (var groupDirectory in Directory.EnumerateDirectories(_offsetDirectory))
        {
            var checkpointPath = Path.Combine(groupDirectory, ConsumerOffsetFileName);
            var checkpoint = new OffsetCheckpoint(checkpointPath);
            if (!checkpoint.TryRead(out var offset))
            {
                throw new InvalidDataException(
                    $"The consumer group offset directory '{groupDirectory}' exists without an offset checkpoint.");
            }

            ValidateCommittedOffsetRange(offset, $"checkpoint '{checkpointPath}'");
            var normalizedOffset = NormalizeCommittedOffsetWithinProducerCheckpoint(offset);
            if (normalizedOffset != offset)
            {
                checkpoint.Write(normalizedOffset);
            }

            minimumOffset = !minimumOffset.HasValue || normalizedOffset < minimumOffset.Value
                ? normalizedOffset
                : minimumOffset;
        }

        return minimumOffset;
    }

    private long NormalizeCommittedOffset(long offset)
    {
        var writeOffset = Volatile.Read(ref _writeOffset);
        while (offset < writeOffset)
        {
            var position = PositionInSegment(offset);
            if (position + MaxRecordOverhead > _segmentSize)
            {
                var tailSegment = GetExistingSegment(SegmentIndex(offset));
                if (position + sizeof(int) <= _segmentSize &&
                    tailSegment.Accessor.ReadInt32(position) != SegmentEndMarker)
                {
                    break;
                }

                offset = NextSegmentOffset(offset);
                continue;
            }

            var segment = GetExistingSegment(SegmentIndex(offset));
            if (segment.Accessor.ReadInt32(position) != SegmentEndMarker)
            {
                break;
            }

            offset = NextSegmentOffset(offset);
        }

        return offset;
    }

    private long NormalizeCommittedOffsetWithinProducerCheckpoint(long offset)
    {
        var normalizedOffset = NormalizeCommittedOffset(offset);
        return normalizedOffset <= _checkpointedProducerOffset ? normalizedOffset : offset;
    }

    private void DeleteSegmentFilesBefore(long offset)
    {
        lock (_segmentsLock)
        {
            var firstRetainedSegmentIndex = SegmentIndex(offset);
            foreach (var filePath in Directory.EnumerateFiles(_partitionDirectory, "*.log"))
            {
                var index = ParseSegmentIndex(filePath);
                if (index >= firstRetainedSegmentIndex)
                {
                    continue;
                }

                if (_segments.Remove(index, out var segment))
                {
                    _dirtySegments.Remove(segment);
                    segment.Dispose();
                }

                try
                {
                    File.Delete(filePath);
                }
                catch (Exception exception) when (exception is IOException or UnauthorizedAccessException)
                {
                    throw new IOException(
                        $"The earliest offset was advanced to {_earliestOffset}, but log segment '{filePath}' could not be deleted. The consumer offset remains committed and cleanup can be retried.",
                        exception);
                }
            }
        }
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

    private Segment GetOrCreateSegment(long index)
    {
        lock (_segmentsLock)
        {
            if (_segments.TryGetValue(index, out var segment))
            {
                return segment;
            }

            var filePath = GetSegmentFilePath(index);
            if (!File.Exists(filePath))
            {
                using var stream = new FileStream(filePath, FileMode.CreateNew, FileAccess.ReadWrite,
                    FileShare.ReadWrite);
                stream.SetLength(_segmentSize);
            }

            return OpenExistingSegment(index, filePath);
        }
    }

    private Segment GetExistingSegment(long index) =>
        TryGetExistingSegment(index, out var segment)
            ? segment
            : throw new InvalidDataException($"Memory-mapped-file log segment {index} does not exist.");

    private bool TryGetExistingSegment(long index, [NotNullWhen(true)] out Segment? segment)
    {
        lock (_segmentsLock)
        {
            if (_segments.TryGetValue(index, out segment))
            {
                return true;
            }

            var filePath = GetSegmentFilePath(index);
            if (!File.Exists(filePath))
            {
                segment = null;
                return false;
            }

            segment = OpenExistingSegment(index, filePath);
            return true;
        }
    }

    private Segment OpenExistingSegment(long index, string filePath)
    {
        var fileInfo = new FileInfo(filePath);
        if (fileInfo.Length != _segmentSize)
        {
            throw new InvalidDataException(
                $"Memory-mapped-file log segment '{filePath}' must be exactly {_segmentSize} bytes, but is {fileInfo.Length} bytes.");
        }

        var memoryMappedFile = System.IO.MemoryMappedFiles.MemoryMappedFile.CreateFromFile(
            filePath,
            FileMode.Open,
            mapName: null,
            _segmentSize,
            MemoryMappedFileAccess.ReadWrite);
        try
        {
            var accessor = memoryMappedFile.CreateViewAccessor(
                0,
                _segmentSize,
                MemoryMappedFileAccess.ReadWrite);
            var segment = new Segment(memoryMappedFile, accessor);

            _segments.Add(index, segment);
            return segment;
        }
        catch
        {
            memoryMappedFile.Dispose();
            throw;
        }
    }

    private string GetSegmentFilePath(long index) =>
        Path.Combine(_partitionDirectory, $"{index:D20}.log");

    private static long ParseSegmentIndex(string filePath)
    {
        var fileName = Path.GetFileName(filePath);
        if (!long.TryParse(Path.GetFileNameWithoutExtension(filePath), out var index) ||
            index < 0 ||
            !string.Equals(fileName, $"{index:D20}.log", StringComparison.Ordinal))
        {
            throw new InvalidDataException(
                $"The memory-mapped-file log file '{filePath}' does not have a valid segment file name.");
        }

        return index;
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

    private long SegmentIndex(long offset) => offset / _segmentSize;

    private long PositionInSegment(long offset) => offset % _segmentSize;

    private long NextSegmentOffset(long offset) => (SegmentIndex(offset) + 1) * _segmentSize;

    private sealed class Reader(
        MemoryMappedFileBufferPartition<T> partition,
        long readOffset,
        bool createdCheckpoint)
    {
        private long _readOffset = readOffset;
        private long _nextReadOffset = readOffset;

        public bool CreatedCheckpoint { get; } = createdCheckpoint;

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

        public void MoveReadOffsetTo(long offset)
        {
            _readOffset = offset;
            _nextReadOffset = Math.Max(_nextReadOffset, offset);
        }
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
