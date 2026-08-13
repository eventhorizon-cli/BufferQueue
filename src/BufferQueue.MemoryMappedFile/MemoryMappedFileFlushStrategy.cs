namespace BufferQueue.MemoryMappedFile;

/// <summary>
/// Specifies when memory-mapped-file writes are flushed to disk.
/// </summary>
public enum MemoryMappedFileFlushStrategy
{
    /// <summary>
    /// Flushes after every record is appended.
    /// </summary>
    Immediate,

    /// <summary>
    /// Flushes after the configured number of records is appended to a partition.
    /// </summary>
    Batch
}
