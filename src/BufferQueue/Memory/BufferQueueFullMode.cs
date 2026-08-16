namespace BufferQueue.Memory;

public enum BufferQueueFullMode
{
    /// <summary>
    /// Asynchronously waits until capacity becomes available and completes the write.
    /// </summary>
    Wait,

    /// <summary>
    /// Fails the write immediately when capacity is unavailable.
    /// </summary>
    Fail
}
