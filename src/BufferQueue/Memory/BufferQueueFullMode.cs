namespace BufferQueue.Memory;

public enum BufferQueueFullMode
{
    /// <summary>
    /// Asynchronously waits until capacity becomes available.
    /// </summary>
    Wait,

    /// <summary>
    /// Fails the write immediately.
    /// </summary>
    Fail
}
