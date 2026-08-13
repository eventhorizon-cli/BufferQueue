using System;

namespace BufferQueue.Memory;

public class MemoryBufferQueueFullException(string message) : Exception(message);
