using System;

namespace BufferQueue;

public class BufferQueueFullException(string message) : Exception(message);
