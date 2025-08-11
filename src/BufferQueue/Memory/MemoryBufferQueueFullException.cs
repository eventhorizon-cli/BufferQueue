// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;

namespace BufferQueue.Memory;

public class MemoryBufferQueueFullException(string message) : Exception(message);
