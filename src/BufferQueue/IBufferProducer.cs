// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.Threading.Tasks;

namespace BufferQueue;

public interface IBufferProducer<in T>
{
    string TopicName { get; }

    ValueTask ProduceAsync(T item);
}
