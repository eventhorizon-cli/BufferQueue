using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;

namespace BufferQueue;

internal interface IBufferPartitionConsumer<TItem>
{
    string GroupName { get; }

    void NotifyNewDataAvailable(IBufferPartition<TItem> partition);
}

internal interface IBufferPartition<TItem>
{
    int PartitionId { get; }

    void RegisterConsumer(IBufferPartitionConsumer<TItem> consumer);

    void UnregisterConsumer(IBufferPartitionConsumer<TItem> consumer);

    void Enqueue(TItem item);

    bool TryPull(string groupName, int batchSize, [NotNullWhen(true)] out IEnumerable<TItem>? items);

    void Commit(string groupName);
}
