// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.Reflection;
using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public class MemoryBufferSegmentTests
{
    private static readonly TimeSpan Timeout = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task TryEnqueue_Does_Not_Publish_Past_An_Unwritten_Slot()
    {
        var segment = new MemoryBufferSegment<int>(2, default);
        var slots = GetField<int[]>(segment, "_slots");
        var slotWritten = GetField<bool[]>(segment, "_slotWritten");
        SetField(segment, "_lastReservedPosition", 0);

        var laterWrite = StartLongRunning(() => segment.TryEnqueue(2));
        var enqueueSucceeded = false;
        try
        {
            Assert.True(SpinWait.SpinUntil(
                () => Volatile.Read(ref slotWritten[1]),
                Timeout));
            Assert.False(laterWrite.IsCompleted);
            Assert.Equal(0, segment.Count);
            Assert.False(segment.TryGet(default, 2, out _));
        }
        finally
        {
            Volatile.Write(ref slots[0], 1);
            Volatile.Write(ref slotWritten[0], true);
            enqueueSucceeded = await laterWrite.WaitAsync(Timeout);
        }

        Assert.True(enqueueSucceeded);
        Assert.Equal(2, segment.Count);
        Assert.True(segment.TryGet(default, 2, out var items));
        Assert.Equal(new[] { 1, 2 }, items.ToArray());
    }

    [Fact]
    public async Task Enqueue_Does_Not_Roll_Over_Past_An_Unwritten_Tail_Slot()
    {
        var partition = new MemoryBufferPartition<int>(0, 2);
        var tail = GetField<MemoryBufferSegment<int>>(partition, "_tail");
        var createSegmentLock = GetField<object>(partition, "_createSegmentLock");
        var slots = GetField<int[]>(tail, "_slots");
        var slotWritten = GetField<bool[]>(tail, "_slotWritten");
        SetField(tail, "_lastReservedPosition", 0);

        var secondWrite = StartLongRunning(() => partition.Enqueue(2));
        Task? rolloverWrite = null;
        try
        {
            Assert.True(SpinWait.SpinUntil(
                () => Volatile.Read(ref slotWritten[1]),
                Timeout));

            rolloverWrite = StartLongRunning(() => partition.Enqueue(3));
            Assert.True(SpinWait.SpinUntil(
                () => rolloverWrite.IsCompleted || IsHeldByAnotherThread(createSegmentLock),
                Timeout));
            Assert.False(rolloverWrite.IsCompleted);
            Assert.Null(tail.NextSegment);
            Assert.False(partition.TryPull("TestGroup", 3, out _));
        }
        finally
        {
            Volatile.Write(ref slots[0], 1);
            Volatile.Write(ref slotWritten[0], true);
            await secondWrite.WaitAsync(Timeout);
            if (rolloverWrite != null)
            {
                await rolloverWrite.WaitAsync(Timeout);
            }
        }

        Assert.True(partition.TryPull("TestGroup", 3, out var items));
        Assert.Equal(new[] { 1, 2, 3 }, items);
    }

    [Fact]
    public void RecycleSlots_Clears_Written_Slot_State()
    {
        var segment = new MemoryBufferSegment<int>(2, default);
        Assert.True(segment.TryEnqueue(1));
        Assert.True(segment.TryEnqueue(2));

        var startOffset = new MemoryBufferPartitionOffset(0, 2);
        var recycledSegment = segment.RecycleSlots(startOffset);

        Assert.True(recycledSegment.TryEnqueue(3));
        Assert.Equal(1, recycledSegment.Count);
        Assert.True(recycledSegment.TryGet(startOffset, 2, out var items));
        Assert.Equal(new[] { 3 }, items.ToArray());
    }

    [Fact]
    public async Task Concurrent_Enqueue_And_Pull_Preserves_Every_Item()
    {
        const int itemCount = 4096;
        const int producerCount = 8;
        var partition = new MemoryBufferPartition<int>(0, 4);
        var consumedItems = new List<int>(itemCount);
        using var cancellationTokenSource = new CancellationTokenSource(Timeout);

        var consumer = Task.Run(async () =>
        {
            while (consumedItems.Count < itemCount)
            {
                cancellationTokenSource.Token.ThrowIfCancellationRequested();
                if (!partition.TryPull("TestGroup", 16, out var items))
                {
                    await Task.Yield();
                    continue;
                }

                consumedItems.AddRange(items);
                partition.Commit("TestGroup");
            }
        }, cancellationTokenSource.Token);

        var producers = Enumerable.Range(1, itemCount)
            .Chunk(itemCount / producerCount)
            .Select(chunk => Task.Run(() =>
            {
                foreach (var item in chunk)
                {
                    partition.Enqueue(item);
                }
            }))
            .ToArray();

        await Task.WhenAll(producers).WaitAsync(Timeout);
        await consumer.WaitAsync(Timeout);

        Assert.Equal(itemCount, consumedItems.Count);
        Assert.Equal(Enumerable.Range(1, itemCount), consumedItems.Order());
    }

    private static Task StartLongRunning(Action action) => Task.Factory.StartNew(
        action,
        CancellationToken.None,
        TaskCreationOptions.LongRunning,
        TaskScheduler.Default);

    private static Task<TResult> StartLongRunning<TResult>(Func<TResult> action) => Task.Factory.StartNew(
        action,
        CancellationToken.None,
        TaskCreationOptions.LongRunning,
        TaskScheduler.Default);

    private static bool IsHeldByAnotherThread(object syncRoot)
    {
        if (!Monitor.TryEnter(syncRoot))
        {
            return true;
        }

        Monitor.Exit(syncRoot);
        return false;
    }

    private static TField GetField<TField>(object instance, string fieldName) =>
        (TField)instance.GetType()
            .GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)!
            .GetValue(instance)!;

    private static void SetField<T>(MemoryBufferSegment<T> segment, string fieldName, object value) =>
        typeof(MemoryBufferSegment<T>)
            .GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic)!
            .SetValue(segment, value);
}
