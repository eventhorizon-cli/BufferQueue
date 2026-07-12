// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public class MemoryBufferCapacityGateTests
{
    [Fact]
    public async Task Concurrent_Acquisition_Does_Not_Exceed_Capacity()
    {
        const int capacity = 257;
        const int workerCount = 32;
        const int attemptsPerWorker = 32;
        var gate = new MemoryBufferCapacityGate(capacity);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ready = new CountdownEvent(workerCount);
        var acquiredCount = 0;

        var tasks = Enumerable.Range(0, workerCount)
            .Select(_ => Task.Run(async () =>
            {
                ready.Signal();
                await start.Task;

                for (var i = 0; i < attemptsPerWorker; i++)
                {
                    if (gate.TryAcquire())
                    {
                        Interlocked.Increment(ref acquiredCount);
                    }
                }
            }))
            .ToArray();

        Assert.True(ready.Wait(TimeSpan.FromSeconds(10)));
        start.SetResult();
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(capacity, acquiredCount);
        Assert.False(gate.TryAcquire());
    }

    [Fact]
    public async Task Contention_Below_Capacity_Does_Not_Lose_Acquisitions()
    {
        const int workerCount = 32;
        const int attemptsPerWorker = 32;
        const int attemptCount = workerCount * attemptsPerWorker;
        var gate = new MemoryBufferCapacityGate(attemptCount + 1UL);
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var ready = new CountdownEvent(workerCount);
        var acquiredCount = 0;

        var tasks = Enumerable.Range(0, workerCount)
            .Select(_ => Task.Run(async () =>
            {
                ready.Signal();
                await start.Task;

                for (var i = 0; i < attemptsPerWorker; i++)
                {
                    if (gate.TryAcquire())
                    {
                        Interlocked.Increment(ref acquiredCount);
                    }
                }
            }))
            .ToArray();

        Assert.True(ready.Wait(TimeSpan.FromSeconds(10)));
        start.SetResult();
        await Task.WhenAll(tasks).WaitAsync(TimeSpan.FromSeconds(10));

        Assert.Equal(attemptCount, acquiredCount);
        Assert.True(gate.TryAcquire());
        Assert.False(gate.TryAcquire());
    }

    [Fact]
    public void Zero_Capacity_Cannot_Be_Acquired()
    {
        var gate = new MemoryBufferCapacityGate(0);

        Assert.False(gate.TryAcquire());
    }

    [Fact]
    public void Release_Makes_Capacity_Available_Again()
    {
        var gate = new MemoryBufferCapacityGate(2);

        Assert.True(gate.TryAcquire());
        Assert.True(gate.TryAcquire());
        Assert.False(gate.TryAcquire());

        gate.Release();

        Assert.True(gate.TryAcquire());
        Assert.False(gate.TryAcquire());
    }

    [Fact]
    public void Release_More_Than_Acquired_Throws()
    {
        var gate = new MemoryBufferCapacityGate(1);

        var exception = Assert.Throws<InvalidOperationException>(() => gate.Release());

        Assert.Equal("Cannot release more bounded queue capacity than was acquired.", exception.Message);
    }
}
