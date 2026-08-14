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

    [Fact]
    public async Task AcquireAsync_Completes_After_Capacity_Is_Released()
    {
        var gate = new MemoryBufferCapacityGate(1);
        Assert.True(gate.TryAcquire());

        var waitingTask = gate.AcquireAsync(1, default).AsTask();
        Assert.False(waitingTask.IsCompleted);

        gate.Release();

        await waitingTask.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.False(gate.TryAcquire());
    }

    [Fact]
    public async Task AcquireAsync_Uses_Whole_Batch_Fifo_Admission()
    {
        var gate = new MemoryBufferCapacityGate(3);
        Assert.True(gate.TryAcquire(3));

        var batchTask = gate.AcquireAsync(3, default).AsTask();
        var singleTask = gate.AcquireAsync(1, default).AsTask();

        gate.Release();

        Assert.False(batchTask.IsCompleted);
        Assert.False(singleTask.IsCompleted);
        Assert.False(gate.TryAcquire());

        gate.Release(2);
        await batchTask.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.False(singleTask.IsCompleted);

        gate.Release();
        await singleTask.WaitAsync(TimeSpan.FromSeconds(10));
    }

    [Fact]
    public async Task Canceling_Head_Waiter_Allows_Next_Waiter_To_Proceed()
    {
        var gate = new MemoryBufferCapacityGate(2);
        Assert.True(gate.TryAcquire(2));
        using var cancellationTokenSource = new CancellationTokenSource();

        var batchTask = gate.AcquireAsync(2, cancellationTokenSource.Token).AsTask();
        var singleTask = gate.AcquireAsync(1, default).AsTask();
        gate.Release();

        cancellationTokenSource.Cancel();

        await Assert.ThrowsAsync<TaskCanceledException>(async () => await batchTask);
        await singleTask.WaitAsync(TimeSpan.FromSeconds(10));
        Assert.False(gate.TryAcquire());
    }

    [Fact]
    public async Task AcquireAsync_Observes_Cancellation_Without_Consuming_Capacity()
    {
        var gate = new MemoryBufferCapacityGate(1);
        using var cancellationTokenSource = new CancellationTokenSource();
        cancellationTokenSource.Cancel();

        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
            await gate.AcquireAsync(1, cancellationTokenSource.Token));

        Assert.True(gate.TryAcquire());
    }
}
