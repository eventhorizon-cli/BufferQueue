using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace BufferQueue.Memory;

internal sealed class MemoryBufferCapacityGate
{
    private readonly object _syncRoot = new();
    private readonly ulong _capacity;
    private readonly LinkedList<Waiter> _waiters = [];
    private int _hasWaiters;
    private ulong _availableSlots;

    public MemoryBufferCapacityGate(ulong capacity)
    {
        _capacity = capacity;
        _availableSlots = capacity;
    }

    public ulong Capacity => _capacity;

    public bool TryAcquire()
    {
        return TryAcquire(1);
    }

    public bool TryAcquire(ulong count)
    {
        if (count == 0)
        {
            return true;
        }

        if (Volatile.Read(ref _hasWaiters) != 0)
        {
            return false;
        }

        return TryAcquireCore(count);
    }

    public ValueTask AcquireAsync(ulong count, CancellationToken cancellationToken)
    {
        if (count > _capacity)
        {
            throw new ArgumentOutOfRangeException(nameof(count),
                "The requested capacity cannot exceed the queue capacity.");
        }

        if (cancellationToken.IsCancellationRequested)
        {
            return ValueTask.FromCanceled(cancellationToken);
        }

        if (count == 0)
        {
            return ValueTask.CompletedTask;
        }

        if (TryAcquire(count))
        {
            return ValueTask.CompletedTask;
        }

        Waiter waiter;
        CancellationTokenRegistration cancellationRegistration = default;
        var unregisterCancellation = false;

        lock (_syncRoot)
        {
            if (cancellationToken.IsCancellationRequested)
            {
                return ValueTask.FromCanceled(cancellationToken);
            }

            Volatile.Write(ref _hasWaiters, 1);
            if (_waiters.Count == 0 && TryAcquireCore(count))
            {
                Volatile.Write(ref _hasWaiters, 0);
                return ValueTask.CompletedTask;
            }

            waiter = new(this, count, cancellationToken);
            waiter.Node = _waiters.AddLast(waiter);

            if (cancellationToken.CanBeCanceled)
            {
                cancellationRegistration = cancellationToken.UnsafeRegister(
                    static state => ((Waiter)state!).Cancel(),
                    waiter);
                waiter.CancellationRegistration = cancellationRegistration;
                unregisterCancellation = waiter.IsCompleted;
            }
        }

        if (unregisterCancellation)
        {
            cancellationRegistration.Unregister();
        }

        return new(waiter.Completion.Task);
    }

    public void Release(ulong count = 1)
    {
        if (count == 0)
        {
            return;
        }

        ReleaseCore(count);
        if (Volatile.Read(ref _hasWaiters) == 0)
        {
            return;
        }

        List<Waiter>? readyWaiters;
        lock (_syncRoot)
        {
            readyWaiters = DrainReadyWaiters();
        }

        CompleteReadyWaiters(readyWaiters);
    }

    private void Cancel(Waiter waiter)
    {
        List<Waiter>? readyWaiters;
        lock (_syncRoot)
        {
            if (waiter.IsCompleted)
            {
                return;
            }

            _waiters.Remove(waiter.Node!);
            waiter.Node = null;
            waiter.IsCompleted = true;
            readyWaiters = DrainReadyWaiters();
        }

        waiter.CancellationRegistration.Unregister();
        waiter.Completion.TrySetCanceled(waiter.CancellationToken);
        CompleteReadyWaiters(readyWaiters);
    }

    private List<Waiter>? DrainReadyWaiters()
    {
        List<Waiter>? readyWaiters = null;
        while (_waiters.First is { } node && TryAcquireCore(node.Value.Count))
        {
            var waiter = node.Value;
            _waiters.RemoveFirst();
            waiter.Node = null;
            waiter.IsCompleted = true;
            (readyWaiters ??= []).Add(waiter);
        }

        if (_waiters.Count == 0)
        {
            Volatile.Write(ref _hasWaiters, 0);
        }

        return readyWaiters;
    }

    private bool TryAcquireCore(ulong count)
    {
        var spinWait = new SpinWait();
        while (true)
        {
            var availableSlots = Volatile.Read(ref _availableSlots);
            if (availableSlots < count)
            {
                return false;
            }

            if (Interlocked.CompareExchange(
                    ref _availableSlots,
                    availableSlots - count,
                    availableSlots)
                == availableSlots)
            {
                return true;
            }

            spinWait.SpinOnce();
        }
    }

    private void ReleaseCore(ulong count)
    {
        var spinWait = new SpinWait();
        while (true)
        {
            var availableSlots = Volatile.Read(ref _availableSlots);
            if (availableSlots > _capacity || count > _capacity - availableSlots)
            {
                throw new InvalidOperationException("Cannot release more bounded queue capacity than was acquired.");
            }

            if (Interlocked.CompareExchange(
                    ref _availableSlots,
                    availableSlots + count,
                    availableSlots)
                == availableSlots)
            {
                return;
            }

            spinWait.SpinOnce();
        }
    }

    private static void CompleteReadyWaiters(List<Waiter>? readyWaiters)
    {
        if (readyWaiters == null)
        {
            return;
        }

        foreach (var waiter in readyWaiters)
        {
            waiter.CancellationRegistration.Dispose();
            waiter.Completion.TrySetResult();
        }
    }

    private sealed class Waiter(
        MemoryBufferCapacityGate owner,
        ulong count,
        CancellationToken cancellationToken)
    {
        public ulong Count { get; } = count;

        public CancellationToken CancellationToken { get; } = cancellationToken;

        public TaskCompletionSource Completion { get; } = new(TaskCreationOptions.RunContinuationsAsynchronously);

        public LinkedListNode<Waiter>? Node { get; set; }

        public CancellationTokenRegistration CancellationRegistration { get; set; }

        public bool IsCompleted { get; set; }

        public void Cancel() => owner.Cancel(this);
    }
}
