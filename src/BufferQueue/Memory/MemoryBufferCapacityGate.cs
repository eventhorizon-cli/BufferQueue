// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System;
using System.Threading;

namespace BufferQueue.Memory;

internal sealed class MemoryBufferCapacityGate
{
    private readonly ulong _capacity;
    private ulong _availableSlots;

    public MemoryBufferCapacityGate(ulong capacity)
    {
        _capacity = capacity;
        _availableSlots = capacity;
    }

    public bool TryAcquire()
    {
        var spinWait = new SpinWait();
        while (true)
        {
            var availableSlots = Volatile.Read(ref _availableSlots);
            if (availableSlots == 0)
            {
                return false;
            }

            if (Interlocked.CompareExchange(
                    ref _availableSlots,
                    availableSlots - 1,
                    availableSlots)
                == availableSlots)
            {
                return true;
            }

            spinWait.SpinOnce();
        }
    }

    public void Release(ulong count = 1)
    {
        if (count == 0)
        {
            return;
        }

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
}
