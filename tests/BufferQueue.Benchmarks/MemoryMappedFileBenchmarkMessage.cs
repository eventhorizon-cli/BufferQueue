// Licensed to the .NET Core Community under one or more agreements.
// The .NET Core Community licenses this file to you under the MIT license.

using System.Runtime.InteropServices;
using MessagePack;

namespace BufferQueue.Benchmarks;

[MessagePackObject]
[StructLayout(LayoutKind.Sequential, Pack = 1)]
public struct MemoryMappedFileBenchmarkMessage
{
    [Key(0)]
    public long Sequence { get; set; }

    [Key(1)]
    public long Timestamp { get; set; }

    [Key(2)]
    public double Price { get; set; }

    [Key(3)]
    public int Quantity { get; set; }

    public static MemoryMappedFileBenchmarkMessage Create(int sequence) => new()
    {
        Sequence = sequence,
        Timestamp = 638882496000000000 + sequence,
        Price = 1234.56 + (sequence % 100) * 0.01,
        Quantity = sequence % 1000 + 1
    };
}
