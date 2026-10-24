using System.Diagnostics;

namespace BufferQueue.Memory;

// Logical item position modulo 2^64; reaching ulong.MaxValue does not exhaust the sequence.
// Generation is unnecessary: only positions in the currently retained window need ordering.
// One ulong reduces the offset from 16 to 8 bytes and removes generation carry/borrow branches.
// Retained segments and reader positions must span fewer than 2^63 items for ordering to be valid.
// Even one byte per retained item would require 8 EiB of memory to reach that span.
// This is not a total order over arbitrary ulong values or offsets from different partitions.
[DebuggerDisplay("Index = {Index}")]
internal readonly record struct MemoryBufferPartitionOffset(ulong Index)
{
    // The signed modular difference makes 0 follow ulong.MaxValue; comparing raw Index values would not.
    // A distance of exactly 2^63 is ambiguous and is excluded by the live-window invariant above.
    public static bool operator >(MemoryBufferPartitionOffset left, MemoryBufferPartitionOffset right)
        => unchecked((long)(left.Index - right.Index)) > 0;

    public static bool operator <(MemoryBufferPartitionOffset left, MemoryBufferPartitionOffset right)
        => unchecked((long)(left.Index - right.Index)) < 0;

    // Modular distance, not another position. Validate the forward range before using it as an item count.
    public static ulong operator -(MemoryBufferPartitionOffset left, MemoryBufferPartitionOffset right)
        => unchecked(left.Index - right.Index);

    // Explicit unchecked arithmetic preserves natural wrap even in builds with overflow checking enabled.
    public static MemoryBufferPartitionOffset operator +(MemoryBufferPartitionOffset offset, ulong value)
        => new(unchecked(offset.Index + value));

    public static MemoryBufferPartitionOffset operator -(MemoryBufferPartitionOffset offset, ulong value)
        => new(unchecked(offset.Index - value));

    public static MemoryBufferPartitionOffset operator ++(MemoryBufferPartitionOffset offset) => offset + 1;

    public static MemoryBufferPartitionOffset operator --(MemoryBufferPartitionOffset offset) => offset - 1;
}
