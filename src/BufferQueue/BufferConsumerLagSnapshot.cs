namespace BufferQueue;

/// <summary>An approximate sum of offset lag across a pull consumer's assigned partitions.</summary>
/// <param name="OffsetLag">Published positions minus committed positions, clamped to zero per partition.</param>
/// <param name="Unit">The unit of the positions used to calculate lag.</param>
public readonly record struct BufferConsumerLagSnapshot(ulong OffsetLag, BufferOffsetUnit Unit);
