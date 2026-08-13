using System;
using System.Numerics;
using System.Threading;

namespace BufferQueue;

internal interface IPartitioner<in TItem>
{
    int SelectPartition(TItem item, int partitionCount);
}

internal sealed class RoundRobinPartitioner<TItem> : IPartitioner<TItem>
{
    private int _partitionIndex;

    public int SelectPartition(TItem item, int partitionCount)
    {
        if (partitionCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partitionCount),
                "Partition count must be greater than zero.");
        }

        var index = _partitionIndex;
        _partitionIndex = index + 1 == partitionCount ? 0 : index + 1;
        return index;
    }
}

internal sealed class ConcurrentRoundRobinPartitioner<TItem> : IPartitioner<TItem>
{
    private int _partitionIndex = -1;

    public int SelectPartition(TItem item, int partitionCount)
    {
        if (partitionCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partitionCount),
                "Partition count must be greater than zero.");
        }

        var index = (uint)Interlocked.Increment(ref _partitionIndex);
        return (int)(index % (uint)partitionCount);
    }
}

internal sealed class KeyPartitioner<TItem> : IPartitioner<TItem>
{
    private readonly Func<TItem, int, int> _partitionIndexSelector;

    public KeyPartitioner(Func<TItem, int, int> partitionIndexSelector)
    {
        ArgumentNullException.ThrowIfNull(partitionIndexSelector);
        _partitionIndexSelector = partitionIndexSelector;
    }

    public int SelectPartition(TItem item, int partitionCount)
    {
        if (partitionCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partitionCount),
                "Partition count must be greater than zero.");
        }

        var partitionIndex = _partitionIndexSelector(item, partitionCount);
        if ((uint)partitionIndex >= (uint)partitionCount)
        {
            throw new InvalidOperationException(
                $"The partition index {partitionIndex} is outside the configured partition range.");
        }

        return partitionIndex;
    }
}

internal static class PartitionKeyRouting
{
    private const int StringPrefixLength = 4;
    private const int StringCharacterMultiplier = 31;

    public static Func<TItem, int, int> CreateNumericPartitionIndexSelector<TItem, TNumber>(
        Func<TItem, TNumber> partitionKeySelector)
        where TNumber : INumber<TNumber>
    {
        ArgumentNullException.ThrowIfNull(partitionKeySelector);

        if (typeof(TNumber) == typeof(int))
        {
            var selector = (Func<TItem, int>)(object)partitionKeySelector;
            return (item, partitionCount) => SelectInt32PartitionCore(selector(item), partitionCount);
        }

        if (typeof(TNumber) == typeof(long))
        {
            var selector = (Func<TItem, long>)(object)partitionKeySelector;
            return (item, partitionCount) => SelectInt64PartitionCore(selector(item), partitionCount);
        }

        if (typeof(TNumber) == typeof(uint))
        {
            var selector = (Func<TItem, uint>)(object)partitionKeySelector;
            return (item, partitionCount) => SelectUInt32PartitionCore(selector(item), partitionCount);
        }

        if (typeof(TNumber) == typeof(ulong))
        {
            var selector = (Func<TItem, ulong>)(object)partitionKeySelector;
            return (item, partitionCount) => SelectUInt64PartitionCore(selector(item), partitionCount);
        }

        return (item, partitionCount) => SelectNumericPartitionCore(partitionKeySelector(item), partitionCount);
    }

    public static int SelectNumericPartition<TNumber>(TNumber partitionKey, int partitionCount)
        where TNumber : INumber<TNumber>
    {
        ValidatePartitionCount(partitionCount);
        return SelectNumericPartitionCore(partitionKey, partitionCount);
    }

    public static int SelectStringPartition(string partitionKey, int partitionCount)
    {
        ArgumentNullException.ThrowIfNull(partitionKey);
        ValidatePartitionCount(partitionCount);

        var characterCount = Math.Min(partitionKey.Length, StringPrefixLength);
        var value = 0;
        for (var index = 0; index < characterCount; index++)
        {
            value = value * StringCharacterMultiplier + partitionKey[index];
        }

        return value % partitionCount;
    }

    private static int SelectNumericPartitionCore<TNumber>(TNumber partitionKey, int partitionCount)
        where TNumber : INumber<TNumber>
    {
        if (!TNumber.IsFinite(partitionKey) || !TNumber.IsInteger(partitionKey))
        {
            throw new ArgumentOutOfRangeException(nameof(partitionKey),
                "Partition key must be a finite integer.");
        }

        return SelectBigIntegerPartitionCore(BigInteger.CreateChecked(partitionKey), partitionCount);
    }

    private static int SelectInt32PartitionCore(int partitionKey, int partitionCount)
    {
        var remainder = partitionKey % partitionCount;
        return remainder <= 0 ? remainder + partitionCount - 1 : remainder - 1;
    }

    private static int SelectInt64PartitionCore(long partitionKey, int partitionCount)
    {
        var remainder = partitionKey % partitionCount;
        return remainder <= 0 ? (int)(remainder + partitionCount - 1) : (int)(remainder - 1);
    }

    private static int SelectUInt32PartitionCore(uint partitionKey, int partitionCount)
    {
        var remainder = partitionKey % (uint)partitionCount;
        return remainder == 0 ? partitionCount - 1 : (int)(remainder - 1);
    }

    private static int SelectUInt64PartitionCore(ulong partitionKey, int partitionCount)
    {
        var remainder = partitionKey % (ulong)partitionCount;
        return remainder == 0 ? partitionCount - 1 : (int)(remainder - 1);
    }

    private static int SelectBigIntegerPartitionCore(BigInteger partitionKey, int partitionCount)
    {
        var remainder = partitionKey % partitionCount;
        if (remainder.Sign <= 0)
        {
            remainder += partitionCount;
        }

        return (int)remainder - 1;
    }

    private static void ValidatePartitionCount(int partitionCount)
    {
        if (partitionCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partitionCount),
                "Partition count must be greater than zero.");
        }
    }
}
