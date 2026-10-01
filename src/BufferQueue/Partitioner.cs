using System;
using System.Numerics;
using System.Threading;

namespace BufferQueue;

internal interface IPartitioner<in TItem>
{
    bool SupportsConcurrentSelection { get; }

    int SelectPartition(TItem item, int partitionCount);
}

internal interface IRoundRobinBatchPartitioner
{
    int ReserveBatch(int itemCount, int partitionCount);
}

internal sealed class RoundRobinPartitioner<TItem> : IPartitioner<TItem>, IRoundRobinBatchPartitioner
{
    private int _partitionIndex;

    public bool SupportsConcurrentSelection => false;

    public int SelectPartition(TItem item, int partitionCount)
    {
        if (partitionCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partitionCount),
                "Partition count must be greater than zero.");
        }

        var partitionIndex = _partitionIndex;
        _partitionIndex = partitionIndex + 1 == partitionCount ? 0 : partitionIndex + 1;
        return partitionIndex;
    }

    public int ReserveBatch(int itemCount, int partitionCount)
    {
        if (itemCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(itemCount),
                "Item count must be greater than zero.");
        }

        if (partitionCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partitionCount),
                "Partition count must be greater than zero.");
        }

        var firstPartitionIndex = _partitionIndex;
        _partitionIndex = (firstPartitionIndex + itemCount % partitionCount) % partitionCount;
        return firstPartitionIndex;
    }
}

internal sealed class ConcurrentRoundRobinPartitioner<TItem>
    : IPartitioner<TItem>, IRoundRobinBatchPartitioner
{
    private int _partitionSequence = -1;

    public bool SupportsConcurrentSelection => true;

    public int SelectPartition(TItem item, int partitionCount)
    {
        if (partitionCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partitionCount),
                "Partition count must be greater than zero.");
        }

        var sequence = (uint)Interlocked.Increment(ref _partitionSequence);
        return (int)(sequence % (uint)partitionCount);
    }

    public int ReserveBatch(int itemCount, int partitionCount)
    {
        if (itemCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(itemCount),
                "Item count must be greater than zero.");
        }

        if (partitionCount <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(partitionCount),
                "Partition count must be greater than zero.");
        }

        var sequenceEnd = Interlocked.Add(ref _partitionSequence, itemCount);
        var sequenceStart = unchecked(sequenceEnd - itemCount + 1);
        return (int)((uint)sequenceStart % (uint)partitionCount);
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

    public bool SupportsConcurrentSelection => true;

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

        if (partitionCount == 1)
        {
            return 0;
        }

        unchecked
        {
            // MurmurHash3 x86_32, seed zero, over UTF-16LE without an encoded byte array.
            // Explicit word assembly preserves the mapping on either native byte order.
            const uint c1 = 0xcc9e2d51U;
            const uint c2 = 0x1b873593U;
            var hash = 0U;
            var index = 0;
            for (; index < partitionKey.Length - 1; index += 2)
            {
                var block = (uint)partitionKey[index] | (uint)partitionKey[index + 1] << 16;
                block = BitOperations.RotateLeft(block * c1, 15) * c2;
                hash = BitOperations.RotateLeft(hash ^ block, 13) * 5 + 0xe6546b64U;
            }

            if (index < partitionKey.Length)
            {
                hash ^= BitOperations.RotateLeft(partitionKey[index] * c1, 15) * c2;
            }

            // Finalize with the byte length, including both bytes of every UTF-16 code unit.
            hash ^= (uint)partitionKey.Length * 2;
            hash ^= hash >> 16;
            hash *= 0x85ebca6bU;
            hash ^= hash >> 13;
            hash *= 0xc2b2ae35U;
            hash ^= hash >> 16;
            return (int)(hash % (uint)partitionCount);
        }
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
