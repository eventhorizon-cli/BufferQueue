using System;
using System.Buffers.Binary;
using System.IO.Hashing;
using System.Numerics;
using System.Runtime.InteropServices;
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

        // Hash UTF-16LE directly when the native string representation already has that byte order.
        var hash = BitConverter.IsLittleEndian && partitionKey.Length <= int.MaxValue / sizeof(char)
            ? XxHash3.HashToUInt64(MemoryMarshal.AsBytes(partitionKey.AsSpan()))
            : HashStringKeyPortable(partitionKey.AsSpan());
        return (int)(hash % (uint)partitionCount);
    }

    internal static ulong HashStringKeyPortable(ReadOnlySpan<char> key)
    {
        // Preserve code units, including unpaired surrogates, with bounded conversion storage.
        var hash = new XxHash3();
        Span<byte> buffer = stackalloc byte[256];
        while (!key.IsEmpty)
        {
            var count = Math.Min(key.Length, buffer.Length / sizeof(char));
            for (var index = 0; index < count; index++)
            {
                BinaryPrimitives.WriteUInt16LittleEndian(buffer.Slice(index * sizeof(char), sizeof(char)), key[index]);
            }

            hash.Append(buffer[..(count * sizeof(char))]);
            key = key[count..];
        }

        return hash.GetCurrentHashAsUInt64();
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
