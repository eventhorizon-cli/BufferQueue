using System.Numerics;

namespace BufferQueue.Tests;

public class PartitionerTests
{
    [Fact]
    public void RoundRobin_Advances_And_Reserves_Batches()
    {
        var partitioner = new RoundRobinPartitioner<int>();

        Assert.False(partitioner.SupportsConcurrentSelection);
        Assert.Equal(new[] { 0, 1, 2, 0 }, new[]
        {
            partitioner.SelectPartition(0, 3),
            partitioner.SelectPartition(0, 3),
            partitioner.SelectPartition(0, 3),
            partitioner.SelectPartition(0, 3)
        });

        Assert.Equal(1, partitioner.ReserveBatch(4, 3));
        Assert.Equal(2, partitioner.SelectPartition(0, 3));
        Assert.Equal(0, partitioner.ReserveBatch(5, 3));

        Assert.Throws<ArgumentOutOfRangeException>(() => partitioner.SelectPartition(0, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => partitioner.ReserveBatch(0, 3));
        Assert.Throws<ArgumentOutOfRangeException>(() => partitioner.ReserveBatch(1, 0));
    }

    [Fact]
    public void ConcurrentRoundRobin_Advances_And_Reserves_Batches()
    {
        var partitioner = new ConcurrentRoundRobinPartitioner<int>();

        Assert.True(partitioner.SupportsConcurrentSelection);
        Assert.Equal(new[] { 0, 1, 2, 0 }, new[]
        {
            partitioner.SelectPartition(0, 3),
            partitioner.SelectPartition(0, 3),
            partitioner.SelectPartition(0, 3),
            partitioner.SelectPartition(0, 3)
        });

        Assert.Equal(1, partitioner.ReserveBatch(4, 3));
        Assert.Equal(2, partitioner.SelectPartition(0, 3));

        Assert.Throws<ArgumentOutOfRangeException>(() => partitioner.SelectPartition(0, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => partitioner.ReserveBatch(0, 3));
        Assert.Throws<ArgumentOutOfRangeException>(() => partitioner.ReserveBatch(1, 0));
    }

    [Fact]
    public void KeyPartitioner_Validates_Selector_And_Result()
    {
        Assert.Throws<ArgumentNullException>(() => new KeyPartitioner<int>(null!));

        var partitioner = new KeyPartitioner<int>((item, count) => item);
        Assert.True(partitioner.SupportsConcurrentSelection);
        Assert.Equal(1, partitioner.SelectPartition(1, 3));
        Assert.Throws<ArgumentOutOfRangeException>(() => partitioner.SelectPartition(1, 0));
        Assert.Throws<InvalidOperationException>(() => partitioner.SelectPartition(-1, 3));
        Assert.Throws<InvalidOperationException>(() => partitioner.SelectPartition(3, 3));
    }

    [Fact]
    public void NumericRouting_Uses_Normalized_Key_Modulo()
    {
        var intSelector = PartitionKeyRouting.CreateNumericPartitionIndexSelector<int, int>(static item => item);
        Assert.Equal(3, intSelector(0, 4));
        Assert.Equal(0, intSelector(1, 4));
        Assert.Equal(2, intSelector(-1, 4));

        var longSelector = PartitionKeyRouting.CreateNumericPartitionIndexSelector<long, long>(static item => item);
        Assert.Equal(2, longSelector(-5, 4));

        var uintSelector = PartitionKeyRouting.CreateNumericPartitionIndexSelector<uint, uint>(static item => item);
        Assert.Equal(3, uintSelector(0, 4));
        Assert.Equal(2, uintSelector(3, 4));

        var ulongSelector = PartitionKeyRouting.CreateNumericPartitionIndexSelector<ulong, ulong>(static item => item);
        Assert.Equal(3, ulongSelector(0, 4));
        Assert.Equal(0, ulongSelector(1, 4));

        var decimalSelector = PartitionKeyRouting.CreateNumericPartitionIndexSelector<decimal, decimal>(static item => item);
        Assert.Equal(0, decimalSelector(9m, 4));
        Assert.Equal(2, decimalSelector(-1m, 4));

        Assert.Equal(2, PartitionKeyRouting.SelectNumericPartition(new BigInteger(-1), 4));
        Assert.Throws<ArgumentOutOfRangeException>(() => PartitionKeyRouting.SelectNumericPartition(1, 0));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            PartitionKeyRouting.CreateNumericPartitionIndexSelector<double, double>(static item => item)(1.5, 4));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            PartitionKeyRouting.CreateNumericPartitionIndexSelector<double, double>(static item => item)(double.NaN, 4));
        Assert.Throws<ArgumentNullException>(() =>
            PartitionKeyRouting.CreateNumericPartitionIndexSelector<int, int>(null!));
    }
}
