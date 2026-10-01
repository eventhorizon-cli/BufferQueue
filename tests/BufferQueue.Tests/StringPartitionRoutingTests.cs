using BufferQueue.Memory;

namespace BufferQueue.Tests;

public class StringPartitionRoutingTests
{
    // Fixed vectors computed independently over UTF-16LE bytes, shared by both target frameworks.
    public static IEnumerable<object[]> Vectors()
    {
        yield return new object[] { "", 0 };
        yield return new object[] { "a", 1867108634 };
        yield return new object[] { "abcd", 1157274932 };
        yield return new object[] { "user:0000", 577620860 };
        yield return new object[] { "user:0999", 1659838573 };
        yield return new object[] { "\u4e2d\u6587", 1328361383 };
        yield return new object[] { "\ud83d\ude80", 1350033453 };
        yield return new object[] { "e\u0301", 1318781978 };
        yield return new object[] { "\u00e9", 1105794559 };
        yield return new object[] { "a\u0000b", 64625737 };
        yield return new object[] { "\uffff", 102326816 };
    }

    [Theory]
    [MemberData(nameof(Vectors))]
    public void Mapping_Is_Deterministic_For_Complete_Ordinal_Strings(string key, int expected)
    {
        Assert.Equal(expected, PartitionKeyRouting.SelectStringPartition(key, int.MaxValue));
        Assert.Equal(0, PartitionKeyRouting.SelectStringPartition(key, 1));
    }

    [Theory]
    [InlineData(0xd800, 23418209)]
    [InlineData(0xdc00, 1589398794)]
    public void Unpaired_Surrogate_Code_Units_Are_Not_Replaced(int codeUnit, int expected)
    {
        // Construct after test discovery, whose string serialization replaces unpaired surrogates.
        var key = new string((char)codeUnit, 1);
        Assert.Equal(expected, PartitionKeyRouting.SelectStringPartition(key, int.MaxValue));
    }

    [Theory]
    [InlineData(8)]
    [InlineData(17)]
    public void Common_Prefix_Keys_Use_All_Partitions(int partitionCount)
    {
        var distribution = new int[partitionCount];
        for (var i = 0; i < 1000; i++)
        {
            var key = $"user:{i:D4}";
            distribution[PartitionKeyRouting.SelectStringPartition(key, partitionCount)]++;
        }
        // A distribution check for this fixed corpus, not a collision-free promise.
        Assert.All(distribution, count => Assert.InRange(count, 500 / partitionCount, 2000 / partitionCount));
    }

    [Fact]
    public void Routing_Validates_Null_And_Partition_Count()
    {
        Assert.Throws<ArgumentNullException>(() => PartitionKeyRouting.SelectStringPartition(null!, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => PartitionKeyRouting.SelectStringPartition("key", 0));
        Assert.Throws<ArgumentOutOfRangeException>(() => PartitionKeyRouting.SelectStringPartition("key", -1));
        var options = new MemoryBufferQueueOptions<string>();
        options.UsePartitionKey(static _ => null!);
        var partitioner = new KeyPartitioner<string>(options.PartitionIndexSelector!);
        Assert.Throws<ArgumentNullException>(() => partitioner.SelectPartition("item", 1));
    }
}
