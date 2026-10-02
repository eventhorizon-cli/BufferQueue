using BufferQueue.Memory;

namespace BufferQueue.Tests;

public class StringPartitionRoutingTests
{
    // Fixed vectors from xxHash v0.8.3 XXH3_64bits over UTF-16LE bytes, modulo int.MaxValue.
    public static IEnumerable<object[]> Vectors()
    {
        yield return new object[] { "", 316708045 };
        yield return new object[] { "a", 588575539 };
        yield return new object[] { "abcd", 1096727712 };
        yield return new object[] { "user:0000", 935093793 };
        yield return new object[] { "user:0999", 101142314 };
        yield return new object[] { "\u4e2d\u6587", 826901811 };
        yield return new object[] { "\ud83d\ude80", 668179238 };
        yield return new object[] { "e\u0301", 1472065229 };
        yield return new object[] { "\u00e9", 1875195261 };
        yield return new object[] { "a\u0000b", 1952856942 };
        yield return new object[] { "\uffff", 2039401464 };
    }

    [Theory]
    [MemberData(nameof(Vectors))]
    public void Mapping_Is_Deterministic_For_Complete_Ordinal_Strings(string key, int expected)
    {
        Assert.Equal(expected, PartitionKeyRouting.SelectStringPartition(key, int.MaxValue));
        Assert.Equal(expected, (int)(PartitionKeyRouting.HashStringKeyPortable(key) % int.MaxValue));
        Assert.Equal(0, PartitionKeyRouting.SelectStringPartition(key, 1));
    }

    [Theory]
    [InlineData(0xd800, 849010906)]
    [InlineData(0xdc00, 1034823504)]
    public void Unpaired_Surrogate_Code_Units_Are_Not_Replaced(int codeUnit, int expected)
    {
        // Construct after test discovery, whose string serialization replaces unpaired surrogates.
        var key = new string((char)codeUnit, 1);
        Assert.Equal(expected, PartitionKeyRouting.SelectStringPartition(key, int.MaxValue));
        Assert.Equal(expected, (int)(PartitionKeyRouting.HashStringKeyPortable(key) % int.MaxValue));
    }

    [Theory]
    [InlineData(0, 316708045)]
    [InlineData(1, 849010906)]
    [InlineData(2, 68784171)]
    [InlineData(3, 43675314)]
    [InlineData(4, 491101833)]
    [InlineData(5, 475044932)]
    [InlineData(7, 1466027221)]
    [InlineData(8, 1561089552)]
    [InlineData(9, 1007822350)]
    [InlineData(16, 1995879019)]
    [InlineData(17, 449002907)]
    [InlineData(32, 1402194476)]
    [InlineData(33, 267228414)]
    [InlineData(63, 72060319)]
    [InlineData(64, 384894199)]
    [InlineData(65, 1025108642)]
    [InlineData(119, 1976157761)]
    [InlineData(120, 652120599)]
    [InlineData(121, 256512789)]
    [InlineData(127, 175030546)]
    [InlineData(128, 2070517781)]
    [InlineData(129, 1071946433)]
    [InlineData(255, 1927622152)]
    [InlineData(256, 1621338832)]
    [InlineData(257, 488641480)]
    [InlineData(511, 1490463337)]
    [InlineData(512, 1646179287)]
    [InlineData(513, 720382939)]
    [InlineData(1024, 1727528115)]
    [InlineData(1025, 692157224)]
    [InlineData(4096, 545720718)]
    public void Length_Boundaries_Match_Independent_Reference(int length, int expected)
    {
        var key = new string(Enumerable.Range(0, length)
            .Select(index => (char)((index * 7919 + 0xd800) & 0xffff)).ToArray());

        Assert.Equal(expected, PartitionKeyRouting.SelectStringPartition(key, int.MaxValue));
        Assert.Equal(expected, (int)(PartitionKeyRouting.HashStringKeyPortable(key) % int.MaxValue));
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
