using System.Runtime.CompilerServices;
using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public class MemoryBufferPartitionOffsetTests
{
    [Fact]
    public void Offset_Occupies_One_UInt64()
    {
        Assert.Equal(sizeof(ulong), Unsafe.SizeOf<MemoryBufferPartitionOffset>());
    }

    [Fact]
    public void Arithmetic_Wraps_In_Both_Directions()
    {
        var last = Create(ulong.MaxValue);
        var first = Create(0);

        checked
        {
            Assert.Equal(first, last + 1);
            Assert.Equal(Create(2), last + 3);
            Assert.Equal(last, first - 1);
            Assert.Equal(Create(ulong.MaxValue - 2), first - 3);
            Assert.Equal(first, ++last);
            Assert.Equal(Create(ulong.MaxValue), --first);
        }
    }

    [Fact]
    public void Postfix_Operators_Preserve_Previous_Position()
    {
        var offset = Create(ulong.MaxValue);
        Assert.Equal(Create(ulong.MaxValue), offset++);
        Assert.Equal(Create(0), offset);
        Assert.Equal(Create(0), offset--);
        Assert.Equal(Create(ulong.MaxValue), offset);
    }

    [Theory]
    [InlineData(0UL)]
    [InlineData(ulong.MaxValue - 15)]
    [InlineData((ulong)long.MaxValue - 15)]
    public void Retained_Window_Preserves_Order_And_Distance(ulong start)
    {
        var origin = Create(start);
        for (var i = 0UL; i < 64; i++)
        {
            var left = origin + i;
            var same = Create(left.Index);
            Assert.False(left > same);
            Assert.False(left < same);
            Assert.Equal(0UL, left - same);

            for (var j = i + 1; j < 64; j++)
            {
                var right = origin + j;
                Assert.True(left < right);
                Assert.True(right > left);
                Assert.False(left > right);
                Assert.False(right < left);
                Assert.NotEqual(left, right);
                Assert.Equal(j - i, right - left);
            }
        }
    }

    [Theory]
    [InlineData(0UL)]
    [InlineData(ulong.MaxValue)]
    [InlineData((ulong)long.MaxValue)]
    public void Largest_Valid_Forward_Distance_Is_Ordered(ulong start)
    {
        var first = Create(start);
        var last = first + (ulong)long.MaxValue;

        Assert.True(last > first);
        Assert.True(first < last);
        Assert.Equal((ulong)long.MaxValue, last - first);
    }

    private static MemoryBufferPartitionOffset Create(ulong index) => new(index);
}
