using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public class MemoryBufferQueueOptionsTests
{
    [Fact]
    public void Full_Mode_Defaults_To_Wait()
    {
        var options = new MemoryBufferQueueOptions();

        Assert.Equal(BufferQueueFullMode.Wait, options.FullMode);
    }

    [Fact]
    public void Validate_Returns_Options_When_Valid()
    {
        var options = new MemoryBufferQueueOptions
        {
            TopicName = "test",
            PartitionNumber = 2
        };

        var validatedOptions = options.Validate();

        Assert.Same(options, validatedOptions);
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void Validate_Throws_When_Topic_Name_Is_Missing(string? topicName)
    {
        var options = new MemoryBufferQueueOptions { TopicName = topicName };

        var exception = Assert.Throws<ArgumentException>(() => options.Validate());

        Assert.Equal("TopicName", exception.ParamName);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Validate_Throws_When_Partition_Number_Is_Not_Positive(int partitionNumber)
    {
        var options = new MemoryBufferQueueOptions
        {
            TopicName = "test",
            PartitionNumber = partitionNumber
        };

        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());

        Assert.Equal("PartitionNumber", exception.ParamName);
    }

    [Fact]
    public void Validate_Throws_When_Bounded_Capacity_Is_Zero()
    {
        var options = new MemoryBufferQueueOptions
        {
            TopicName = "test",
            BoundedCapacity = 0
        };

        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());

        Assert.Equal("BoundedCapacity", exception.ParamName);
    }

    [Fact]
    public void Validate_Throws_When_Full_Mode_Is_Not_Supported()
    {
        var options = new MemoryBufferQueueOptions
        {
            TopicName = "test",
            FullMode = (BufferQueueFullMode)int.MaxValue
        };

        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());

        Assert.Equal("FullMode", exception.ParamName);
    }
}
