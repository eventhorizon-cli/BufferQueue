using Microsoft.Extensions.DependencyInjection;

namespace BufferQueue.MemoryMappedFile.Tests;

public class MemoryMappedFileBufferQueueOptionsTests
{
    [Fact]
    public void Validate_Returns_Options_When_Valid()
    {
        var options = new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test"
        };

        Assert.Same(options, options.Validate());
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    public void Validate_Throws_When_Topic_Name_Is_Missing(string? topicName)
    {
        var options = new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = topicName
        };

        var exception = Assert.Throws<ArgumentException>(() => options.Validate());

        Assert.Equal("TopicName", exception.ParamName);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    public void Validate_Throws_When_Partition_Number_Is_Not_Positive(int partitionNumber)
    {
        var options = new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            PartitionNumber = partitionNumber
        };

        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => options.Validate());

        Assert.Equal("PartitionNumber", exception.ParamName);
    }

    [Fact]
    public void AddTopic_Validates_Options_Immediately()
    {
        var services = new ServiceCollection();

        var exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new MemoryMappedFileBufferOptionsBuilder(services).AddTopic<int>(options =>
            {
                options.TopicName = "test";
                options.SegmentSizeInBytes = 0;
            }));

        Assert.Equal("SegmentSizeInBytes", exception.ParamName);
        Assert.Empty(services);
    }

    [Fact]
    public void Queue_Validates_Options_Before_Creating_Partitions()
    {
        using var temporaryDirectory = new TemporaryDirectory();
        var options = new MemoryMappedFileBufferQueueOptions<int>
        {
            TopicName = "test",
            DataDirectory = temporaryDirectory.Path,
            PartitionNumber = 0
        };

        var exception = Assert.Throws<ArgumentOutOfRangeException>(() =>
            new MemoryMappedFileBufferQueue<int>(options));

        Assert.Equal("PartitionNumber", exception.ParamName);
        Assert.False(Directory.Exists(Path.Combine(temporaryDirectory.Path, "test")));
    }
}
