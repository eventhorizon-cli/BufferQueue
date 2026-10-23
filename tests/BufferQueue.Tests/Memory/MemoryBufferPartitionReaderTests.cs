using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public sealed class MemoryBufferPartitionReaderTests
{
    [Fact]
    public void Registered_Unpulled_Group_Blocks_Release_Until_Unregistered()
    {
        var partition = new MemoryBufferPartition<int>(0, 2);
        ulong releasedCapacity = 0;
        partition.SetCapacityReleaseHandler(count => releasedCapacity += count);
        var slowConsumer = new TestConsumer("SlowGroup");

        partition.RegisterConsumer(slowConsumer);
        partition.Enqueue(1);

        Assert.True(partition.TryPull("FastGroup", 1, out _));
        partition.Commit("FastGroup");
        Assert.Equal(0UL, releasedCapacity);

        partition.UnregisterConsumer(slowConsumer);

        Assert.Equal(1UL, releasedCapacity);
    }

    [Fact]
    public void Direct_Reader_Is_Preserved_By_Same_Group_Registration_And_Recreated_From_Logical_Earliest()
    {
        var partition = new MemoryBufferPartition<int>(0, 2);
        ulong releasedCapacity = 0;
        partition.SetCapacityReleaseHandler(count => releasedCapacity += count);
        var registeredConsumer = new TestConsumer("DuplicateGroup");

        partition.Enqueue(10);
        partition.Enqueue(20);

        Assert.True(partition.TryPull("DuplicateGroup", 1, out var firstBatch));
        Assert.Equal(new[] { 10 }, firstBatch);

        Assert.True(partition.TryPull("RemainingGroup", 1, out var remainingFirstBatch));
        Assert.Equal(new[] { 10 }, remainingFirstBatch);

        partition.Commit("DuplicateGroup");
        partition.Commit("RemainingGroup");
        Assert.Equal(1UL, releasedCapacity);

        partition.RegisterConsumer(registeredConsumer);
        Assert.True(partition.TryPull("DuplicateGroup", 1, out var preservedBatch));
        Assert.Equal(new[] { 20 }, preservedBatch);

        partition.UnregisterConsumer(registeredConsumer);
        Assert.Equal(1UL, releasedCapacity);

        Assert.True(partition.TryPull("RemainingGroup", 1, out var remainingSecondBatch));
        Assert.Equal(new[] { 20 }, remainingSecondBatch);
        partition.Commit("RemainingGroup");
        Assert.Equal(2UL, releasedCapacity);

        partition.Enqueue(30);
        partition.RegisterConsumer(registeredConsumer);

        Assert.True(partition.TryPull("DuplicateGroup", 1, out var recreatedBatch));
        Assert.Equal(new[] { 30 }, recreatedBatch);
    }

    private sealed class TestConsumer(string groupName) : IBufferPartitionConsumer<int>
    {
        public string GroupName => groupName;

        public void NotifyNewDataAvailable(IBufferPartition<int> partition)
        {
        }
    }
}
