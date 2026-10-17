using BufferQueue.Memory;

namespace BufferQueue.Tests.Memory;

public class MemoryBufferPartitionCommitTests
{
    [Fact]
    public async Task Unbounded_Concurrent_Append_And_Commit_Preserve_All_Items()
    {
        const int itemCount = 50_000;
        var partition = new MemoryBufferPartition<int>(0, 2);
        var producerTask = Task.Run(() =>
        {
            for (var item = 0; item < itemCount; item++)
            {
                partition.Enqueue(item);
            }
        });

        var consumedItems = new List<int>(itemCount);
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        while (consumedItems.Count < itemCount)
        {
            timeout.Token.ThrowIfCancellationRequested();
            if (partition.TryPull("TestGroup", 1, out var items))
            {
                consumedItems.AddRange(items);
                partition.Commit("TestGroup");
            }
            else
            {
                await Task.Yield();
            }
        }

        await producerTask.WaitAsync(timeout.Token);

        Assert.Equal(Enumerable.Range(0, itemCount), consumedItems);
    }

    [Fact]
    public void Unbounded_Commit_Remains_Readable_When_Append_Recycles_Previous_Segment()
    {
        var partition = new MemoryBufferPartition<int>(0, 2);

        partition.Enqueue(0);
        partition.Enqueue(1);

        Assert.True(partition.TryPull("TestGroup", 1, out var items));
        Assert.Equal(new[] { 0 }, items);
        partition.Commit("TestGroup");

        Assert.True(partition.TryPull("TestGroup", 1, out items));
        Assert.Equal(new[] { 1 }, items);
        partition.Commit("TestGroup");

        partition.Enqueue(2);
        partition.Enqueue(3);
        partition.Enqueue(4);

        Assert.True(partition.TryPull("TestGroup", 1, out items));
        Assert.Equal(new[] { 2 }, items);
        partition.Commit("TestGroup");
    }
}
